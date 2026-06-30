//! Harmonic scheduler binary
#![allow(clippy::arithmetic_side_effects)]

mod admin_rpc;
mod auth;
mod block_engine;
mod config;
mod consts;
mod ipc;
mod remote_tpu;
mod state;
mod validator;

use admin_rpc::TpuConfig;
use anyhow::Result;
use arc_swap::ArcSwap;
use block_engine::LeaderNotification;
use clap::Parser;
use consts::{BLOCK_QUEUE_CAPACITY, REMOTE_TPU_QUEUE_CAPACITY};
use log::info;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tip_manager::{TipManager, TipManagerConfig};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{oneshot, watch};

const NUM_ASYNC_WORKER_THREADS: usize = 4;

fn main() -> Result<()> {
    // Abort the whole process on any thread's panic
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default_panic(info);
        std::process::abort();
    }));

    let config = config::Config::parse();
    agave_logger::setup_with_default("info");
    if let Some(logfile) = &config.log {
        agave_logger::redirect_stderr(logfile);
    }
    info!("{config:#?}");
    rdtsc::calibrate();

    let admin_rpc_path = config.ledger.join("admin.rpc");
    let validator_socket = config.ledger.join("scheduler_bindings.ipc");

    // admin rpc -> remote tpu / block engine / scheduler
    let (identity_tx, identity_rx) = watch::channel(Arc::new(Keypair::new()));
    // admin rpc -> scheduler
    let (tip_manager_tx, tip_manager_rx) =
        watch::channel(Arc::new(TipManager::new(TipManagerConfig::default())));

    // remote tpu -> validator
    let (packet_tx, packet_rx) = rtrb::RingBuffer::new(REMOTE_TPU_QUEUE_CAPACITY);
    // remote tpu -> admin rpc
    let remote_tpu: Arc<ArcSwap<Option<TpuConfig>>> = Arc::new(ArcSwap::from_pointee(None));

    // block engine -> scheduler
    let (block_tx, block_rx) = rtrb::RingBuffer::new(BLOCK_QUEUE_CAPACITY);
    let fee_info = Arc::new(ArcSwap::from_pointee(tip_manager::BlockBuilderFeeInfo {
        block_builder: Pubkey::default(),
        block_builder_commission: 0,
    }));

    // scheduler -> block engine
    let (leader_tx, leader_rx) = tokio::sync::watch::channel::<Option<LeaderNotification>>(None);

    // main -> admin rpc
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    std::thread::Builder::new()
        .name("hmonic-sched".into())
        .spawn({
            let fee_info = fee_info.clone();
            let identity_rx = identity_rx.clone();
            let tip_manager_rx = tip_manager_rx.clone();
            move || {
                validator::run(
                    config.validator,
                    validator_socket,
                    identity_rx,
                    tip_manager_rx,
                    fee_info,
                    packet_rx,
                    block_rx,
                    leader_tx,
                    config.block_engine.strategy,
                )
            }
        })?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(NUM_ASYNC_WORKER_THREADS)
        .enable_all()
        .thread_name_fn(|| {
            static ID: AtomicUsize = AtomicUsize::new(0);
            format!("hmonic-async-{}", ID.fetch_add(1, Ordering::SeqCst))
        })
        .build()?;
    let admin_rpc_handle = rt.spawn(admin_rpc::run(
        admin_rpc_path,
        config.tip,
        identity_tx,
        tip_manager_tx,
        remote_tpu.clone(),
        shutdown_rx,
    ));
    rt.spawn(remote_tpu::run(
        config.tpu,
        identity_rx.clone(),
        packet_tx,
        remote_tpu,
    ));
    rt.spawn(block_engine::run(
        config.block_engine,
        identity_rx,
        block_tx,
        leader_rx,
        fee_info,
    ));
    if let Some(logfile) = config.log {
        rt.spawn(async move {
            let mut sigusr1 =
                signal(SignalKind::user_defined1()).expect("SIGUSR1 handler should install");
            while sigusr1.recv().await.is_some() {
                info!(
                    "received SIGUSR1, reopening log file: {}",
                    logfile.display()
                );
                agave_logger::redirect_stderr(&logfile);
            }
        });
    }
    rt.block_on(async {
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler should install");
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler should install");
        tokio::select! {
            _ = sigint.recv() => info!("received SIGINT, shutting down"),
            _ = sigterm.recv() => info!("received SIGTERM, shutting down"),
        }
        shutdown_tx.send(()).expect("shutdown_rx should be open");
        admin_rpc_handle
            .await
            .expect("admin_rpc task should not panic");
    });

    Ok(())
}
