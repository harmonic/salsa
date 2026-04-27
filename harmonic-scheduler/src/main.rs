//! Harmonic scheduler binary

mod auth;
mod block_engine;
mod config;
mod error;
mod ipc;
mod remote_tpu;
mod tip_manager;
mod validator;

use block_engine::LeaderNotification;
use clap::Parser;
use log::{error, info};
use solana_signer::Signer;
use std::sync::Arc;

/// Remote TPU -> validator: nonvote queue capacity
const NONVOTE_QUEUE_CAPACITY: usize = 16384;
/// Block engine -> validator: block queue capacity
const BLOCK_QUEUE_CAPACITY: usize = 16384;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rdtsc::calibrate(); // Must be run before using any rdtsc methods
    let config = config::Config::parse();
    agave_logger::redirect_stderr_to_file(config.log.clone());
    agave_logger::setup_with_default("info");
    info!("{config:#?}");
    let identity = Arc::new(
        solana_keypair::read_keypair_file(&config.identity)
            .map_err(|e| format!("failed to read keypair: {e}"))?,
    );

    // Remote TPU -> validator: packet queue
    let (packet_tx, packet_rx) = rtrb::RingBuffer::new(NONVOTE_QUEUE_CAPACITY);
    // Remote TPU -> validator: remote TPU address
    let (remote_tpu_tx, remote_tpu_rx) = tokio::sync::watch::channel(None);

    // Block engine -> scheduler: block queue
    let (block_tx, block_rx) = rtrb::RingBuffer::new(BLOCK_QUEUE_CAPACITY);
    // Block engine -> scheduler: BlockBuilderFeeInfo, defaults to validator pubkey + 0% until first refresh
    let fee_info = Arc::new(arc_swap::ArcSwap::from_pointee(
        block_engine::BlockBuilderFeeInfo {
            block_builder: identity.pubkey(),
            block_builder_commission: 0,
        },
    ));

    // Scheduler -> block engine: leader window notifications
    let (leader_tx, mut leader_rx) = tokio::sync::watch::channel(LeaderNotification::default());
    // mark the sentinel value as seen so the first woken value is valid
    leader_rx.borrow_and_update();

    let remote_tpu_handle = tokio::spawn(remote_tpu::run(
        config.tpu,
        identity.clone(),
        packet_tx,
        remote_tpu_tx,
    ));

    let block_engine_handle = tokio::spawn(block_engine::run(
        config.block_engine,
        identity.clone(),
        block_tx,
        leader_rx,
        fee_info.clone(),
    ));

    let validator_handle = tokio::spawn(validator::run(
        config.validator,
        identity,
        remote_tpu_rx,
        fee_info,
        packet_rx,
        block_rx,
        leader_tx,
    ));

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received SIGINT, shutting down");
            std::process::exit(0);
        }
        r = remote_tpu_handle => error!("remote TPU client exited unexpectedly: {r:?}"),
        r = block_engine_handle => error!("block engine client exited unexpectedly: {r:?}"),
        r = validator_handle => error!("validator client exited unexpectedly: {r:?}"),
    }
    // Force exit for all threads
    std::process::exit(1);
}
