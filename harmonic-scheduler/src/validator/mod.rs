//! Validator client: IPC session lifecycle and worker thread orchestration

pub mod packets;
pub mod schedule;
pub mod scheduler;

use crate::block_engine::{BlockBuilderFeeInfo, LeaderNotification};
use crate::config::ValidatorConfig;
use crate::ipc;
use crate::tip_manager::TipManager;
use arc_swap::ArcSwap;
use log::info;
use solana_keypair::Keypair;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinError;

/// One allocator per client thread (packets + scheduler)
const NUM_CLIENT_ALLOCATORS: usize = 2;
/// Reconnect poll interval between IPC sessions
const REMOTE_TPU_POLL_INTERVAL: Duration = Duration::from_millis(400);
/// Packets → scheduler ring capacity
const TX_CHANNEL_CAPACITY: usize = 65_536;

/// Validator session loop with automatic reconnect
pub async fn run(
    config: ValidatorConfig,
    identity: Arc<Keypair>,
    remote_tpu: watch::Receiver<Option<SocketAddrV4>>,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    packet_rx: rtrb::Consumer<Vec<u8>>,
    block_rx: rtrb::Consumer<Vec<u8>>,
    leader_tx: watch::Sender<LeaderNotification>,
) -> Result<(), JoinError> {
    let exit = Arc::new(AtomicBool::new(false));
    let tracker = tokio::spawn(track_remote_tpu(exit.clone(), remote_tpu.clone()));
    let blocking = tokio::task::spawn_blocking(move || {
        main(
            config, identity, fee_info, remote_tpu, packet_rx, block_rx, leader_tx, exit,
        )
    });
    tokio::select! {
        r = tracker => r,
        r = blocking => r,
    }
}

/// Mirror remote-TPU presence into `exit` to gate the session threads
async fn track_remote_tpu(
    exit: Arc<AtomicBool>,
    mut remote_tpu: watch::Receiver<Option<SocketAddrV4>>,
) {
    loop {
        exit.store(remote_tpu.borrow().is_none(), Ordering::Relaxed);
        if remote_tpu.changed().await.is_err() {
            return;
        }
    }
}

/// Connect to Agave and run one session until disconnect, reconnecting forever
#[allow(clippy::too_many_arguments)]
fn main(
    config: ValidatorConfig,
    identity: Arc<Keypair>,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    remote_tpu: watch::Receiver<Option<SocketAddrV4>>,
    mut packet_rx: rtrb::Consumer<Vec<u8>>,
    mut block_rx: rtrb::Consumer<Vec<u8>>,
    leader_tx: watch::Sender<LeaderNotification>,
    exit: Arc<AtomicBool>,
) {
    let tip_manager = TipManager::new(config.tip.into());
    loop {
        let current_tpu = *remote_tpu.borrow();
        let Some(tpu_override) = current_tpu else {
            thread::sleep(REMOTE_TPU_POLL_INTERVAL);
            continue;
        };
        let Some(mut session) = ipc::connect(
            &config.validator_socket,
            config.num_workers,
            NUM_CLIENT_ALLOCATORS,
            tpu_override,
            &exit,
        ) else {
            thread::sleep(REMOTE_TPU_POLL_INTERVAL);
            continue;
        };
        let (vote_tx, vote_rx) = rtrb::RingBuffer::new(TX_CHANNEL_CAPACITY);
        let (nonvote_tx, nonvote_rx) = rtrb::RingBuffer::new(TX_CHANNEL_CAPACITY);

        thread::scope(|s| {
            let tpu_to_pack = session.tpu_to_pack;
            let progress = ipc::ProgressTracker::new(session.progress_tracker);
            let workers = session.workers;
            let packets_allocator = session.allocators.pop().expect("missing allocator");
            let scheduler_allocator = session.allocators.pop().expect("missing allocator");
            let tip_manager = &tip_manager;
            let leader_tx = &leader_tx;
            let packet_rx = &mut packet_rx;
            let block_rx = &mut block_rx;

            s.spawn({
                let exit = exit.clone();
                move || {
                    packets::Packets::new(
                        tpu_to_pack,
                        packets_allocator,
                        packet_rx,
                        vote_tx,
                        nonvote_tx,
                        exit.clone(),
                    )
                    .run();
                    exit.store(true, Ordering::Relaxed);
                }
            });

            s.spawn({
                let exit = exit.clone();
                let identity = identity.clone();
                let fee_info = fee_info.clone();
                move || {
                    let _ = scheduler::Scheduler::new(
                        progress,
                        workers,
                        &scheduler_allocator,
                        vote_rx,
                        nonvote_rx,
                        block_rx,
                        leader_tx,
                        tip_manager,
                        identity,
                        fee_info,
                        exit.clone(),
                    )
                    .run();
                    exit.store(true, Ordering::Relaxed);
                }
            });
        });
        info!("validator IPC disconnected, reconnecting");
    }
}
