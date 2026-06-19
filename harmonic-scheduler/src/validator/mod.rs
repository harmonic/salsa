//! Validator client: IPC session lifecycle and worker thread orchestration

pub mod block_stage;
pub mod fallback_stage;
pub mod scheduler;
pub mod storage;
pub mod tpu;

use crate::block_engine::LeaderNotification;
use crate::config::ValidatorConfig;
use crate::consts::{NONVOTE_QUEUE_CAPACITY, VOTE_QUEUE_CAPACITY};
use crate::ipc;
use crate::state::{scheduler_active, tpu_active, validator_exit};
use arc_swap::ArcSwap;
use bytes::Bytes;
use log::{info, warn};
use solana_keypair::Keypair;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;
use tip_manager::{BlockBuilderFeeInfo, TipManager};
use tokio::sync::{broadcast, mpsc, watch};

/// Backoff between ipc connection attempts
const IPC_CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// Drain up to `max` items from `rx` as an iterator
fn drain<T>(rx: &mut rtrb::Consumer<T>, max: usize) -> impl Iterator<Item = T> + '_ {
    let n = if rx.cached_slots() >= max {
        max
    } else {
        max.min(rx.slots())
    };
    (n != 0)
        .then(|| rx.read_chunk(n).expect("n slots should be available"))
        .into_iter()
        .flatten()
}

/// Validator session loop with automatic reconnect
pub fn run(
    config: ValidatorConfig,
    validator_socket: PathBuf,
    identity_rx: watch::Receiver<Arc<Keypair>>,
    tip_manager_rx: watch::Receiver<Arc<TipManager>>,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    mut packet_rx: rtrb::Consumer<Bytes>,
    mut block_rx: rtrb::Consumer<(u64, Vec<Bytes>)>,
    leader_tx: watch::Sender<Option<LeaderNotification>>,
    backrun_tx: broadcast::Sender<Bytes>,
    is_leader: Arc<AtomicBool>,
    mut backrun_rx: mpsc::UnboundedReceiver<Vec<Bytes>>,
) {
    loop {
        for _ in drain(&mut block_rx, usize::MAX) {}
        for _ in drain(&mut packet_rx, usize::MAX) {}

        // wait for block engine and remote tpu; keep draining so they don't backpressure
        if validator_exit() {
            thread::sleep(IPC_CONNECTION_BACKOFF);
            continue;
        }

        // connect to ipc
        let mut session = match ipc::connect(&validator_socket, config.num_workers) {
            Ok(session) => session,
            Err(e) => {
                warn!("IPC connect failed: {e}");
                thread::sleep(IPC_CONNECTION_BACKOFF);
                continue;
            }
        };

        // packets -> scheduler txn queues
        let (vote_tx, vote_rx) = rtrb::RingBuffer::new(VOTE_QUEUE_CAPACITY);
        let (nonvote_tx, nonvote_rx) = rtrb::RingBuffer::new(NONVOTE_QUEUE_CAPACITY);

        // per-thread allocator handles
        let scheduler_allocator = session
            .allocators
            .pop()
            .expect("session should provide 2 allocator handles");
        let packets_allocator = session
            .allocators
            .pop()
            .expect("session should provide 2 allocator handles");

        // Set both active bits before spawning to avoid a race
        let tpu_guard = tpu_active();
        let scheduler_guard = scheduler_active();

        thread::scope(|s| {
            thread::Builder::new()
                .name("hmonic-tpu".into())
                .spawn_scoped(s, || {
                    let _guard = tpu_guard;
                    tpu::run(
                        session.tpu_to_pack,
                        packets_allocator,
                        &mut packet_rx,
                        vote_tx,
                        nonvote_tx,
                        backrun_tx.clone(),
                        is_leader.clone(),
                    );
                })
                .expect("tpu thread should spawn");

            thread::Builder::new()
                .name("hmonic-leader".into())
                .spawn_scoped(s, || {
                    let _guard = scheduler_guard;
                    let _ = scheduler::Scheduler::new(
                        ipc::ProgressTracker::new(session.progress_tracker),
                        session.workers,
                        &scheduler_allocator,
                        vote_rx,
                        nonvote_rx,
                        &mut block_rx,
                        &leader_tx,
                        identity_rx.clone(),
                        tip_manager_rx.clone(),
                        fee_info.clone(),
                        is_leader.clone(),
                        &mut backrun_rx,
                    )
                    .run();
                })
                .expect("scheduler thread should spawn");
        });
        info!("validator IPC disconnected, reconnecting");
    }
}
