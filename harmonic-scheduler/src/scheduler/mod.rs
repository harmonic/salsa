//! Agave scheduler bindings client and the threads that schedule leader slots

mod block_stage;
pub mod channel;
mod check;
mod execute;
mod fallback_stage;
mod leader;
mod progress;
mod storage;
mod utils;

use {
    agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse},
    agave_scheduler_handshake::{ClientLogon, ClientSession, client::connect as handshake},
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    anyhow::{Context, Result, anyhow, bail},
    channel::{BundleMeta, Channel},
    check::{CheckMeta, MAX_CHECK_TXS},
    leader::Inputs,
    progress::PROGRESS_MESSAGE_TIMEOUT,
    rts_alloc::MAX_ALLOC_SIZE,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        path::Path,
        thread::{self, JoinHandle},
        time::{Duration, SystemTime},
    },
    tokio::{
        sync::watch,
        time::{sleep, timeout},
    },
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

/// Most bundles the block stage holds in a slot; the rest of the block waits in its channel
pub const STAGE_MAX_TASKS: usize = 64 * 1024;
/// Stage passes between progress polls
pub const PASSES_PER_PROGRESS_POLL: usize = 64;
/// Most transactions in a bundle, the block engine's limit
pub const MAX_BUNDLE_TXS: usize = 5;

/// Block stream -> leader thread: bundles of the block; the stream waits while it is full
const BLOCK_QUEUE_CAPACITY: usize = 4096;
/// Bundle stream -> leader thread: bundles
const BUNDLE_QUEUE_CAPACITY: usize = 4096;
/// Block engine packet stream -> leader thread: packets
const BLOCK_ENGINE_PACKET_QUEUE_CAPACITY: usize = 16 * 1024;
/// Remote TPU packet stream -> leader thread: packets
const REMOTE_TPU_QUEUE_CAPACITY: usize = 64 * 1024;
/// Tip cranks -> leader thread: one per leader slot of a window
const TIP_CRANK_QUEUE_CAPACITY: usize = 4;

/// Transaction storage: the checked packets and bundles held for fallback blocks, in transactions
pub const STORAGE_CAPACITY: usize = 64 * 1024;

/// `tpu_to_pack` queue: `TpuToPackMessage`
const TPU_TO_PACK_CAPACITY: usize = 64 * 1024;
/// `pack_to_worker` queues: EXECUTEs per worker, so one waits while another replies
pub const PACK_TO_WORKER_CAPACITY: usize = 2;
/// `worker_to_pack` queues: every EXECUTE in flight, so a worker's replies always fit
const WORKER_TO_PACK_CAPACITY: usize = PACK_TO_WORKER_CAPACITY;
/// Check queues: the CHECKs in flight from every owner together, so a reply always fits
pub const CHECK_QUEUE_CAPACITY: usize = 64;
/// `progress_tracker` queue: the validator ends the session when it fills
const PROGRESS_TRACKER_CAPACITY: usize = 64;

/// Bytes of one batch allocation: bundles carry `BundleMeta`, CHECK batches `CheckMeta`
const BATCH_BYTES: usize = {
    let task = TransactionPtrBatch::<(), BundleMeta>::TRANSACTION_BATCH_META_END;
    let check = TransactionPtrBatch::<CheckMeta, ()>::TRANSACTION_BATCH_META_END;
    if task > check { task } else { check }
};
const _: () = assert!(BATCH_BYTES <= MAX_ALLOC_SIZE);

/// Allocator handles: the leader's and five streams' (block, bundle, packet, remote TPU, crank)
const NUM_ALLOCATOR_HANDLES: usize = 6;
/// rts-alloc slab size the validator creates the allocator with
const SLAB_SIZE: usize = 2 * 1024 * 1024;
/// rts-alloc size classes
const NUM_SIZE_CLASSES: usize = 5;
/// Allocators the validator creates besides the workers' and ours
const GLOBAL_ALLOCATORS: usize = 1;
/// rts-alloc's smallest size class
const MIN_ALLOC_SIZE: usize = 256;
/// rts-alloc metadata per slab, at most: a `u16` per smallest block, plus list entries
const SLAB_METADATA: usize = 2 * (SLAB_SIZE / MIN_ALLOC_SIZE) + 64;
/// Allocator handles that fill the store: TPU, remote TPU, block engine packets and bundles
const STORE_FEEDS: usize = 4;

/// Timeout per IPC handshake attempt
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Backoff between IPC connection attempts
const CONNECT_BACKOFF: Duration = Duration::from_secs(1);
/// How long the leader thread may take to finish its leader window
const LEADER_EXIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Shared-memory size for a session with `num_workers` execution and check workers
fn allocator_size(num_workers: usize) -> usize {
    const BUNDLES: usize = BLOCK_QUEUE_CAPACITY + BUNDLE_QUEUE_CAPACITY + STAGE_MAX_TASKS;
    // A slab stays with the handle that took it until it is empty, so each
    // handle that fills the store may keep a full store's worth: its ranked
    // transactions, and those its CHECKs hold
    const TXS: usize = BUNDLES * MAX_BUNDLE_TXS
        + BLOCK_ENGINE_PACKET_QUEUE_CAPACITY
        + REMOTE_TPU_QUEUE_CAPACITY
        + TIP_CRANK_QUEUE_CAPACITY
        + TPU_TO_PACK_CAPACITY
        + STORE_FEEDS * (STORAGE_CAPACITY + CHECK_QUEUE_CAPACITY * MAX_CHECK_TXS);
    // Each tip crank is a batch too; an EXECUTE sends its task's batch; each
    // store entry is a batch, packets wrapped by the leader thread's handle and
    // bundles from the bundle stream's
    const BATCHES: usize = BUNDLES
        + TIP_CRANK_QUEUE_CAPACITY
        + CHECK_QUEUE_CAPACITY
        + 2 * (STORAGE_CAPACITY + CHECK_QUEUE_CAPACITY * MAX_CHECK_TXS);
    // Each transaction a CHECK resolves may get its lookup-table keys, which
    // the account lock limit keeps within the largest size class
    const KEYS: usize = CHECK_QUEUE_CAPACITY * MAX_CHECK_TXS;
    const SESSION_BYTES: usize = MAX_ALLOC_SIZE * (TXS + KEYS)
        + size_class(BATCH_BYTES) * BATCHES
        + size_class(size_of::<CheckResponse>() * MAX_CHECK_TXS) * CHECK_QUEUE_CAPACITY;
    // An EXECUTE's response holds one entry per transaction of its task
    const EXECUTE_RESPONSE: usize = size_class(size_of::<ExecutionResponse>() * MAX_BUNDLE_TXS);
    let execute_responses =
        EXECUTE_RESPONSE.saturating_mul(num_workers.saturating_mul(PACK_TO_WORKER_CAPACITY));
    let allocators = num_workers
        .saturating_mul(2)
        .saturating_add(GLOBAL_ALLOCATORS + NUM_ALLOCATOR_HANDLES);
    let slabs = SESSION_BYTES
        .saturating_add(execute_responses)
        .div_ceil(SLAB_SIZE)
        .saturating_add(allocators.saturating_mul(NUM_SIZE_CLASSES));
    // Plus a slab for rts-alloc's header and per-allocator lists
    let metadata = slabs
        .saturating_mul(SLAB_METADATA)
        .div_ceil(SLAB_SIZE)
        .saturating_add(1);
    // A saturated size fails the handshake's validation instead of overflowing
    slabs.saturating_add(metadata).saturating_mul(SLAB_SIZE)
}

/// The rts-alloc size class an allocation of `bytes` takes
const fn size_class(bytes: usize) -> usize {
    if bytes < MIN_ALLOC_SIZE {
        MIN_ALLOC_SIZE
    } else {
        bytes.next_power_of_two()
    }
}

/// Connected validator IPC session
pub struct Scheduler {
    session: ClientSession,
}

impl Scheduler {
    /// Connect to the validator IPC, retrying until the validator runs the session
    pub async fn connect(path: &Path, num_workers: usize) -> Self {
        info!(path = %path.display(), "connecting to validator IPC");
        let mut failed = false;
        loop {
            let attempt = {
                let path = path.to_path_buf();
                // Blocks for up to HANDSHAKE_TIMEOUT plus PROGRESS_MESSAGE_TIMEOUT
                tokio::task::spawn_blocking(move || try_connect(&path, num_workers))
                    .await
                    .unwrap_or_else(|e| Err(e.into()))
            };
            match attempt {
                Ok(session) => {
                    info!(workers = num_workers, "connected to validator IPC");
                    return Self { session };
                }
                Err(e) if !failed => {
                    warn!(error = %format_args!("{e:#}"), "IPC connect failed, retrying");
                }
                Err(e) => debug!(error = %format_args!("{e:#}"), "IPC connect failed"),
            }
            failed = true;
            sleep(CONNECT_BACKOFF).await;
        }
    }

    /// Start the leader thread; its exit cancels `token`
    pub fn spawn(
        self,
        token: &CancellationToken,
        tip_program: Pubkey,
    ) -> Result<(SchedulerThreads, Channels)> {
        let session = self.session;
        let allocator = &session.allocator;
        let (blocks, blocks_rx) = Channel::new(allocator, BLOCK_QUEUE_CAPACITY)?;
        let (bundles, bundles_rx) = Channel::new(allocator, BUNDLE_QUEUE_CAPACITY)?;
        let (packets, packets_rx) = Channel::new(allocator, BLOCK_ENGINE_PACKET_QUEUE_CAPACITY)?;
        let (remote_tpu, remote_tpu_rx) = Channel::new(allocator, REMOTE_TPU_QUEUE_CAPACITY)?;
        let (tip_cranks, tip_cranks_rx) = Channel::new(allocator, TIP_CRANK_QUEUE_CAPACITY)?;
        let (leader_tx, leader_rx) = watch::channel(None);

        let leader = thread::Builder::new()
            .name("hmonic-leader".into())
            .spawn({
                let token = token.clone();
                move || -> Result<()> {
                    // Cancels the session on return or panic
                    let _guard = token.clone().drop_guard();
                    leader::run(
                        &token,
                        session,
                        Inputs {
                            remote_tpu: remote_tpu_rx,
                            packets: packets_rx,
                            blocks: blocks_rx,
                            bundles: bundles_rx,
                            tip_cranks: tip_cranks_rx,
                        },
                        &leader_tx,
                        tip_program,
                    )
                }
            })
            .context("spawn leader thread")?;
        let channels = Channels {
            block_engine: BlockEngineChannels {
                blocks,
                bundles,
                packets,
                tip_cranks,
                leader_rx,
            },
            remote_tpu,
        };
        Ok((SchedulerThreads { leader }, channels))
    }
}

/// Handshake, then block until the validator runs the session
fn try_connect(path: &Path, num_workers: usize) -> Result<ClientSession> {
    let mut session = handshake(
        path,
        ClientLogon {
            worker_count: num_workers,
            // As many check workers as execution workers, so checks keep pace with execution
            check_worker_count: num_workers,
            allocator_size: allocator_size(num_workers),
            allocator_handles: NUM_ALLOCATOR_HANDLES,
            tpu_to_pack_capacity: TPU_TO_PACK_CAPACITY,
            progress_tracker_capacity: PROGRESS_TRACKER_CAPACITY,
            pack_to_worker_capacity: PACK_TO_WORKER_CAPACITY,
            worker_to_pack_capacity: WORKER_TO_PACK_CAPACITY,
            pack_to_check_worker_capacity: CHECK_QUEUE_CAPACITY,
            check_worker_to_pack_capacity: CHECK_QUEUE_CAPACITY,
            flags: 0,
        },
        HANDSHAKE_TIMEOUT,
    )
    .context("handshake")?;
    // The validator publishes progress only once it runs this session
    session
        .progress_tracker
        .wait_readable_timeout(PROGRESS_MESSAGE_TIMEOUT)
        .context("validator did not start the session")?;
    Ok(session)
}

/// The block engine's and remote TPU's ends of the scheduler's channels
pub struct Channels {
    pub block_engine: BlockEngineChannels,
    pub remote_tpu: Channel<TransactionPtr>,
}

/// The block engine's channels into the scheduler, and the leader slots to announce
pub struct BlockEngineChannels {
    pub blocks: Channel<TransactionPtrBatch<(), BundleMeta>>,
    pub bundles: Channel<TransactionPtrBatch<(), BundleMeta>>,
    pub packets: Channel<TransactionPtr>,
    /// Each leader slot's tip crank; empty if none is needed or signing failed
    pub tip_cranks: Channel<TransactionPtrBatch<(), BundleMeta>>,
    pub leader_rx: watch::Receiver<Option<LeaderNotification>>,
}

/// Scheduler -> block engine leader-slot announcement
#[derive(Clone, Copy)]
pub struct LeaderNotification {
    /// The slot we are leader for
    pub slot: Slot,
    /// Wall-clock time the slot was announced, used by the block engine to measure latency
    pub start_time: SystemTime,
    /// Wall-clock time at which the leader slot ends
    pub end_time: SystemTime,
}

/// How long the leader waits for a tip crank; signing measured p99 23us plus an 18us RPC
pub const TIP_CRANK_TIMEOUT: Duration = Duration::from_millis(10);

/// The running leader thread
pub struct SchedulerThreads {
    leader: JoinHandle<Result<()>>,
}

impl SchedulerThreads {
    /// Wait for the leader thread; Err if it outlives `LEADER_EXIT_TIMEOUT`, else how it stopped
    pub async fn join(self) -> Result<Result<()>> {
        // The leader thread may run until its leader window ends; join off the runtime
        let joined = tokio::task::spawn_blocking(move || self.leader.join());
        let Ok(Ok(leader)) = timeout(LEADER_EXIT_TIMEOUT, joined).await else {
            bail!("scheduler threads did not stop");
        };
        // A panic message is already on stderr
        Ok(match leader {
            Err(_) => Err(anyhow!("leader thread panicked")),
            Ok(result) => result.context("leader thread"),
        })
    }
}
