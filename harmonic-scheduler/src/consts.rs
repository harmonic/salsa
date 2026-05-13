//! System tuning constants

use agave_scheduler_bindings::{
    MAX_ALLOCATION_SIZE, MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionRegion,
};
use agave_scheduling_utils::handshake::MAX_WORKERS;
use solana_packet::PACKET_DATA_SIZE;

/// Standardized queue pull/dispatch size per tick
pub const BATCH_SIZE: usize = MAX_TRANSACTIONS_PER_MESSAGE;

/// Block engine -> scheduler block queue: `(usize, Vec<Vec<u8>>)`
pub const BLOCK_QUEUE_CAPACITY: usize = 128;
/// Remote TPU -> validator nonvote queue: `Vec<u8>`
pub const REMOTE_TPU_QUEUE_CAPACITY: usize = 1024;
/// TPU ingest -> scheduler queue capacity: `SharableTransactionRegion`
pub const VOTE_QUEUE_CAPACITY: usize = 2 * 1024;
/// TPU ingest -> scheduler queue capacity: `SharableTransactionRegion`
pub const NONVOTE_QUEUE_CAPACITY: usize = 8 * 1024;

/// Vote storage maximum capacity: `SharableTransactionRegion`
pub const VOTE_STORAGE_CAPACITY: usize = 4 * 1024;
/// Nonvote storage maximum capacity: `SharableTransactionRegion`
pub const NONVOTE_STORAGE_CAPACITY: usize = 64 * 1024;

/// `tpu_to_pack` queue: `TpuToPackMessage` (`SharableTransactionRegion`)
pub const TPU_TO_PACK_CAPACITY: usize = 1024;
/// `pack_to_worker` queue: `PackToWorkerMessage` (`SharableTransactionBatchRegion`)
pub const PACK_TO_WORKER_CAPACITY: usize = 8;
/// `worker_to_pack` queue: `WorkerToPackMessage` (`SharableTransactionBatchRegion` + `TransactionResponseRegion`)
pub const WORKER_TO_PACK_CAPACITY: usize = 16;
/// `progress_tracker` queue: 64 ticks per slot == one slot of messages
pub const PROGRESS_TRACKER_CAPACITY: usize = 64;

/// The number of [`rts_alloc::Allocator`] handles we need; one per thread
pub const NUM_ALLOCATOR_HANDLES: usize = 2;

/// matches `Allocator::create` slab_size in `scheduling-utils/src/handshake/server.rs`
pub const SLAB_SIZE: u32 = 2 * 1024 * 1024;
/// matches `rts_alloc::size_classes::NUM_SIZE_CLASSES`
const NUM_SIZE_CLASSES: usize = 5;
/// matches `agave-scheduling-utils::handshake::shared::GLOBAL_ALLOCATORS`
const GLOBAL_ALLOCATORS: usize = 1;
/// Worst-case allocator-ID count under protocol caps
const MAX_ALLOCATOR_IDS: usize = GLOBAL_ALLOCATORS + MAX_WORKERS + NUM_ALLOCATOR_HANDLES;
/// rts-alloc max slot size for `SharableTransactionRegion`.
const TX_SLOT_SIZE: usize = PACKET_DATA_SIZE.next_power_of_two();
/// rts-alloc max slot size for `SharableTransactionBatchRegion`.
const BATCH_SLOT_SIZE: usize =
    (MAX_TRANSACTIONS_PER_MESSAGE * size_of::<SharableTransactionRegion>()).next_power_of_two();
/// rts-alloc max slot size for the inner `CheckResponse[]` / `ReadResponse[]` response array.
const RESPONSE_ARRAY_SLOT_SIZE: usize = (MAX_ALLOCATION_SIZE as usize).next_power_of_two();
/// rts-alloc max slot size for a per-inner `SharablePubkeys` or `SharableAccountData`.
const INNER_RESPONSE_SLOT_SIZE: usize = (MAX_ALLOCATION_SIZE as usize).next_power_of_two();
/// Total SHM allocator file size, shared by all allocator handles.
///
/// Computed as worst-case live data across every channel and message slot,
/// plus per-(allocator, size-class) partial-slab fragmentation,
/// rounded up to the rts-alloc slab boundary. Running out of allocator
/// memory is unrecoverable, so this is sized to be a generous upper bound.
pub const ALLOCATOR_SIZE: usize = (
    // SharableTransactionRegion bytes in each tx-bearing queue
    TX_SLOT_SIZE * (VOTE_QUEUE_CAPACITY
        + NONVOTE_QUEUE_CAPACITY
        + VOTE_STORAGE_CAPACITY
        + NONVOTE_STORAGE_CAPACITY
        + TPU_TO_PACK_CAPACITY)
    // SharableTransactionRegion bytes inside in-flight IPC batches
    + TX_SLOT_SIZE * (PACK_TO_WORKER_CAPACITY + WORKER_TO_PACK_CAPACITY) * MAX_TRANSACTIONS_PER_MESSAGE
    // SharableTransactionBatchRegion bytes per in-flight IPC message
    + BATCH_SLOT_SIZE * (PACK_TO_WORKER_CAPACITY + WORKER_TO_PACK_CAPACITY)
    // 1 response-array allocation per in-flight worker_to_pack message
    // (CheckResponse[] or ReadResponse[] laid out back-to-back)
    + RESPONSE_ARRAY_SLOT_SIZE * WORKER_TO_PACK_CAPACITY
    // 1 per-inner allocation per response in each in-flight worker_to_pack message:
    // SharablePubkeys (CheckResponse) or SharableAccountData (ReadResponse)
    + INNER_RESPONSE_SLOT_SIZE * WORKER_TO_PACK_CAPACITY * MAX_TRANSACTIONS_PER_MESSAGE
    // Per-(allocator, size-class) partial-slab fragmentation
    + MAX_ALLOCATOR_IDS * NUM_SIZE_CLASSES * SLAB_SIZE as usize
).next_multiple_of(SLAB_SIZE as usize);
