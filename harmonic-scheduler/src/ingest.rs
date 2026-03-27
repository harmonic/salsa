//! Transaction ingestion: dedup, classification, and submission to the scheduling engine.

use {
    crate::{
        fallback, ipc::IpcClient, scheduling::SchedulingEngine, tpu_proxy_client::RelayerPacket,
    },
    agave_scheduler_bindings::tpu_message_flags,
    lru::LruCache,
    rts_alloc::Allocator,
};

const VOTE_DEDUP_CAPACITY: usize = 400_000;
const NONVOTE_DEDUP_CAPACITY: usize = 100_000;

/// Transaction ingestion pipeline: dedup, classification, and engine submission.
///
/// Because relayer packets arrive as mixed vote/nonvote batches on a single
/// channel, and the scheduler processes votes and nonvotes in different
/// phases of the slot, this struct buffers out-of-phase packets. When
/// `process_relayer_nonvotes` encounters votes, it buffers them for later
/// retrieval by `process_relayer_votes`, and vice versa.
pub struct Ingest {
    vote_dedup: LruCache<[u8; 64], ()>,
    nonvote_dedup: LruCache<[u8; 32], ()>,
    /// Votes encountered during nonvote processing, buffered for vote phase.
    pending_votes: Vec<Vec<u8>>,
    /// Nonvotes encountered during vote processing, buffered for next fallback.
    pending_nonvotes: Vec<Vec<u8>>,
}

impl Ingest {
    pub fn new() -> Self {
        Self {
            vote_dedup: LruCache::new(VOTE_DEDUP_CAPACITY),
            nonvote_dedup: LruCache::new(NONVOTE_DEDUP_CAPACITY),
            pending_votes: Vec::new(),
            pending_nonvotes: Vec::new(),
        }
    }

    /// Drain relayer channel, dedup and submit non-votes to the engine.
    /// Votes encountered are buffered in `pending_votes` for later retrieval.
    pub fn process_relayer_nonvotes(
        &mut self,
        packet_rx: &crossbeam_channel::Receiver<Vec<RelayerPacket>>,
        engine: &mut SchedulingEngine,
        allocator: &Allocator,
    ) {
        // First, submit any nonvotes buffered from a prior vote phase.
        for data in self.pending_nonvotes.drain(..) {
            let key = dedup_key(&data);
            if self.nonvote_dedup.push(key, ()).is_some() {
                continue;
            }
            if let Some(region) = fallback::allocate_transaction(&data, allocator) {
                let _ = engine.submit(region, &data);
            }
        }

        while let Ok(packets) = packet_rx.try_recv() {
            for packet in packets {
                if packet.is_vote {
                    // Buffer for later vote phase — don't drop.
                    self.pending_votes.push(packet.data);
                    continue;
                }
                let key = dedup_key(&packet.data);
                if self.nonvote_dedup.push(key, ()).is_some() {
                    continue;
                }
                if let Some(region) = fallback::allocate_transaction(&packet.data, allocator) {
                    let _ = engine.submit(region, &packet.data);
                }
            }
        }
    }

    /// Drain relayer channel, dedup and submit votes to the engine.
    /// Nonvotes encountered are buffered in `pending_nonvotes` for later retrieval.
    pub fn process_relayer_votes(
        &mut self,
        packet_rx: &crossbeam_channel::Receiver<Vec<RelayerPacket>>,
        engine: &mut SchedulingEngine,
        allocator: &Allocator,
    ) {
        // First, submit any votes buffered from a prior fallback phase.
        for data in self.pending_votes.drain(..) {
            if data.len() < 64 {
                continue;
            }
            let mut key = [0u8; 64];
            key.copy_from_slice(&data[..64]);
            if self.vote_dedup.push(key, ()).is_some() {
                continue;
            }
            if let Some(region) = fallback::allocate_transaction(&data, allocator) {
                let _ = engine.submit(region, &data);
            }
        }

        while let Ok(packets) = packet_rx.try_recv() {
            for packet in packets {
                if !packet.is_vote {
                    // Buffer for later fallback phase — don't drop.
                    self.pending_nonvotes.push(packet.data);
                    continue;
                }
                if packet.data.len() < 64 {
                    continue;
                }
                let mut key = [0u8; 64];
                key.copy_from_slice(&packet.data[..64]);
                if self.vote_dedup.push(key, ()).is_some() {
                    continue;
                }
                if let Some(region) = fallback::allocate_transaction(&packet.data, allocator) {
                    let _ = engine.submit(region, &packet.data);
                }
            }
        }
    }

    /// Process TpuToPackMessage: dedup, submit non-votes to engine.
    pub fn process_tpu_to_pack(&mut self, ipc: &mut IpcClient, engine: &mut SchedulingEngine) {
        let (consumer, tpu_alloc, sched_alloc) = ipc.tpu_to_pack_refs();

        consumer.sync();
        while let Some(msg) = consumer.try_read() {
            let is_vote = msg.flags & tpu_message_flags::IS_SIMPLE_VOTE != 0;
            let data = unsafe {
                let ptr = tpu_alloc.ptr_from_offset(msg.transaction.offset);
                std::slice::from_raw_parts(ptr.as_ptr(), msg.transaction.length as usize)
            };

            let is_dup = if is_vote {
                if data.len() >= 64 {
                    let mut key = [0u8; 64];
                    key.copy_from_slice(&data[..64]);
                    self.vote_dedup.push(key, ()).is_some()
                } else {
                    true
                }
            } else {
                let key = dedup_key(data);
                self.nonvote_dedup.push(key, ()).is_some()
            };

            if !is_dup && !is_vote {
                if let Some(region) = fallback::allocate_transaction(data, sched_alloc) {
                    let _ = engine.submit(region, data);
                }
            }

            // Always free the tpu_to_pack allocation
            unsafe {
                let ptr = tpu_alloc.ptr_from_offset(msg.transaction.offset);
                tpu_alloc.free(ptr);
            }
        }
        consumer.finalize();
    }

    pub fn clear(&mut self) {
        self.vote_dedup.clear();
        self.nonvote_dedup.clear();
        self.pending_votes.clear();
        self.pending_nonvotes.clear();
    }
}

fn dedup_key(data: &[u8]) -> [u8; 32] {
    let mut key = [0u8; 32];
    let len = data.len().min(32);
    key[..len].copy_from_slice(&data[..len]);
    key
}
