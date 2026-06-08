//! Shared memory transaction storage

use crate::consts::BATCH_SIZE;
use crate::ipc::shmem::{Free, Slice, allocate_batch, signature};
use crate::ipc::{pack_to_worker, worker_to_pack};
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, status_check_flags};
use agave_scheduler_bindings::{
    PackToWorkerMessage, SharableTransactionBatchRegion, SharableTransactionRegion,
    WorkerToPackMessage, pack_message_flags, processed_codes,
};
use indexmap::IndexMap;
use indexmap::map::Entry;
use rts_alloc::Allocator;
use rustc_hash::FxBuildHasher;

/// Shared memory transaction storage
pub struct Storage<'a> {
    transactions: IndexMap<[u8; 64], SharableTransactionRegion, FxBuildHasher>,
    cursor: usize,
    inflight: usize,
    capacity: usize,
    dropped: usize,
    allocator: &'a Allocator,
}

impl<'a> Storage<'a> {
    pub fn new(capacity: usize, allocator: &'a Allocator) -> Self {
        Self {
            transactions: IndexMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            cursor: 0,
            inflight: 0,
            capacity,
            dropped: 0,
            allocator,
        }
    }

    /// Get and clear the dropped counter
    pub fn dropped(&mut self) -> usize {
        std::mem::take(&mut self.dropped)
    }

    /// Reset the checked cursor for a new pass over storage
    pub fn reset_cursor(&mut self) {
        self.cursor = 0;
    }

    /// How many CHECKs are currently being processed
    pub fn inflight(&self) -> usize {
        self.inflight
    }

    /// Check if storage is empty
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Whether any unchecked transactions remain in storage
    pub fn needs_check(&self) -> bool {
        self.cursor < self.transactions.len()
    }

    /// Insert transactions, dropping on full to clear backpressure on the upstream
    pub fn insert(&mut self, txs: impl IntoIterator<Item = SharableTransactionRegion>) {
        for tx in txs {
            if self.transactions.len() >= self.capacity {
                tx.free(self.allocator);
                self.dropped = self.dropped.saturating_add(1);
                continue;
            }
            match self.transactions.entry(*signature(&tx, self.allocator)) {
                // the old tx may be inflight, so free the new copy
                Entry::Occupied(_) => tx.free(self.allocator),
                Entry::Vacant(v) => _ = v.insert(tx),
            }
        }
    }

    /// Send a batch of CHECK transactions per worker
    /// Bounded to one full pass over storage between `reset_cursor` calls
    pub fn check(
        &mut self,
        slot: u64,
        producers: &mut [shaq::spsc::Producer<PackToWorkerMessage>],
    ) {
        if !self.needs_check() {
            return;
        }
        let Some((last, initial)) = producers.split_last_mut() else {
            return;
        };
        let mut message = PackToWorkerMessage {
            flags: pack_message_flags::CHECK | check_flags::STATUS_CHECKS,
            max_working_slot: slot,
            batch: self.next_batch(),
        };
        for producer in initial {
            match pack_to_worker::send(producer, message) {
                Ok(()) => {
                    self.inflight = self.inflight.saturating_add(1);
                    if !self.needs_check() {
                        return;
                    }
                    message.batch = self.next_batch();
                }
                Err(msg) => message = msg,
            }
        }
        match pack_to_worker::send(last, message) {
            Ok(()) => self.inflight = self.inflight.saturating_add(1),
            Err(msg) => {
                // Batch never sent: rewind cursor so it's re-checked next pass
                self.cursor = self
                    .cursor
                    .saturating_sub(msg.batch.num_transactions as usize);
                msg.batch.free(self.allocator);
            }
        }
    }

    /// Drain outstanding responses and drop failed CHECKs from store
    pub fn resolve(&mut self, consumers: &mut [shaq::spsc::Consumer<WorkerToPackMessage>]) {
        if self.inflight == 0 {
            return;
        }
        // No need to batch here, naturally bounded by at most `inflight` batches
        for consumer in consumers.iter_mut() {
            for message in worker_to_pack::iter(consumer) {
                self.inflight = self.inflight.saturating_sub(1);
                match message.processed_code {
                    processed_codes::PROCESSED => {
                        let txs = message.batch.slice(self.allocator);
                        let responses: &[CheckResponse] = message.responses.slice(self.allocator);
                        for (tx, response) in txs.iter().zip(responses) {
                            if check_failed(response) {
                                self.remove(signature(tx, self.allocator));
                            }
                            response.free(self.allocator);
                        }
                    }
                    processed_codes::MAX_WORKING_SLOT_EXCEEDED => {}
                    other => unreachable!("unexpected processed_code: {other}"),
                }
                message.free(self.allocator);
            }
        }
    }

    /// Get the next batch of transactions to check, walking cursor forward
    fn next_batch(&mut self) -> SharableTransactionBatchRegion {
        let end = self
            .cursor
            .saturating_add(BATCH_SIZE)
            .min(self.transactions.len());
        let batch = allocate_batch(
            self.transactions[self.cursor..end].iter().map(|(_, &r)| r),
            self.allocator,
        );
        self.cursor = end;
        batch
    }

    /// Remove an invalid transaction while preserving the `[0..cursor)` checked invariant
    fn remove(&mut self, sig: &[u8; 64]) {
        let remove = self
            .transactions
            .get_index_of(sig)
            .expect("transaction should be in storage");
        let last_checked = self.cursor.saturating_sub(1);
        // move the tx to remove forward in storage to the last_checked position
        self.transactions.swap_indices(remove, last_checked);
        // swap an unchecked tx from the end to last_checked
        let (_, tx) = self
            .transactions
            .swap_remove_index(last_checked)
            .expect("target index should be in bounds");
        // mark last_checked position unchecked
        self.cursor = self.cursor.saturating_sub(1);
        tx.free(self.allocator);
    }

    /// Drain up to `max` transactions from storage (oldest entries first, FIFO)
    /// Cursor is rewound to preserve the `[0..cursor)` checked-or-inflight invariant
    pub fn drain(
        &mut self,
        max: usize,
    ) -> impl Iterator<Item = SharableTransactionRegion> + use<'_> {
        let end = self.transactions.len().min(max);
        self.cursor = self.cursor.saturating_sub(end);
        self.transactions.drain(..end).map(|(_, tx)| tx)
    }
}

impl Drop for Storage<'_> {
    fn drop(&mut self) {
        for (_, tx) in self.transactions.drain(..) {
            tx.free(self.allocator);
        }
    }
}

fn check_failed(response: &CheckResponse) -> bool {
    const MASK: u8 = status_check_flags::PERFORMED
        | status_check_flags::TOO_OLD
        | status_check_flags::ALREADY_PROCESSED
        | status_check_flags::INVALID_NONCE;
    response.status_check_flags & MASK != status_check_flags::PERFORMED
}
