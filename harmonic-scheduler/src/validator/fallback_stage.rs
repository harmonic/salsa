use crate::consts::PACK_TO_WORKER_CAPACITY;
use crate::ipc::shmem::{Free, Slice, allocate_batch};
use crate::ipc::{pack_to_worker, worker_to_pack};
use agave_scheduler_bindings::worker_message_types::{
    self, ExecutionResponse, not_included_reasons,
};
use agave_scheduler_bindings::{
    PackToWorkerMessage, SharableTransactionRegion, WorkerToPackMessage, pack_message_flags,
    processed_codes,
};
use anyhow::{Result, bail};
use log::{info, trace};
use rdtsc::Instant;
use rts_alloc::Allocator;
use std::collections::VecDeque;

/// Monotonic per-slot counters for the fallback stage
#[derive(Default, Clone, Copy, PartialEq)]
struct Metrics {
    /// Transactions inserted
    total: usize,
    /// Transactions that landed
    success: usize,
    /// Transactions that did not land
    fail: usize,
    /// Transactions that we dropped without landing
    dropped: usize,
}

impl std::fmt::Display for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            total,
            success,
            fail,
            dropped,
        } = self;
        write!(
            f,
            "total={total} success={success} fail={fail} dropped={dropped}"
        )
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            total,
            success,
            fail,
            dropped,
        } = self;
        write!(f, "{total:04x}{success:04x}{fail:04x}{dropped:04x}")
    }
}

pub struct FallbackStage<'a> {
    allocator: &'a Allocator,
    /// Transaction storage
    txs: VecDeque<SharableTransactionRegion>,
    /// How many workers we dispatch to
    num_workers: usize,
    /// Next worker for round-robin dispatch
    next_worker: usize,
    /// Dispatched message count
    processing: usize,
    /// Reached the end of the slot
    slot_end: bool,
    /// Per-slot metrics
    metrics: Metrics,
    /// Timer for slot measurements
    timer: Instant,
    /// Timing snapshots through the slot, collected only at log level Trace
    timing: Vec<(u64, Metrics)>,
}

impl<'a> FallbackStage<'a> {
    const CAPACITY: usize = 1024;
    const RESET_TIMEOUT_MS: u64 = 25;

    pub fn new(num_workers: usize, allocator: &'a Allocator) -> Self {
        Self {
            allocator,
            txs: VecDeque::with_capacity(Self::CAPACITY),
            num_workers,
            next_worker: 0,
            processing: 0,
            slot_end: false,
            metrics: Metrics::default(),
            timer: Instant::now(),
            timing: if log::log_enabled!(log::Level::Trace) {
                Vec::with_capacity(4096)
            } else {
                Vec::new()
            },
        }
    }

    /// Advance the pipeline
    pub fn tick(
        &mut self,
        slot: u64,
        txs: impl IntoIterator<Item = SharableTransactionRegion>,
        producers: &mut [shaq::Producer<PackToWorkerMessage>],
        consumers: &mut [shaq::Consumer<WorkerToPackMessage>],
    ) {
        if log::log_enabled!(log::Level::Trace) {
            if self.metrics.total == 0 {
                self.timer = Instant::now();
            } else if self.timer.elapsed_us() >= 250 {
                self.timing.push((self.timer.elapsed_us(), self.metrics));
            }
        }
        self.execute(slot, txs, producers);
        self.resolve(consumers);
    }

    pub fn backpressured(&self) -> bool {
        !self.txs.is_empty()
    }

    /// Nothing more can land this slot, so the stage can end early
    pub fn done(&self) -> bool {
        self.slot_end
    }

    pub fn reset(&mut self, consumers: &mut [shaq::Consumer<WorkerToPackMessage>]) -> Result<()> {
        let timer = Instant::now();
        while self.processing != 0 && timer.elapsed_ms() < Self::RESET_TIMEOUT_MS {
            self.resolve(consumers);
        }
        if self.processing != 0 {
            bail!(
                "timeout waiting for worker response: processing={}",
                self.processing
            );
        }
        self.metrics.dropped += self.txs.len();
        if log::log_enabled!(log::Level::Trace) && self.metrics.total != 0 {
            self.timing.push((self.timer.elapsed_us(), self.metrics));
        }
        info!("fallback_metrics: {}", self.metrics);
        trace!("fallback_timing: {:?}", self.timing);
        self.metrics = Metrics::default();
        self.timing.clear();
        self.slot_end = false;
        for tx in self.txs.drain(..) {
            tx.free(self.allocator);
        }
        Ok(())
    }

    fn execute(
        &mut self,
        slot: u64,
        txs: impl IntoIterator<Item = SharableTransactionRegion>,
        producers: &mut [shaq::Producer<PackToWorkerMessage>],
    ) {
        for tx in txs {
            self.metrics.total += 1;
            self.txs.push_back(tx);
        }
        'batch: while !self.slot_end
            && self.processing <= PACK_TO_WORKER_CAPACITY * self.num_workers
        {
            let Some(tx) = self.txs.pop_front() else {
                break;
            };
            let mut message = PackToWorkerMessage {
                flags: pack_message_flags::EXECUTE,
                max_working_slot: slot,
                batch: allocate_batch(std::iter::once(tx), self.allocator),
            };
            for worker in (self.next_worker..self.num_workers).chain(0..self.next_worker) {
                match pack_to_worker::send(&mut producers[worker], message) {
                    Ok(()) => {
                        self.processing += 1;
                        self.next_worker = (worker + 1) % self.num_workers;
                        continue 'batch;
                    }
                    Err(returned) => message = returned,
                }
            }
            // All workers full
            message.batch.free(self.allocator);
            self.txs.push_front(tx);
            break;
        }
    }

    fn resolve(&mut self, consumers: &mut [shaq::Consumer<WorkerToPackMessage>]) {
        for consumer in consumers.iter_mut() {
            for message in worker_to_pack::iter(consumer) {
                self.processing -= 1;
                if message.processed_code == processed_codes::MAX_WORKING_SLOT_EXCEEDED {
                    // Slot rolled mid-flight: drop the batch
                    self.metrics.dropped += message.batch.num_transactions as usize;
                    message.free_full(self.allocator);
                    continue;
                }
                assert_eq!(
                    message.processed_code,
                    processed_codes::PROCESSED,
                    "unexpected processed_code {}",
                    message.processed_code
                );
                assert_eq!(
                    message.responses.tag,
                    worker_message_types::EXECUTION_RESPONSE,
                    "unexpected response tag {}",
                    message.responses.tag
                );
                let txs = message.batch.slice(self.allocator);
                let results: &[ExecutionResponse] = message.responses.slice(self.allocator);
                for (tx, result) in txs.iter().zip(results) {
                    if result.not_included_reason == not_included_reasons::NONE {
                        self.metrics.success += 1;
                    } else {
                        // Block is full, stop trying to pack more
                        if matches!(
                            result.not_included_reason,
                            not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
                                | not_included_reasons::BANK_NOT_AVAILABLE
                        ) {
                            self.slot_end = true;
                        }
                        self.metrics.fail += 1;
                    }
                    tx.free(self.allocator);
                }
                message.free(self.allocator);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{PACK_TO_WORKER_CAPACITY, SLAB_SIZE, WORKER_TO_PACK_CAPACITY};
    use crate::ipc::shmem::allocate;
    use agave_scheduler_bindings::TransactionResponseRegion;
    use bincode::serialize;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaChaRng;
    use solana_instruction::{AccountMeta, Instruction};
    use solana_message::Message;
    use solana_pubkey::Pubkey;
    use solana_transaction::{Signature, Transaction};
    use tempfile::tempfile;

    const TEST_ALLOC_SIZE: usize = 128 * 1024 * 1024;
    const SLOT: u64 = 1;

    fn shaq_channel<T>(capacity: usize) -> (shaq::Producer<T>, shaq::Consumer<T>) {
        let size = shaq::minimum_file_size::<T>(capacity);
        let file = tempfile().unwrap();
        let producer = unsafe { shaq::Producer::create(&file, size) }.unwrap();
        let consumer = unsafe { shaq::Consumer::join(&file) }.unwrap();
        (producer, consumer)
    }

    fn random_transaction(rng: &mut impl Rng, accounts: &[Pubkey]) -> Transaction {
        let num_accounts = rng.random_range(1..=8);
        let metas = rand::seq::index::sample(rng, accounts.len(), num_accounts)
            .into_iter()
            .enumerate()
            .map(|(i, j)| AccountMeta::new(accounts[j], i == 0))
            .collect();
        let instruction = Instruction::new_with_bytes(Pubkey::default(), &[], metas);
        let mut tx = Transaction::new_unsigned(Message::new(&[instruction], None));
        tx.signatures[0] = Signature::new_unique();
        tx
    }

    /// Feeds more transactions than the workers can hold so the overflow backs
    /// up into storage (backpressured), then drains with a mock worker that
    /// lands, fails, and slot-exceeds, asserting the accounting closes and every
    /// transaction reaches exactly one terminal state.
    #[test]
    fn dispatch_and_drain() {
        const NUM_WORKERS: usize = 4;
        const NUM_TXS: usize = 512;

        rdtsc::calibrate();
        let mut rng = ChaChaRng::seed_from_u64(0x0123456789ABCDEF);
        let allocator =
            unsafe { Allocator::create(&tempfile().unwrap(), TEST_ALLOC_SIZE, 1, SLAB_SIZE) }
                .unwrap();
        let mut fallback = FallbackStage::new(NUM_WORKERS, &allocator);
        let accounts: Vec<Pubkey> = (0..32).map(|_| Pubkey::new_unique()).collect();
        let (mut pack_to_worker, worker_rx): (Vec<shaq::Producer<_>>, Vec<shaq::Consumer<_>>) = (0
            ..NUM_WORKERS)
            .map(|_| shaq_channel::<PackToWorkerMessage>(PACK_TO_WORKER_CAPACITY))
            .unzip();
        let (worker_tx, mut worker_to_pack): (Vec<shaq::Producer<_>>, Vec<shaq::Consumer<_>>) = (0
            ..NUM_WORKERS)
            .map(|_| shaq_channel::<WorkerToPackMessage>(WORKER_TO_PACK_CAPACITY))
            .unzip();
        let mut workers = worker_rx.into_iter().zip(worker_tx).collect::<Vec<_>>();

        let txs: Vec<Transaction> = (0..NUM_TXS)
            .map(|_| random_transaction(&mut rng, &accounts))
            .collect();
        let mut messages = 0usize;
        let mut iterations = 0usize;

        // First tick feeds every transaction; with only NUM_WORKERS *
        // PACK_TO_WORKER_CAPACITY worker slots the rest backs up into storage
        fallback.tick(
            SLOT,
            txs.iter()
                .map(|tx| allocate(serialize(tx).unwrap(), &allocator)),
            &mut pack_to_worker,
            &mut worker_to_pack,
        );
        assert!(
            fallback.backpressured(),
            "overflow beyond worker capacity backs up"
        );

        loop {
            workers.shuffle(&mut rng);
            for (rx, tx) in workers.iter_mut() {
                rx.sync();
                tx.sync();
                let Some(message) = rx.try_read() else {
                    rx.finalize();
                    tx.commit();
                    continue;
                };
                messages += 1;
                let n = message.batch.num_transactions;
                // Every 16th message simulates the slot ending mid-flight
                if messages.is_multiple_of(16) {
                    tx.try_write(WorkerToPackMessage {
                        batch: message.batch,
                        processed_code: processed_codes::MAX_WORKING_SLOT_EXCEEDED,
                        responses: TransactionResponseRegion {
                            tag: worker_message_types::EXECUTION_RESPONSE,
                            num_transaction_responses: 0,
                            transaction_responses_offset: 0,
                        },
                    })
                    .unwrap();
                } else {
                    let bytes = u32::try_from(n as usize * size_of::<ExecutionResponse>()).unwrap();
                    let ptr = allocator.allocate(bytes).unwrap();
                    let slots = ptr.as_ptr().cast::<ExecutionResponse>();
                    // Alternate landed and permanently-failed outcomes by message
                    let reason = if messages.is_multiple_of(2) {
                        not_included_reasons::NONE
                    } else {
                        not_included_reasons::INSTRUCTION_ERROR
                    };
                    for i in 0..usize::from(n) {
                        unsafe {
                            slots.add(i).write(ExecutionResponse {
                                execution_slot: 0,
                                not_included_reason: reason,
                                cost_units: 0,
                                fee_payer_balance: 0,
                            });
                        }
                    }
                    let offset = unsafe { allocator.offset(ptr) };
                    tx.try_write(WorkerToPackMessage {
                        batch: message.batch,
                        processed_code: processed_codes::PROCESSED,
                        responses: TransactionResponseRegion {
                            tag: worker_message_types::EXECUTION_RESPONSE,
                            num_transaction_responses: n,
                            transaction_responses_offset: offset,
                        },
                    })
                    .unwrap();
                }
                rx.finalize();
                tx.commit();
            }
            fallback.tick(
                SLOT,
                std::iter::empty(),
                &mut pack_to_worker,
                &mut worker_to_pack,
            );

            iterations += 1;
            assert!(iterations < 1_000_000, "drain did not converge");
            if fallback.processing == 0 && fallback.txs.is_empty() {
                break;
            }
        }

        let m = &fallback.metrics;
        assert_eq!(m.total, NUM_TXS, "every inserted tx is counted");
        assert_eq!(
            m.success + m.fail + m.dropped,
            m.total,
            "every tx reaches a terminal state ({m})",
        );
        assert!(m.success > 0, "success path exercised ({m})");
        assert!(m.fail > 0, "fail path exercised ({m})");
        assert!(m.dropped > 0, "dropped path exercised ({m})");
        assert!(!fallback.backpressured(), "backlog drained");

        fallback.reset(&mut worker_to_pack).unwrap();
        assert!(fallback.txs.is_empty());
    }
}
