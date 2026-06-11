use crate::consts::PACK_TO_WORKER_CAPACITY;
use crate::ipc::shmem::{Free, Slice, allocate_batch, signature};
use crate::ipc::{pack_to_worker, worker_to_pack};
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    self, CheckResponse, ExecutionResponse, not_included_reasons, resolve_flags,
};
use agave_scheduler_bindings::{
    PackToWorkerMessage, SharableTransactionBatchRegion, SharableTransactionRegion,
    WorkerToPackMessage, pack_message_flags, processed_codes,
};
use agave_scheduling_utils::handshake::MAX_WORKERS;
use agave_transaction_view::transaction_view::UnsanitizedTransactionView;
use anyhow::{Result, bail};
use indexmap::IndexMap;
use indexmap::map::Entry;
use log::{error, info, warn};
use rdtsc::Instant;
use rts_alloc::Allocator;
use rustc_hash::{FxBuildHasher, FxHashMap};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use solana_sdk_ids::compute_budget;

/// A batch of transactions executed as an atomic unit.
struct Task {
    /// SHM region holding the serialized batch
    batch: SharableTransactionBatchRegion,
    /// Transaction accounts stored as (account, is_write)
    accounts: SmallVec<[(Pubkey, bool); 32]>,
    /// Sum of the batch's requested compute-unit limits, parsed at insert
    cu: u32,
    /// Lifecycle stage of the task
    state: TaskState,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TaskState {
    /// References ALTs; account set unknown until its CHECK returns.
    Unresolved,
    /// Account set known; eligible for promotion and dispatch.
    Resolved,
    /// Dispatched to a worker for EXECUTE; account locks held in `running`.
    Executing,
    /// Completed, or dropped after a failed CHECK; SHM freed.
    Done,
}

struct AccountLocks {
    locks: FxHashMap<Pubkey, u64>,
}

impl AccountLocks {
    const CAPACITY: usize = 4096;
    const WRITE_LOCK: u64 = u64::MAX;

    fn new() -> Self {
        Self {
            locks: FxHashMap::with_capacity_and_hasher(Self::CAPACITY, FxBuildHasher),
        }
    }

    fn clear(&mut self) {
        self.locks.clear();
    }

    /// true if the set of account locks blocks a Task
    fn blocks(&self, task: &Task) -> bool {
        for (account, is_write) in &task.accounts {
            match self.locks.get(account) {
                // Account is write locked
                Some(&n) if n == Self::WRITE_LOCK => return true,
                // Account is read locked and we need a write lock
                Some(&n) if n != 0 && *is_write => return true,
                _ => (),
            }
        }
        false
    }

    /// Add a Task to a set of account locks
    fn lock(&mut self, task: &Task) {
        for (account, is_write) in &task.accounts {
            self.locks
                .entry(*account)
                .and_modify(|n| {
                    *n = if *is_write {
                        Self::WRITE_LOCK
                    } else {
                        n.saturating_add(1)
                    }
                })
                .or_insert(if *is_write { Self::WRITE_LOCK } else { 1 });
        }
    }

    /// Remove a Task from a set of account locks
    fn unlock(&mut self, task: &Task) {
        for (account, is_write) in &task.accounts {
            *self.locks.get_mut(account).expect("account should exist") -=
                if *is_write { Self::WRITE_LOCK } else { 1 };
        }
    }
}

#[derive(Default, Clone, Copy, PartialEq)]
struct Metrics {
    total: usize,
    alt: usize,
    unresolved: usize,
    checking: usize,
    resolved: usize,
    executing: usize,
    success: usize,
    fail: usize,
}

impl std::fmt::Display for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            total,
            alt,
            unresolved,
            checking,
            resolved,
            executing,
            success,
            fail,
        } = self;
        write!(
            f,
            "total={total} alt={alt} unresolved={unresolved} checking={checking} resolved={resolved} executing={executing} success={success} fail={fail}",
        )
    }
}

pub struct BlockStage<'a> {
    allocator: &'a Allocator,
    /// Global task storage in priority order.
    /// `resolved_idx <= try_idx <= unresolved_gate <= check_idx <= tasks.len()`.
    tasks: IndexMap<[u8; 64], Task, FxBuildHasher>,
    /// First unCHECKed task. tasks[check_idx..] need CHECK.
    /// Increases monotonically as CHECKs are sent.
    check_idx: usize,
    /// First Unresolved task. tasks[unresolved..] may not EXECUTE.
    /// Increases monotonically as CHECK_RESPONSEs are received.
    unresolved_idx: usize,
    /// Cursor for iterating from resolved..check per tick.
    try_idx: usize,
    /// First Resolved task. tasks[resolved..check] need EXECUTE.
    /// Increases monotonically as EXECUTE_RESPONSEs are received.
    resolved_idx: usize,
    /// Index of the first vote transaction
    vote_idx: usize,

    /// Account locks held by currently executing transactions
    running: AccountLocks,
    /// Account locks held by account-blocked tasks `try_idx` has skipped
    priority: AccountLocks,

    /// How many workers the Schedule has
    num_workers: usize,
    /// How many CUs are currently assigned per worker
    compute_units: [u64; MAX_WORKERS],
    /// How many total messages are currently being processed by the workers
    processing: usize,

    /// Metrics for the block stage transactions
    block_metrics: Metrics,
    /// Metrics for the vote stage transactions
    vote_metrics: Metrics,
}

impl<'a> BlockStage<'a> {
    const VOTE_CUS: u32 = 3765;
    const RESET_TIMEOUT_MS: u64 = 25;

    pub fn new(num_workers: usize, allocator: &'a Allocator) -> Self {
        Self {
            allocator,
            tasks: IndexMap::with_capacity_and_hasher(4096, FxBuildHasher),
            check_idx: 0,
            unresolved_idx: 0,
            try_idx: 0,
            resolved_idx: 0,
            vote_idx: usize::MAX,
            running: AccountLocks::new(),
            priority: AccountLocks::new(),
            num_workers,
            compute_units: [0; MAX_WORKERS],
            processing: 0,
            block_metrics: Metrics::default(),
            vote_metrics: Metrics::default(),
        }
    }

    /// Advance the execution pipeline
    pub fn tick<I>(
        &mut self,
        slot: u64,
        batches: impl IntoIterator<Item = I>,
        producers: &mut [shaq::Producer<PackToWorkerMessage>],
        consumers: &mut [shaq::Consumer<WorkerToPackMessage>],
    ) where
        I: IntoIterator<Item = SharableTransactionRegion>,
        I::IntoIter: ExactSizeIterator,
    {
        self.insert(batches);
        self.check(slot, producers);
        self.execute(slot, producers);
        self.resolve(consumers);
    }

    pub fn vote_stage(&mut self) {
        self.vote_idx = self.tasks.len();
    }

    /// Clear the schedule, returning unexecuted simple votes as a lazy iterator.
    pub fn reset(
        &mut self,
        consumers: &mut [shaq::Consumer<WorkerToPackMessage>],
    ) -> Result<impl Iterator<Item = SharableTransactionRegion> + use<'_, 'a>> {
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
        info!("block_metrics: {}", self.block_metrics);
        info!("vote_metrics: {}", self.vote_metrics);
        let dropped =
            self.block_metrics.total - self.block_metrics.success - self.block_metrics.fail;
        if dropped != 0 {
            error!("failed to execute full block: dropped={dropped}");
        }
        let vote_idx = self.vote_idx;
        self.check_idx = 0;
        self.unresolved_idx = 0;
        self.try_idx = 0;
        self.resolved_idx = 0;
        self.vote_idx = usize::MAX;
        self.running.clear();
        self.priority.clear();
        self.compute_units = [0; MAX_WORKERS];
        self.block_metrics = Metrics::default();
        self.vote_metrics = Metrics::default();

        let allocator = self.allocator;
        Ok(self.tasks.drain(..).enumerate().filter_map(move |(i, (_, task))| {
            if task.state == TaskState::Done {
                None
            } else if i >= vote_idx {
                let tx = task.batch.slice(allocator)[0];
                task.batch.free(allocator);
                Some(tx)
            } else {
                task.batch.free_full(allocator);
                None
            }
        }))
    }

    /// Insert batches. Duplicates (by signature) are freed and dropped.
    fn insert<I>(&mut self, batches: impl IntoIterator<Item = I>)
    where
        I: IntoIterator<Item = SharableTransactionRegion>,
        I::IntoIter: ExactSizeIterator,
    {
        let is_vote = self.vote_idx != usize::MAX;
        'batch: for batch in batches {
            let batch = allocate_batch(batch, self.allocator);
            let txs = batch.slice(self.allocator);
            let len = txs.len();
            let entry = match self.tasks.entry(*signature(&txs[0], self.allocator)) {
                Entry::Occupied(_) => {
                    batch.free_full(self.allocator);
                    continue;
                }
                Entry::Vacant(entry) => entry,
            };

            let mut accounts: SmallVec<[(Pubkey, bool); 32]> = SmallVec::new();
            let mut cu: u32 = 0;
            let mut alt = 0;
            for tx in txs {
                let view = match UnsanitizedTransactionView::try_new_unsanitized(
                    tx.slice(self.allocator),
                ) {
                    Ok(view) => view,
                    Err(e) => {
                        error!("dropping malformed batch: txs={len} error={e:?}");
                        batch.free_full(self.allocator);
                        continue 'batch;
                    }
                };
                for (key, is_write) in static_accounts(&view) {
                    merge_account(&mut accounts, key, is_write);
                }
                if is_vote {
                    cu = cu.saturating_add(Self::VOTE_CUS);
                } else {
                    cu = cu.saturating_add(compute_unit_limit(&view));
                    if view.num_address_table_lookups() != 0 {
                        alt += 1;
                    }
                }
            }

            let metrics = if is_vote {
                &mut self.vote_metrics
            } else {
                &mut self.block_metrics
            };
            metrics.total += len;
            metrics.alt += alt;
            let state = if alt == 0 {
                metrics.resolved += len;
                TaskState::Resolved
            } else {
                metrics.unresolved += len;
                TaskState::Unresolved
            };
            entry.insert(Task { batch, accounts, cu, state });
        }
    }

    /// Dispatch pending ALT resolution CHECKs
    fn check(&mut self, slot: u64, producers: &mut [shaq::Producer<PackToWorkerMessage>]) {
        // Update cursor to the first Unresolved task
        while self.check_idx < self.tasks.len()
            && (self.tasks[self.check_idx].state != TaskState::Unresolved)
        {
            self.check_idx += 1;
        }
        if self.check_idx == self.tasks.len() {
            return;
        }

        // Dispatch CHECKs to workers round robin ordered by estimated CU load
        let mut order: [usize; MAX_WORKERS] = std::array::from_fn(|i| i);
        order[..self.num_workers].sort_unstable_by_key(|&w| self.compute_units[w]);
        let mut workers = order[..self.num_workers].iter().cycle();

        // CHECK the whole batch in one message; transactions without ALTs resolve trivially
        'batch: while self.check_idx < self.tasks.len()
            && self.processing <= PACK_TO_WORKER_CAPACITY * self.num_workers
        {
            if self.tasks[self.check_idx].state != TaskState::Unresolved {
                self.check_idx += 1;
                continue;
            }
            let task = &self.tasks[self.check_idx];
            let len = task.batch.num_transactions as usize;
            let mut message = PackToWorkerMessage {
                flags: pack_message_flags::CHECK | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
                max_working_slot: slot,
                batch: task.batch,
            };
            for &worker in workers.by_ref().take(self.num_workers) {
                if let Err(returned) = pack_to_worker::send(&mut producers[worker], message) {
                    message = returned;
                } else {
                    self.processing += 1;
                    self.block_metrics.unresolved -= len;
                    self.block_metrics.checking += len;
                    self.check_idx += 1;
                    continue 'batch;
                }
            }
            // All workers are full
            break;
        }
    }

    /// Dispatch unblocked transactions to EXECUTE
    fn execute(&mut self, slot: u64, producers: &mut [shaq::Producer<PackToWorkerMessage>]) {
        while self.unresolved_idx < self.tasks.len()
            && self.tasks[self.unresolved_idx].state != TaskState::Unresolved
        {
            self.unresolved_idx += 1;
        }
        while self.try_idx < self.unresolved_idx
            && self.processing <= PACK_TO_WORKER_CAPACITY * self.num_workers
        {
            let task = &mut self.tasks[self.try_idx];
            if task.state != TaskState::Resolved {
                // already dispatched (Executing) or done; skip
            } else if self.running.blocks(task) || self.priority.blocks(task) {
                self.priority.lock(task);
            } else {
                let worker = (0..self.num_workers)
                    .min_by_key(|&w| self.compute_units[w])
                    .expect("num_workers is nonzero");
                let message = PackToWorkerMessage {
                    flags: pack_message_flags::EXECUTE
                        | pack_message_flags::execution_flags::ALL_OR_NOTHING,
                    max_working_slot: slot,
                    batch: task.batch,
                };
                if pack_to_worker::send(&mut producers[worker], message).is_err() {
                    // Lowest CU worker is full - resume here next tick
                    break;
                }
                let len = task.batch.num_transactions as usize;
                task.state = TaskState::Executing;
                self.running.lock(task);
                self.compute_units[worker] += task.cu as u64;
                self.processing += 1;
                if self.try_idx >= self.vote_idx {
                    self.vote_metrics.resolved -= len;
                    self.vote_metrics.executing += len;
                } else {
                    self.block_metrics.resolved -= len;
                    self.block_metrics.executing += len;
                }
            }
            self.try_idx += 1;
        }
    }

    /// Drain and process worker responses
    pub fn resolve(&mut self, consumers: &mut [shaq::Consumer<WorkerToPackMessage>]) {
        let mut execute_received = false;
        for (worker, consumer) in consumers.iter_mut().enumerate() {
            for message in worker_to_pack::iter(consumer) {
                self.processing -= 1;
                if message.processed_code == processed_codes::MAX_WORKING_SLOT_EXCEEDED {
                    // Slot ended - leave tx cleanup for reset()
                    continue;
                }
                assert_eq!(
                    message.processed_code,
                    processed_codes::PROCESSED,
                    "unexpected processed_code {}",
                    message.processed_code
                );

                match message.responses.tag {
                    worker_message_types::EXECUTION_RESPONSE => {
                        execute_received = true;
                        self.resolve_execute(worker, &message);
                    }
                    worker_message_types::CHECK_RESPONSE => self.resolve_check(&message),
                    other => unreachable!("unexpected response tag: {other}"),
                }
            }
        }
        if execute_received {
            while self.resolved_idx < self.tasks.len()
                && matches!(
                    self.tasks[self.resolved_idx].state,
                    TaskState::Done | TaskState::Executing
                )
            {
                self.resolved_idx += 1;
            }
            self.try_idx = self.resolved_idx;
            self.priority.clear();
        }
    }

    fn resolve_execute(&mut self, worker: usize, message: &WorkerToPackMessage) {
        let results: &[ExecutionResponse] = message.responses.slice(self.allocator);
        // Bank torn down at the leader boundary; leave tx cleanup for reset()
        if results.first().map(|r| r.not_included_reason)
            == Some(not_included_reasons::BANK_NOT_AVAILABLE)
        {
            message.responses.free_full(self.allocator);
            return;
        }
        let txs = message.batch.slice(self.allocator);
        let (idx, _, task) = self
            .tasks
            .get_full_mut(signature(&txs[0], self.allocator))
            .expect("transaction should exist");
        task.state = TaskState::Done;
        self.running.unlock(task);
        self.compute_units[worker] -= task.cu as u64;
        let metrics = if idx >= self.vote_idx {
            &mut self.vote_metrics
        } else {
            &mut self.block_metrics
        };
        for result in results {
            metrics.executing -= 1;
            match result.not_included_reason {
                not_included_reasons::NONE => metrics.success += 1,
                // with our design we should never hit this
                not_included_reasons::ACCOUNT_IN_USE => {
                    error!(
                        "unexpected transaction failure: not_included_reason={}",
                        result.not_included_reason
                    );
                    metrics.fail += 1;
                }
                // expected transaction failures
                _ => metrics.fail += 1,
            }
        }
        message.free_full(self.allocator);
    }

    fn resolve_check(&mut self, message: &WorkerToPackMessage) {
        const MASK: u8 = resolve_flags::PERFORMED | resolve_flags::FAILED;

        let txs = message.batch.slice(self.allocator);
        let results: &[CheckResponse] = message.responses.slice(self.allocator);
        let len = txs.len();
        let task = self
            .tasks
            .get_mut(signature(&txs[0], self.allocator))
            .expect("transaction should exist");
        self.block_metrics.checking -= len;

        // Any failed transaction dooms the whole batch
        if let Some(response) = results
            .iter()
            .find(|r| r.resolve_flags & MASK != resolve_flags::PERFORMED)
        {
            warn!(
                "unexpected CHECK failure: parsing_and_sanitization_flags={:#04x} resolve_flags={:#04x}",
                response.parsing_and_sanitization_flags, response.resolve_flags,
            );
            task.state = TaskState::Done;
            message.free_full(self.allocator);
            self.block_metrics.fail += len;
            return;
        }

        // Merge each transaction's resolved ALT pubkeys into the batch union
        for (tx, result) in txs.iter().zip(results) {
            if result.resolved_pubkeys.num_pubkeys == 0 {
                continue;
            }
            let view = UnsanitizedTransactionView::try_new_unsanitized(tx.slice(self.allocator))
                .expect("transaction parsed at insert");
            let writable = view.total_writable_lookup_accounts() as usize;
            for (i, key) in result.resolved_pubkeys.slice(self.allocator).iter().enumerate() {
                merge_account(&mut task.accounts, *key, i < writable);
            }
        }
        task.state = TaskState::Resolved;
        // Keep the batch region for EXECUTE; free only the responses
        message.responses.free_full(self.allocator);
        self.block_metrics.resolved += len;
    }
}

/// Merge an account into a batch's union, OR-ing writability on duplicates
fn merge_account(accounts: &mut SmallVec<[(Pubkey, bool); 32]>, key: Pubkey, is_write: bool) {
    for (account, write) in accounts.iter_mut() {
        if *account == key {
            *write |= is_write;
            return;
        }
    }
    accounts.push((key, is_write));
}

/// Parse the requested compute-unit limit
fn compute_unit_limit(view: &UnsanitizedTransactionView<&[u8]>) -> u32 {
    const DEFAULT_CU: u32 = 200_000;
    let keys = view.static_account_keys();
    for ix in view.instructions_iter() {
        if keys.get(ix.program_id_index as usize) != Some(&compute_budget::ID) {
            continue;
        }
        // SetComputeUnitLimit wire: 1-byte discriminator 0x02 + u32 LE
        if ix.data.len() >= 5 && ix.data[0] == 0x02 {
            return u32::from_le_bytes(ix.data[1..5].try_into().unwrap());
        }
    }
    DEFAULT_CU
}

/// Flatten a tx's static account keys into `(pubkey, is_write)` pairs.
fn static_accounts(view: &UnsanitizedTransactionView<&[u8]>) -> SmallVec<[(Pubkey, bool); 32]> {
    let num_signed = view.num_required_signatures() as usize;
    let num_readonly_signed = view.num_readonly_signed_static_accounts() as usize;
    let num_readonly_unsigned = view.num_readonly_unsigned_static_accounts() as usize;
    let keys = view.static_account_keys();
    let num_writable_signed = num_signed.saturating_sub(num_readonly_signed);
    let num_writable_unsigned = keys
        .len()
        .saturating_sub(num_signed)
        .saturating_sub(num_readonly_unsigned);
    let mut accounts = SmallVec::with_capacity(keys.len());
    for (i, key) in keys.iter().enumerate() {
        let is_write = if i < num_signed {
            i < num_writable_signed
        } else {
            i - num_signed < num_writable_unsigned
        };
        accounts.push((*key, is_write));
    }
    accounts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{PACK_TO_WORKER_CAPACITY, SLAB_SIZE, WORKER_TO_PACK_CAPACITY};
    use crate::ipc::shmem::allocate;
    use agave_scheduler_bindings::TransactionResponseRegion;
    use agave_scheduler_bindings::worker_message_types::{
        EXECUTION_RESPONSE, ExecutionResponse, not_included_reasons,
    };
    use bincode::serialize;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaChaRng;
    use solana_instruction::{AccountMeta, Instruction};
    use solana_message::Message;
    use solana_transaction::{Signature, Transaction};
    use std::collections::HashMap;
    use tempfile::tempfile;

    const TEST_ALLOC_SIZE: usize = 128 * 1024 * 1024;

    fn shaq_channel<T>(capacity: usize) -> (shaq::Producer<T>, shaq::Consumer<T>) {
        let size = shaq::minimum_file_size::<T>(capacity);
        let file = tempfile().unwrap();
        let producer = unsafe { shaq::Producer::create(&file, size) }.unwrap();
        let consumer = unsafe { shaq::Consumer::join(&file) }.unwrap();
        (producer, consumer)
    }

    fn random_transaction(rng: &mut impl Rng, accounts: &[Pubkey], num_accounts: usize) -> Transaction {
        let num_writable = rng.random_range(1..=num_accounts);
        let metas = rand::seq::index::sample(rng, accounts.len(), num_accounts)
            .into_iter()
            .enumerate()
            .map(|(i, j)| {
                if i < num_writable {
                    AccountMeta::new(accounts[j], i == 0)
                } else {
                    AccountMeta::new_readonly(accounts[j], false)
                }
            })
            .collect();
        let instruction = Instruction::new_with_bytes(Pubkey::default(), &[], metas);
        let mut tx = Transaction::new_unsigned(Message::new(&[instruction], None));
        tx.signatures[0] = Signature::new_unique();
        tx
    }

    #[test]
    fn fifo_random_workload() {
        const NUM_WORKERS: usize = 8;
        const NUM_ACCOUNTS: usize = 256;
        const NUM_TXS: usize = 32 * 1024;

        rdtsc::calibrate();
        let mut rng = ChaChaRng::seed_from_u64(0x0123456789ABCDEF);
        let allocator =
            unsafe { Allocator::create(&tempfile().unwrap(), TEST_ALLOC_SIZE, 1, SLAB_SIZE) }
                .unwrap();
        let mut block_stage = BlockStage::new(NUM_WORKERS, &allocator);
        let accounts: Vec<Pubkey> = (0..NUM_ACCOUNTS).map(|_| Pubkey::new_unique()).collect();
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
            .map(|_| {
                let num_accounts = rng.random_range(1..=37);
                random_transaction(&mut rng, &accounts, num_accounts)
            })
            .collect();

        block_stage.insert(
            txs.iter()
                .map(|tx| std::iter::once(allocate(serialize(tx).unwrap(), &allocator))),
        );

        let mut processed = Vec::new();
        block_stage.execute(1, &mut pack_to_worker);
        while block_stage.processing != 0 {
            workers.shuffle(&mut rng);
            for (rx, tx) in workers.iter_mut() {
                rx.sync();
                tx.sync();
                if let Some(message) = rx.try_read() {
                    for tx in message.batch.slice(&allocator) {
                        processed.push(*signature(tx, &allocator));
                    }
                    let n = message.batch.num_transactions;
                    let bytes = u32::try_from(n as usize * size_of::<ExecutionResponse>()).unwrap();
                    let ptr = allocator.allocate(bytes).unwrap();
                    let slots = ptr.as_ptr().cast::<ExecutionResponse>();
                    for i in 0..n {
                        unsafe {
                            slots.add(i as usize).write(ExecutionResponse {
                                execution_slot: 0,
                                not_included_reason: not_included_reasons::NONE,
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
                            tag: EXECUTION_RESPONSE,
                            num_transaction_responses: n,
                            transaction_responses_offset: offset,
                        },
                    })
                    .unwrap();
                }
                rx.finalize();
                tx.commit();
            }
            block_stage.resolve(&mut worker_to_pack);
            block_stage.execute(1, &mut pack_to_worker);
        }

        let processed: HashMap<[u8; 64], usize> = processed
            .into_iter()
            .enumerate()
            .map(|(pos, sig)| (sig, pos))
            .collect();
        assert_eq!(processed.len(), NUM_TXS, "every inserted tx must dispatch");

        let mut last_writer: HashMap<Pubkey, usize> = HashMap::new();
        let mut last_any: HashMap<Pubkey, usize> = HashMap::new();
        for (i, tx) in txs.iter().enumerate() {
            let pos = processed[tx.signatures[0].as_array()];
            for (j, &key) in tx.message.account_keys.iter().enumerate() {
                if tx.message.is_maybe_writable(j, None) {
                    let prior = *last_any.get(&key).unwrap_or(&0);
                    assert!(
                        pos >= prior,
                        "writer tx {i} on {key} dispatched at {pos} before prior at {prior}",
                    );
                    last_writer.insert(key, pos);
                    last_any.insert(key, pos);
                } else {
                    let prior = *last_writer.get(&key).unwrap_or(&0);
                    assert!(
                        pos >= prior,
                        "reader tx {i} on {key} dispatched at {pos} before prior writer at {prior}",
                    );
                    let any = last_any.entry(key).or_default();
                    *any = (*any).max(pos);
                }
            }
        }
    }

    #[test]
    fn fifo_random_bundles() {
        const NUM_WORKERS: usize = 8;
        const NUM_ACCOUNTS: usize = 4096;
        const NUM_BUNDLES: usize = 1024;
        const BUNDLE_SIZE: usize = 5;
        // Static account keys cap at 38 per transaction (PACKET_DATA_SIZE / 32),
        // so the 64 distinct accounts are spread across the bundle's transactions
        const BUNDLE_ACCOUNTS: usize = 64;

        rdtsc::calibrate();
        let mut rng = ChaChaRng::seed_from_u64(0x0123456789ABCDEF);
        let allocator =
            unsafe { Allocator::create(&tempfile().unwrap(), TEST_ALLOC_SIZE, 1, SLAB_SIZE) }
                .unwrap();
        let mut block_stage = BlockStage::new(NUM_WORKERS, &allocator);
        let accounts: Vec<Pubkey> = (0..NUM_ACCOUNTS).map(|_| Pubkey::new_unique()).collect();
        let (mut pack_to_worker, worker_rx): (Vec<shaq::Producer<_>>, Vec<shaq::Consumer<_>>) = (0
            ..NUM_WORKERS)
            .map(|_| shaq_channel::<PackToWorkerMessage>(PACK_TO_WORKER_CAPACITY))
            .unzip();
        let (worker_tx, mut worker_to_pack): (Vec<shaq::Producer<_>>, Vec<shaq::Consumer<_>>) = (0
            ..NUM_WORKERS)
            .map(|_| shaq_channel::<WorkerToPackMessage>(WORKER_TO_PACK_CAPACITY))
            .unzip();
        let mut workers = worker_rx.into_iter().zip(worker_tx).collect::<Vec<_>>();

        let bundles: Vec<Vec<Transaction>> = (0..NUM_BUNDLES)
            .map(|_| {
                let chosen: Vec<Pubkey> =
                    rand::seq::index::sample(&mut rng, NUM_ACCOUNTS, BUNDLE_ACCOUNTS)
                        .into_iter()
                        .map(|i| accounts[i])
                        .collect();
                chosen
                    .chunks(BUNDLE_ACCOUNTS.div_ceil(BUNDLE_SIZE))
                    .map(|chunk| random_transaction(&mut rng, chunk, chunk.len()))
                    .collect()
            })
            .collect();

        block_stage.insert(bundles.iter().map(|txs| {
            txs.iter()
                .map(|tx| allocate(serialize(tx).unwrap(), &allocator))
        }));

        let mut processed = Vec::new();
        block_stage.execute(1, &mut pack_to_worker);
        while block_stage.processing != 0 {
            workers.shuffle(&mut rng);
            for (rx, tx) in workers.iter_mut() {
                rx.sync();
                tx.sync();
                if let Some(message) = rx.try_read() {
                    assert_eq!(message.batch.num_transactions as usize, BUNDLE_SIZE);
                    for tx in message.batch.slice(&allocator) {
                        processed.push(*signature(tx, &allocator));
                    }
                    let n = message.batch.num_transactions;
                    let bytes = u32::try_from(n as usize * size_of::<ExecutionResponse>()).unwrap();
                    let ptr = allocator.allocate(bytes).unwrap();
                    let slots = ptr.as_ptr().cast::<ExecutionResponse>();
                    for i in 0..n {
                        unsafe {
                            slots.add(i as usize).write(ExecutionResponse {
                                execution_slot: 0,
                                not_included_reason: not_included_reasons::NONE,
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
                            tag: EXECUTION_RESPONSE,
                            num_transaction_responses: n,
                            transaction_responses_offset: offset,
                        },
                    })
                    .unwrap();
                }
                rx.finalize();
                tx.commit();
            }
            block_stage.resolve(&mut worker_to_pack);
            block_stage.execute(1, &mut pack_to_worker);
        }

        let processed: HashMap<[u8; 64], usize> = processed
            .into_iter()
            .enumerate()
            .map(|(pos, sig)| (sig, pos))
            .collect();
        assert_eq!(
            processed.len(),
            NUM_BUNDLES * BUNDLE_SIZE,
            "every inserted tx must dispatch"
        );

        let mut last_writer: HashMap<Pubkey, usize> = HashMap::new();
        let mut last_any: HashMap<Pubkey, usize> = HashMap::new();
        for (i, txs) in bundles.iter().enumerate() {
            // The whole bundle dispatches contiguously in one message
            let pos = processed[txs[0].signatures[0].as_array()];
            for (j, tx) in txs.iter().enumerate() {
                assert_eq!(
                    processed[tx.signatures[0].as_array()],
                    pos + j,
                    "bundle {i} tx {j} dispatched out of order",
                );
            }

            // Conflicts resolve on the bundle's account union with OR-ed writability
            let mut union: HashMap<Pubkey, bool> = HashMap::new();
            for tx in txs {
                for (j, &key) in tx.message.account_keys.iter().enumerate() {
                    *union.entry(key).or_default() |= tx.message.is_maybe_writable(j, None);
                }
            }
            for (&key, &is_write) in &union {
                if is_write {
                    let prior = *last_any.get(&key).unwrap_or(&0);
                    assert!(
                        pos >= prior,
                        "writer bundle {i} on {key} dispatched at {pos} before prior at {prior}",
                    );
                    last_writer.insert(key, pos);
                    last_any.insert(key, pos);
                } else {
                    let prior = *last_writer.get(&key).unwrap_or(&0);
                    assert!(
                        pos >= prior,
                        "reader bundle {i} on {key} dispatched at {pos} before prior writer at {prior}",
                    );
                    let any = last_any.entry(key).or_default();
                    *any = (*any).max(pos);
                }
            }
        }
    }
}
