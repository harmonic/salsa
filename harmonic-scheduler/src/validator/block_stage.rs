use crate::consts::PACK_TO_WORKER_CAPACITY;
use crate::ipc::shmem::{Free, Slice, allocate_batch, signature};
use crate::ipc::{pack_to_worker, worker_to_pack};
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    self, CheckResponse, ExecutionResponse, not_included_reasons, resolve_flags,
};
use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, SharableTransactionBatchRegion,
    SharableTransactionRegion, WorkerToPackMessage, pack_message_flags, processed_codes,
};
use agave_scheduling_utils::handshake::MAX_WORKERS;
use agave_transaction_view::transaction_view::UnsanitizedTransactionView;
use anyhow::{Result, bail};
use indexmap::IndexMap;
use indexmap::map::Entry;
use log::{error, info, trace, warn};
use rdtsc::Instant;
use rts_alloc::Allocator;
use rustc_hash::{FxBuildHasher, FxHashMap};
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use solana_sdk_ids::compute_budget;
use std::collections::VecDeque;

/// A batch executed atomically: a single transaction, or a bundle sent as one
/// all-or-nothing EXECUTE. Locking uses the union of member accounts.
struct Task {
    /// The full batch, allocated at insert; `[0]`'s signature keys the task.
    batch: SharableTransactionBatchRegion,
    /// Deduplicated (account, is_write) union over members; grows as ALTs resolve.
    accounts: SmallVec<[(Pubkey, bool); 32]>,
    /// Saturating sum of the members' requested compute-unit limits
    cu: u32,
    /// ALT members whose CHECK response has not yet arrived
    alt_remaining: u8,
    /// Whether or not this is a batch of vote transactions
    is_vote: bool,
    /// Lifecycle stage of the task
    state: TaskState,
}

/// A transaction queued for an ALT-resolution CHECK, linked to its task.
struct Check {
    /// SHM region holding the serialized transaction (owned by the task's batch)
    tx: SharableTransactionRegion,
    /// How many of this transaction's resolved ALT keys are writable
    alt_writable: u16,
    /// Index of the parent task in `tasks` (stable until `reset` drains it)
    task: usize,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TaskState {
    /// References ALTs; account set unknown until its CHECK returns.
    Unresolved,
    /// Account set known; eligible for promotion and dispatch.
    Resolved,
    /// Dispatched to a worker for EXECUTE; account locks held in `running`.
    Executing,
    /// Terminal; the batch has been freed (executed, or dropped after its CHECKs returned).
    Done,
    /// A member CHECK failed; freed once its remaining CHECKs return, else at reset.
    Dropped,
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
            "total={total} alt={alt} unresolved={unresolved} checking={checking} \
             resolved={resolved} executing={executing} success={success} fail={fail}",
        )
    }
}

impl std::fmt::Debug for Metrics {
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
            "{total:04x}{alt:04x}{unresolved:04x}{checking:04x}{resolved:04x}{executing:\
             04x}{success:04x}{fail:04x}",
        )
    }
}

pub struct BlockStage<'a> {
    allocator: &'a Allocator,
    /// Global task storage in priority order.
    /// `resolved_idx <= try_idx <= unresolved_idx <= tasks.len()`.
    tasks: IndexMap<[u8; 64], Task, FxBuildHasher>,
    /// Single transactions awaiting an ALT-resolution CHECK, in priority order.
    check_pending: VecDeque<Check>,
    /// Dispatched CHECK batches by SHM offset, to route responses to their `Check`s.
    check_inflight: FxHashMap<usize, Vec<Check>>,
    /// First Unresolved task. tasks[unresolved..] may not EXECUTE.
    /// Increases monotonically as CHECK_RESPONSEs are received.
    unresolved_idx: usize,
    /// Cursor iterating resolved..unresolved tasks per tick.
    try_idx: usize,
    /// First Resolved task. tasks[resolved..unresolved] need EXECUTE.
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
    /// Timer for slot measurements
    timer: Instant,
    /// Timing metrics for the full block stage
    /// Only collected when log level == Trace
    block_timing: Vec<(u64, Metrics)>,
    vote_timing: Vec<(u64, Metrics)>,
}

impl<'a> BlockStage<'a> {
    const VOTE_CUS: u32 = 3765;
    const RESET_TIMEOUT_MS: u64 = 25;

    pub fn new(num_workers: usize, allocator: &'a Allocator) -> Self {
        Self {
            allocator,
            tasks: IndexMap::with_capacity_and_hasher(4096, FxBuildHasher),
            check_pending: VecDeque::with_capacity(4096),
            check_inflight: FxHashMap::with_capacity_and_hasher(
                PACK_TO_WORKER_CAPACITY * num_workers,
                FxBuildHasher,
            ),
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
            timer: Instant::now(),
            block_timing: if log::log_enabled!(log::Level::Trace) {
                Vec::with_capacity(4096)
            } else {
                Vec::new()
            },
            vote_timing: if log::log_enabled!(log::Level::Trace) {
                Vec::with_capacity(4096)
            } else {
                Vec::new()
            },
        }
    }

    /// Advance the execution pipeline
    pub fn tick(
        &mut self,
        slot: u64,
        bundles: impl IntoIterator<
            Item = impl IntoIterator<Item = SharableTransactionRegion, IntoIter: ExactSizeIterator>,
        >,
        producers: &mut [shaq::spsc::Producer<PackToWorkerMessage>],
        consumers: &mut [shaq::spsc::Consumer<WorkerToPackMessage>],
    ) {
        if log::log_enabled!(log::Level::Trace) {
            if self.tasks.is_empty() {
                self.timer = Instant::now();
            } else {
                let us = self.timer.elapsed_us();
                if self.block_timing.last().map(|&(_, m)| m) != Some(self.block_metrics) {
                    self.block_timing.push((us, self.block_metrics));
                }
                if self.vote_idx != usize::MAX
                    && self.vote_timing.last().map(|&(_, m)| m) != Some(self.vote_metrics)
                {
                    self.vote_timing.push((us, self.vote_metrics));
                }
            }
        }

        for bundle in bundles {
            self.insert(bundle);
        }
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
        consumers: &mut [shaq::spsc::Consumer<WorkerToPackMessage>],
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
        if log::log_enabled!(log::Level::Trace) {
            let us = self.timer.elapsed_us();
            self.block_timing.push((us, self.block_metrics));
            self.vote_timing.push((us, self.vote_metrics));
        }
        trace!("block_timing: {:?}", self.block_timing);
        trace!("vote_timing: {:?}", self.vote_timing);
        self.unresolved_idx = 0;
        self.try_idx = 0;
        self.resolved_idx = 0;
        self.vote_idx = usize::MAX;
        self.running.clear();
        self.priority.clear();
        self.check_pending.clear();
        self.check_inflight.clear();
        self.compute_units = [0; MAX_WORKERS];
        self.block_metrics = Metrics::default();
        self.vote_metrics = Metrics::default();
        self.block_timing.clear();
        self.vote_timing.clear();

        let allocator = self.allocator;
        Ok(self.tasks.drain(..).filter_map(move |(_, task)| {
            if task.state == TaskState::Done {
                None
            } else if task.is_vote {
                let vote = task.batch.slice(allocator)[0];
                task.batch.free(allocator);
                Some(vote)
            } else {
                task.batch.free_full(allocator);
                None
            }
        }))
    }

    /// Insert one batch as a [`Task`], deduplicated by its first signature.
    fn insert(
        &mut self,
        bundle: impl IntoIterator<Item = SharableTransactionRegion, IntoIter: ExactSizeIterator>,
    ) {
        let txs = bundle.into_iter();
        if txs.len() == 0 || txs.len() > MAX_TRANSACTIONS_PER_MESSAGE {
            txs.for_each(|tx| tx.free(self.allocator));
            return;
        }
        let batch = allocate_batch(txs, self.allocator);
        let entry = match self
            .tasks
            .entry(*signature(&batch.slice(self.allocator)[0], self.allocator))
        {
            // The old copy may be inflight, so free the new bundle.
            Entry::Occupied(_) => {
                batch.free_full(self.allocator);
                return;
            }
            Entry::Vacant(entry) => entry,
        };

        let is_vote = self.vote_idx != usize::MAX;
        let task_idx = entry.index();
        let mut accounts: SmallVec<[(Pubkey, bool); 32]> = SmallVec::new();
        let mut checks: SmallVec<[Check; 4]> = SmallVec::new();
        let mut cu: u32 = 0;

        for (i, &tx) in batch.slice(self.allocator).iter().enumerate() {
            let view =
                match UnsanitizedTransactionView::try_new_unsanitized(tx.slice(self.allocator)) {
                    Ok(view) => view,
                    Err(e) => {
                        // A malformed member invalidates the whole atomic bundle.
                        warn!("dropping bundle with unparsable transaction: {e:?}");
                        batch.free_full(self.allocator);
                        return;
                    }
                };
            if i == 0 {
                // Single-member fast path: a valid tx has unique account keys.
                accounts = static_accounts(&view);
            } else {
                union_extend(&mut accounts, static_accounts(&view).into_iter());
            }
            cu = cu.saturating_add(if is_vote {
                Self::VOTE_CUS
            } else {
                compute_unit_limit(&view)
            });
            if !is_vote && view.num_address_table_lookups() != 0 {
                checks.push(Check {
                    tx,
                    alt_writable: view.total_writable_lookup_accounts(),
                    task: task_idx,
                });
            }
        }

        let n = batch.num_transactions as usize;
        let alt_remaining = checks.len() as u8;
        let state = if alt_remaining > 0 {
            TaskState::Unresolved
        } else {
            TaskState::Resolved
        };
        let metrics = if is_vote {
            &mut self.vote_metrics
        } else {
            &mut self.block_metrics
        };
        metrics.total += n;
        if alt_remaining > 0 {
            metrics.alt += 1;
            metrics.unresolved += 1;
        } else {
            metrics.resolved += 1;
        }

        self.check_pending.extend(checks);
        entry.insert(Task {
            batch,
            accounts,
            cu,
            alt_remaining,
            is_vote,
            state,
        });
    }

    /// Dispatch pending ALT resolution CHECKs
    fn check(&mut self, slot: u64, producers: &mut [shaq::spsc::Producer<PackToWorkerMessage>]) {
        if self.check_pending.is_empty() {
            return;
        }

        // Dispatch CHECKs to workers round robin ordered by estimated CU load
        let mut order: [usize; MAX_WORKERS] = std::array::from_fn(|i| i);
        order[..self.num_workers].sort_unstable_by_key(|&w| self.compute_units[w]);
        let mut workers = order[..self.num_workers].iter().cycle();

        // CHECKs are fast to execute and have no dependencies - submit full batches
        'batch: while !self.check_pending.is_empty()
            && self.processing <= PACK_TO_WORKER_CAPACITY * self.num_workers
        {
            let n = self.check_pending.len().min(MAX_TRANSACTIONS_PER_MESSAGE);
            let mut message = PackToWorkerMessage {
                flags: pack_message_flags::CHECK | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
                max_working_slot: slot,
                batch: allocate_batch(
                    self.check_pending.iter().take(n).map(|c| c.tx),
                    self.allocator,
                ),
            };
            let offset = message.batch.transactions_offset;
            for &worker in workers.by_ref().take(self.num_workers) {
                if let Err(returned) = pack_to_worker::send(&mut producers[worker], message) {
                    message = returned;
                } else {
                    self.processing += 1;
                    self.block_metrics.checking += n;
                    let checks: Vec<Check> = self.check_pending.drain(..n).collect();
                    self.check_inflight.insert(offset, checks);
                    continue 'batch;
                }
            }
            // All workers are full
            message.batch.free(self.allocator);
            break;
        }
    }

    /// Dispatch unblocked transactions to EXECUTE
    fn execute(&mut self, slot: u64, producers: &mut [shaq::spsc::Producer<PackToWorkerMessage>]) {
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
                // Bundles execute atomically: any failure aborts the whole batch.
                let flags = if task.batch.num_transactions > 1 {
                    pack_message_flags::EXECUTE
                        | pack_message_flags::execution_flags::ALL_OR_NOTHING
                        | pack_message_flags::execution_flags::DROP_ON_FAILURE
                } else {
                    pack_message_flags::EXECUTE
                };
                let message = PackToWorkerMessage {
                    flags,
                    max_working_slot: slot,
                    batch: task.batch,
                };
                if pack_to_worker::send(&mut producers[worker], message).is_err() {
                    // Worker full; the batch stays with the task, retried next tick.
                    break;
                }
                task.state = TaskState::Executing;
                self.running.lock(task);
                self.compute_units[worker] += task.cu as u64;
                self.processing += 1;
                if task.is_vote {
                    self.vote_metrics.resolved -= 1;
                    self.vote_metrics.executing += 1;
                } else {
                    self.block_metrics.resolved -= 1;
                    self.block_metrics.executing += 1;
                }
            }
            self.try_idx += 1;
        }
    }

    /// Drain and process worker responses
    pub fn resolve(&mut self, consumers: &mut [shaq::spsc::Consumer<WorkerToPackMessage>]) {
        let mut execute_received = false;
        for (worker, consumer) in consumers.iter_mut().enumerate() {
            for message in worker_to_pack::iter(consumer) {
                self.processing -= 1;
                if message.processed_code == processed_codes::MAX_WORKING_SLOT_EXCEEDED {
                    // Slot ended. The tag is undefined now, so membership tells
                    // a transient CHECK array from a task-owned EXECUTE batch.
                    if self
                        .check_inflight
                        .remove(&message.batch.transactions_offset)
                        .is_some()
                    {
                        message.batch.free(self.allocator);
                    }
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
                    TaskState::Done | TaskState::Dropped | TaskState::Executing
                )
            {
                self.resolved_idx += 1;
            }
            self.try_idx = self.resolved_idx;
            self.priority.clear();
        }
    }

    fn resolve_execute(&mut self, worker: usize, message: &WorkerToPackMessage) {
        let txs = message.batch.slice(self.allocator);
        let results: &[ExecutionResponse] = message.responses.slice(self.allocator);

        // Bank torn down at the leader boundary: leave the batch for reset().
        if results
            .iter()
            .any(|r| r.not_included_reason == not_included_reasons::BANK_NOT_AVAILABLE)
        {
            message.responses.free(self.allocator);
            return;
        }

        // The batch is keyed by its first member's signature.
        let key = *signature(&txs[0], self.allocator);
        let task = self.tasks.get_mut(&key).expect("task should exist");
        task.state = TaskState::Done;
        self.running.unlock(task);
        self.compute_units[worker] -= task.cu as u64;
        let n = task.batch.num_transactions as usize;
        let metrics = if task.is_vote {
            &mut self.vote_metrics
        } else {
            &mut self.block_metrics
        };
        metrics.executing -= 1;
        // An all-or-nothing batch lands in full or not at all.
        if results
            .iter()
            .all(|r| r.not_included_reason == not_included_reasons::NONE)
        {
            metrics.success += n;
        } else {
            // Account locks should prevent ACCOUNT_IN_USE entirely.
            if results
                .iter()
                .any(|r| r.not_included_reason == not_included_reasons::ACCOUNT_IN_USE)
            {
                error!("unexpected ACCOUNT_IN_USE in scheduled batch");
            }
            metrics.fail += n;
        }
        task.batch.free_full(self.allocator);
        message.responses.free(self.allocator);
    }

    fn resolve_check(&mut self, message: &WorkerToPackMessage) {
        const MASK: u8 = resolve_flags::PERFORMED | resolve_flags::FAILED;

        let checks = self
            .check_inflight
            .remove(&message.batch.transactions_offset)
            .expect("check batch should be in flight");
        let results: &[CheckResponse] = message.responses.slice(self.allocator);
        for (check, result) in checks.iter().zip(results) {
            self.block_metrics.checking -= 1;
            let performed = result.resolve_flags & MASK == resolve_flags::PERFORMED;
            let (_, task) = self
                .tasks
                .get_index_mut(check.task)
                .expect("task should exist");
            task.alt_remaining -= 1;

            if performed {
                // Extend the union unless a sibling already dropped the bundle.
                if task.state != TaskState::Dropped {
                    let alt_writable = check.alt_writable as usize;
                    let resolved = result.resolved_pubkeys.slice(self.allocator);
                    let keys = resolved
                        .iter()
                        .enumerate()
                        .map(|(i, k)| (*k, i < alt_writable));
                    if task.batch.num_transactions == 1 {
                        // Singleton: a valid tx's static + ALT keys are unique.
                        task.accounts.extend(keys);
                    } else {
                        union_extend(&mut task.accounts, keys);
                    }
                }
                result.resolved_pubkeys.free(self.allocator);
            } else if task.state != TaskState::Dropped {
                warn!(
                    "unexpected CHECK failure: parsing_and_sanitization_flags={:#04x} \
                     resolve_flags={:#04x}",
                    result.parsing_and_sanitization_flags, result.resolve_flags,
                );
                task.state = TaskState::Dropped;
                self.block_metrics.unresolved -= 1;
                self.block_metrics.fail += task.batch.num_transactions as usize;
            }

            // Once every CHECK has returned, free a dropped bundle (nothing is
            // in flight now) or promote a healthy one to Resolved.
            if task.alt_remaining == 0 {
                if task.state == TaskState::Dropped {
                    task.batch.free_full(self.allocator);
                    task.state = TaskState::Done;
                } else {
                    task.state = TaskState::Resolved;
                    self.block_metrics.unresolved -= 1;
                    self.block_metrics.resolved += 1;
                }
            }
        }
        // Transactions belong to their tasks; free only the transient array.
        message.batch.free(self.allocator);
        message.responses.free(self.allocator);
    }
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

/// Merge `keys` into `acc`, OR-ing writability so each account appears once;
/// duplicates would desync `AccountLocks` lock/unlock.
fn union_extend(
    acc: &mut SmallVec<[(Pubkey, bool); 32]>,
    keys: impl Iterator<Item = (Pubkey, bool)>,
) {
    for (key, is_write) in keys {
        if let Some(entry) = acc.iter_mut().find(|(k, _)| *k == key) {
            entry.1 |= is_write;
        } else {
            acc.push((key, is_write));
        }
    }
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

    fn shaq_channel<T>(capacity: usize) -> (shaq::spsc::Producer<T>, shaq::spsc::Consumer<T>) {
        let size = shaq::spsc::minimum_file_size::<T>(capacity);
        let file = tempfile().unwrap();
        let producer = unsafe { shaq::spsc::Producer::create(&file, size) }.unwrap();
        let consumer = unsafe { shaq::spsc::Consumer::join(&file) }.unwrap();
        (producer, consumer)
    }

    fn random_transaction(rng: &mut impl Rng, accounts: &[Pubkey]) -> Transaction {
        let num_accounts = rng.random_range(1..=37);
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
        let (mut pack_to_worker, worker_rx): (
            Vec<shaq::spsc::Producer<_>>,
            Vec<shaq::spsc::Consumer<_>>,
        ) = (0..NUM_WORKERS)
            .map(|_| shaq_channel::<PackToWorkerMessage>(PACK_TO_WORKER_CAPACITY))
            .unzip();
        let (worker_tx, mut worker_to_pack): (
            Vec<shaq::spsc::Producer<_>>,
            Vec<shaq::spsc::Consumer<_>>,
        ) = (0..NUM_WORKERS)
            .map(|_| shaq_channel::<WorkerToPackMessage>(WORKER_TO_PACK_CAPACITY))
            .unzip();
        let mut workers = worker_rx.into_iter().zip(worker_tx).collect::<Vec<_>>();

        let txs: Vec<Transaction> = (0..NUM_TXS)
            .map(|_| random_transaction(&mut rng, &accounts))
            .collect();

        // Random bundles of 1..=5 to exercise the atomic-batch pathways.
        let mut i = 0;
        while i < txs.len() {
            let size = rng.random_range(1..=5).min(txs.len() - i);
            block_stage.insert(
                txs[i..i + size]
                    .iter()
                    .map(|tx| allocate(serialize(tx).unwrap(), &allocator)),
            );
            i += size;
        }

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
}
