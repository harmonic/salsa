//! Per-slot transaction schedule. Single-threaded — all mutation and SHM
//! frees happen on the scheduler thread

use crate::ipc::pack_to_worker;
use crate::ipc::shmem::{Free, Slice, allocate_batch};
use agave_scheduler_bindings::worker_message_types::{self, CheckResponse, resolve_flags};
use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, SharablePubkeys, SharableTransactionRegion,
    WorkerToPackMessage, pack_message_flags, processed_codes,
};
use agave_scheduling_utils::handshake::MAX_WORKERS;
use agave_transaction_view::transaction_view::UnsanitizedTransactionView;
use log::{debug, error, warn};
use rts_alloc::Allocator;
use rustc_hash::FxBuildHasher;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, VecDeque};

/// Per-slot scheduler state
pub struct Schedule<'a> {
    allocator: &'a Allocator,
    num_threads: usize,
    slot: u64,
    /// Task storage; id == vec index
    tasks: Vec<Task<'a>>,
    /// Next task to try_schedule; advances monotonically
    unlocked: usize,
    /// ALT tasks awaiting CHECK dispatch, FIFO
    unresolved: VecDeque<usize>,
    /// Per-worker FIFO of locked tasks awaiting EXECUTE dispatch
    ready: Vec<VecDeque<usize>>,
    /// Per-account lock state + blocked queue
    locks: HashMap<Pubkey, AccountLock, FxBuildHasher>,
    /// Per-worker FIFO of dispatched batches
    inflight: Vec<VecDeque<SmallVec<[usize; MAX_TRANSACTIONS_PER_MESSAGE]>>>,
    /// Per-worker bitmask: bit w set => last dispatch to worker w failed; cleared on response from w
    backpressured: u64,
    /// Bitmask of all valid worker bits (low `num_threads` bits set)
    workers_mask: u64,
}

impl<'a> Schedule<'a> {
    pub fn new(num_threads: usize, allocator: &'a Allocator) -> Self {
        assert!(
            num_threads > 0 && num_threads <= MAX_WORKERS,
            "num_threads {num_threads} must be in 1..={MAX_WORKERS}"
        );
        Self {
            allocator,
            num_threads,
            slot: 0,
            tasks: Vec::with_capacity(8192),
            unlocked: 0,
            unresolved: VecDeque::with_capacity(256),
            ready: (0..num_threads)
                .map(|_| VecDeque::with_capacity(128))
                .collect(),
            locks: HashMap::with_capacity_and_hasher(4096, FxBuildHasher),
            inflight: (0..num_threads)
                .map(|_| VecDeque::with_capacity(8))
                .collect(),
            backpressured: 0,
            workers_mask: u64::MAX >> u64::BITS.saturating_sub(num_threads as u32),
        }
    }

    /// True only when every worker has a pending failed dispatch — partial saturation keeps flowing
    pub fn at_capacity(&self) -> bool {
        self.backpressured == self.workers_mask
    }

    /// Insert a transaction into the schedule
    pub fn insert(&mut self, tx_ref: SharableTransactionRegion) {
        let view =
            match UnsanitizedTransactionView::try_new_unsanitized(tx_ref.slice(self.allocator)) {
                Ok(v) => v,
                Err(err) => {
                    warn!("dropping malformed tx: {err:?}");
                    tx_ref.free(self.allocator);
                    return;
                }
            };
        if view.num_address_table_lookups() > 0 {
            self.unresolved.push_back(self.tasks.len());
        }
        self.tasks.push(Task {
            tx_ref,
            view,
            alt_pubkeys: None,
        });
    }

    /// Advance the state machine, then dispatch EXECUTE first (forward progress)
    /// and fill remaining ring headroom with CHECK
    pub fn schedule(&mut self, producers: &mut [shaq::Producer<PackToWorkerMessage>]) {
        self.advance_unlocked();
        self.schedule_execute(producers);
        if !self.unresolved.is_empty() {
            self.schedule_check(producers);
        }
    }

    /// Process a worker response: update task state, release locks, free SHM
    pub fn resolve(&mut self, worker: usize, msg: &WorkerToPackMessage) {
        // Response from worker w means a pack_to_worker[w] slot has been drained
        self.backpressured &= !(1u64 << worker);
        let Some(task_ids) = self.inflight[worker].pop_front() else {
            // Stale response from a previous slot
            msg.free_full(self.allocator);
            return;
        };
        match msg.responses.tag {
            worker_message_types::CHECK_RESPONSE => self.handle_check_response(&task_ids, msg),
            worker_message_types::EXECUTION_RESPONSE => {
                self.handle_execute_response(&task_ids, worker, msg)
            }
            other => {
                // Unknown tag: shallow-free only (inner entries are untyped)
                error!("unexpected response tag {other}");
                for &id in &task_ids {
                    self.abort(id);
                }
                msg.responses.free(self.allocator);
            }
        }
        msg.batch.free(self.allocator);
    }

    /// Abandon the slot, re-target for `next_slot`, free all owned tx_refs.
    /// In-flight tx_refs get freed when their stale response arrives via `resolve`
    pub fn reset(&mut self, next_slot: u64) {
        let mut in_flight = vec![false; self.tasks.len()];
        let mut in_flight_count = 0usize;
        for q in &self.inflight {
            for batch in q {
                for &id in batch {
                    in_flight[id] = true;
                    in_flight_count = in_flight_count.saturating_add(1);
                }
            }
        }

        let mut alive_undispatched = 0usize;
        for (id, task) in self.tasks.iter_mut().enumerate() {
            if let Some(pk) = task.alt_pubkeys.take() {
                pk.free(self.allocator);
            }
            if task.tx_ref.length != 0 && !in_flight[id] {
                alive_undispatched = alive_undispatched.saturating_add(1);
                task.tx_ref.free(self.allocator);
            }
        }
        let total = self.tasks.len();
        let tombstoned = total
            .saturating_sub(alive_undispatched)
            .saturating_sub(in_flight_count);
        debug!(
            "slot reset: {total} tasks ({tombstoned} done, {in_flight_count} in-flight, \
             {alive_undispatched} undispatched)"
        );

        self.slot = next_slot;
        self.tasks.clear();
        self.unlocked = 0;
        self.unresolved.clear();
        for q in &mut self.ready {
            q.clear();
        }
        self.locks.clear();
        for q in &mut self.inflight {
            q.clear();
        }
        self.backpressured = 0;
    }

    /// Try to schedule up to one batch of newly-resolved tasks
    fn advance_unlocked(&mut self) {
        let end = self
            .unlocked
            .saturating_add(MAX_TRANSACTIONS_PER_MESSAGE)
            .min(self.tasks.len());
        while self.unlocked < end {
            let task = &self.tasks[self.unlocked];
            if task.tx_ref.length == 0 {
                // transaction failed CHECK, skip it
                self.unlocked = self.unlocked.saturating_add(1);
                continue;
            }
            if task.view.num_address_table_lookups() > 0 && task.alt_pubkeys.is_none() {
                // transaction has unresolved ALTs, nothing more to do
                break;
            }
            self.try_schedule(self.unlocked);
            self.unlocked = self.unlocked.saturating_add(1);
        }
    }

    /// Dispatch up to one CHECK batch per worker, preferring workers with the most ring headroom
    fn schedule_check(&mut self, producers: &mut [shaq::Producer<PackToWorkerMessage>]) {
        for p in producers.iter_mut().take(self.num_threads) {
            p.sync();
        }
        let mut order: SmallVec<[usize; MAX_WORKERS]> = (0..self.num_threads).collect();
        order.sort_by_key(|&w| {
            std::cmp::Reverse(producers[w].capacity().saturating_sub(producers[w].len()))
        });

        for worker in order {
            if self.unresolved.is_empty() {
                break;
            }
            if producers[worker].capacity() == producers[worker].len() {
                break;
            }
            let batch = allocate_batch(
                self.unresolved
                    .iter()
                    .take(MAX_TRANSACTIONS_PER_MESSAGE)
                    .map(|&id| self.tasks[id].tx_ref),
                self.allocator,
            )
            .expect("unresolved non-empty and bounded by MAX_TRANSACTIONS_PER_MESSAGE");
            let n = batch.num_transactions as usize;
            let msg = PackToWorkerMessage {
                flags: pack_message_flags::CHECK
                    | pack_message_flags::check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
                max_working_slot: self.slot,
                batch,
            };
            match pack_to_worker::send(&mut producers[worker], msg) {
                Ok(()) => self.inflight[worker].push_back(self.unresolved.drain(..n).collect()),
                Err(returned) => {
                    returned.batch.free(self.allocator);
                    self.backpressured |= 1u64 << worker;
                }
            }
        }
    }

    /// Send one EXECUTE batch per worker with a non-empty ready queue
    fn schedule_execute(&mut self, producers: &mut [shaq::Producer<PackToWorkerMessage>]) {
        for (worker, producer) in producers.iter_mut().enumerate().take(self.num_threads) {
            if self.backpressured & (1u64 << worker) != 0 {
                continue;
            }
            let Some(batch) = allocate_batch(
                self.ready[worker]
                    .iter()
                    .take(MAX_TRANSACTIONS_PER_MESSAGE)
                    .map(|&id| self.tasks[id].tx_ref),
                self.allocator,
            ) else {
                continue;
            };
            let n = batch.num_transactions as usize;
            let msg = PackToWorkerMessage {
                flags: pack_message_flags::EXECUTE,
                max_working_slot: self.slot,
                batch,
            };
            match pack_to_worker::send(producer, msg) {
                Ok(()) => self.inflight[worker].push_back(self.ready[worker].drain(..n).collect()),
                Err(returned) => {
                    returned.batch.free(self.allocator);
                    self.backpressured |= 1u64 << worker;
                }
            }
        }
    }

    fn handle_check_response(&mut self, task_ids: &[usize], msg: &WorkerToPackMessage) {
        if msg.processed_code != processed_codes::PROCESSED {
            // Entries undefined on non-PROCESSED; shallow-free only
            msg.responses.free(self.allocator);
            for &id in task_ids {
                self.abort(id);
            }
            return;
        }
        let entries: &[CheckResponse] = msg.responses.slice(self.allocator);
        for (&id, resp) in task_ids.iter().zip(entries.iter()) {
            // resolved_pubkeys is only defined when PERFORMED is set
            let performed = resp.resolve_flags & resolve_flags::PERFORMED != 0;
            let failed = resp.resolve_flags & resolve_flags::FAILED != 0;
            if performed && !failed {
                self.tasks[id].alt_pubkeys = Some(resp.resolved_pubkeys);
            } else if performed {
                resp.resolved_pubkeys.free(self.allocator);
                self.abort(id);
            } else {
                self.abort(id);
            }
        }
        // resolved_pubkeys now owned by Tasks (success) or freed (failure)
        msg.responses.free(self.allocator);
    }

    fn handle_execute_response(
        &mut self,
        task_ids: &[usize],
        worker: usize,
        msg: &WorkerToPackMessage,
    ) {
        for &id in task_ids {
            self.complete(id, worker);
        }
        msg.responses.free(self.allocator);
    }

    /// Acquire locks: success → ready queue, failure → blocker's blocked list
    fn try_schedule(&mut self, id: usize) {
        let accounts = self.collect_accounts(id);
        let mut allowed = self.workers_mask;
        let mut first_blocker: Option<Pubkey> = None;
        for &(key, is_write) in &accounts {
            let s = match self.locks.get(&key) {
                None => self.workers_mask,
                Some(lock) if !is_write => match lock.writer {
                    None => self.workers_mask,
                    Some((t, _)) => 1u64 << t,
                },
                Some(lock) => {
                    let mut held = 0u64;
                    if let Some((t, _)) = lock.writer {
                        held |= 1u64 << t;
                    }
                    for (t, &count) in lock.readers.iter().enumerate() {
                        if count > 0 {
                            held |= 1u64 << t;
                        }
                    }
                    if held == 0 {
                        self.workers_mask
                    } else if held.count_ones() == 1 {
                        held
                    } else {
                        0
                    }
                }
            };
            let narrowed = allowed & s;
            if narrowed == 0 && first_blocker.is_none() {
                first_blocker = Some(key);
            }
            allowed = narrowed;
        }
        if allowed == 0 {
            let blocker = first_blocker.expect("empty allowed implies a blocker");
            self.locks.entry(blocker).or_default().blocked.push(id);
            return;
        }
        let worker = (0..self.num_threads)
            .filter(|&w| allowed & (1u64 << w) != 0)
            .min_by_key(|&w| self.ready[w].len())
            .expect("non-empty allowed implies at least one worker");
        for &(key, is_write) in &accounts {
            let entry = self.locks.entry(key).or_default();
            if is_write {
                entry.acquire_write(worker);
            } else {
                entry.acquire_read(worker);
            }
        }
        self.ready[worker].push_back(id);
    }

    /// Release locks, wake blocked dependents, free SHM, tombstone the task
    fn complete(&mut self, id: usize, worker: usize) {
        let accounts = self.collect_accounts(id);
        let mut to_retry: SmallVec<[usize; 16]> = SmallVec::new();
        for &(key, is_write) in &accounts {
            if let Some(lock) = self.locks.get_mut(&key) {
                if is_write {
                    lock.release_write(worker);
                } else {
                    lock.release_read(worker);
                }
                to_retry.extend(lock.blocked.drain(..));
            }
        }
        let task = &mut self.tasks[id];
        if let Some(pk) = task.alt_pubkeys.take() {
            pk.free(self.allocator);
        }
        task.tx_ref.free(self.allocator);
        task.tx_ref.length = 0;
        for retry_id in to_retry {
            self.try_schedule(retry_id);
        }
    }

    /// Mark a task failed; free SHM and tombstone via `tx_ref.length = 0`
    fn abort(&mut self, id: usize) {
        let task = &mut self.tasks[id];
        if let Some(pk) = task.alt_pubkeys.take() {
            pk.free(self.allocator);
        }
        task.tx_ref.free(self.allocator);
        task.tx_ref.length = 0;
    }

    /// Collect (key, is_write) pairs: static keys then ALT-resolved
    fn collect_accounts(&self, id: usize) -> SmallVec<[(Pubkey, bool); 16]> {
        let task = &self.tasks[id];
        let view = &task.view;
        let num_signed = view.num_required_signatures() as usize;
        let num_ros = view.num_readonly_signed_static_accounts() as usize;
        let num_rou = view.num_readonly_unsigned_static_accounts() as usize;
        let static_keys = view.static_account_keys();
        let num_ws = num_signed.saturating_sub(num_ros);
        let num_unsigned = static_keys.len().saturating_sub(num_signed);
        let num_wu = num_unsigned.saturating_sub(num_rou);

        let mut out: SmallVec<[(Pubkey, bool); 16]> = SmallVec::new();
        for (i, &key) in static_keys.iter().enumerate() {
            let is_write = if i < num_signed {
                i < num_ws
            } else {
                i.saturating_sub(num_signed) < num_wu
            };
            out.push((key, is_write));
        }
        if let Some(pk) = &task.alt_pubkeys {
            let alt_writable_count = view.total_writable_lookup_accounts() as usize;
            for (i, &key) in pk.slice(self.allocator).iter().enumerate() {
                out.push((key, i < alt_writable_count));
            }
        }
        out
    }
}

struct Task<'a> {
    tx_ref: SharableTransactionRegion,
    view: UnsanitizedTransactionView<&'a [u8]>,
    alt_pubkeys: Option<SharablePubkeys>,
}

/// Per-account lock: single writer-worker (reentrant) or per-worker readers
struct AccountLock {
    /// (worker, refcount) — reentrant for the same worker
    writer: Option<(usize, u16)>,
    readers: [u16; MAX_WORKERS],
    /// Tasks whose current blocker is this account
    blocked: SmallVec<[usize; 4]>,
}

impl Default for AccountLock {
    fn default() -> Self {
        Self {
            writer: None,
            readers: [0; MAX_WORKERS],
            blocked: SmallVec::new(),
        }
    }
}

impl AccountLock {
    /// Acquire a write lock; panics if another worker already holds it
    fn acquire_write(&mut self, worker: usize) {
        match &mut self.writer {
            Some((t, count)) if *t == worker => {
                *count = count.saturating_add(1);
            }
            None => self.writer = Some((worker, 1)),
            _ => panic!("acquire_write: writer on different worker"),
        }
    }

    fn acquire_read(&mut self, worker: usize) {
        self.readers[worker] = self.readers[worker].saturating_add(1);
    }

    fn release_write(&mut self, worker: usize) {
        match &mut self.writer {
            Some((t, count)) if *t == worker => {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.writer = None;
                }
            }
            _ => error!("no matching writer to release"),
        }
    }

    fn release_read(&mut self, worker: usize) {
        self.readers[worker] = self.readers[worker].saturating_sub(1);
    }
}
