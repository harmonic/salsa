//! Per-slot transaction schedule

use crate::ipc::shmem::{Free, Slice, allocate_batch, signature};
use crate::ipc::{pack_to_worker, worker_to_pack};
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    self, CheckResponse, ExecutionResponse, resolve_flags,
};
use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, SharablePubkeys, SharableTransactionRegion,
    WorkerToPackMessage, pack_message_flags, processed_codes,
};
use agave_transaction_view::transaction_version::TransactionVersion;
use agave_transaction_view::transaction_view::UnsanitizedTransactionView;
use indexmap::IndexMap;
use indexmap::map::Entry;
use rts_alloc::Allocator;
use rustc_hash::FxBuildHasher;
use smallvec::SmallVec;
use solana_pubkey::Pubkey;
use solana_transaction::simple_vote_transaction_checker::is_simple_vote_transaction_impl;
use std::collections::VecDeque;
use std::mem;

const NONE: usize = usize::MAX;

/// Per-slot scheduler state
pub struct Schedule<'a> {
    allocator: &'a Allocator,
    /// Per-account dependency state
    accounts: IndexMap<Pubkey, AccountState, FxBuildHasher>,
    /// Transaction tasks to be executed
    tasks: IndexMap<[u8; 64], Task<'a>, FxBuildHasher>,
    /// Next task to resolve
    cursor: usize,
    /// Tasks awaiting CHECK dispatch for ALT resolution
    unresolved: VecDeque<usize>,
    /// Globally ready tasks
    ready: VecDeque<usize>,
    /// Per-worker chain tail: the last task assigned to this worker's FIFO
    tails: Vec<usize>,
    /// Count of in flight batches on workers
    inflight: usize,
}

/// Per-account state for building the dependency DAG
struct AccountState {
    /// Latest task to write this account
    last_writer: usize,
    /// Tasks that have read this account since `last_writer`
    readers_since: Vec<usize>,
}

impl Default for AccountState {
    fn default() -> Self {
        Self {
            last_writer: NONE,
            readers_since: Vec::new(),
        }
    }
}

/// A node in the dependency DAG containing a transaction to execute
struct Task<'a> {
    /// SHM region holding the serialized transaction
    tx_ref: SharableTransactionRegion,
    /// Parsed view over `tx_ref`'s bytes
    view: UnsanitizedTransactionView<&'a [u8]>,
    /// Whether this transaction references address lookup tables
    has_alts: bool,
    /// Resolved ALT pubkeys from the CHECK response
    alt_pubkeys: Option<SharablePubkeys>,
    /// Out-edges: tasks that depend on this one
    successors: Vec<usize>,
    /// Number of incomplete predecessors
    pending: usize,
    /// Lifecycle stage
    state: TaskState,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TaskState {
    /// Awaiting ALT CHECK response or cursor reach
    Unresolved,
    /// Resolved by the cursor; predecessor edges are built
    Resolved,
    /// Sent to a worker for EXECUTE
    Assigned,
    /// Terminal. Either EXECUTED or failed CHECK
    Done,
}

impl<'a> Schedule<'a> {
    pub fn new(num_workers: usize, allocator: &'a Allocator) -> Self {
        Self {
            allocator,
            accounts: IndexMap::with_capacity_and_hasher(4096, FxBuildHasher),
            tasks: IndexMap::with_capacity_and_hasher(8192, FxBuildHasher),
            cursor: 0,
            unresolved: VecDeque::with_capacity(256),
            ready: VecDeque::with_capacity(1024),
            tails: vec![NONE; num_workers],
            inflight: 0,
        }
    }

    /// Insert transactions
    pub fn insert(&mut self, txs: impl IntoIterator<Item = SharableTransactionRegion>) {
        for tx in txs {
            match self.tasks.entry(*signature(&tx, self.allocator)) {
                Entry::Occupied(_) => {
                    tx.free(self.allocator);
                }
                Entry::Vacant(v) => {
                    let view =
                        UnsanitizedTransactionView::try_new_unsanitized(tx.slice(self.allocator))
                            .expect("transaction should be valid");
                    let has_alts = view.num_address_table_lookups() > 0;
                    let idx = v.index();
                    v.insert(Task {
                        tx_ref: tx,
                        view,
                        has_alts,
                        alt_pubkeys: None,
                        successors: Vec::new(),
                        pending: 0,
                        state: TaskState::Unresolved,
                    });
                    if has_alts {
                        self.unresolved.push_back(idx);
                    }
                }
            }
        }
    }

    /// Build batches and send to workers
    pub fn send_batch(&mut self, slot: u64, producers: &mut [shaq::Producer<PackToWorkerMessage>]) {
        self.advance_cursor();
        for (w, producer) in producers.iter_mut().enumerate() {
            let (batch, ready_consumed) = self.build_batch(w);
            if !batch.is_empty() {
                self.send_execute(slot, w, producer, &batch, ready_consumed);
            } else if !self.unresolved.is_empty() {
                self.send_check(slot, producer);
            }
        }
    }

    /// Drain and process worker responses
    pub fn handle_response(&mut self, consumers: &mut [shaq::Consumer<WorkerToPackMessage>]) {
        if self.inflight == 0 {
            return;
        }
        for consumer in consumers.iter_mut() {
            for message in worker_to_pack::iter(consumer) {
                self.inflight = self
                    .inflight
                    .checked_sub(1)
                    .expect("inflight should be nonzero");

                if message.processed_code == processed_codes::MAX_WORKING_SLOT_EXCEEDED {
                    message.free(self.allocator);
                    continue;
                }
                assert_eq!(
                    message.processed_code,
                    processed_codes::PROCESSED,
                    "unexpected processed_code {}",
                    message.processed_code
                );

                let batch = message.batch.slice(self.allocator);
                match message.responses.tag {
                    worker_message_types::EXECUTION_RESPONSE => {
                        let responses: &[ExecutionResponse] =
                            message.responses.slice(self.allocator);
                        assert_eq!(
                            responses.len(),
                            batch.len(),
                            "worker returned mismatched response count"
                        );
                        for tx in batch {
                            let id = self
                                .tasks
                                .get_index_of(signature(tx, self.allocator))
                                .expect("transaction should exist in schedule");
                            self.handle_execution_response(id);
                        }
                    }
                    worker_message_types::CHECK_RESPONSE => {
                        let responses: &[CheckResponse] = message.responses.slice(self.allocator);
                        assert_eq!(
                            responses.len(),
                            batch.len(),
                            "worker returned mismatched response count"
                        );
                        for (tx, response) in batch.iter().zip(responses) {
                            let id = self
                                .tasks
                                .get_index_of(signature(tx, self.allocator))
                                .expect("transaction should exist in schedule");
                            self.handle_check_response(id, response);
                        }
                    }
                    other => unreachable!("unexpected response tag: {other}"),
                }
                message.free(self.allocator);
            }
        }
    }

    /// Clear the schedule, returning unexecuted votes as a lazy iterator
    pub fn reset(&mut self) -> impl Iterator<Item = SharableTransactionRegion> + use<'_, 'a> {
        assert_eq!(self.inflight, 0, "inflight transactions not drained");
        self.accounts.clear();
        self.cursor = 0;
        self.unresolved.clear();
        self.ready.clear();
        for t in &mut self.tails {
            *t = NONE;
        }
        let allocator = self.allocator;
        self.tasks.drain(..).filter_map(move |(_, task)| {
            if task.state == TaskState::Done {
                return None;
            }
            if let Some(pk) = task.alt_pubkeys {
                pk.free(allocator);
            }
            let static_keys = task.view.static_account_keys();
            let programs = task
                .view
                .instructions_iter()
                .map(|ix| &static_keys[ix.program_id_index as usize]);
            if is_simple_vote_transaction_impl(
                task.view.signatures(),
                matches!(task.view.version(), TransactionVersion::Legacy),
                programs,
            ) {
                Some(task.tx_ref)
            } else {
                task.tx_ref.free(allocator);
                None
            }
        })
    }

    /// True iff any batch is in flight on a worker
    pub fn active(&self) -> bool {
        self.inflight != 0
    }

    /// Walk cursor forward, resolving each task in insertion order
    fn advance_cursor(&mut self) {
        while self.cursor < self.tasks.len() {
            let task = &self.tasks[self.cursor];
            match task.state {
                TaskState::Unresolved => {
                    if task.has_alts && task.alt_pubkeys.is_none() {
                        return;
                    }
                    self.resolve_node(self.cursor);
                }
                // CHECK-failed, skip this task
                TaskState::Done => {}
                TaskState::Resolved | TaskState::Assigned => {
                    unreachable!("cursor past resolved task: state={:?}", task.state)
                }
            }
            self.cursor += 1;
        }
    }

    /// Resolve a nodes edges from the transactions accounts
    fn resolve_node(&mut self, idx: usize) {
        let predecessors = self.predecessors(idx);
        let task = &mut self.tasks[idx];
        task.state = TaskState::Resolved;
        task.pending = predecessors.len();
        if task.pending == 0 {
            self.ready.push_back(idx);
        }
        for p in predecessors {
            self.tasks[p].successors.push(idx);
        }
    }

    /// Walk a tx's accounts and return the unique predecessor task indices
    fn predecessors(&mut self, idx: usize) -> SmallVec<[usize; 32]> {
        let alt_pubkeys = self.tasks[idx].alt_pubkeys.take();
        let allocator = self.allocator;
        let view = &self.tasks[idx].view;
        let num_signed = view.num_required_signatures() as usize;
        let num_ros = view.num_readonly_signed_static_accounts() as usize;
        let num_rou = view.num_readonly_unsigned_static_accounts() as usize;
        let static_keys = view.static_account_keys();
        let num_ws = num_signed.saturating_sub(num_ros);
        let num_unsigned = static_keys.len().saturating_sub(num_signed);
        let num_wu = num_unsigned.saturating_sub(num_rou);
        let alt_writable_count = view.total_writable_lookup_accounts() as usize;

        let alt_keys: &[Pubkey] = alt_pubkeys
            .as_ref()
            .map(|pk| pk.slice(allocator))
            .unwrap_or(&[]);
        let segments = [
            (&static_keys[..num_signed], num_ws),
            (&static_keys[num_signed..], num_wu),
            (alt_keys, alt_writable_count),
        ];

        let mut preds: SmallVec<[usize; 32]> = SmallVec::new();
        for (keys, num_writable) in segments {
            for (i, key) in keys.iter().enumerate() {
                let is_write = i < num_writable;
                let acct = self.accounts.entry(*key).or_default();
                let last_writer = acct.last_writer;
                let readers = if is_write {
                    acct.last_writer = idx;
                    mem::take(&mut acct.readers_since)
                } else {
                    acct.readers_since.push(idx);
                    Vec::new()
                };
                for pred in readers
                    .into_iter()
                    .chain((last_writer != NONE).then_some(last_writer))
                {
                    assert!(pred < idx, "edge must point from older task to newer task");
                    if self.tasks[pred].state == TaskState::Done {
                        continue;
                    }
                    if !preds.contains(&pred) {
                        preds.push(pred);
                    }
                }
            }
        }

        if let Some(pk) = alt_pubkeys {
            pk.free(allocator);
        }
        preds
    }

    /// Mark a task terminal, free its SHM, fire successors
    fn handle_execution_response(&mut self, idx: usize) {
        let task = &mut self.tasks[idx];
        assert_eq!(task.state, TaskState::Assigned);
        assert!(task.alt_pubkeys.is_none(), "alt_pubkeys freed at resolve");
        task.state = TaskState::Done;
        task.tx_ref.free(self.allocator);
        for s in std::mem::take(&mut task.successors) {
            let task = &mut self.tasks[s];
            task.pending = task
                .pending
                .checked_sub(1)
                .expect("pending should be nonzero");
            if task.state == TaskState::Resolved && task.pending == 0 {
                self.ready.push_back(s);
            }
        }
    }

    /// Apply a CHECK response: take pubkeys on success, abort on failure
    fn handle_check_response(&mut self, idx: usize, response: &CheckResponse) {
        const MASK: u8 = resolve_flags::PERFORMED | resolve_flags::FAILED;
        let task = &mut self.tasks[idx];
        assert_eq!(task.state, TaskState::Unresolved);
        if response.resolve_flags & MASK == resolve_flags::PERFORMED {
            task.alt_pubkeys = Some(response.resolved_pubkeys);
        } else {
            task.state = TaskState::Done;
            task.tx_ref.free(self.allocator);
        }
    }

    /// Append chain-eligible successors of `parent` to `batch`
    fn extend_chain(
        &self,
        parent: usize,
        batch: &mut SmallVec<[usize; MAX_TRANSACTIONS_PER_MESSAGE]>,
    ) {
        for &s in &self.tasks[parent].successors {
            if batch.len() >= MAX_TRANSACTIONS_PER_MESSAGE {
                return;
            }
            let task = &self.tasks[s];
            if task.state == TaskState::Resolved && task.pending == 1 && !batch.contains(&s) {
                batch.push(s);
            }
        }
    }

    /// Build a batch for a worker
    /// Returns (batch, number of entries consumed from `ready`)
    /// Greedily packs chain-eligible descendants of the worker's tail
    fn build_batch(
        &self,
        worker: usize,
    ) -> (SmallVec<[usize; MAX_TRANSACTIONS_PER_MESSAGE]>, usize) {
        let mut batch: SmallVec<[usize; MAX_TRANSACTIONS_PER_MESSAGE]> = SmallVec::new();
        let mut ready_idx = 0usize;
        let mut explore_idx = 0usize;

        if self.tails[worker] != NONE {
            self.extend_chain(self.tails[worker], &mut batch);
        }

        while batch.len() < MAX_TRANSACTIONS_PER_MESSAGE {
            if explore_idx < batch.len() {
                let from = batch[explore_idx];
                explore_idx += 1;
                self.extend_chain(from, &mut batch);
            } else if let Some(&r) = self.ready.get(ready_idx) {
                ready_idx += 1;
                batch.push(r);
            } else {
                break;
            }
        }

        (batch, ready_idx)
    }

    /// Send an EXECUTE batch and commit state on success. No-op if the
    /// worker's channel is full
    fn send_execute(
        &mut self,
        slot: u64,
        worker: usize,
        producer: &mut shaq::Producer<PackToWorkerMessage>,
        batch: &[usize],
        ready_consumed: usize,
    ) {
        let allocator = self.allocator;
        let txs = batch.iter().map(|&id| self.tasks[id].tx_ref);
        let message = PackToWorkerMessage {
            flags: pack_message_flags::EXECUTE,
            max_working_slot: slot,
            batch: allocate_batch(txs, allocator),
        };
        if let Err(message) = pack_to_worker::send(producer, message) {
            message.batch.free(allocator);
            return;
        }
        for &id in batch {
            self.tasks[id].state = TaskState::Assigned;
        }
        self.tails[worker] = *batch.last().expect("non-empty batch");
        self.ready.drain(..ready_consumed);
        self.inflight += 1;
    }

    /// Dispatch a CHECK batch for ALT resolution. No-op if the worker's
    /// channel is full
    fn send_check(&mut self, slot: u64, producer: &mut shaq::Producer<PackToWorkerMessage>) {
        let n = self.unresolved.len().min(MAX_TRANSACTIONS_PER_MESSAGE);
        if n == 0 {
            return;
        }
        let allocator = self.allocator;
        let txs = self
            .unresolved
            .iter()
            .take(n)
            .map(|&id| self.tasks[id].tx_ref);
        let message = PackToWorkerMessage {
            flags: pack_message_flags::CHECK | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
            max_working_slot: slot,
            batch: allocate_batch(txs, allocator),
        };
        if let Err(message) = pack_to_worker::send(producer, message) {
            message.batch.free(allocator);
            return;
        }
        self.unresolved.drain(..n);
        self.inflight += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::{PACK_TO_WORKER_CAPACITY, SLAB_SIZE, WORKER_TO_PACK_CAPACITY};
    use crate::ipc::shmem::allocate;
    use agave_scheduler_bindings::TransactionResponseRegion;
    use agave_scheduler_bindings::worker_message_types::{
        EXECUTION_RESPONSE, not_included_reasons,
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

        let mut rng = ChaChaRng::seed_from_u64(0x0123456789ABCDEF);
        let allocator =
            unsafe { Allocator::create(&tempfile().unwrap(), TEST_ALLOC_SIZE, 1, SLAB_SIZE) }
                .unwrap();
        let mut schedule = Schedule::new(NUM_WORKERS, &allocator);
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
            .map(|_| random_transaction(&mut rng, &accounts))
            .collect();

        schedule.insert(
            txs.iter()
                .map(|tx| allocate(serialize(tx).unwrap(), &allocator)),
        );

        let mut processed = Vec::new();
        schedule.send_batch(1, &mut pack_to_worker);
        while schedule.inflight != 0 {
            schedule.handle_response(&mut worker_to_pack);
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
            schedule.send_batch(1, &mut pack_to_worker);
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
