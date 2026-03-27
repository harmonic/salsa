//! Transaction scheduling algorithm for the Harmonic external scheduler.
//!
//! Dependency-aware scheduling that preserves topological ordering,
//! resolves address lookup tables via validator CHECK requests, and
//! dispatches independent transactions in parallel across workers.
//!
//! ```text
//! submit() → [Unresolved] → pump_resolve_stage() → resolve_luts() → [Resolved]
//!          → [Resolved]   → admit_resolved_prefix() → register accounts
//!                          → mark_ready() if unblocked → [Staged]
//!          → flush_ready_work() → [Processing]
//!          → drain_worker_responses() → complete_task() → release locks → wake
//! ```

mod account_queue;
mod task;
mod worker;

pub use task::{Task, TaskId, TaskState};
pub use worker::WorkerState;

use {
    account_queue::AccountStates,
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, SharableTransactionBatchRegion,
        SharableTransactionRegion, TransactionResponseRegion, pack_message_flags, processed_codes,
        worker_message_types,
    },
    agave_scheduling_utils::handshake::client::ClientWorkerSession,
    log::warn,
    rts_alloc::Allocator,
    smallvec::SmallVec,
    solana_pubkey::Pubkey,
    worker::SmallBatch,
};

/// Core scheduling engine.
pub struct SchedulingEngine {
    /// Tasks in submission order. TaskId = index.
    pub tasks: Vec<Task>,
    /// Next task to try admitting through the prefix gate.
    pub admit_offset: usize,
    /// Next Unresolved task to send a CHECK request for.
    pub resolve_offset: usize,
    /// Per-account lock tracking.
    pub accounts: AccountStates,
    /// Per-worker staging and inflight tracking.
    pub workers: Vec<WorkerState>,
    /// Number of workers.
    pub num_workers: usize,
}

impl SchedulingEngine {
    pub fn new(num_workers: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(4096),
            admit_offset: 0,
            resolve_offset: 0,
            accounts: AccountStates::new(),
            workers: (0..num_workers).map(|_| WorkerState::new()).collect(),
            num_workers,
        }
    }

    // --- Submission ---

    /// Submit a transaction for scheduling. Returns its TaskId (Vec index).
    pub fn submit(
        &mut self,
        tx_ref: SharableTransactionRegion,
        raw_bytes: &[u8],
    ) -> Result<TaskId, agave_transaction_view::result::TransactionViewError> {
        let task = Task::new(tx_ref, raw_bytes)?;
        let task_id = self.tasks.len() as TaskId;
        self.tasks.push(task);
        Ok(task_id)
    }

    // --- Resolution ---

    /// Send CHECK requests for Unresolved tasks that haven't been sent yet.
    pub fn pump_resolve_stage(
        &mut self,
        worker_sessions: &mut [ClientWorkerSession],
        allocator: &Allocator,
        current_slot: u64,
    ) {
        while self.resolve_offset < self.tasks.len() {
            let task = &self.tasks[self.resolve_offset];

            if task.state != TaskState::Unresolved {
                self.resolve_offset = self.resolve_offset.saturating_add(1);
                continue;
            }

            let w = self.choose_worker();
            let region = task.tx_ref;

            if send_check_request(&[region], &mut worker_sessions[w], allocator, current_slot) {
                // Task stays Unresolved until CHECK response arrives.
                // Advancing resolve_offset ensures we don't re-send.
                self.resolve_offset = self.resolve_offset.saturating_add(1);
            } else {
                break; // Worker queue full.
            }
        }
    }

    /// Apply ALT resolution results for a task.
    pub fn apply_resolution(&mut self, task_id: TaskId, resolved_pubkeys: &[Pubkey]) {
        if let Some(task) = self.tasks.get_mut(task_id as usize) {
            task.resolve_luts(resolved_pubkeys);
        }
    }

    // --- Admission ---

    /// Admit resolved tasks through the prefix gate in submission order.
    /// Stops at the first Unresolved task.
    pub fn admit_resolved_prefix(&mut self) {
        while self.admit_offset < self.tasks.len() {
            let task = &self.tasks[self.admit_offset];

            if task.state == TaskState::Unresolved {
                break;
            }

            if task.state != TaskState::Resolved {
                self.admit_offset = self.admit_offset.saturating_add(1);
                continue;
            }

            let task_id = self.admit_offset as TaskId;
            let reads: SmallVec<[Pubkey; 16]> = task.reads.clone();
            let writes: SmallVec<[Pubkey; 8]> = task.writes.clone();

            let mut blocked_count: u16 = 0;
            for account in &reads {
                if self.accounts.register_read(account, task_id) {
                    blocked_count = blocked_count.saturating_add(1);
                }
            }
            for account in &writes {
                if self.accounts.register_write(account, task_id) {
                    blocked_count = blocked_count.saturating_add(1);
                }
            }

            self.tasks[self.admit_offset].pending_accounts = blocked_count;

            if blocked_count == 0 {
                self.mark_ready(task_id);
            }

            self.admit_offset = self.admit_offset.saturating_add(1);
        }
    }

    fn mark_ready(&mut self, task_id: TaskId) {
        self.tasks[task_id as usize].state = TaskState::Staged;
        let w = self.choose_worker();
        self.workers[w].stage(task_id);
    }

    // --- Dispatch ---

    /// Send staged batches to workers via IPC.
    pub fn flush_ready_work(
        &mut self,
        worker_sessions: &mut [ClientWorkerSession],
        allocator: &Allocator,
        current_slot: u64,
    ) {
        for (w, session) in worker_sessions
            .iter_mut()
            .enumerate()
            .take(self.num_workers)
        {
            if !self.workers[w].should_flush() {
                continue;
            }

            let batch = self.workers[w].take_batch();
            if batch.is_empty() {
                continue;
            }

            let regions: SmallVec<[SharableTransactionRegion; 64]> = batch
                .iter()
                .filter_map(|&id| self.tasks.get(id as usize).map(|t| t.tx_ref))
                .collect();

            if send_execute_batch(&regions, session, allocator, current_slot) {
                for &id in &batch {
                    if let Some(task) = self.tasks.get_mut(id as usize) {
                        task.state = TaskState::Processing;
                    }
                }
                self.workers[w].record_inflight(&batch);
            } else {
                warn!("worker {w} queue full, re-staging batch");
                for id in batch.into_iter().rev() {
                    self.workers[w].staged.push_front(id);
                }
            }
        }
    }

    // --- Completion ---

    /// Drain worker responses, release locks, wake dependents.
    pub fn drain_worker_responses(
        &mut self,
        worker_sessions: &mut [ClientWorkerSession],
        allocator: &Allocator,
    ) {
        for (w, session) in worker_sessions
            .iter_mut()
            .enumerate()
            .take(self.num_workers)
        {
            if session.worker_to_pack.is_empty() {
                session.worker_to_pack.sync();
            }

            while let Some(msg) = session.worker_to_pack.try_read() {
                let batch = self.workers[w].pop_inflight_batch();
                let is_check = msg.responses.tag == worker_message_types::CHECK_RESPONSE;

                if msg.processed_code == processed_codes::PROCESSED && is_check {
                    self.handle_check_responses(&msg.responses, &batch, allocator);
                } else if msg.processed_code == processed_codes::PROCESSED {
                    for &task_id in &batch {
                        self.complete_task(task_id, allocator);
                    }
                } else {
                    for &task_id in &batch {
                        if let Some(t) = self.tasks.get_mut(task_id as usize) {
                            t.state = TaskState::Done;
                        }
                    }
                }

                // Free batch array.
                if msg.batch.num_transactions > 0 {
                    unsafe {
                        let ptr = allocator.ptr_from_offset(msg.batch.transactions_offset);
                        allocator.free(ptr);
                    }
                }

                // Free response array.
                if msg.responses.num_transaction_responses > 0 {
                    unsafe {
                        let ptr =
                            allocator.ptr_from_offset(msg.responses.transaction_responses_offset);
                        allocator.free(ptr);
                    }
                }

                session.worker_to_pack.finalize();
            }
        }
    }

    fn complete_task(&mut self, task_id: TaskId, allocator: &Allocator) {
        let Some(task) = self.tasks.get_mut(task_id as usize) else {
            return;
        };
        task.state = TaskState::Done;

        // Free transaction shared memory.
        unsafe {
            let ptr = allocator.ptr_from_offset(task.tx_ref.offset);
            allocator.free(ptr);
        }

        let reads: SmallVec<[Pubkey; 16]> = task.reads.clone();
        let writes: SmallVec<[Pubkey; 8]> = task.writes.clone();

        // Release locks, collect admitted tasks, decrement pending_accounts.
        let mut all_admitted: SmallVec<[TaskId; 8]> = SmallVec::new();
        for account in &reads {
            all_admitted.extend(self.accounts.release_read(account));
        }
        for account in &writes {
            all_admitted.extend(self.accounts.release_write(account));
        }

        all_admitted.sort_unstable();
        all_admitted.dedup();
        for admitted_id in all_admitted {
            if let Some(t) = self.tasks.get_mut(admitted_id as usize) {
                t.pending_accounts = t.pending_accounts.saturating_sub(1);
                if t.pending_accounts == 0 && t.state == TaskState::Resolved {
                    self.mark_ready(admitted_id);
                }
            }
        }
    }

    fn handle_check_responses(
        &mut self,
        responses: &TransactionResponseRegion,
        batch: &SmallBatch,
        allocator: &Allocator,
    ) {
        for (i, &task_id) in batch.iter().enumerate() {
            if i >= responses.num_transaction_responses as usize {
                break;
            }

            let check_response = unsafe {
                let base = allocator.ptr_from_offset(responses.transaction_responses_offset);
                let ptr = base.as_ptr() as *const worker_message_types::CheckResponse;
                &*ptr.add(i)
            };

            let resolve_failed =
                check_response.resolve_flags & worker_message_types::resolve_flags::FAILED != 0;
            let resolve_performed =
                check_response.resolve_flags & worker_message_types::resolve_flags::PERFORMED != 0;

            if resolve_failed || !resolve_performed {
                if let Some(t) = self.tasks.get_mut(task_id as usize) {
                    t.state = TaskState::Done;
                }
                continue;
            }

            let pubkeys = &check_response.resolved_pubkeys;
            if pubkeys.num_pubkeys > 0 {
                let resolved: Vec<Pubkey> = unsafe {
                    let base = allocator.ptr_from_offset(pubkeys.offset);
                    std::slice::from_raw_parts(
                        base.as_ptr() as *const Pubkey,
                        pubkeys.num_pubkeys as usize,
                    )
                    .to_vec()
                };

                self.apply_resolution(task_id, &resolved);

                unsafe {
                    let ptr = allocator.ptr_from_offset(pubkeys.offset);
                    allocator.free(ptr);
                }
            } else if let Some(t) = self.tasks.get_mut(task_id as usize) {
                t.state = TaskState::Resolved;
            }
        }
    }

    // --- Query ---

    pub fn has_inflight(&self) -> bool {
        self.workers
            .iter()
            .any(|w| !w.inflight_batch_sizes.is_empty())
    }

    /// Free shared memory for uncompleted tasks and reset all state.
    pub fn clear_and_free(&mut self, allocator: &Allocator) {
        for task in &self.tasks {
            if task.state != TaskState::Done {
                unsafe {
                    let ptr = allocator.ptr_from_offset(task.tx_ref.offset);
                    allocator.free(ptr);
                }
            }
        }
        self.tasks.clear();
        self.admit_offset = 0;
        self.resolve_offset = 0;
        self.accounts.clear();
        for w in &mut self.workers {
            *w = WorkerState::new();
        }
    }

    fn choose_worker(&self) -> usize {
        self.workers
            .iter()
            .enumerate()
            .min_by_key(|(_, w)| {
                w.inflight_batch_sizes
                    .len()
                    .saturating_mul(2)
                    .saturating_add(w.staged.len())
            })
            .map(|(i, _)| i)
            .unwrap_or(0)
    }
}

// --- IPC helpers ---

fn send_batch_to_worker(
    regions: &[SharableTransactionRegion],
    session: &mut ClientWorkerSession,
    allocator: &Allocator,
    current_slot: u64,
    flags: u16,
) -> bool {
    if regions.is_empty() {
        return false;
    }

    let num_transactions = regions.len().min(MAX_TRANSACTIONS_PER_MESSAGE) as u8;
    let batch_size = (num_transactions as usize)
        .saturating_mul(std::mem::size_of::<SharableTransactionRegion>());
    let Some(batch_ptr) = allocator.allocate(batch_size as u32) else {
        warn!("failed to allocate batch array");
        return false;
    };

    unsafe {
        std::ptr::copy_nonoverlapping(
            regions.as_ptr() as *const u8,
            batch_ptr.as_ptr(),
            batch_size,
        );
    }

    let batch = SharableTransactionBatchRegion {
        num_transactions,
        transactions_offset: unsafe { allocator.offset(batch_ptr) },
    };

    let message = PackToWorkerMessage {
        flags,
        max_working_slot: current_slot,
        batch,
    };

    session.pack_to_worker.sync();
    if session.pack_to_worker.try_write(message).is_err() {
        unsafe {
            let ptr = allocator.ptr_from_offset(batch.transactions_offset);
            allocator.free(ptr);
        }
        false
    } else {
        session.pack_to_worker.commit();
        true
    }
}

fn send_execute_batch(
    regions: &[SharableTransactionRegion],
    session: &mut ClientWorkerSession,
    allocator: &Allocator,
    current_slot: u64,
) -> bool {
    send_batch_to_worker(
        regions,
        session,
        allocator,
        current_slot,
        pack_message_flags::EXECUTE,
    )
}

fn send_check_request(
    regions: &[SharableTransactionRegion],
    session: &mut ClientWorkerSession,
    allocator: &Allocator,
    current_slot: u64,
) -> bool {
    send_batch_to_worker(
        regions,
        session,
        allocator,
        current_slot,
        pack_message_flags::check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
    )
}
