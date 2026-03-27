//! Per-worker scheduling state.
//!
//! Each worker has a staging queue where tasks are placed after passing
//! conflict checks. The staging queue is flushed as batches when it's
//! full, timed out, or the slot is ending. Inflight batches are tracked
//! so completion responses can be matched back to tasks.

use {
    super::task::TaskId,
    smallvec::SmallVec,
    std::{collections::VecDeque, time::Instant},
};

/// A batch of task IDs, stack-allocated for typical batch sizes.
pub type SmallBatch = SmallVec<[TaskId; 64]>;

/// Maximum time to wait before flushing a partial batch (microseconds).
const MAX_READY_WAIT_US: u64 = 250;

/// Per-worker scheduling state.
pub struct WorkerState {
    pub staged: VecDeque<TaskId>,
    pub last_flush_ts: Instant,
    pub inflight_task_ids: VecDeque<TaskId>,
    pub inflight_batch_sizes: VecDeque<u8>,
}

impl WorkerState {
    pub fn new() -> Self {
        Self {
            staged: VecDeque::new(),
            last_flush_ts: Instant::now(),
            inflight_task_ids: VecDeque::new(),
            inflight_batch_sizes: VecDeque::new(),
        }
    }

    /// Stage a task on this worker.
    pub fn stage(&mut self, id: TaskId) {
        if self.staged.is_empty() {
            self.last_flush_ts = Instant::now();
        }
        self.staged.push_back(id);
    }

    /// Check if this worker's staged tasks should be flushed as a batch.
    pub fn should_flush(&self) -> bool {
        if self.staged.is_empty() {
            return false;
        }
        if self.staged.len() >= agave_scheduler_bindings::MAX_TRANSACTIONS_PER_MESSAGE {
            return true;
        }
        self.last_flush_ts.elapsed().as_micros() as u64 >= MAX_READY_WAIT_US
    }

    /// Build a batch from staged tasks (up to MAX_TRANSACTIONS_PER_MESSAGE).
    /// Resets the flush timer so remaining tasks have 250us to accumulate
    /// more before the next time-based flush.
    pub fn take_batch(&mut self) -> SmallBatch {
        let count = self
            .staged
            .len()
            .min(agave_scheduler_bindings::MAX_TRANSACTIONS_PER_MESSAGE);

        let batch: SmallBatch = self.staged.drain(..count).collect();

        if !self.staged.is_empty() {
            self.last_flush_ts = Instant::now();
        }

        batch
    }

    /// Record a batch as inflight after successful IPC send.
    pub fn record_inflight(&mut self, batch: &SmallBatch) {
        self.inflight_task_ids.extend(batch.iter().copied());
        // batch.len() is capped by MAX_TRANSACTIONS_PER_MESSAGE (fits in u8).
        self.inflight_batch_sizes.push_back(batch.len() as u8);
    }

    /// Pop the oldest inflight batch (called when a worker response arrives).
    pub fn pop_inflight_batch(&mut self) -> SmallBatch {
        let Some(batch_size) = self.inflight_batch_sizes.pop_front() else {
            log::error!("pop_inflight_batch called with no inflight batches");
            return SmallBatch::new();
        };

        self.inflight_task_ids
            .drain(..batch_size as usize)
            .collect()
    }
}
