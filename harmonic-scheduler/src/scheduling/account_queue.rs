//! Per-account lock tracking with waiter queues.
//!
//! Each account tracks whether it has an active writer or active readers,
//! and a FIFO queue of tasks waiting for access. When a lock is released,
//! the queue drains greedily: one writer OR consecutive readers from the front.
//!
//! This module only manages locks — it does not modify task state. The engine
//! is responsible for decrementing `pending_accounts` on admitted tasks.

use {
    super::task::TaskId,
    rustc_hash::FxBuildHasher,
    solana_pubkey::Pubkey,
    std::collections::{HashMap, VecDeque},
};

/// A task waiting for access to an account.
#[derive(Debug, Clone)]
struct Waiter {
    task: TaskId,
    is_write: bool,
}

/// Lock state for a single account.
#[derive(Debug, Default)]
struct AccountState {
    /// Whether a writer currently holds the lock.
    writer_active: bool,
    /// Number of readers currently holding the lock.
    reader_count: u16,
    /// Tasks waiting for lock access, in submission order.
    waiters: VecDeque<Waiter>,
}

/// Collection of per-account lock states.
pub struct AccountStates {
    states: HashMap<Pubkey, AccountState, FxBuildHasher>,
}

impl AccountStates {
    pub fn new() -> Self {
        Self {
            states: HashMap::with_hasher(FxBuildHasher),
        }
    }

    /// Register a read lock request. Returns true if blocked.
    pub fn register_read(&mut self, account: &Pubkey, task_id: TaskId) -> bool {
        let state = self.states.entry(*account).or_default();
        if !state.writer_active && state.waiters.is_empty() {
            state.reader_count = state.reader_count.saturating_add(1);
            false
        } else {
            state.waiters.push_back(Waiter {
                task: task_id,
                is_write: false,
            });
            true
        }
    }

    /// Register a write lock request. Returns true if blocked.
    pub fn register_write(&mut self, account: &Pubkey, task_id: TaskId) -> bool {
        let state = self.states.entry(*account).or_default();
        if !state.writer_active && state.reader_count == 0 && state.waiters.is_empty() {
            state.writer_active = true;
            false
        } else {
            state.waiters.push_back(Waiter {
                task: task_id,
                is_write: true,
            });
            true
        }
    }

    /// Release a read lock. Returns task IDs admitted from the waiter queue.
    pub fn release_read(&mut self, account: &Pubkey) -> Vec<TaskId> {
        if let Some(state) = self.states.get_mut(account) {
            state.reader_count = state.reader_count.saturating_sub(1);
        } else {
            log::error!("release_read on unknown account {account}");
            return Vec::new();
        }
        self.wake_waiters(account)
    }

    /// Release a write lock. Returns task IDs admitted from the waiter queue.
    pub fn release_write(&mut self, account: &Pubkey) -> Vec<TaskId> {
        if let Some(state) = self.states.get_mut(account) {
            state.writer_active = false;
        } else {
            log::error!("release_write on unknown account {account}");
            return Vec::new();
        }
        self.wake_waiters(account)
    }

    /// Admit waiters from the front of the queue and remove empty state.
    fn wake_waiters(&mut self, account: &Pubkey) -> Vec<TaskId> {
        let Some(state) = self.states.get_mut(account) else {
            log::error!("wake_waiters called on unknown account {account}");
            return Vec::new();
        };

        let mut admitted = Vec::new();

        if !state.writer_active {
            while let Some(front) = state.waiters.front() {
                if front.is_write {
                    if state.reader_count > 0 {
                        break;
                    }
                    let w = state.waiters.pop_front().unwrap();
                    state.writer_active = true;
                    admitted.push(w.task);
                    break;
                } else {
                    let w = state.waiters.pop_front().unwrap();
                    state.reader_count = state.reader_count.saturating_add(1);
                    admitted.push(w.task);
                }
            }
        }

        if state.reader_count == 0 && !state.writer_active && state.waiters.is_empty() {
            self.states.remove(account);
        }

        admitted
    }

    pub fn clear(&mut self) {
        self.states.clear();
    }
}
