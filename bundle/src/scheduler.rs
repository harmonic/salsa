use {
    log::*,
    rustc_hash::{FxBuildHasher, FxHashMap},
    solana_cost_model::cost_model::CostModel,
    solana_message::SanitizedMessage,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction::sanitized::SanitizedTransaction,
    std::{collections::VecDeque, ops::Range},
};

pub struct Scheduler {
    /// Map of the account locks for the popped but unfinished transactions
    pub running_locks: FxHashMap<[u8; 32], u32>,
    /// Map of the account locks for the skipped over but unpopped transactions
    pub skipped_locks: FxHashMap<[u8; 32], u32>,
    /// Indices of transactions skipped over while popping
    pub indices: VecDeque<usize>,
    /// Which transaction in `transactions` to pop next, if available
    pub next: usize,
    /// How many transactions have been scheduled
    pub completed: usize,
    /// Flag for when execution is done
    pub finished: bool,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            // TODO: tune initial allocations
            running_locks: FxHashMap::with_capacity_and_hasher(10_000, FxBuildHasher),
            skipped_locks: FxHashMap::with_capacity_and_hasher(10_000, FxBuildHasher),
            indices: VecDeque::with_capacity(100_000),
            next: 0,
            completed: 0,
            finished: false,
        }
    }
}

impl Scheduler {
    /// Total transaction slice cost at which to stop batching
    const MAX_COST: u64 = 1_400_000;
    /// Largest number of transactions to include in a batch
    const MAX_RANGE: usize = 64;

    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a transaction is blocked by a set of locks
    #[inline(always)]
    fn is_blocked(message: &SanitizedMessage, locks: &FxHashMap<[u8; 32], u32>) -> bool {
        for (i, account) in message.account_keys().iter().enumerate() {
            if let Some(&lock) = locks.get(account.as_array()) {
                // lock == u32::MAX => Another transaction holds a write lock
                // lock != 0 && writable => Another transaction holds a read lock,
                //                          but this transaction needs a write lock
                if lock == u32::MAX || (lock != 0 && message.is_writable(i)) {
                    return true;
                }
            }
        }
        false
    }

    /// Add a transaction's account accesses to a set of locks
    #[inline(always)]
    fn lock_accounts(message: &SanitizedMessage, locks: &mut FxHashMap<[u8; 32], u32>) {
        for (i, account) in message.account_keys().iter().enumerate() {
            let write = message.is_writable(i);
            locks
                .entry(account.to_bytes())
                .and_modify(|v| {
                    if write {
                        *v = u32::MAX
                    } else {
                        // Use a saturating add here so that adding a read
                        // lock to a write locked account just leaves the write lock
                        // in place
                        *v = v.saturating_add(1)
                    }
                })
                .or_insert(if write { u32::MAX } else { 1 });
        }
    }

    /// Remove a transaction's account accesses to a set of locks
    #[inline(always)]
    fn unlock_accounts(message: &SanitizedMessage, locks: &mut FxHashMap<[u8; 32], u32>) {
        for (i, account) in message.account_keys().iter().enumerate() {
            locks.entry(account.to_bytes()).and_modify(|v| {
                if message.is_writable(i) {
                    *v = 0
                } else {
                    *v = v.saturating_sub(1)
                }
            });
        }
    }

    /// Initialize the scheduler for a slice of transactions
    pub fn init(&mut self, transactions: &[RuntimeTransaction<SanitizedTransaction>]) {
        info!("indices: {:?}", self.indices);
        info!("running_locks: {:?}", self.running_locks);
        info!("running_locks: {:?}", self.running_locks);
        self.indices.extend(0..transactions.len());
        self.running_locks.clear();
        self.completed = 0;
        self.finished = false;
    }

    /// Get the next available range of transactions to schedule
    ///
    /// In order to avoid searching too deeply, this function just checks
    /// the next available range of indices. This function should be called
    /// in a tight loop with `done()`.
    ///
    /// ```rs
    /// while !scheduler.finish {
    ///     if let Some(range) = scheduler.pop() {
    ///         // execute a transaction
    ///         running.push(range)
    ///     }
    ///
    ///     // Execute transaction
    ///     while let Some(range) = running.pop() {
    ///         // Mark the transaction as done in the scheduler
    ///         schduler.finish(range)
    ///     };
    /// }
    /// ```
    pub fn pop(
        &mut self,
        transactions: &[RuntimeTransaction<SanitizedTransaction>],
        bank: &Bank,
    ) -> Option<Range<usize>> {
        info!("\n    indices: {:?}\n    running_locks: {:?}\n    skipped_locks: {:?}\n    next: {}, completed: {}, finished: {}", self.indices, self.running_locks, self.skipped_locks, self.next, self.completed, self.finished);
        let mut value: Option<Range<usize>> = None;
        let mut cost = 0;
        // Look through at most Self::MAX_RANGE transactions
        for &i in self.indices.iter().skip(self.next).take(Self::MAX_RANGE) {
            // We can only execute contiguous ranges, so ensure that this
            // transaction doesn't skip indices
            if let Some(ref range) = value {
                if i != range.end {
                    break;
                }
            }
            // Check if the transaction is blocked by any running transactions
            // or any earlier transactions in the bundle that we haven't
            // scheduled yet
            let message = transactions[i].message();
            if Self::is_blocked(message, &self.running_locks)
                || Self::is_blocked(message, &self.skipped_locks)
            {
                // We will return the first time we hit a blocked transaction,
                // to keep this function call fast. Whether or not we have found
                // a valid range to schedule, this transaction is blocked until
                // we get a `finish()` call
                self.next += 1;
                // This transaction also blocks any future transactions in the
                // bundle
                Self::lock_accounts(message, &mut self.skipped_locks);
                break;
            }
            // This transaction can be scheduled. Add it to the set of running
            // locks and update the return value
            Self::lock_accounts(message, &mut self.running_locks);
            if let Some(ref mut range) = value {
                range.end += 1;
            } else {
                value = Some(i..i + 1);
            }
            // Check the total cost of this transaction slice. If it gets too
            // expensive, return early to avoid having too long running an
            // execution on one thread blocking other transactions.
            cost += CostModel::calculate_cost(&transactions[i], &bank.feature_set).sum();
            if cost > Self::MAX_COST {
                break;
            }
        }
        if let Some(ref range) = value {
            // We are scheduling this range, so we need to remove it from the
            // indices of remaining transactions
            let start = self.indices.iter().position(|&v| v == range.start).unwrap();
            self.indices.drain(start..start + range.len());
        }
        value
    }

    /// Mark a transaction as finished
    pub fn finish(
        &mut self,
        range: Range<usize>,
        transactions: &[RuntimeTransaction<SanitizedTransaction>],
    ) {
        // Count of completed transactions
        self.completed += range.len();
        // Unlock all of the finished transactions' locks
        for i in range {
            let message = transactions[i].message();
            Self::unlock_accounts(message, &mut self.running_locks);
        }
        // Reset the pop() transaction search, because there may now be  newly
        // unblocked transactions to schedule
        self.skipped_locks.clear();
        self.next = 0;
        // Update the finished flag once all transactions are complete
        self.finished = self.completed == transactions.len();
    }
}
