//! Transaction task representation for the scheduling algorithm.
//!
//! A Task wraps a transaction with the metadata the scheduler needs:
//! account read/write sets for conflict detection, shared memory location
//! for dispatch, and state tracking through the task lifecycle.

use {
    agave_scheduler_bindings::SharableTransactionRegion,
    agave_transaction_view::{
        result::TransactionViewError, transaction_data::TransactionData,
        transaction_view::SanitizedTransactionView,
    },
    smallvec::SmallVec,
    solana_pubkey::Pubkey,
};

/// Slab key type for tasks.
pub type TaskId = u32;

/// Lifecycle state of a scheduled transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Has address lookup tables that haven't been resolved yet.
    /// Account sets are incomplete. Blocks the prefix gate.
    Unresolved,
    /// Admitted into account queues, blocked on lock conflicts.
    Resolved,
    /// Assigned to a worker's staging queue, waiting for batch flush.
    Staged,
    /// Batch sent to worker, awaiting execution response.
    Processing,
    /// Terminal — transaction completed, failed, or was dropped.
    Done,
}

/// A transaction being tracked by the scheduling engine.
///
/// Contains the transaction's location in shared memory, account sets
/// for conflict detection, and scheduling metadata. Submission order
/// is tracked externally by the engine's `ordered` queue.
pub struct Task {
    /// Location of the raw transaction bytes in shared memory.
    pub tx_ref: SharableTransactionRegion,
    /// Current lifecycle state.
    pub state: TaskState,
    /// Accounts this transaction reads.
    pub reads: SmallVec<[Pubkey; 16]>,
    /// Accounts this transaction writes.
    pub writes: SmallVec<[Pubkey; 8]>,
    /// Number of writable accounts from address table lookups.
    /// Used to split resolved pubkeys into writable/readonly.
    pub alt_writable_count: u16,
    /// Number of accounts still blocking this task.
    pub pending_accounts: u16,
}

impl Task {
    /// Create a new task by parsing account sets from raw transaction bytes.
    ///
    /// Uses a zero-copy `SanitizedTransactionView` to extract the read/write
    /// account sets. Tasks with ALTs start as `Unresolved`. Tasks without ALTs
    /// have no initial state — they're admitted directly by the engine's prefix
    /// scan.
    pub fn new(
        tx_ref: SharableTransactionRegion,
        raw_bytes: &[u8],
    ) -> Result<Self, TransactionViewError> {
        let view = SanitizedTransactionView::try_new_sanitized(
            raw_bytes, true, // enable_static_instruction_limit (active on mainnet)
            true, // enable_instruction_accounts_limit (active on mainnet)
        )?;

        let (reads, writes) = extract_account_sets(&view);
        let has_alts = view.num_address_table_lookups() > 0;

        let alt_writable_count = if has_alts {
            view.total_writable_lookup_accounts()
        } else {
            0
        };

        Ok(Self {
            tx_ref,
            state: if has_alts {
                TaskState::Unresolved
            } else {
                TaskState::Resolved
            },
            reads,
            writes,
            alt_writable_count,
            pending_accounts: 0,
        })
    }

    /// Apply ALT resolution results. Merges resolved pubkeys and marks resolved.
    ///
    /// Splits resolved pubkeys into writable/readonly using `alt_writable_count`
    /// and merges them with the static account sets. Transitions to WaitingAccounts
    /// so the prefix scan can admit the task.
    pub fn resolve_luts(&mut self, resolved_pubkeys: &[Pubkey]) {
        let split = (self.alt_writable_count as usize).min(resolved_pubkeys.len());
        self.writes.extend_from_slice(&resolved_pubkeys[..split]);
        self.reads.extend_from_slice(&resolved_pubkeys[split..]);
        self.state = TaskState::Resolved;
    }
}

/// Extract read and write account sets from a transaction view.
///
/// NOTE: this does not check reserved account keys or program ID demotion.
/// For auction blocks from a trusted source, this is acceptable. For
/// untrusted sources, use ResolvedTransactionView::is_writable() instead.
pub fn extract_account_sets<D: TransactionData>(
    view: &SanitizedTransactionView<D>,
) -> (SmallVec<[Pubkey; 16]>, SmallVec<[Pubkey; 8]>) {
    let keys = view.static_account_keys();
    let num_signed = view.num_required_signatures() as usize;
    let num_writable_signed = view.num_writable_signed_static_accounts() as usize;
    let num_writable_unsigned = view.num_writable_unsigned_static_accounts() as usize;

    let mut reads = SmallVec::new();
    let mut writes = SmallVec::new();

    for (i, key) in keys.iter().enumerate() {
        let is_writable = if i < num_signed {
            i < num_writable_signed
        } else {
            // Safe: i >= num_signed is guaranteed by the else branch.
            i.saturating_sub(num_signed) < num_writable_unsigned
        };

        if is_writable {
            writes.push(*key);
        } else {
            reads.push(*key);
        }
    }

    (reads, writes)
}
