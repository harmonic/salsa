//! General purpose utility functions and types

use {
    crate::scheduler::{
        MAX_BUNDLE_TXS,
        channel::BundleMeta,
        check::{CheckMeta, CheckResult},
    },
    agave_scheduler_bindings::{SharableTransactionRegion, worker_message_types::resolve_flags},
    agave_scheduling_utils::transaction_ptr::TransactionPtrBatch,
    agave_transaction_view::{
        transaction_data::TransactionData, transaction_view::UnsanitizedTransactionView,
    },
    rts_alloc::Allocator,
    smallvec::SmallVec,
    solana_pubkey::Pubkey,
};

/// Each account a batch uses once, with whether it writes it
pub type Accounts = SmallVec<[(Pubkey, bool); 32]>;

/// A batch's members that load lookup tables, as lookup-table CHECK entries
pub type Lookups = SmallVec<[(SharableTransactionRegion, CheckMeta); MAX_BUNDLE_TXS]>;

/// Free `batch` and the transactions it owns
///
/// # Safety
///
/// The caller owns `batch` exclusively: no worker or other handle holds it
pub unsafe fn free_batch(batch: TransactionPtrBatch<(), BundleMeta>, allocator: &Allocator) {
    for (tx, ()) in batch.iter() {
        // SAFETY: The batch owns its transactions, and the caller guarantees no
        // worker holds it.
        unsafe { tx.free(allocator) };
    }
    // SAFETY: As above; the batch is consumed here.
    unsafe { batch.free(allocator) };
}

/// Tag `batch` as `id`, and take its accounts and lookup-table members; None if unusable
pub fn parse(
    id: u32,
    batch: &mut TransactionPtrBatch<(), BundleMeta>,
    trusted: bool,
    tip_program: &Pubkey,
    allocator: &Allocator,
) -> Option<(Accounts, Lookups)> {
    // The validator rejects an empty EXECUTE as invalid
    if batch.is_empty() {
        return None;
    }
    let mut accounts = SmallVec::new();
    let mut lookups = SmallVec::new();
    for (tx, ()) in batch.iter() {
        let view = UnsanitizedTransactionView::try_new_unsanitized(tx.data()).ok()?;
        union_extend(&mut accounts, static_accounts(&view));
        if view.num_address_table_lookups() != 0 {
            // SAFETY: The batch's transactions are in this session's mapping,
            // which `allocator` is a handle of.
            let region = unsafe { tx.to_sharable_transaction_region(allocator) };
            let writable = view.total_writable_lookup_accounts();
            lookups.push((region, CheckMeta { id, writable }));
        }
    }
    // Program ids are static keys; CPI targets may come from lookup tables
    // and are checked when they resolve
    if !trusted && accounts.iter().any(|(key, _)| key == tip_program) {
        return None;
    }
    batch.set_batch_meta(BundleMeta {
        id,
        ..batch.batch_meta()
    });
    Some((accounts, lookups))
}

/// Add a member's resolved keys to `accounts`; false if unresolved or `tip_program` is among them
pub fn add_resolved(
    accounts: &mut Accounts,
    checked: &CheckResult<'_, CheckMeta>,
    tip_program: &Pubkey,
) -> bool {
    let resolved = checked.response.resolve_flags
        & (resolve_flags::PERFORMED | resolve_flags::FAILED)
        == resolve_flags::PERFORMED;
    let Some(keys) = checked
        .keys
        .filter(|keys| resolved && !keys.contains(tip_program))
    else {
        return false;
    };
    let writable = usize::from(checked.meta.writable);
    union_extend(
        accounts,
        keys.iter().enumerate().map(|(i, key)| (*key, i < writable)),
    );
    true
}

/// Merge `keys` into `accounts`, each once with writability OR-ed, so locks and unlocks match
pub fn union_extend(accounts: &mut Accounts, keys: impl IntoIterator<Item = (Pubkey, bool)>) {
    accounts.extend(keys);
    accounts.sort_unstable_by_key(|&(key, _)| key);
    // `dedup_by` passes (later, earlier); keep the earlier with OR-ed writability
    accounts.dedup_by(|later, earlier| {
        let same = later.0 == earlier.0;
        earlier.1 |= same && later.1;
        same
    });
}

/// A transaction's static account keys, with whether it writes each
pub fn static_accounts(
    view: &UnsanitizedTransactionView<&[u8]>,
) -> impl Iterator<Item = (Pubkey, bool)> {
    let num_signed = usize::from(view.num_required_signatures());
    let num_readonly_signed = usize::from(view.num_readonly_signed_static_accounts());
    let num_readonly_unsigned = usize::from(view.num_readonly_unsigned_static_accounts());
    let keys = view.static_account_keys();
    let num_writable_signed = num_signed.saturating_sub(num_readonly_signed);
    let num_writable_unsigned = keys
        .len()
        .saturating_sub(num_signed)
        .saturating_sub(num_readonly_unsigned);
    keys.iter().enumerate().map(move |(i, key)| {
        let is_write = if i < num_signed {
            i < num_writable_signed
        } else {
            i.saturating_sub(num_signed) < num_writable_unsigned
        };
        (*key, is_write)
    })
}
