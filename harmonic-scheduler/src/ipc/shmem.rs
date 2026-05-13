//! Shared-memory primitives for the validator IPC surface
//!
//! # Ownership model
//!
//! - `allocate_*` returns an allocation owned by the caller
//!
//! - Sending to a worker transfers recursive ownership of the batch
//!   Freeing any transferred memory before the matching response arrives is
//!   a use-after-free on the worker
//!
//! - Receiving from a worker gives or returns ownership of all memory. The
//!   receiver must free all response memory
//!
//! - Cross-handle free routes to the owner's remote free list. Handles must
//!   periodically reclaim via `clean_remote_free_lists`

use agave_scheduler_bindings::worker_message_types::{
    self, CheckResponse, ExecutionResponse, ReadResponse, read_results, resolve_flags,
};
use agave_scheduler_bindings::{
    MAX_ALLOCATION_SIZE, MAX_TRANSACTIONS_PER_MESSAGE, SharableAccountData, SharablePubkeys,
    SharableTransactionBatchRegion, SharableTransactionRegion, TransactionResponseRegion,
    WorkerToPackMessage, processed_codes,
};
use rts_alloc::Allocator;
use solana_pubkey::Pubkey;
use std::mem::size_of;
use std::ptr::copy_nonoverlapping;
use std::slice;

/// Release SHM owned by `Self`
pub trait Free {
    /// Shallow free: the direct allocation only. No-op for types without one
    ///
    /// # Safety
    /// Caller must own the allocation and must not have freed it already
    fn free(&self, alloc: &Allocator);

    /// Deep free: children first, then this allocation
    /// Defaults to [`Self::free`] for types with no children
    fn free_full(&self, alloc: &Allocator) {
        self.free(alloc);
    }
}

/// Borrow the SHM region backing `Self` as `&[T]`. Use `Slice<u8>` for raw bytes
///
/// # Safety
/// Caller must own the allocation and the count field must reflect the
/// populated elements
pub trait Slice<T> {
    fn slice<'a>(&self, alloc: &'a Allocator) -> &'a [T];
}

/// Borrow the first signature (64 bytes after the leading length byte) from a transaction
pub fn signature<'a>(tx: &SharableTransactionRegion, alloc: &'a Allocator) -> &'a [u8; 64] {
    tx.slice(alloc)[1..65]
        .try_into()
        .expect("transaction should have at least one signature")
}

/// Allocate a SharableTransactionRegion and copy `data` into it
/// Caller owns the returned allocation
pub fn allocate(data: impl AsRef<[u8]>, alloc: &Allocator) -> SharableTransactionRegion {
    let data = data.as_ref();
    assert!(!data.is_empty(), "data must be non-empty");
    assert!(
        data.len() <= MAX_ALLOCATION_SIZE as usize,
        "data exceeds MAX_ALLOCATION_SIZE",
    );
    let length = data.len() as u32;
    let ptr = alloc
        .allocate(length)
        .expect("SHM allocator should have capacity");
    // SAFETY: ptr was just allocated, source and destination do not overlap
    let offset = unsafe {
        copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        alloc.offset(ptr)
    };
    SharableTransactionRegion { offset, length }
}

/// Allocate a SharableTransactionBatch and populate it from `items`
/// Caller owns the returned allocation
pub fn allocate_batch<I>(txs: I, alloc: &Allocator) -> SharableTransactionBatchRegion
where
    I: IntoIterator<Item = SharableTransactionRegion>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = txs.into_iter();
    assert!(iter.len() != 0, "batch must be non-empty");
    assert!(
        iter.len() <= MAX_TRANSACTIONS_PER_MESSAGE,
        "batch exceeds MAX_TRANSACTIONS_PER_MESSAGE",
    );
    let num_transactions = iter.len() as u8;
    let bytes = iter
        .len()
        .saturating_mul(size_of::<SharableTransactionRegion>()) as u32;
    let batch_ptr = alloc
        .allocate(bytes)
        .expect("SHM allocator should have capacity");
    // SAFETY: batch_ptr was just allocated with `size` bytes
    let slots = batch_ptr.as_ptr() as *mut SharableTransactionRegion;
    for (i, item) in iter.enumerate() {
        unsafe { slots.add(i).write(item) };
    }
    // Convert the local pointer back to a cross-process SHM offset
    let transactions_offset = unsafe { alloc.offset(batch_ptr) };
    SharableTransactionBatchRegion {
        num_transactions,
        transactions_offset,
    }
}

/// Always allocated: `allocate` asserts non-empty
impl Free for SharableTransactionRegion {
    fn free(&self, alloc: &Allocator) {
        unsafe { alloc.free_offset(self.offset) };
    }
}

/// Always allocated: `allocate_batch` asserts non-empty
impl Free for SharableTransactionBatchRegion {
    fn free(&self, alloc: &Allocator) {
        unsafe { alloc.free_offset(self.transactions_offset) };
    }

    fn free_full(&self, alloc: &Allocator) {
        for region in <Self as Slice<SharableTransactionRegion>>::slice(self, alloc) {
            region.free(alloc);
        }
        self.free(alloc);
    }
}

/// Only valid to call when the source `WorkerToPackMessage` is PROCESSED
/// The `num_transaction_responses != 0` guard matches Agave's zeroing on
/// non-PROCESSED as a defensive backstop
impl Free for TransactionResponseRegion {
    fn free(&self, alloc: &Allocator) {
        if self.num_transaction_responses != 0 {
            unsafe { alloc.free_offset(self.transaction_responses_offset) };
        }
    }

    /// Dispatches on `tag` to free per-entry sub-allocations. Caller is
    /// responsible for only invoking on PROCESSED responses
    fn free_full(&self, alloc: &Allocator) {
        match self.tag {
            worker_message_types::CHECK_RESPONSE => {
                for r in <Self as Slice<CheckResponse>>::slice(self, alloc) {
                    r.free(alloc);
                }
            }
            worker_message_types::READ_RESPONSE => {
                for r in <Self as Slice<ReadResponse>>::slice(self, alloc) {
                    r.free(alloc);
                }
            }
            worker_message_types::EXECUTION_RESPONSE => {
                for r in <Self as Slice<ExecutionResponse>>::slice(self, alloc) {
                    r.free(alloc);
                }
            }
            _ => {}
        }
        self.free(alloc);
    }
}

/// `num_pubkeys == 0` means no allocation (per bindings)
impl Free for SharablePubkeys {
    fn free(&self, alloc: &Allocator) {
        if self.num_pubkeys != 0 {
            unsafe { alloc.free_offset(self.offset) };
        }
    }
}

/// `length == 0` means no allocation (per bindings)
impl Free for SharableAccountData {
    fn free(&self, alloc: &Allocator) {
        if self.length != 0 {
            unsafe { alloc.free_offset(self.offset) };
        }
    }
}

/// `resolved_pubkeys` is only defined when `resolve_flags::PERFORMED` is set
impl Free for CheckResponse {
    fn free(&self, alloc: &Allocator) {
        if self.resolve_flags & resolve_flags::PERFORMED != 0 {
            self.resolved_pubkeys.free(alloc);
        }
    }
}

/// `data` is allocated only on SUCCESS + `LOAD_DATA`. The `SUCCESS` check
/// honors the bindings contract; the `length != 0` guard in
/// [`SharableAccountData::free`] covers SUCCESS-without-`LOAD_DATA`
impl Free for ReadResponse {
    fn free(&self, alloc: &Allocator) {
        if self.read_result == read_results::SUCCESS {
            self.data.free(alloc);
        }
    }
}

/// No sub-allocations to free
impl Free for ExecutionResponse {
    fn free(&self, _alloc: &Allocator) {}
}

/// `responses` is only allocated on `processed_codes::PROCESSED`
impl Free for WorkerToPackMessage {
    fn free(&self, alloc: &Allocator) {
        self.batch.free(alloc);
        if self.processed_code == processed_codes::PROCESSED {
            self.responses.free(alloc);
        }
    }

    fn free_full(&self, alloc: &Allocator) {
        self.batch.free_full(alloc);
        if self.processed_code == processed_codes::PROCESSED {
            self.responses.free_full(alloc);
        }
    }
}

macro_rules! impl_slice {
    ($container:ty, $elem:ty, $offset:ident, $count:ident) => {
        impl Slice<$elem> for $container {
            fn slice<'a>(&self, alloc: &'a Allocator) -> &'a [$elem] {
                // `count == 0` sentinel: no allocation, don't deref `offset`
                if self.$count == 0 {
                    return &[];
                }
                unsafe {
                    let data = alloc.ptr_from_offset(self.$offset);
                    slice::from_raw_parts(data.as_ptr() as *const $elem, self.$count as usize)
                }
            }
        }
    };
}

impl_slice!(
    SharableTransactionBatchRegion,
    SharableTransactionRegion,
    transactions_offset,
    num_transactions
);
impl_slice!(
    TransactionResponseRegion,
    CheckResponse,
    transaction_responses_offset,
    num_transaction_responses
);
impl_slice!(
    TransactionResponseRegion,
    ReadResponse,
    transaction_responses_offset,
    num_transaction_responses
);
impl_slice!(
    TransactionResponseRegion,
    ExecutionResponse,
    transaction_responses_offset,
    num_transaction_responses
);
impl_slice!(SharablePubkeys, Pubkey, offset, num_pubkeys);
impl_slice!(SharableTransactionRegion, u8, offset, length);
impl_slice!(SharableAccountData, u8, offset, length);
