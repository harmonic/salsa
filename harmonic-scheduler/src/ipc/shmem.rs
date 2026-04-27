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

/// Allocate a SharableTransactionRegion and copy `data` into it
/// Caller owns the returned allocation
///
/// # Returns
/// `None` if the data is empty or exceeds `MAX_ALLOCATION_SIZE`;
/// panics if the allocator is out of space
pub fn allocate_transaction(
    data: impl AsRef<[u8]>,
    alloc: &Allocator,
) -> Option<SharableTransactionRegion> {
    let data = data.as_ref();
    if data.is_empty() || data.len() > MAX_ALLOCATION_SIZE as usize {
        return None;
    }
    let length = data.len() as u32;
    let ptr = alloc.allocate(length).expect("failed to allocate SHM");
    // SAFETY: ptr was just allocated, source and destination do not overlap
    let offset = unsafe {
        copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len());
        alloc.offset(ptr)
    };
    Some(SharableTransactionRegion { offset, length })
}

/// Allocate a SharableTransactionBatch and populate it from `items`
/// Caller owns the returned allocation
///
/// # Returns
/// `None` if `items` is empty or exceeds `MAX_TRANSACTIONS_PER_MESSAGE`;
/// panics if the allocator is out of space
pub fn allocate_batch<I>(items: I, alloc: &Allocator) -> Option<SharableTransactionBatchRegion>
where
    I: IntoIterator<Item = SharableTransactionRegion>,
    I::IntoIter: ExactSizeIterator,
{
    // Compile-time guarantee that a full batch fits in one rts-alloc allocation
    const _: () = assert!(
        MAX_TRANSACTIONS_PER_MESSAGE * size_of::<SharableTransactionRegion>()
            <= MAX_ALLOCATION_SIZE as usize,
        "batch pointer-array must fit in a single allocation",
    );
    let iter = items.into_iter();
    let n = iter.len();
    if n == 0 || n > MAX_TRANSACTIONS_PER_MESSAGE {
        return None;
    }
    let size = (n as u32).saturating_mul(size_of::<SharableTransactionRegion>() as u32);
    let batch_ptr = alloc.allocate(size).expect("failed to allocate SHM");
    // SAFETY: batch_ptr was just allocated with `size` bytes
    let slots = batch_ptr.as_ptr() as *mut SharableTransactionRegion;
    for (i, item) in iter.enumerate() {
        unsafe { slots.add(i).write(item) };
    }
    // Convert the local pointer back to a cross-process SHM offset
    let transactions_offset = unsafe { alloc.offset(batch_ptr) };
    Some(SharableTransactionBatchRegion {
        num_transactions: n as u8,
        transactions_offset,
    })
}

/// Always allocated: `allocate_transaction` rejects empty
impl Free for SharableTransactionRegion {
    fn free(&self, alloc: &Allocator) {
        unsafe { alloc.free_offset(self.offset) };
    }
}

/// Always allocated: `allocate_batch` rejects empty
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

/// `WorkerToPackMessage` owns no SHM directly; its fields do
/// `free` is a no-op; `free_full` cascades deep
impl Free for WorkerToPackMessage {
    fn free(&self, _alloc: &Allocator) {}

    fn free_full(&self, alloc: &Allocator) {
        self.batch.free_full(alloc);
        if self.processed_code == processed_codes::PROCESSED {
            self.responses.free_full(alloc);
        } else {
            // Entries undefined per contract; only touch the outer array
            self.responses.free(alloc);
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
