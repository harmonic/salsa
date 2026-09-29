use {
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionBatchRegion, SharableTransactionRegion,
    },
    agave_transaction_view::transaction_data::TransactionData,
    core::ptr::NonNull,
    rts_alloc::Allocator,
    std::marker::PhantomData,
};

#[derive(Debug)]
pub struct TransactionPtr {
    ptr: NonNull<u8>,
    count: usize,
}

// SAFETY: The pointee lives in the shared mapping, which is valid on every
// thread, and any allocator handle can free it.
unsafe impl Send for TransactionPtr {}

impl TransactionData for TransactionPtr {
    fn data(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
    }
}

impl TransactionData for &TransactionPtr {
    fn data(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
    }
}

impl TransactionPtr {
    /// Allocates a region in `allocator` and copies `data` into it.
    ///
    /// Returns `None` if `data` is empty, larger than the allocator's largest
    /// size class, or the allocator is out of memory. The caller owns the
    /// allocation and is responsible for freeing it.
    pub fn from_slice(data: &[u8], allocator: &Allocator) -> Option<Self> {
        let ptr = allocator.allocate(u32::try_from(data.len()).ok()?)?;
        // SAFETY: `ptr` is a fresh allocation of `data.len()` bytes that does not
        // overlap `data`.
        unsafe { core::ptr::copy_nonoverlapping(data.as_ptr(), ptr.as_ptr(), data.len()) };

        Some(Self {
            ptr,
            count: data.len(),
        })
    }

    /// Constructions a [`TransactionPtr`] from raw parts.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for reads.
    /// - `count` must be accurate and not overrun the end of `ptr`.
    ///
    /// # Note
    ///
    /// If you are trying to construct a pointer for use by Agave, you almost certainly want to use
    /// [`Self::from_sharable_transaction_region`].
    pub unsafe fn from_raw_parts(ptr: NonNull<u8>, count: usize) -> Self {
        Self { ptr, count }
    }

    /// # Safety
    /// - `sharable_transaction_region` must reference a valid offset and length
    ///   within the `allocator`.
    pub unsafe fn from_sharable_transaction_region(
        sharable_transaction_region: &SharableTransactionRegion,
        allocator: &Allocator,
    ) -> Self {
        // SAFETY: `sharable_transaction_region.offset` was allocated by `allocator`.
        let ptr = unsafe { allocator.ptr_from_offset(sharable_transaction_region.offset) };
        Self {
            ptr,
            count: sharable_transaction_region.length as usize,
        }
    }

    /// Translate the ptr type into a sharable region.
    ///
    /// # Safety
    /// - `allocator` must be the allocator owning the memory region pointed
    ///   to by `self`.
    pub unsafe fn to_sharable_transaction_region(
        &self,
        allocator: &Allocator,
    ) -> SharableTransactionRegion {
        // SAFETY: The `TransactionPtr` creation `Self::from_sharable_transaction_region`
        // is already conditioned on the offset being valid, if that safety constraint
        // was satisfied translation back to offset is safe.
        let offset = unsafe { allocator.offset(self.ptr) };
        SharableTransactionRegion {
            offset,
            length: self.count as u32,
        }
    }

    /// Frees the memory region pointed to in the `allocator`.
    /// This should only be called by the owner of the memory
    /// i.e. the external scheduler.
    ///
    /// # Safety
    /// - Data region pointed to by `TransactionPtr` belongs to the `allocator`.
    /// - Inner `ptr` must not have been previously freed.
    pub unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.ptr) }
    }
}

/// A batch of transaction pointers that can be iterated over, with `M`
/// metadata per transaction and `B` metadata for the whole batch.
///
/// The layout is the transaction regions, then each transaction's `M`, then
/// one `B`. With the default `B = ()` it is just the regions and the `M`s.
///
/// Like [`TransactionPtr`] it holds no allocator, so it can move between
/// threads; freeing takes one.
#[derive(Debug)]
pub struct TransactionPtrBatch<M = (), B = ()> {
    tx_ptr: NonNull<SharableTransactionRegion>,
    meta_ptr: NonNull<M>,
    num_transactions: usize,
    /// Where the batch is in the mapping; offsets count from the mapping's
    /// start, so this also locates the batch's transactions
    offset: usize,

    _meta: PhantomData<(M, B)>,
}

// SAFETY: The batch lives in the shared mapping, which is valid on every
// thread, and any allocator handle can free it.
unsafe impl<M: Send, B: Send> Send for TransactionPtrBatch<M, B> {}

impl<M, B> TransactionPtrBatch<M, B> {
    pub const TRANSACTION_CORE_SIZE: usize = size_of::<SharableTransactionRegion>();
    pub const TRANSACTION_CORE_END: usize =
        Self::TRANSACTION_CORE_SIZE * MAX_TRANSACTIONS_PER_MESSAGE;

    pub const TRANSACTION_META_START: usize =
        Self::TRANSACTION_CORE_END.next_multiple_of(align_of::<M>());
    pub const TRANSACTION_META_SIZE: usize = size_of::<M>() * MAX_TRANSACTIONS_PER_MESSAGE;
    pub const TRANSACTION_META_END: usize =
        Self::TRANSACTION_META_START + Self::TRANSACTION_META_SIZE;

    pub const TRANSACTION_BATCH_META_START: usize =
        Self::TRANSACTION_META_END.next_multiple_of(align_of::<B>());
    pub const TRANSACTION_BATCH_META_SIZE: usize = size_of::<B>();
    pub const TRANSACTION_BATCH_META_END: usize =
        Self::TRANSACTION_BATCH_META_START + Self::TRANSACTION_BATCH_META_SIZE;

    const TRANSACTION_BATCH_SIZE_ASSERT: () = assert!(Self::TRANSACTION_BATCH_META_END <= 4096);

    /// Allocate a batch of transactions, their `M` metadata, and the batch's
    /// own `B` metadata.
    ///
    /// The batch references the transactions without owning them: free it with
    /// [`Self::free`] and the transactions separately.
    ///
    /// Returns `None` if `transactions` is longer than
    /// [`MAX_TRANSACTIONS_PER_MESSAGE`], or the allocation fails. An empty batch
    /// still carries its `B` metadata.
    ///
    /// # Safety
    /// - Every transaction must be allocated in the `allocator`'s mapping, and
    ///   stay allocated while the batch is in use.
    pub unsafe fn from_transactions<'t>(
        transactions: impl ExactSizeIterator<Item = (&'t TransactionPtr, M)>,
        batch_meta: B,
        allocator: &Allocator,
    ) -> Option<Self> {
        let regions = transactions.map(|(transaction, meta)| {
            // SAFETY: The caller vouches that the transaction is in the
            // `allocator`'s mapping.
            let region = unsafe { transaction.to_sharable_transaction_region(allocator) };
            (region, meta)
        });
        // SAFETY: The regions are the caller's live transactions in this mapping.
        unsafe { Self::from_iter(regions, batch_meta, allocator) }
    }

    /// Allocate a batch of transaction regions, their `M` metadata, and the
    /// batch's own `B` metadata.
    ///
    /// The batch references the transactions without owning them: free it with
    /// [`Self::free`] and the transactions separately.
    ///
    /// Returns `None` if `iter` is longer than [`MAX_TRANSACTIONS_PER_MESSAGE`], or
    /// the allocation fails. An empty batch still carries its `B` metadata.
    ///
    /// # Safety
    /// - ALL [`SharableTransactionRegion`] must reference transactions in the
    ///   `allocator`'s mapping that stay allocated while the batch is in use.
    pub unsafe fn from_iter(
        iter: impl ExactSizeIterator<Item = (SharableTransactionRegion, M)>,
        batch_meta: B,
        allocator: &Allocator,
    ) -> Option<Self> {
        let () = Self::TRANSACTION_BATCH_SIZE_ASSERT;
        let num_transactions = iter.len();
        if num_transactions > MAX_TRANSACTIONS_PER_MESSAGE {
            return None;
        }

        let base = allocator.allocate(Self::TRANSACTION_BATCH_META_END as u32)?;
        let tx_ptr = base.cast::<SharableTransactionRegion>();
        // SAFETY: The allocation is `TRANSACTION_BATCH_META_END` bytes.
        let (meta_ptr, batch_meta_ptr) = unsafe {
            (
                base.byte_add(Self::TRANSACTION_META_START).cast::<M>(),
                base.byte_add(Self::TRANSACTION_BATCH_META_START)
                    .cast::<B>(),
            )
        };
        for (index, (transaction, meta)) in iter.enumerate() {
            // SAFETY: The allocation fits `MAX_TRANSACTIONS_PER_MESSAGE` entries, which
            // is at least `iter.len()`.
            unsafe {
                tx_ptr.add(index).write(transaction);
                meta_ptr.add(index).write(meta);
            }
        }
        // SAFETY: The allocation ends with room for one `B`.
        unsafe { batch_meta_ptr.write(batch_meta) };

        Some(Self {
            tx_ptr,
            meta_ptr,
            num_transactions,
            // SAFETY: `base` was allocated by `allocator`.
            offset: unsafe { allocator.offset(base) },
            _meta: PhantomData,
        })
    }

    /// # Safety
    /// - [`SharableTransactionBatchRegion`] must reference a valid offset and length
    ///   within the `allocator`.
    /// - ALL [`SharableTransactionRegion`]  within the batch must be valid while
    ///   the batch is in use. See [`TransactionPtr::from_sharable_transaction_region`]
    ///   for details.
    /// - `M` and `B` must match the actual `M` and `B` used within this allocation.
    pub unsafe fn from_sharable_transaction_batch_region(
        sharable_transaction_batch_region: &SharableTransactionBatchRegion,
        allocator: &Allocator,
    ) -> Self {
        let () = Self::TRANSACTION_BATCH_SIZE_ASSERT;
        // SAFETY: `sharable_transaction_batch_region.transactions_offset` was allocated by `allocator`.
        let base = unsafe {
            allocator.ptr_from_offset(sharable_transaction_batch_region.transactions_offset)
        };
        let tx_ptr = base.cast();
        // SAFETY:
        // - Assuming the batch was originally allocated to support `M`, this call will also be
        //   safe.
        let meta_ptr = unsafe { base.byte_add(Self::TRANSACTION_META_START).cast() };

        Self {
            tx_ptr,
            meta_ptr,
            num_transactions: usize::from(sharable_transaction_batch_region.num_transactions),
            offset: sharable_transaction_batch_region.transactions_offset,

            _meta: PhantomData,
        }
    }

    /// Translate the batch into the sharable region a worker message carries.
    pub const fn to_sharable_transaction_batch_region(&self) -> SharableTransactionBatchRegion {
        SharableTransactionBatchRegion {
            num_transactions: self.num_transactions as u8,
            transactions_offset: self.offset,
        }
    }

    /// The number of transactions in this batch.
    pub const fn len(&self) -> usize {
        self.num_transactions
    }

    /// Whether the batch is empty.
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The batch's own metadata.
    pub fn batch_meta(&self) -> B
    where
        B: Copy,
    {
        // SAFETY: The batch was written with `B`, after its `M`s.
        unsafe {
            self.tx_ptr
                .byte_add(Self::TRANSACTION_BATCH_META_START)
                .cast::<B>()
                .read()
        }
    }

    /// Replaces the batch's own metadata.
    pub fn set_batch_meta(&mut self, meta: B)
    where
        B: Copy,
    {
        // SAFETY: The batch was written with `B`, after its `M`s.
        unsafe {
            self.tx_ptr
                .byte_add(Self::TRANSACTION_BATCH_META_START)
                .cast::<B>()
                .write(meta)
        }
    }

    /// Iterator returning [`TransactionPtr`] for each transaction in the batch.
    pub fn iter(&self) -> impl Iterator<Item = (TransactionPtr, M)> + '_
    where
        M: Copy,
    {
        // SAFETY: Offsets count from the start of the mapping, so the batch's
        // own offset leads back to it.
        let mapping = unsafe { self.tx_ptr.cast::<u8>().byte_sub(self.offset) };
        // SAFETY: The batch holds `num_transactions` regions and `M`s, each
        // region valid in this mapping per the constructor.
        (0..self.num_transactions).map(move |idx| unsafe {
            let region = self.tx_ptr.add(idx).as_ref();
            let tx = TransactionPtr::from_raw_parts(
                mapping.byte_add(region.offset),
                region.length as usize,
            );
            let meta = self.meta_ptr.add(idx).read();

            (tx, meta)
        })
    }

    /// Free the transaction batch container.
    ///
    /// # Safety
    ///
    /// - The batch must belong to the `allocator`'s mapping.
    /// - [`SharableTransactionBatchRegion`] must be exclusively owned by this pointer.
    ///
    /// # Note
    ///
    /// This will not free the underlying transactions as their lifetimes may be differ from that of
    /// the batch.
    pub unsafe fn free(self, allocator: &Allocator) {
        unsafe { allocator.free(self.tx_ptr.cast()) }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, tempfile::tempfile};

    fn allocator() -> Allocator {
        // SAFETY: A fresh file, joined by no one else.
        unsafe { Allocator::create(&tempfile().unwrap(), 16 * 1024 * 1024, 1, 2 * 1024 * 1024) }
            .unwrap()
    }

    #[test]
    fn from_slice_bounds() {
        let allocator = allocator();
        assert!(TransactionPtr::from_slice(&[], &allocator).is_none());
        assert!(
            TransactionPtr::from_slice(&[0; rts_alloc::MAX_ALLOC_SIZE + 1], &allocator).is_none()
        );

        let tx = TransactionPtr::from_slice(&[7; 300], &allocator).unwrap();
        assert_eq!(tx.data(), &[7; 300]);
        // SAFETY: Allocated above and not shared.
        unsafe { tx.free(&allocator) };
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn batch_from_iter_round_trips_transactions_and_meta() {
        let allocator = allocator();
        let transactions: Vec<_> = (0..3u8)
            .map(|i| {
                let tx = TransactionPtr::from_slice(&[i; 100], &allocator).unwrap();
                // SAFETY: Allocated by `allocator` above.
                unsafe { tx.to_sharable_transaction_region(&allocator) }
            })
            .collect();

        // SAFETY: The transactions above stay allocated until the end of the test.
        let built = unsafe {
            TransactionPtrBatch::<u32, u64>::from_iter(
                transactions
                    .iter()
                    .zip(10..13)
                    .map(|(&tx, meta)| (tx, meta)),
                7,
                &allocator,
            )
        }
        .unwrap();
        let region = built.to_sharable_transaction_batch_region();
        assert_eq!(region.num_transactions, 3);

        // SAFETY: Written above with metadata types `u32` and `u64`; `built` is
        // not used again.
        let batch = unsafe {
            TransactionPtrBatch::<u32, u64>::from_sharable_transaction_batch_region(
                &region, &allocator,
            )
        };
        assert_eq!(batch.batch_meta(), 7);
        let read: Vec<_> = batch
            .iter()
            .map(|(tx, meta)| (tx.data()[0], meta))
            .collect();
        assert_eq!(read, [(0, 10), (1, 11), (2, 12)]);

        // SAFETY: The batch and each transaction are freed exactly once.
        unsafe {
            batch.free(&allocator);
            for tx in &transactions {
                allocator.free(allocator.ptr_from_offset(tx.offset));
            }
        }
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn batch_from_iter_rejects_oversized_batches() {
        let allocator = allocator();
        let tx = TransactionPtr::from_slice(&[1; 100], &allocator).unwrap();
        // SAFETY: Allocated by `allocator` above.
        let region = unsafe { tx.to_sharable_transaction_region(&allocator) };

        let oversized = core::iter::repeat_n((region, ()), MAX_TRANSACTIONS_PER_MESSAGE + 1);
        // SAFETY: `tx` stays allocated until the call returns.
        let batch = unsafe { TransactionPtrBatch::from_iter(oversized, (), &allocator) };
        assert!(batch.is_none());

        // SAFETY: Allocated above and not shared.
        unsafe { tx.free(&allocator) };
        assert_eq!(
            allocator.outstanding_allocation_bytes(),
            0,
            "rejected batches allocate nothing"
        );
    }

    #[test]
    fn empty_batch_carries_batch_meta() {
        let allocator = allocator();
        // SAFETY: There are no transactions to keep allocated.
        let batch = unsafe {
            TransactionPtrBatch::<(), u64>::from_iter(core::iter::empty(), 7, &allocator)
        }
        .unwrap();
        assert!(batch.is_empty());
        assert_eq!(batch.batch_meta(), 7);

        // SAFETY: Allocated above and not shared.
        unsafe { batch.free(&allocator) };
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    #[test]
    fn batch_meta_can_be_replaced() {
        let allocator = allocator();
        let tx = TransactionPtr::from_slice(&[1, 2, 3], &allocator).unwrap();
        // SAFETY: `tx` stays allocated until the end of the test.
        let mut batch = unsafe {
            TransactionPtrBatch::<u32, u64>::from_transactions(
                [(&tx, 5)].into_iter(),
                7,
                &allocator,
            )
        }
        .unwrap();
        batch.set_batch_meta(9);
        assert_eq!(batch.batch_meta(), 9);
        // The transactions and their metadata are untouched
        let (read, meta) = batch.iter().next().unwrap();
        assert_eq!((read.data(), meta), (&[1, 2, 3][..], 5));

        // SAFETY: Allocated above and not shared.
        unsafe { batch.free(&allocator) };
        // SAFETY: As above; the batch no longer references it.
        unsafe { tx.free(&allocator) };
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }
}
