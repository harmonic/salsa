use {
    agave_scheduler_bindings::{
        SharablePubkeys,
        worker_message_types::{CheckResponse, resolve_flags},
    },
    rts_alloc::Allocator,
    solana_pubkey::Pubkey,
    std::ptr::NonNull,
};

#[derive(Debug)]
pub struct PubkeysPtr {
    ptr: NonNull<Pubkey>,
    count: usize,
}

// SAFETY: The pointee lives in the shared mapping, which is valid on every
// thread, and any allocator handle can free it.
unsafe impl Send for PubkeysPtr {}

impl PubkeysPtr {
    /// Constructions a [`PubkeysPtr`] from raw parts.
    ///
    /// # Safety
    ///
    /// - `ptr` must be valid for reads.
    /// - `count` must be accurate (in number of pubkeys) and not overrun the end of `ptr`.
    ///
    /// # Note
    ///
    /// If you are trying to construct a pointer for use by Agave, you almost certainly want to use
    /// [`Self::from_sharable_pubkeys`].
    pub unsafe fn from_raw_parts(ptr: NonNull<Pubkey>, count: usize) -> Self {
        Self { ptr, count }
    }

    /// Constructs the pointer from a [`SharablePubkeys`].
    ///
    /// An empty region has no allocation and yields an empty pointer.
    ///
    /// # Safety
    ///
    /// - `sharable_pubkeys.offset` must have been allocated by `allocator`, unless
    ///   `sharable_pubkeys.num_pubkeys` is 0.
    /// - The allocation pointed to by this region must not have previously been freed.
    /// - Pointer must be exclusive so that calling [`Self::free`] is safe.
    /// - `sharable_pubkeys.num_pubkeys` must be accurate and not overrun the allocation.
    pub unsafe fn from_sharable_pubkeys(
        sharable_pubkeys: &SharablePubkeys,
        allocator: &Allocator,
    ) -> Self {
        if sharable_pubkeys.num_pubkeys == 0 {
            return Self {
                ptr: NonNull::dangling(),
                count: 0,
            };
        }
        // SAFETY: `sharable_pubkeys.offset` was allocated by `allocator`.
        let ptr = unsafe { allocator.ptr_from_offset(sharable_pubkeys.offset) }.cast();

        Self {
            ptr,
            count: sharable_pubkeys.num_pubkeys as usize,
        }
    }

    /// The pubkeys `response` resolved, or `None` if resolution was not
    /// performed, which leaves `response.resolved_pubkeys` undefined.
    ///
    /// # Safety
    ///
    /// Same as [`Self::from_sharable_pubkeys`] for `response.resolved_pubkeys`.
    pub unsafe fn from_check_response(
        response: &CheckResponse,
        allocator: &Allocator,
    ) -> Option<Self> {
        // SAFETY: Defined when resolution was performed; the caller upholds the rest.
        (response.resolve_flags & resolve_flags::PERFORMED != 0)
            .then(|| unsafe { Self::from_sharable_pubkeys(&response.resolved_pubkeys, allocator) })
    }

    /// Returns the allocation as a slice.
    pub fn as_slice(&self) -> &[Pubkey] {
        // SAFETY
        // - Constructor invariants guarantee that we don't overrun the end of the allocation.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.count) }
    }

    /// Frees the underlying allocation.
    ///
    /// # Safety
    ///
    /// - `Self` must be exclusively owned.
    pub unsafe fn free(self, allocator: &Allocator) {
        // An empty pointer has no allocation: rts-alloc rejects zero-sized ones
        if self.count == 0 {
            return;
        }
        // SAFETY
        // - Caller guarantees that we exclusively own this pointer.
        unsafe { allocator.free(self.ptr.cast()) };
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

    fn check_response(resolve_flags: u8, resolved_pubkeys: SharablePubkeys) -> CheckResponse {
        CheckResponse {
            resolve_flags,
            resolved_pubkeys,
            // SAFETY: `CheckResponse` is plain data; all-zero is a valid value.
            ..unsafe { core::mem::zeroed() }
        }
    }

    fn pubkeys(allocator: &Allocator, keys: &[Pubkey]) -> SharablePubkeys {
        let ptr = allocator
            .allocate(core::mem::size_of_val(keys) as u32)
            .unwrap()
            .cast::<Pubkey>();
        // SAFETY: A fresh allocation sized for `keys`.
        unsafe {
            core::ptr::copy_nonoverlapping(keys.as_ptr(), ptr.as_ptr(), keys.len());
            SharablePubkeys {
                offset: allocator.offset(ptr.cast()),
                num_pubkeys: keys.len() as u32,
            }
        }
    }

    #[test]
    fn resolved_pubkeys_are_read_and_freed() {
        let allocator = allocator();
        let keys = [Pubkey::new_unique(), Pubkey::new_unique()];
        let responses = [
            check_response(resolve_flags::PERFORMED, pubkeys(&allocator, &keys)),
            // Performed, but nothing resolved: no allocation.
            check_response(
                resolve_flags::PERFORMED,
                SharablePubkeys {
                    offset: 0,
                    num_pubkeys: 0,
                },
            ),
            // Not performed: `resolved_pubkeys` is undefined and must be ignored.
            check_response(
                resolve_flags::FAILED,
                SharablePubkeys {
                    offset: usize::MAX,
                    num_pubkeys: 7,
                },
            ),
        ];

        let resolved: Vec<_> = responses
            .iter()
            // SAFETY: The pubkeys were allocated above.
            .map(|response| unsafe { PubkeysPtr::from_check_response(response, &allocator) })
            .collect();
        let read: Vec<_> = resolved
            .iter()
            .map(|pubkeys| pubkeys.as_ref().map(|pubkeys| pubkeys.as_slice().to_vec()))
            .collect();
        assert_eq!(read, [Some(keys.to_vec()), Some(vec![]), None]);

        for pubkeys in resolved.into_iter().flatten() {
            // SAFETY: Each allocation is freed once; the empty pointer owns none.
            unsafe { pubkeys.free(&allocator) };
        }
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }
}
