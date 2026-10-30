use core::sync::atomic::AtomicUsize;

// NB: To simplify casting we only support 64bit or wider systems.
const _: () = assert!(size_of::<usize>() >= size_of::<u64>());

pub mod broadcast;
pub mod error;
mod futex;
pub mod mpmc;
mod shmem;
pub mod spsc;

/// Stored queue identifier when no identifier is supplied.
pub(crate) const DEFAULT_QUEUE_IDENTIFIER: u64 = 0;

pub(crate) const VERSION_MAJOR: u16 = 3;
pub(crate) const VERSION_PATCH: u16 = 0;
/// Packed shared-memory ABI version.
pub const VERSION: u32 = (VERSION_MAJOR as u32) << 16 | VERSION_PATCH as u32;

/// Computes the unrounded byte size, preserving a header-only size for zero capacity.
pub(crate) const fn checked_queue_size(
    capacity: usize,
    item_size: usize,
    buffer_offset: usize,
) -> Option<usize> {
    let capacity = if capacity == 0 {
        0
    } else {
        match capacity.checked_next_power_of_two() {
            Some(capacity) => capacity,
            None => return None,
        }
    };
    let buffer_size = match capacity.checked_mul(item_size) {
        Some(size) => size,
        None => return None,
    };
    buffer_offset.checked_add(buffer_size)
}

/// `AtomicUsize` with 64-byte alignment for better performance.
#[derive(Default)]
#[repr(C, align(64))]
struct CacheAlignedAtomicSize {
    inner: AtomicUsize,
}

impl core::ops::Deref for CacheAlignedAtomicSize {
    type Target = AtomicUsize;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::{checked_queue_size, error::Error::InvalidBufferSize, mpmc, spsc};

    #[test]
    fn sizing() {
        for size in [
            spsc::try_minimum_file_size::<u64>,
            spsc::try_minimum_region_size::<u64>,
            mpmc::try_minimum_file_size::<u64>,
            mpmc::try_minimum_region_size::<u64>,
        ] {
            let header_size = size(0).unwrap();
            for (capacity, normalized) in [(0, 0), (1, 1), (2, 2), (3, 4), (4, 4), (5, 8)] {
                let actual_size = size(capacity).unwrap();
                let expected_size = header_size + normalized * size_of::<u64>();
                assert_eq!(actual_size, expected_size);
            }
            // Normalization overflow and multiplication overflow.
            for capacity in [usize::MAX, 1usize << (usize::BITS - 3)] {
                assert!(matches!(size(capacity), Err(InvalidBufferSize)));
            }
        }
        // Multiplication fits; header addition overflows only in the second case.
        assert_eq!(checked_queue_size(2, usize::MAX / 2, 1), Some(usize::MAX));
        assert_eq!(checked_queue_size(2, usize::MAX / 2, 2), None);
    }

    #[test]
    fn legacy_sizing_panics_on_overflow() {
        for size in [
            spsc::minimum_file_size::<u64>,
            spsc::minimum_region_size::<u64>,
            mpmc::minimum_file_size::<u64>,
            mpmc::minimum_region_size::<u64>,
        ] {
            for capacity in [usize::MAX, 1usize << (usize::BITS - 3)] {
                assert!(std::panic::catch_unwind(|| size(capacity)).is_err());
            }
        }
    }
}
