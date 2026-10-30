//! Consumer state for a broadcast queue.
//!
//! This module owns the global consumer-index ownership table and the per-lane
//! reserve-limit slots that publish each consumer's read progress.

use core::mem::{align_of, size_of, MaybeUninit};
use core::ptr::NonNull;
use core::sync::atomic::{fence, AtomicU64, AtomicUsize, Ordering};

use crate::error::Error;
use crate::CacheAlignedAtomicSize;

const CONSUMER_FREE: u64 = 0;
const CONSUMER_ACTIVE: u64 = 1;
const CONSUMER_JOINING: u64 = 2;

pub(crate) enum ConsumerRecoveryMode {
    /// The global ownership slot was active, which proves the previous join
    /// installed a final cursor on every lane. Recovery can reconstruct those
    /// cursors from the surviving reserve limits and continue from them.
    Resume,

    /// The global ownership slot was free or still joining, so its per-lane
    /// limits may be absent or may only be provisional handshake values.
    /// Recovery must join every lane again at its reservation frontier before
    /// marking the consumer active.
    RestartJoin,
}

/// Value stored in a lane consumer slot that no consumer owns.
///
/// A real reserve limit never reaches `usize::MAX` — that would take millennia
/// at any real publish rate — so this is an unambiguous "no constraint"
/// sentinel: it sits at the top, so it drops out of the producer's `min` and
/// never gates a reserve.
const UNCLAIMED: usize = usize::MAX;

/// Global consumer-index ownership table.
///
/// The table is a contiguous `[AtomicU64; consumer_slots]` block. Each slot is
/// free, joining its lanes, or active.
#[derive(Clone, Copy)]
pub(crate) struct ConsumerState {
    slots: NonNull<AtomicU64>,
    slot_count: usize,
}

impl ConsumerState {
    pub(crate) fn block_size(slot_count: usize) -> Option<usize> {
        slot_count.checked_mul(size_of::<AtomicU64>())
    }

    pub(crate) const fn block_align() -> usize {
        align_of::<AtomicU64>()
    }

    /// Initializes a consumer ownership table with every index free.
    ///
    /// # Safety
    /// - `block` must point at a [`Self::block_size`] region for `slot_count`
    ///   slots and be initialized at most once.
    pub(crate) unsafe fn init(block: NonNull<u8>, slot_count: usize) {
        let mut slots =
            NonNull::slice_from_raw_parts(block.cast::<MaybeUninit<AtomicU64>>(), slot_count);
        // SAFETY: layout reserves `slot_count` AtomicU64s here; init runs once
        // with no other handle joined, so &mut is exclusive.
        let slots = unsafe { slots.as_mut() };
        for slot in slots {
            slot.write(AtomicU64::new(CONSUMER_FREE));
        }
    }

    /// Builds a view over an initialized consumer ownership table.
    ///
    /// # Safety
    /// - `block` must reference a table initialized by [`Self::init`] with the
    ///   same `slot_count`, alive for the view's use.
    pub(crate) unsafe fn from_block(block: NonNull<u8>, slot_count: usize) -> Self {
        Self {
            slots: block.cast(),
            slot_count,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.slot_count
    }

    /// Claims a free consumer index in the global ownership table. The index
    /// remains in the joining phase until every lane has installed its cursor.
    pub(crate) fn acquire(&self) -> Result<usize, Error> {
        for index in 0..self.slot_count {
            if self
                .slot(index)
                .compare_exchange(
                    CONSUMER_FREE,
                    CONSUMER_JOINING,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Ok(index);
            }
        }
        Err(Error::ConsumerSlotsExhausted)
    }

    /// Marks a joining consumer active after all of its lane cursors have been
    /// installed.
    pub(crate) fn activate(&self, index: usize) {
        self.slot(index).store(CONSUMER_ACTIVE, Ordering::Release);
    }

    /// Releases a consumer index back to free.
    pub(crate) fn release(&self, index: usize) {
        self.slot(index).store(CONSUMER_FREE, Ordering::Release);
    }

    /// Begins recovery of an index whose previous owner died. An active consumer
    /// has complete lane cursors and can resume them. A free or joining index is
    /// kept in the joining phase so recovery can restart every lane safely.
    ///
    /// The caller guarantees that no live handle uses the index and serializes
    /// recovery with joins, drops, and other recovery operations.
    pub(crate) fn begin_recovery(&self, index: usize) -> ConsumerRecoveryMode {
        let slot = self.slot(index);
        if slot.load(Ordering::Acquire) == CONSUMER_ACTIVE {
            ConsumerRecoveryMode::Resume
        } else {
            slot.store(CONSUMER_JOINING, Ordering::Release);
            ConsumerRecoveryMode::RestartJoin
        }
    }

    /// The global ownership word for consumer index `index`.
    #[inline]
    fn slot(&self, index: usize) -> &AtomicU64 {
        debug_assert!(index < self.slot_count);
        // SAFETY: `index < slot_count`, so the offset stays in bounds.
        let slot = unsafe { self.slots.add(index) };
        // SAFETY: the slot was initialized at construction.
        unsafe { slot.as_ref() }
    }
}

/// Per-lane consumer reserve-limit slots.
///
/// A slot does **not** store the consumer's raw read cursor. It stores that
/// consumer's **reserve limit**: `next_to_read + capacity`, i.e. the lowest
/// sequence the producer may NOT yet reserve to avoid overwriting an active
/// read. An unowned slot holds [`UNCLAIMED`].
#[derive(Clone, Copy)]
pub(crate) struct LaneConsumerState {
    limits: NonNull<CacheAlignedAtomicSize>,
    slot_count: usize,
    capacity: usize,
}

impl LaneConsumerState {
    pub(crate) fn block_size(slot_count: usize) -> Option<usize> {
        slot_count.checked_mul(size_of::<CacheAlignedAtomicSize>())
    }

    pub(crate) const fn block_align() -> usize {
        align_of::<CacheAlignedAtomicSize>()
    }

    /// Initializes a lane's consumer reserve-limit slots as unowned.
    ///
    /// # Safety
    /// - `block` must point at a [`Self::block_size`] region for `slot_count`
    ///   slots and be initialized at most once.
    pub(crate) unsafe fn init(block: NonNull<u8>, slot_count: usize) {
        let mut slots = NonNull::slice_from_raw_parts(
            block.cast::<MaybeUninit<CacheAlignedAtomicSize>>(),
            slot_count,
        );
        // SAFETY: layout reserves `slot_count` aligned slots here; init runs
        // once with no other handle joined, so &mut is exclusive.
        let slots = unsafe { slots.as_mut() };
        for slot in slots {
            slot.write(CacheAlignedAtomicSize {
                inner: AtomicUsize::new(UNCLAIMED),
            });
        }
    }

    /// Builds a view over initialized per-lane consumer slots.
    ///
    /// # Safety
    /// - `block` must reference slots initialized by [`Self::init`] with the
    ///   same `slot_count`, alive for the view's use.
    pub(crate) unsafe fn from_block(
        block: NonNull<u8>,
        slot_count: usize,
        capacity: usize,
    ) -> Self {
        Self {
            limits: block.cast(),
            slot_count,
            capacity,
        }
    }

    /// The binding reserve limit across consumers: the lowest sequence the
    /// producer may NOT yet reserve. Each slot holds `next_to_read + capacity`;
    /// unowned slots hold [`UNCLAIMED`] and so drop out of the `min`. With every
    /// slot unowned the result is [`UNCLAIMED`] (no constraint).
    pub(crate) fn reserve_limit(&self) -> usize {
        self.limits()
            .iter()
            .map(|limit| limit.load(Ordering::Acquire))
            .min()
            .unwrap_or(UNCLAIMED)
    }

    /// Joins `consumer_index` to this lane at the current reservation frontier.
    ///
    /// The caller must already own `consumer_index` through the broadcast's
    /// global consumer-ownership state, so this slot has a single writer (the
    /// owning consumer): no CAS is needed — a release store publishes the limit.
    ///
    /// The first reservation sample supplies an initial limit that permits a
    /// full ring of producer progress. After publishing that limit, the consumer
    /// fences and samples the reservation again. The producer either observes
    /// the initial limit or the second sample observes its previously committed
    /// frontier. A reservation racing after the second sample starts at that
    /// frontier, so it is future data for this consumer rather than an overwrite
    /// of a cell the consumer may read.
    pub(crate) fn join(&self, consumer_index: usize, read_reserved: impl Fn() -> usize) -> usize {
        let initial_start = read_reserved();
        let initial_limit = initial_start.wrapping_add(self.capacity);
        debug_assert!(initial_limit != UNCLAIMED);
        self.limit(consumer_index)
            .store(initial_limit, Ordering::Release);

        // Consumer half of the join handshake: order the initial limit before
        // re-reading the reservation frontier. This pairs with the producer's
        // fence before it reads reserve limits.
        fence(Ordering::SeqCst);

        let start = read_reserved();
        let limit = start.wrapping_add(self.capacity);
        debug_assert!(limit != UNCLAIMED);
        self.limit(consumer_index).store(limit, Ordering::Release);
        start
    }

    /// Publishes consumer `consumer_index`'s progress: its next-to-read sequence
    /// `next_to_read`. The slot stores the reserve limit `next_to_read +
    /// capacity` — the lowest sequence the producer may not yet overwrite for
    /// this consumer.
    pub(crate) fn set_cursor(&self, consumer_index: usize, next_to_read: usize) {
        let limit = next_to_read.wrapping_add(self.capacity);
        debug_assert!(limit != UNCLAIMED);
        self.limit(consumer_index).store(limit, Ordering::Release);
    }

    /// Releases consumer slot `consumer_index` back to unowned (the reserve
    /// limit then ignores it).
    pub(crate) fn release(&self, consumer_index: usize) {
        self.limit(consumer_index)
            .store(UNCLAIMED, Ordering::Release);
    }

    /// Reconstructs a recovering consumer's next-to-read on this lane from its
    /// surviving reserve limit (`next_to_read + capacity`), so it resumes where
    /// the dead owner left off — those unread cells are still pinned by the
    /// limit. This only reads the slot, so this consumer's backpressure is never
    /// dropped. If the slot was never claimed, start fresh at the reservation
    /// frontier.
    pub(crate) fn recover(
        &self,
        consumer_index: usize,
        read_reserved: impl Fn() -> usize,
    ) -> usize {
        let limit = self.limit(consumer_index).load(Ordering::Acquire);
        if limit == UNCLAIMED {
            self.join(consumer_index, read_reserved)
        } else {
            limit.wrapping_sub(self.capacity)
        }
    }

    #[inline]
    fn limits(&self) -> &[CacheAlignedAtomicSize] {
        let limits = NonNull::slice_from_raw_parts(self.limits, self.slot_count);
        // SAFETY: layout reserves `slot_count` aligned slots here.
        unsafe { limits.as_ref() }
    }

    /// The slot holding consumer `consumer_index`'s reserve limit
    /// (`next_to_read + capacity`, or [`UNCLAIMED`]).
    #[inline]
    fn limit(&self, consumer_index: usize) -> &CacheAlignedAtomicSize {
        debug_assert!(consumer_index < self.slot_count);
        // SAFETY: `consumer_index < slot_count`, so the offset stays in bounds.
        let limit = unsafe { self.limits.add(consumer_index) };
        // SAFETY: the slot was initialized at construction.
        unsafe { limit.as_ref() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::Region;
    use std::cell::Cell;
    use std::num::NonZeroUsize;

    fn consumer_state(slot_count: usize) -> (std::sync::Arc<Region>, ConsumerState) {
        let size = ConsumerState::block_size(slot_count).unwrap().max(1);
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: freshly allocated block, initialized once.
        unsafe { ConsumerState::init(region.addr(), slot_count) };
        // SAFETY: just initialized with this slot count.
        let state = unsafe { ConsumerState::from_block(region.addr(), slot_count) };
        (region, state)
    }

    fn lane_consumer_state(
        slot_count: usize,
        capacity: usize,
    ) -> (std::sync::Arc<Region>, LaneConsumerState) {
        let size = LaneConsumerState::block_size(slot_count).unwrap().max(1);
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: freshly allocated block, initialized once.
        unsafe { LaneConsumerState::init(region.addr(), slot_count) };
        // SAFETY: just initialized with this slot count.
        let state = unsafe { LaneConsumerState::from_block(region.addr(), slot_count, capacity) };
        (region, state)
    }

    #[test]
    fn consumer_state_acquires_releases_and_recovers_indices() {
        let (_region, state) = consumer_state(2);

        assert_eq!(state.len(), 2);
        assert_eq!(state.acquire().unwrap(), 0);
        assert_eq!(state.acquire().unwrap(), 1);
        assert!(matches!(
            state.acquire(),
            Err(Error::ConsumerSlotsExhausted)
        ));

        assert!(matches!(
            state.begin_recovery(0),
            ConsumerRecoveryMode::RestartJoin
        ));
        state.activate(0);
        assert!(matches!(
            state.begin_recovery(0),
            ConsumerRecoveryMode::Resume
        ));

        state.release(0);
        assert_eq!(state.acquire().unwrap(), 0);

        state.release(1);
        assert!(matches!(
            state.begin_recovery(1),
            ConsumerRecoveryMode::RestartJoin
        ));
        assert!(matches!(
            state.acquire(),
            Err(Error::ConsumerSlotsExhausted)
        ));
    }

    #[test]
    fn consumer_state_zero_slots_are_exhausted() {
        let (_region, state) = consumer_state(0);

        assert_eq!(state.len(), 0);
        assert!(matches!(
            state.acquire(),
            Err(Error::ConsumerSlotsExhausted)
        ));
    }

    #[test]
    fn lane_consumer_state_tracks_minimum_reserve_limit() {
        let (_region, state) = lane_consumer_state(2, 4);

        assert_eq!(state.reserve_limit(), UNCLAIMED);

        assert_eq!(state.join(0, || 10), 10);
        assert_eq!(state.reserve_limit(), 14);

        state.set_cursor(0, 12);
        assert_eq!(state.reserve_limit(), 16);

        assert_eq!(state.join(1, || 10), 10);
        assert_eq!(state.reserve_limit(), 14);

        state.release(1);
        assert_eq!(state.reserve_limit(), 16);

        state.release(0);
        assert_eq!(state.reserve_limit(), UNCLAIMED);
    }

    #[test]
    fn lane_consumer_state_join_resamples_reservation_frontier() {
        let (_region, state) = lane_consumer_state(1, 4);
        let samples = [10, 13];
        let sample_index = Cell::new(0);

        assert_eq!(
            state.join(0, || {
                let index = sample_index.get();
                sample_index.set(index + 1);
                let sample = samples[index];
                if sample == 13 {
                    assert_eq!(state.reserve_limit(), 14);
                }
                sample
            }),
            13
        );
        assert_eq!(sample_index.get(), 2);
        assert_eq!(state.reserve_limit(), 17);
    }

    #[test]
    fn lane_consumer_state_recover_resumes_claimed_slot() {
        let (_region, state) = lane_consumer_state(1, 4);

        state.set_cursor(0, 7);
        assert_eq!(
            state.recover(0, || panic!("claimed slot should not rejoin")),
            7
        );
        assert_eq!(state.reserve_limit(), 11);
    }

    #[test]
    fn lane_consumer_state_recover_unclaimed_slot_joins_frontier() {
        let (_region, state) = lane_consumer_state(1, 4);

        assert_eq!(state.recover(0, || 12), 12);
        assert_eq!(state.reserve_limit(), 16);
    }
}
