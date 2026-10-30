//! One producer's lane: a self-contained shared-memory block holding the lane's
//! ownership state, its reserve/publication cursors, the per-consumer reserve
//! limits, and the ring of payloads. See [`ProducerLane`].

use core::alloc::Layout;
use core::mem::{align_of, size_of};
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::sync::atomic::{fence, AtomicU64, Ordering};
use std::marker::PhantomData;

use crate::broadcast::{InitializedLane, LaneIndex, ProducerId, UnverifiedLane};
use crate::CacheAlignedAtomicSize;

use super::consumer_state::LaneConsumerState;

const LANE_FREE: u64 = 0;
const LANE_CLAIMING: u64 = 1;
const LANE_ACTIVE: u64 = 2;
const LANE_RETIRED: u64 = 3;

/// Fixed-size head of a producer-lane block.
#[repr(C)]
pub(super) struct LaneHeader {
    /// Lane ownership: `LANE_FREE`, `LANE_CLAIMING`, `LANE_ACTIVE`, or `LANE_RETIRED`.
    state: AtomicU64,
    /// Supplied [`ProducerId`], meaningful once the lane is active or retired.
    producer_id: AtomicU64,
    /// Count of messages refused by backpressure.
    rejected_items: AtomicU64,
    /// Claimed-up-to sequence: advanced before a ring cell is written.
    producer_reservation: CacheAlignedAtomicSize,
    /// Visible-up-to sequence: advanced after a ring cell is written; consumers
    /// read sequences `< producer_publication`.
    producer_publication: CacheAlignedAtomicSize,
}

/// A single producer's lane.
///
/// The lane holds ownership state, the reserve/publication cursors, the
/// per-lane consumer reserve-limit state, and the ring of fixed-size payload
/// cells. The owning
/// producer mutates the ring and producer cursors (`&mut self`); consumers read
/// published payloads and publish their own progress through [`LaneConsumerState`].
///
/// Block layout: `LaneHeader`, then `[CacheAlignedAtomicSize; consumer_slots]`
/// limits (one per cache line), then the payload ring.
pub(crate) struct ProducerLane {
    header: NonNull<LaneHeader>,
    consumer_state: LaneConsumerState,
    ring: NonNull<u8>,
    payload_size: usize,

    mask: usize, // capacity - 1
}

/// A borrowed view of one producer lane's metadata.
///
/// The view cannot outlive the broadcast mapping from which it was obtained.
#[derive(Clone, Copy)]
pub struct LaneMetadata<'a> {
    header: &'a LaneHeader,
    lane: LaneIndex<InitializedLane>,
}

impl<'a> LaneMetadata<'a> {
    /// Builds a metadata view if the lane has completed acquisition.
    pub(super) fn try_new(header: &'a LaneHeader, lane: LaneIndex<UnverifiedLane>) -> Option<Self> {
        let state = header.state.load(Ordering::Acquire);

        let lane_is_acquired = matches!(state, LANE_ACTIVE | LANE_RETIRED);
        if !lane_is_acquired {
            return None;
        }

        Some(Self {
            header,
            lane: LaneIndex {
                index: lane.get(),
                _state: PhantomData,
            },
        })
    }

    /// Builds a metadata view over a borrowed lane header.
    pub(super) fn from_initialized_lane(
        header: &'a LaneHeader,
        lane: LaneIndex<InitializedLane>,
    ) -> Self {
        Self { header, lane }
    }

    /// The index of this producer lane.
    #[inline]
    pub fn lane(&self) -> usize {
        self.lane.get()
    }

    /// The producer chosen [`ProducerId`] permanently associated with this lane.
    #[inline]
    pub fn producer_id(&self) -> ProducerId {
        ProducerId::new(self.header.producer_id.load(Ordering::Relaxed))
    }

    /// Count of messages refused by backpressure on this lane.
    #[inline]
    pub fn rejected_items(&self) -> u64 {
        self.header.rejected_items.load(Ordering::Relaxed)
    }
}

impl core::fmt::Debug for LaneMetadata<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LaneMetadata")
            .field("lane", &self.lane())
            .field("producer_id", &self.producer_id())
            .field("rejected_items", &self.rejected_items())
            .finish()
    }
}

#[inline]
const fn consumer_state_offset() -> usize {
    size_of::<LaneHeader>().next_multiple_of(LaneConsumerState::block_align())
}

pub(crate) fn ring_offset_for_payload(
    consumer_slots: usize,
    payload_layout: Layout,
) -> Option<usize> {
    if payload_layout.align() > ProducerLane::block_align() {
        return None;
    }

    consumer_state_offset()
        .checked_add(LaneConsumerState::block_size(consumer_slots)?)?
        .checked_next_multiple_of(payload_layout.align())
}

pub(crate) fn block_size_for_payload(
    capacity: u32,
    consumer_slots: usize,
    payload_layout: Layout,
) -> Option<usize> {
    ring_offset_for_payload(consumer_slots, payload_layout)?
        .checked_add((capacity as usize).checked_mul(payload_layout.size())?)
}

impl ProducerLane {
    pub(crate) const fn block_align() -> usize {
        align_of::<LaneHeader>()
    }

    /// Initializes a lane block: ownership/cursors zeroed, every consumer slot
    /// free. The ring is left uninitialized (each cell is written before it is
    /// published, and read only after).
    ///
    /// # Safety
    /// - `block` must point at a [`block_size_for_payload`] region for the
    ///   queue's `(capacity, consumer_slots, payload_layout)` and be initialized
    ///   at most once.
    pub(crate) unsafe fn init(block: NonNull<u8>, consumer_slots: usize) {
        let header = LaneHeader {
            state: AtomicU64::new(LANE_FREE),
            producer_id: AtomicU64::new(0),
            rejected_items: AtomicU64::new(0),
            producer_reservation: CacheAlignedAtomicSize::default(),
            producer_publication: CacheAlignedAtomicSize::default(),
        };
        // SAFETY: `block` begins with a `LaneHeader`.
        unsafe { block.cast().write(header) };
        // SAFETY: layout reserves `consumer_slots` aligned slots here.
        let consumer_state = unsafe { block.byte_add(consumer_state_offset()) };
        // SAFETY: freshly initialized lane block; consumer slots are initialized once.
        unsafe { LaneConsumerState::init(consumer_state, consumer_slots) };
    }

    /// Builds a lane view over an initialized block.
    ///
    /// # Safety
    /// - `block` must reference a block initialized by [`Self::init`] with the
    ///   same `consumer_slots`, sized for `(capacity, payload_layout)`, alive
    ///   for the view's use.
    pub(crate) unsafe fn from_block(
        block: NonNull<u8>,
        capacity: u32,
        consumer_slots: usize,
        payload_layout: Layout,
    ) -> Self {
        debug_assert!(capacity.is_power_of_two());

        let header = block.cast();
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for consumer state to fit (if init succeeded).
        let consumer_state_block = unsafe { block.byte_add(consumer_state_offset()) };
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for consumer state to fit (if init succeeded).
        let consumer_state = unsafe {
            LaneConsumerState::from_block(consumer_state_block, consumer_slots, capacity as usize)
        };
        let ring_offset =
            ring_offset_for_payload(consumer_slots, payload_layout).expect("validated_lane layout");
        // SAFETY: `block` is an initialized region - it must be large enough
        //         for ring data to fit (if init succeeded).
        let ring = unsafe { block.byte_add(ring_offset) };

        Self {
            header,
            consumer_state,
            ring,
            payload_size: payload_layout.size(),
            mask: (capacity as usize).wrapping_sub(1),
        }
    }

    /// Returns borrowed metadata for this lane.
    #[inline]
    pub(crate) fn metadata(&self, lane: LaneIndex<InitializedLane>) -> LaneMetadata<'_> {
        LaneMetadata::from_initialized_lane(self.header(), lane)
    }

    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.mask.wrapping_add(1)
    }

    #[inline]
    pub(crate) fn mask(&self, sequence: usize) -> usize {
        sequence & self.mask
    }

    #[inline]
    fn header(&self) -> &LaneHeader {
        // SAFETY: The lane has an initialized header that lives for the lane's lifetime.
        unsafe { self.header.as_ref() }
    }

    #[inline]
    pub(crate) fn consumer_state(&self) -> LaneConsumerState {
        self.consumer_state
    }

    /// Pointer to the ring cell holding `sequence` — used by the producer to
    /// write a reserved cell and by consumers to read a published one.
    #[inline]
    pub(crate) fn payload_ptr(&self, sequence: usize) -> NonNull<u8> {
        let offset = self.mask(sequence).wrapping_mul(self.payload_size);
        // SAFETY: `sequence & mask < capacity`; the ring has `capacity` cells of
        // `payload_size` bytes. Zero-sized payloads always point at the ring base.
        unsafe { self.ring.byte_add(offset) }
    }

    /// Claims the lane for a producer, installing its `producer_id`. Returns
    /// `false` if already owned.
    #[must_use]
    pub(crate) fn try_acquire(&self, producer_id: ProducerId) -> bool {
        let acquire_result = self.header().state.compare_exchange(
            LANE_FREE,
            LANE_CLAIMING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        if acquire_result.is_err() {
            return false;
        }

        self.header()
            .producer_id
            .store(producer_id.get(), Ordering::Relaxed);

        self.header().state.store(LANE_ACTIVE, Ordering::Release);

        true
    }

    /// Permanently retires the lane. A retired lane never returns to the free
    /// pool, so a lane binds to at most one producer for the queue's lifetime.
    pub(crate) fn retire(&self) {
        let _ = self.header().state.compare_exchange(
            LANE_ACTIVE,
            LANE_RETIRED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Reserves `count` consecutive sequences for writing, returning the first.
    /// `None` on backpressure: the batch would overwrite a cell an active
    /// consumer has not yet read, or it exceeds the ring capacity.
    /// On success this returns Some(seqnum) - with seqnum being
    /// the starting sequence number of the reservation.
    ///
    /// Write each reserved cell via [`Self::payload_ptr`], then [`Self::publish`].
    pub(crate) fn try_reserve(&mut self, count: NonZeroUsize) -> Option<usize> {
        if count.get() > self.capacity() {
            return None;
        }
        let start = self.header().producer_reservation.load(Ordering::Acquire);
        // Producer half of the join handshake: order the previous reserve's
        // `producer_reservation` store before this reserve's limit loads. A
        // racing consumer publishes a limit from its first reservation sample,
        // fences, then samples again. Either this reserve observes that limit or
        // the consumer observes our committed frontier. If both race with this
        // reserve's later store, the consumer starts at `start`, making this
        // reservation future data rather than an overwrite.
        fence(Ordering::SeqCst);
        // Each slot already stores `next_to_read + capacity`, so the gate is a
        // plain comparison: rejecting once the batch would reach a sequence a
        // consumer still needs. Unowned slots sit at the top, so they never
        // gate.
        if start.wrapping_add(count.get()) > self.consumer_state.reserve_limit() {
            self.header()
                .rejected_items
                .fetch_add(count.get() as u64, Ordering::Relaxed);
            return None;
        }
        // Claim before the writes; consumers only read `< producer_publication`.
        self.header()
            .producer_reservation
            .store(start.wrapping_add(count.get()), Ordering::Release);
        Some(start)
    }

    /// Publishes `start..start + count`, making it visible to consumers. Call
    /// after the cells are written.
    pub(crate) fn publish(&mut self, start: usize, count: NonZeroUsize) {
        self.header()
            .producer_publication
            .store(start.wrapping_add(count.get()), Ordering::Release);
    }
    #[inline]
    pub(crate) fn published(&self) -> usize {
        self.header().producer_publication.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn reserved(&self) -> usize {
        self.header().producer_reservation.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::Region;
    use std::num::NonZeroUsize;

    type Payload = u64;

    const BOGUS_PRODUCER_ID: ProducerId = ProducerId::new(1111);

    /// Allocates and initializes a standalone lane block.
    fn lane(capacity: u32, consumer_slots: usize) -> (std::sync::Arc<Region>, ProducerLane) {
        let payload_layout = Layout::new::<Payload>();
        let size = block_size_for_payload(capacity, consumer_slots, payload_layout).unwrap();
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).unwrap();
        // SAFETY: freshly allocated block, initialized once.
        unsafe { ProducerLane::init(region.addr(), consumer_slots) };
        // SAFETY: just initialized with these parameters.
        let lane = unsafe {
            ProducerLane::from_block(region.addr(), capacity, consumer_slots, payload_layout)
        };
        (region, lane)
    }

    fn read(lane: &ProducerLane, sequence: usize) -> Payload {
        // SAFETY: `sequence` was published, so its cell is initialized.
        unsafe { lane.payload_ptr(sequence).cast().read() }
    }

    fn join_consumer(lane: &ProducerLane, consumer_index: usize) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.join(consumer_index, || lane.reserved())
    }

    fn metadata(lane: &ProducerLane) -> LaneMetadata<'_> {
        LaneMetadata::try_new(lane.header(), LaneIndex::new(0)).expect("lane is active or retired")
    }

    /// Reserves, writes, and publishes one value; `false` on backpressure.
    fn publish_value(lane: &mut ProducerLane, value: Payload) -> bool {
        let one = NonZeroUsize::new(1).unwrap();
        let Some(start) = lane.try_reserve(one) else {
            return false;
        };
        // SAFETY: the cell is reserved and not yet published.
        unsafe { lane.payload_ptr(start).cast().write(value) };
        lane.publish(start, one);
        true
    }

    #[test]
    fn lane_ownership_is_exclusive() {
        let (_region, lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        assert!(!lane.try_acquire(BOGUS_PRODUCER_ID));
    }

    #[test]
    fn retired_lane_is_never_reclaimed() {
        let (_region, lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        lane.retire();
        assert!(!lane.try_acquire(BOGUS_PRODUCER_ID));
    }

    #[test]
    fn never_owned_lane_has_not_completed_acquisition() {
        let (_region, lane) = lane(4, 1);

        let metadata = LaneMetadata::try_new(lane.header(), LaneIndex::new(0));

        assert!(metadata.is_none());
    }

    #[test]
    fn claiming_lane_has_not_completed_acquisition() {
        let (_region, lane) = lane(4, 1);
        let header = lane.header();
        header
            .state
            .compare_exchange(
                LANE_FREE,
                LANE_CLAIMING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .expect("lane is free");
        header.producer_id.store(42, Ordering::Relaxed);

        let metadata = LaneMetadata::try_new(lane.header(), LaneIndex::new(0));

        assert!(metadata.is_none());
    }

    #[test]
    fn acquiring_installs_the_producer_id() {
        let (_region, lane) = lane(4, 1);
        let producer_id = ProducerId::new(42);

        let _ = lane.try_acquire(producer_id);

        assert_eq!(metadata(&lane).producer_id(), producer_id);
    }

    #[test]
    fn refused_reserves_count_rejected_items() {
        let (_region, mut lane) = lane(4, 1);
        let _ = lane.try_acquire(BOGUS_PRODUCER_ID);
        let _ = join_consumer(&lane, 0);
        for value in 0..4u64 {
            let _ = publish_value(&mut lane, value);
        }

        let _ = publish_value(&mut lane, 99);
        let _ = lane.try_reserve(NonZeroUsize::new(2).unwrap());

        assert_eq!(metadata(&lane).rejected_items(), 3);
    }

    #[test]
    fn retired_lane_keeps_the_last_owner_id() {
        let (_region, lane) = lane(4, 1);
        let producer_id = ProducerId::new(42);
        let _ = lane.try_acquire(producer_id);

        lane.retire();

        assert_eq!(metadata(&lane).producer_id(), producer_id);
    }

    #[test]
    fn retired_lane_keeps_the_rejected_items_count() {
        let (_region, mut lane) = lane(4, 1);
        let _ = lane.try_acquire(ProducerId::new(42));
        let _ = join_consumer(&lane, 0);
        for value in 0..4u64 {
            let _ = publish_value(&mut lane, value);
        }
        let _ = publish_value(&mut lane, 99);

        lane.retire();

        assert_eq!(metadata(&lane).rejected_items(), 1);
    }

    #[test]
    fn publishes_and_advances_cursors() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value * 10));
        }
        assert_eq!(lane.published(), 4);
        assert_eq!(lane.reserved(), 4);
        for seq in 0..4usize {
            assert_eq!(read(&lane, seq), seq as u64 * 10);
        }
    }

    #[test]
    fn reserves_and_publishes_a_batch() {
        let (_region, mut lane) = lane(8, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        let count = NonZeroUsize::new(3).unwrap();
        let start = lane.try_reserve(count).expect("reserve");
        for offset in 0..count.get() {
            // SAFETY: each cell in the batch is reserved and unpublished.
            unsafe {
                lane.payload_ptr(start.wrapping_add(offset))
                    .cast()
                    .write((offset as u64) + 1)
            };
        }
        // Reserved but not yet visible.
        assert_eq!(lane.reserved(), 3);
        assert_eq!(lane.published(), 0);
        lane.publish(start, count);
        assert_eq!(lane.published(), 3);
        for offset in 0..3usize {
            assert_eq!(read(&lane, offset), offset as u64 + 1);
        }
    }

    #[test]
    fn reserve_rejects_count_above_capacity() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        assert!(lane.try_reserve(NonZeroUsize::new(5).unwrap()).is_none());
        // A caller error is not backpressure, so it is not counted as
        // rejected items.
        assert_eq!(metadata(&lane).rejected_items(), 0);
    }

    #[test]
    fn no_active_consumers_allows_free_overwrite() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        // Publish well past one revolution; with no active consumer there is
        // nothing to protect, so every reserve succeeds.
        for value in 0..16u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert_eq!(lane.published(), 16);
        // The ring holds the most recent generation (sequences 12..16).
        for seq in 12..16usize {
            assert_eq!(read(&lane, seq), seq as u64);
        }
    }

    #[test]
    fn backpressure_when_consumer_lags() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        // Join consumer 0; nothing published yet, so it starts at sequence 0.
        assert_eq!(join_consumer(&lane, 0), 0);

        // Fill the ring; the consumer has read nothing, so the next reserve laps.
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Consumer consumes sequences 0 and 1; two cells free up.
        lane.consumer_state().set_cursor(0, 2);
        assert!(publish_value(&mut lane, 100));
        assert!(publish_value(&mut lane, 101));
        // Now full again relative to the watermark (cursor 2, capacity 4).
        assert!(!publish_value(&mut lane, 102));

        // The recycled cells hold the new payloads.
        assert_eq!(read(&lane, 4), 100);
        assert_eq!(read(&lane, 5), 101);
        assert_eq!(lane.published(), 6);
    }

    #[test]
    fn released_consumer_no_longer_constrains() {
        let (_region, mut lane) = lane(4, 1);
        assert!(lane.try_acquire(BOGUS_PRODUCER_ID));
        assert_eq!(join_consumer(&lane, 0), 0);
        for value in 0..4u64 {
            assert!(publish_value(&mut lane, value));
        }
        assert!(!publish_value(&mut lane, 99));

        // Releasing the slot removes the constraint.
        lane.consumer_state().release(0);
        assert!(publish_value(&mut lane, 99));
    }
}
