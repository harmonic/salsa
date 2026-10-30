//! Multi-producer / multi-consumer broadcast queue in shared memory.
//!
//! Each producer owns its own ring (a `ProducerLane`); every consumer reads
//! every lane, so each published item reaches all consumers. Backpressure is
//! lossless — a producer cannot overwrite a cell until every consumer has read
//! it.
//!
//! [`Broadcast`] is the entry point: [`Broadcast::create`]/[`Broadcast::join`]
//! set up or attach to the queue itself, without claiming a producer or
//! consumer lane. Call [`Broadcast::producer`]/[`Broadcast::consumer`] as many
//! times as needed (once per lane/index wanted) to mint the handles below; a
//! single [`Broadcast`] can outlive and mint lanes for many threads.
//! [`Producer::create`]/[`Producer::join`], [`Consumer::create`]/
//! [`Consumer::join`], and [`SliceConsumer::join`] are convenience entry points
//! for processes that only need one endpoint.
//!
//! [`Producer`] writes via by-value [`Producer::try_write`], an in-place
//! [`WriteGuard`], or a [`WriteBatch`]; [`Consumer`] reads via
//! [`Consumer::try_read`], a [`ReadGuard`], or a [`ReadBatch`], with blocking
//! `*_timeout` variants that park on the queue's futex when idle. A consumer
//! joins at each lane's reservation frontier, skipping values that were already
//! reserved. After a crash, a consumer index can be taken over with
//! [`Broadcast::recover_consumer`] or returned to the pool with
//! [`Broadcast::force_release`]. A fully joined consumer
//! resumes where the dead owner was; an interrupted join restarts at each
//! lane's current reservation frontier.
//!
//! Producer lanes bind to at most one producer for the queue's lifetime: a lane
//! is claimed on join and permanently retired when its producer drops; a
//! crashed producer's lane stays claimed. Neither returns to the free pool, so
//! per-lane state always describes a single producer. Replacing producers
//! means recreating the queue.
//!
//! Each lane carries metadata: a caller-supplied [`ProducerId`] and a counter
//! of items rejected by backpressure. The producer ID is permanently associated
//! with its lane, including after the producer is dropped. Shaq treats IDs as
//! opaque values and does not enforce uniqueness; callers requiring distinct
//! producer attribution must provide unique IDs on producer creation.
//!
//! A read guard exposes the publishing lane's metadata via
//! [`ReadGuard::lane_metadata`]. To poll metadata by lane index, borrow
//! [`LaneMetadata`] with [`Broadcast::lane_metadata`].
//!
//! Typed payloads require `T: Copy`, which ensures that `T` cannot implement
//! [`Drop`] and therefore does not require a destructor when a cell is
//! duplicated or reused. File-backed typed construction remains unsafe because
//! the queue cannot verify that every participant uses the same `T` and layout,
//! or that embedded pointers and references are valid in every process that
//! reads them.
//!
//! Recovery is an externally serialized operation. Do not race recovery or
//! force-release with other recovery operations, with joins/drops for the same
//! queue, or with a still-live owner of the index being recovered.
//!
//! The region is a fixed header (magic/version, the global consumer-ownership
//! table, and the blocked-consumer wake counter) followed by one lane block per
//! producer.

mod consumer_state;
mod producer_lane;

pub use producer_lane::LaneMetadata;

use core::alloc::Layout;
use core::marker::PhantomData;
use core::mem::size_of;
use core::num::NonZeroUsize;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicU64, Ordering};
use std::fs::File;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, WaitError};
use crate::futex::{Waiters, SPIN_ATTEMPTS};
use crate::shmem::Region;
use crate::{CacheAlignedAtomicSize, DEFAULT_QUEUE_IDENTIFIER, VERSION};

use consumer_state::{ConsumerRecoveryMode, ConsumerState};
use producer_lane::{LaneHeader, ProducerLane};

const MAGIC: u64 = u64::from_be_bytes(*b"shaqcast");

/// Runtime configuration for a broadcast queue.
///
/// `capacity` is the per-lane ring capacity (rounded up to a power of two);
/// `producer_slots` / `consumer_slots` bound the lanes / consumers.
#[derive(Debug, Clone)]
pub struct BroadcastConfig {
    pub capacity: usize,
    pub producer_slots: usize,
    pub consumer_slots: usize,
}

pub struct Broadcast<MessageType> {
    shared_queue: SharedQueue,
    _message_type: PhantomData<MessageType>,
    // Prevent payload-lifetime coercions.
    _payload_invariant: PhantomData<fn(MessageType) -> MessageType>,
}

impl<MessageType> core::fmt::Debug for Broadcast<MessageType> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Broadcast")
            .field("producer_slots", &self.producer_slots())
            .finish_non_exhaustive()
    }
}

impl<MessageType> Clone for Broadcast<MessageType> {
    fn clone(&self) -> Self {
        Self {
            shared_queue: self.shared_queue.clone(),
            _message_type: PhantomData,
            _payload_invariant: PhantomData,
        }
    }
}

/// Marker payload type for [`Broadcast::join_untyped`].
#[derive(Debug)]
pub struct UnknownType;

impl<T> Broadcast<T>
where
    T: Copy,
{
    /// Creates a broadcast queue in `file`.
    ///
    /// # Safety
    /// - `file` must be initialized as a broadcast queue exactly once (by the
    ///   designated initializer), and not resized while any handle is joined.
    /// - Every participant must use the same `T` and layout, and each queued
    ///   value must be valid in every process that reads it. The `Copy` bound
    ///   does not make embedded pointers or references process-portable.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: the caller upholds the same requirements as create_with_identifier.
        unsafe { Self::create_with_identifier(file, config, DEFAULT_QUEUE_IDENTIFIER) }
    }

    /// Creates a broadcast queue in `file` with a caller chosen identifier.
    /// The identifier is not enforced to be unique.
    ///
    /// # Safety
    /// The same requirements as [`Self::create`] apply.
    pub unsafe fn create_with_identifier(
        file: &File,
        config: BroadcastConfig,
        queue_identifier: u64,
    ) -> Result<Self, Error> {
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let shared_queue = unsafe { SharedQueue::create::<T>(file, &config, queue_identifier) }?;
        Ok(Self::from_queue(shared_queue))
    }

    /// Joins an existing broadcast queue in `file`.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue and not be resized while
    ///   joined.
    /// - Every participant must use the same `T` and layout, and each queued
    ///   value must be valid in every process that reads it. The `Copy` bound
    ///   does not make embedded pointers or references process-portable.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let shared_queue = unsafe { SharedQueue::join::<T>(file) }?;
        Ok(Self {
            shared_queue,
            _message_type: PhantomData,
            _payload_invariant: PhantomData,
        })
    }

    /// Creates a new [`Producer`] if there is a free producer slot.
    pub fn producer(&self, producer_id: ProducerId) -> Result<Producer<T>, Error> {
        Producer::from_queue(self.shared_queue.clone(), producer_id)
    }

    /// Creates a new [`Consumer`] if there is a free consumer slot.
    pub fn consumer(&self) -> Result<Consumer<T>, Error> {
        Consumer::from_queue(self.shared_queue.clone())
    }

    /// Takes over a consumer index whose owner died. A fully joined consumer
    /// **resumes where the dead owner left off** on each lane — its unread items
    /// are still pinned by its reserve limit, so they are delivered. If the
    /// owner died before joining every lane, recovery instead restarts every
    /// lane at its current reservation frontier. To unconditionally restart
    /// fresh, [`force_release`](Self::force_release) the index and `join` it.
    ///
    /// # Safety
    /// - The consumer that previously owned `consumer_index` must be dead and
    ///   no other live handle may use it. Two consumers sharing an index
    ///   corrupts each other's cursor.
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    pub unsafe fn recover_consumer(&self, consumer_index: usize) -> Result<Consumer<T>, Error> {
        Consumer::recover_in_queue(self.shared_queue.clone(), consumer_index)
    }
}

impl Broadcast<UnknownType> {
    /// Joins an existing broadcast queue in `file` as an untyped consumer,
    /// starting at each lane's reservation frontier. Values already reserved
    /// are skipped, even if they have not yet been published.
    ///
    /// Pinned to [`UnknownType`] rather than generic over the caller's choice
    /// of `T`: the queue's payload layout is never checked here (that's the
    /// point of "untyped"), so a caller-chosen `Copy` `T` would let
    /// [`Broadcast::producer`]/[`Broadcast::consumer`] hand out typed handles
    /// whose `T` was never validated against the real payload — a silent
    /// out-of-bounds write/read the moment sizes differ.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue and not be resized while
    ///   joined.
    /// - The payload must satisfy [`SliceConsumer`]'s full-byte initialization
    ///   requirement.
    /// - Payload bytes must be meaningful to the caller without relying on Rust
    ///   type validation.
    ///
    /// Only untyped consumers are reachable from [`Broadcast<UnknownType>`]
    ///
    /// ```
    /// use shaq::broadcast::{Broadcast, BroadcastConfig, UnknownType};
    /// use std::fs::OpenOptions;
    ///
    /// # let path = std::env::temp_dir().join(format!("shaq-doctest-{}-untyped", std::process::id()));
    /// # let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    /// # let config = BroadcastConfig { capacity: 4, producer_slots: 1, consumer_slots: 1 };
    /// # unsafe { Broadcast::<u64>::create(&file, config) }.unwrap();
    /// #
    /// let broadcast = unsafe { Broadcast::join_untyped(&file) }.unwrap();
    /// // SAFETY: `u64`'s entire representation is initialized.
    /// assert!(unsafe { broadcast.slice_consumer() }.is_ok());
    /// # std::fs::remove_file(&path).ok();
    /// ```
    ///
    /// #### [`producer`](Broadcast::producer)/[`consumer`](Broadcast::consumer) can not be created
    /// from a broadcast that is joined untyped.
    ///
    /// ```compile_fail
    /// # use shaq::broadcast::{Broadcast, BroadcastConfig, ProducerId, UnknownType};
    /// # use std::fs::OpenOptions;
    /// # let path = std::env::temp_dir().join(format!("shaq-doctest-{}-producer", std::process::id()));
    /// # let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    /// # let config = BroadcastConfig { capacity: 4, producer_slots: 1, consumer_slots: 1 };
    /// # unsafe { Broadcast::<u64>::create(&file, config) }.unwrap();
    /// #
    /// let broadcast = unsafe { Broadcast::join_untyped(&file) }.unwrap();
    /// let _ = broadcast.producer(ProducerId::new(1)); // Untyped broadcast can not create typed producer
    /// ```
    ///
    /// ```compile_fail
    /// # use shaq::broadcast::{Broadcast, BroadcastConfig, UnknownType};
    /// # use std::fs::OpenOptions;
    /// # let path = std::env::temp_dir().join(format!("shaq-doctest-{}-consumer", std::process::id()));
    /// # let file = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&path).unwrap();
    /// # let config = BroadcastConfig { capacity: 4, producer_slots: 1, consumer_slots: 1 };
    /// # unsafe { Broadcast::<u64>::create(&file, config) }.unwrap();
    /// #
    /// let broadcast = unsafe { Broadcast::join_untyped(&file) }.unwrap();
    /// let _ = broadcast.consumer(); // Untyped broadcast can not create typed consumer
    /// ```
    pub unsafe fn join_untyped(file: &File) -> Result<Self, Error> {
        // SAFETY: validated against the stored header.
        let shared_queue = unsafe { SharedQueue::join_untyped(file) }?;
        Ok(Self {
            shared_queue,
            _message_type: PhantomData,
            _payload_invariant: PhantomData,
        })
    }
}

impl<T> Broadcast<T> {
    fn from_queue(shared_queue: SharedQueue) -> Self {
        Self {
            shared_queue,
            _message_type: PhantomData,
            _payload_invariant: PhantomData,
        }
    }

    /// Creates a new [`SliceConsumer`] if there is a free consumer slot. The
    /// consumer discovers the payload size from the queue header and reads
    /// each payload as bytes.
    ///
    /// # Safety
    /// - The payload must satisfy [`SliceConsumer`]'s full-byte initialization
    ///   requirement.
    pub unsafe fn slice_consumer(&self) -> Result<SliceConsumer, Error> {
        SliceConsumer::from_queue(self.shared_queue.clone())
    }

    /// Takes over a consumer index whose owner died. A fully joined consumer
    /// resumes where the dead owner left off on each lane. If the owner died
    /// before joining every lane, recovery instead restarts every lane at its
    /// current reservation frontier.
    ///
    /// # Safety
    /// - the consumer that owned `index` must be dead and no other live handle
    ///   may use it.
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    pub unsafe fn recover_slice_consumer(
        &self,
        consumer_index: usize,
    ) -> Result<SliceConsumer, Error> {
        SliceConsumer::recover_in_queue(self.shared_queue.clone(), consumer_index)
    }

    /// Force-releases a consumer index whose owner died, returning it to the
    /// free pool.
    ///
    /// # Safety
    /// - `index` must be dead
    ///   and no other live handle may use it.
    /// - Force-release must be serialized externally, with the same restrictions
    ///   as [`Self::recover_consumer`]/[`Self::recover_slice_consumer`].
    pub unsafe fn force_release(&self, index: usize) -> Result<(), Error> {
        ConsumerCore::force_release(&self.shared_queue, index)
    }

    /// Number of producer lanes on the queue
    pub fn producer_slots(&self) -> usize {
        self.shared_queue.producer_slots()
    }

    /// Returns the identifier of the queue.
    ///
    /// Returns `0` when created without an explicit identifier.
    pub fn queue_identifier(&self) -> u64 {
        self.shared_queue.header().identifier
    }

    /// Returns borrowed [`LaneMetadata`] for the lane at `lane_index`.
    ///
    /// Returns [`None`] if the index is out of range or no producer has
    /// completed acquisition of the lane. Once acquired, a lane retains its
    /// metadata after retirement.
    pub fn lane_metadata(&self, lane_index: usize) -> Option<LaneMetadata<'_>> {
        self.shared_queue.lane_metadata(lane_index)
    }
}

/// An advisory identifier for [`Producer`]s.
///
/// ### Warning ⚠️
/// **A [`ProducerId`] is not guaranteed to be unique** across lanes. The
/// values chosen for producer ids are chosen by callers with no uniqueness enforcement
/// by shaq.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProducerId(u64);

impl ProducerId {
    /// Callers of this constructor are responsible for choosing unique IDs
    /// if producer-level attribution is desired.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy)]
struct UnverifiedLane;
#[derive(Clone, Copy)]
struct InitializedLane;

#[derive(Clone, Copy)]
struct LaneIndex<State> {
    index: usize,
    _state: PhantomData<State>,
}

impl LaneIndex<UnverifiedLane> {
    fn new(index: usize) -> Self {
        Self {
            index,
            _state: PhantomData,
        }
    }
}

impl<State> LaneIndex<State> {
    fn get(self) -> usize {
        self.index
    }
}

/// Shared header at the start of the region. The producer-lane cursors and the
/// per-consumer reserve limits live in the lane blocks; consumer-index ownership
/// and the blocked-consumer wake state are global.
#[repr(C)]
struct SharedQueueHeader {
    magic: AtomicU64,
    version: u32,
    capacity: u32, // per-lane ring capacity (power of two)
    producer_slots: u32,
    consumer_slots: u32,
    payload_size: usize,
    payload_align: usize,
    identifier: u64,
    /// Count of consumers blocked waiting for any lane to publish.
    waiters: Waiters,
    /// Futex word for blocked consumers: bumped only when a publish wakes one
    /// (the lane cursors carry the data; this only breaks a racing wait).
    wake_seq: CacheAlignedAtomicSize,
}

impl SharedQueueHeader {
    /// Initializes the shared queue header and publishes the region.
    ///
    /// # Safety
    /// - `header` must be non-null, properly aligned, and large enough for
    ///   [`SharedQueueHeader`].
    /// - Access to `header` must be unique.
    /// - The queue's non-header sections must already be initialized.
    unsafe fn init(header: NonNull<Self>, layout: &QueueLayout, identifier: u64) {
        let value = Self {
            magic: AtomicU64::new(0),
            version: VERSION,
            capacity: layout.capacity,
            producer_slots: layout.producer_slots as u32,
            consumer_slots: layout.consumer_slots as u32,
            payload_size: layout.payload_layout.size(),
            payload_align: layout.payload_layout.align(),
            identifier,
            waiters: Waiters::default(),
            wake_seq: CacheAlignedAtomicSize::default(),
        };
        // SAFETY: caller guarantees valid, uniquely-owned storage for the header.
        unsafe { header.write(value) };

        // Publish initialization last.
        // SAFETY: the header was initialized by the write above.
        unsafe { header.as_ref() }
            .magic
            .store(MAGIC, Ordering::Release);
    }
}

/// Byte offsets and sizes of the region's sections (a construction-time helper;
/// `SharedQueue` keeps only the runtime scalars from it).
struct QueueLayout {
    capacity: u32,
    producer_slots: usize,
    consumer_slots: usize,
    payload_layout: Layout,
    consumer_state_offset: usize,
    producer_blocks_offset: usize,
    block_stride: usize,
    total: usize,
}

impl QueueLayout {
    fn new<T>(config: &BroadcastConfig) -> Result<Self, Error> {
        Self::checked_new_for_payload(config, Layout::new::<T>()).ok_or(Error::InvalidBufferSize)
    }

    fn new_for_payload(config: &BroadcastConfig, payload: Layout) -> Result<Self, Error> {
        Self::checked_new_for_payload(config, payload).ok_or(Error::InvalidBufferSize)
    }

    fn checked_new_for_payload(config: &BroadcastConfig, payload: Layout) -> Option<Self> {
        if payload.align() > ProducerLane::block_align() {
            return None;
        }
        if config.capacity == 0 {
            return None;
        }
        let capacity = config.capacity.checked_next_power_of_two()?;
        if capacity > u32::MAX as usize {
            return None;
        }
        // `consumer_slots == 0` is allowed: producers then run free (no consumer
        // can constrain the reserve limit), useful for measuring a producer in
        // isolation. `producer_slots` must be at least one — a queue with no
        // lanes can hold nothing.
        if config.producer_slots == 0
            || config.producer_slots > u32::MAX as usize
            || config.consumer_slots > u32::MAX as usize
        {
            return None;
        }
        let capacity = capacity as u32;

        let consumer_state_offset =
            size_of::<SharedQueueHeader>().next_multiple_of(ConsumerState::block_align());
        let consumer_state_bytes = ConsumerState::block_size(config.consumer_slots)?;

        let block_align = ProducerLane::block_align();
        let producer_blocks_offset = consumer_state_offset
            .checked_add(consumer_state_bytes)?
            .checked_next_multiple_of(block_align)?;
        let block_stride =
            producer_lane::block_size_for_payload(capacity, config.consumer_slots, payload)?
                .checked_next_multiple_of(block_align)?;
        let producer_blocks_bytes = block_stride.checked_mul(config.producer_slots)?;
        let total = producer_blocks_offset.checked_add(producer_blocks_bytes)?;

        Some(Self {
            capacity,
            producer_slots: config.producer_slots,
            consumer_slots: config.consumer_slots,
            payload_layout: payload,
            consumer_state_offset,
            producer_blocks_offset,
            block_stride,
            total,
        })
    }

    /// Reconstructs and validates the layout from an initialized header.
    fn from_header<T>(header: &SharedQueueHeader, region_size: usize) -> Result<Self, Error> {
        let payload = Self::payload_from_header(header)?;
        let expected = Layout::new::<T>();
        if payload.size() != expected.size() || payload.align() != expected.align() {
            return Err(Error::InvalidBufferSize);
        }
        Self::from_header_with_payload(header, region_size, payload)
    }

    /// Reconstructs and validates the layout from an initialized header without
    /// knowing the producer's Rust payload type.
    fn from_header_payload(header: &SharedQueueHeader, region_size: usize) -> Result<Self, Error> {
        let payload = Self::payload_from_header(header)?;
        Self::from_header_with_payload(header, region_size, payload)
    }

    fn payload_from_header(header: &SharedQueueHeader) -> Result<Layout, Error> {
        Layout::from_size_align(header.payload_size, header.payload_align)
            .map_err(|_| Error::InvalidBufferSize)
    }

    fn from_header_with_payload(
        header: &SharedQueueHeader,
        region_size: usize,
        payload: Layout,
    ) -> Result<Self, Error> {
        let capacity = header.capacity as usize;
        let config = BroadcastConfig {
            capacity,
            producer_slots: header.producer_slots as usize,
            consumer_slots: header.consumer_slots as usize,
        };
        // `capacity` is already a power of two, so the recomputed layout must
        // match the stored capacity exactly.
        let layout = QueueLayout::new_for_payload(&config, payload)?;
        if layout.capacity as usize != capacity || region_size < layout.total {
            return Err(Error::InvalidBufferSize);
        }
        Ok(layout)
    }
}

/// A handle onto the shared region: the header plus the section base pointers.
struct SharedQueue {
    region: Arc<Region>,
    header: NonNull<SharedQueueHeader>,
    consumer_state: ConsumerState,
    producer_blocks: NonNull<u8>,
    // Runtime scalars (the layout offsets are construction-only, so not kept).
    capacity: u32,
    producer_slots: usize,
    block_stride: usize,
    payload: Layout,
}

impl SharedQueue {
    /// Initializes a broadcast region and returns a handle.
    ///
    /// # Safety
    /// - `region` must be initialized as a broadcast queue at most once.
    unsafe fn create_in_region<T>(
        region: &Arc<Region>,
        config: &BroadcastConfig,
        identifier: u64,
    ) -> Result<Self, Error> {
        let layout = QueueLayout::new::<T>(config)?;
        if region.size() < layout.total {
            return Err(Error::InvalidBufferSize);
        }
        // SAFETY: region is large enough and (per the contract) initialized once.
        unsafe { Self::initialize(region, &layout, identifier) };
        Ok(Self::from_region(Arc::clone(region), layout))
    }

    /// Validates an initialized broadcast region and returns a handle.
    ///
    /// # Safety
    /// - `region` must reference memory laid out by [`Self::create_in_region`].
    unsafe fn join_region<T>(region: &Arc<Region>) -> Result<Self, Error> {
        // SAFETY: caller guarantees `region` was laid out by `create_in_region`.
        unsafe { Self::join_region_with(region, QueueLayout::from_header::<T>) }
    }

    /// Validates an initialized broadcast region and returns a handle without
    /// checking against a Rust payload type.
    ///
    /// # Safety
    /// - `region` must reference memory laid out by [`Self::create_in_region`].
    unsafe fn join_region_untyped(region: &Arc<Region>) -> Result<Self, Error> {
        // SAFETY: caller guarantees `region` was laid out by `create_in_region`.
        unsafe { Self::join_region_with(region, QueueLayout::from_header_payload) }
    }

    unsafe fn join_region_with(
        region: &Arc<Region>,
        layout_from_header: impl FnOnce(&SharedQueueHeader, usize) -> Result<QueueLayout, Error>,
    ) -> Result<Self, Error> {
        let header = region.addr().cast::<SharedQueueHeader>();
        // SAFETY: regions are page-aligned (>= align_of::<SharedQueueHeader>()).
        let header_ref = unsafe { header.as_ref() };
        if header_ref.magic.load(Ordering::Acquire) != MAGIC {
            return Err(Error::InvalidMagic);
        }
        if header_ref.version != VERSION {
            return Err(Error::InvalidVersion {
                expected: VERSION,
                actual: header_ref.version,
            });
        }
        let layout = layout_from_header(header_ref, region.size())?;
        Ok(Self::from_region(Arc::clone(region), layout))
    }

    /// # Safety
    /// - `region` must be at least `layout.total` bytes and initialized once.
    unsafe fn initialize(region: &Arc<Region>, layout: &QueueLayout, identifier: u64) {
        let base = region.addr();

        // Global consumer-ownership table: every index free.
        // SAFETY: `consumer_state_offset` lies within the region (>= `layout.total`).
        let consumer_state = unsafe { base.byte_add(layout.consumer_state_offset) };
        // SAFETY: the layout reserves `consumer_slots` AtomicU64s here.
        unsafe { ConsumerState::init(consumer_state, layout.consumer_slots) };

        // Producer-lane blocks.
        // SAFETY: the layout reserves `producer_slots` blocks of `block_stride`.
        let producer_blocks = unsafe { base.byte_add(layout.producer_blocks_offset) };
        for lane in 0..layout.producer_slots {
            // SAFETY: `lane < producer_slots`; blocks are `block_stride` apart.
            let block = unsafe { producer_blocks.byte_add(lane.wrapping_mul(layout.block_stride)) };
            // SAFETY: the block is sized for `(capacity, consumer_slots)`.
            unsafe { ProducerLane::init(block, layout.consumer_slots) };
        }

        // Header initialization publishes the queue, so it runs after every
        // other region section is initialized.
        let header = base.cast();
        // SAFETY: region is page-aligned, large enough for the header, uniquely
        // initialized here, and all non-header sections are initialized above.
        unsafe { SharedQueueHeader::init(header, layout, identifier) };
    }

    fn from_region(region: Arc<Region>, layout: QueueLayout) -> Self {
        let base = region.addr();
        let header = base.cast();
        // SAFETY: offsets lie within the region.
        let consumer_state_block = unsafe { base.byte_add(layout.consumer_state_offset) };
        // SAFETY: offsets lie within the region.
        let consumer_state =
            unsafe { ConsumerState::from_block(consumer_state_block, layout.consumer_slots) };
        // SAFETY: offsets lie within the region.
        let producer_blocks = unsafe { base.byte_add(layout.producer_blocks_offset) };
        Self {
            region,
            header,
            consumer_state,
            producer_blocks,
            capacity: layout.capacity,
            producer_slots: layout.producer_slots,
            block_stride: layout.block_stride,
            payload: layout.payload_layout,
        }
    }

    #[inline]
    fn producer_slots(&self) -> usize {
        self.producer_slots
    }

    /// Pointer to producer lane block `lane`.
    ///
    /// # Safety:
    /// - `lane` must be in range (0..self.producer_slots)
    unsafe fn lane_block(&self, lane: usize) -> NonNull<u8> {
        // SAFETY: caller guarantees `lane < producer_slots`; blocks
        // are `block_stride` apart.
        unsafe {
            self.producer_blocks
                .byte_add(lane.wrapping_mul(self.block_stride))
        }
    }

    /// Every producer lane on this queue, in order.
    fn producer_lanes(&self) -> impl Iterator<Item = ProducerLane> + '_ {
        (0..self.producer_slots).map(|lane_index| {
            // SAFETY: lane_index is in range (0..self.producer_slots)
            let block = unsafe { self.lane_block(lane_index) };
            // SAFETY: the block was initialized with these parameters.
            unsafe {
                ProducerLane::from_block(block, self.capacity, self.consumer_slots(), self.payload)
            }
        })
    }

    /// Borrows metadata for the given `lane_index`.
    /// Returns [`None`] if `lane_index` is out of range or no producer has completed
    /// an acquisition of that lane.
    fn lane_metadata(&self, lane_index: usize) -> Option<LaneMetadata<'_>> {
        let lane_index_is_valid = lane_index < self.producer_slots;
        if !lane_index_is_valid {
            return None;
        }
        // SAFETY: lane_index is checked to in range above
        let block = unsafe { self.lane_block(lane_index) };

        // SAFETY: every producer block begins with an initialized `LaneHeader`,
        // and the returned reference is tied to `&self`, whose `Arc<Region>`
        // keeps the mapping alive.
        let lane_header = unsafe { block.cast::<LaneHeader>().as_ref() };

        LaneMetadata::try_new(lane_header, LaneIndex::new(lane_index))
    }

    /// Claims a free producer lane, returning its index and cached view.
    fn acquire_producer_lane(
        &self,
        producer_id: ProducerId,
    ) -> Result<(usize, ProducerLane), Error> {
        self.producer_lanes()
            .enumerate()
            .find(|(_, lane)| lane.try_acquire(producer_id))
            .ok_or(Error::ProducerSlotsExhausted)
    }

    #[inline]
    fn consumer_slots(&self) -> usize {
        self.consumer_state.len()
    }

    #[inline]
    fn header(&self) -> &SharedQueueHeader {
        // SAFETY: the header sits at the region base and outlives this handle.
        unsafe { self.header.as_ref() }
    }

    /// Bumps the wake counter and wakes blocked consumers, if any. Called after
    /// a publish (the lane cursor is already advanced); a no-op on the hot path
    /// when nothing is blocked.
    fn wake(&self) {
        let header = self.header();
        header.waiters.bump_and_wake(&header.wake_seq);
    }

    /// Blocks until `check` succeeds or `timeout` elapses, sleeping on the global
    /// wake counter (a publish on any lane bumps it). `check` reads the lane
    /// cursors, which carry the actual data.
    fn wait_for<R>(
        &self,
        timeout: Duration,
        check: impl FnMut() -> Option<R>,
    ) -> Result<R, WaitError> {
        // `check` scans every lane, so scale the per-check baseline down by the
        // lane count to keep total spin work comparable to a single-cursor queue.
        let spins = (SPIN_ATTEMPTS / self.producer_slots).max(1);
        let header = self.header();
        header
            .waiters
            .wait_for(&header.wake_seq, spins, timeout, check)
    }

    /// Claims a free consumer index in the global ownership table.
    fn acquire_consumer_index(&self) -> Result<usize, Error> {
        self.consumer_state.acquire()
    }

    /// Marks a consumer index active after every lane cursor is installed.
    fn activate_consumer_index(&self, index: usize) {
        self.consumer_state.activate(index);
    }

    /// Releases a consumer index back to free.
    fn release_consumer_index(&self, index: usize) {
        self.consumer_state.release(index);
    }

    /// Determines whether recovery can resume complete lane cursors or must
    /// restart an interrupted join.
    fn begin_consumer_recovery(&self, index: usize) -> ConsumerRecoveryMode {
        self.consumer_state.begin_recovery(index)
    }

    /// Maps `file`, initializing it as a broadcast queue.
    ///
    /// # Safety
    /// - `file` must be initialized as a queue at most once (by the designated
    ///   initializer) and not resized while any handle is joined.
    unsafe fn create<T>(
        file: &File,
        config: &BroadcastConfig,
        identifier: u64,
    ) -> Result<Self, Error> {
        let layout = QueueLayout::new::<T>(config)?;
        file.set_len(layout.total as u64)?;
        let region = Region::map_file(file, layout.total)?;
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        unsafe { Self::create_in_region::<T>(&region, config, identifier) }
    }

    /// Maps and validates an existing broadcast queue in `file`.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue, not resized while joined.
    unsafe fn join<T>(file: &File) -> Result<Self, Error> {
        let file_size = file.metadata()?.len() as usize;
        let region = Region::map_file(file, file_size)?;
        // SAFETY: validated against the stored header.
        unsafe { Self::join_region::<T>(&region) }
    }

    /// Maps and validates an existing broadcast queue in `file` without
    /// checking against a Rust payload type.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue, not resized while joined.
    unsafe fn join_untyped(file: &File) -> Result<Self, Error> {
        let file_size = file.metadata()?.len() as usize;
        let region = Region::map_file(file, file_size)?;
        // SAFETY: validated against the stored header.
        unsafe { Self::join_region_untyped(&region) }
    }
}

impl Clone for SharedQueue {
    fn clone(&self) -> Self {
        Self {
            region: Arc::clone(&self.region),
            header: self.header,
            consumer_state: self.consumer_state,
            producer_blocks: self.producer_blocks,
            capacity: self.capacity,
            producer_slots: self.producer_slots,
            block_stride: self.block_stride,
            payload: self.payload,
        }
    }
}

// SAFETY: the region is shared (file-backed / heap) and access is synchronized
// by the queue protocol; the pointers are stable for the region's lifetime.
unsafe impl Send for SharedQueue {}
// SAFETY: shared access to the region is synchronized by the queue protocol.
unsafe impl Sync for SharedQueue {}

/// A producer: owns one lane and publishes into it. Single-threaded use (its
/// write ops take `&mut self`); move it between threads to hand off ownership.
///
/// Holds the lane view directly (stable for the producer's lifetime); the
/// `queue` is kept to wake blocked consumers and to keep the region mapping
/// alive.
pub struct Producer<T: Copy> {
    queue: SharedQueue,
    lane: ProducerLane,
    index: usize,
    producer_id: ProducerId,
    _marker: PhantomData<T>,
    _invariant: PhantomData<fn(T) -> T>,
}

impl<T: Copy> core::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Producer")
            .field("lane_index", &self.index())
            .field("producer_id", &self.producer_id())
            .finish_non_exhaustive()
    }
}

impl<T: Copy> Producer<T> {
    /// Creates a broadcast queue in `file` and joins as a producer.
    ///
    /// # Safety
    /// - `file` must be initialized as a broadcast queue exactly once (by the
    ///   designated initializer), and not resized while any handle is joined.
    /// - Every participant must use the same `T` and layout, and each queued
    ///   value must be valid in every process that reads it. The `Copy` bound
    ///   does not make embedded pointers or references process-portable.
    pub unsafe fn create(
        file: &File,
        config: BroadcastConfig,
        producer_id: ProducerId,
    ) -> Result<Self, Error> {
        // SAFETY: the caller upholds the Broadcast::create requirements.
        let broadcast = unsafe { Broadcast::<T>::create(file, config) }?;
        broadcast.producer(producer_id)
    }

    /// Joins an existing broadcast queue in `file` as a producer.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue (not resized while joined),
    ///   with the same `T` as every other handle (see [`Self::create`]).
    pub unsafe fn join(file: &File, producer_id: ProducerId) -> Result<Self, Error> {
        // SAFETY: the caller upholds the Broadcast::join requirements.
        let broadcast = unsafe { Broadcast::<T>::join(file) }?;
        broadcast.producer(producer_id)
    }

    /// Returns a lane-free handle that shares this producer's queue mapping.
    pub fn broadcast_handle(&self) -> Broadcast<T> {
        Broadcast::from_queue(self.queue.clone())
    }

    fn from_queue(queue: SharedQueue, producer_id: ProducerId) -> Result<Self, Error> {
        let (index, lane) = queue.acquire_producer_lane(producer_id)?;

        Ok(Self {
            queue,
            lane,
            index,
            producer_id,
            _marker: PhantomData,
            _invariant: PhantomData,
        })
    }

    /// The lane this producer owns.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The [`ProducerId`] of this producer.
    pub fn producer_id(&self) -> ProducerId {
        self.producer_id
    }

    /// Publishes one value, or returns it on backpressure (the slowest consumer
    /// has not freed the cell that publishing would overwrite).
    pub fn try_write(&mut self, value: T) -> Result<(), T> {
        // SAFETY: on successful reservation, `value` is written before the guard
        // is dropped and publishes the cell.
        match unsafe { self.try_reserve_write() } {
            Some(guard) => {
                guard.write(value);
                Ok(())
            }
            None => Err(value),
        }
    }

    /// Writes items from a slice into this producer's lane.
    ///
    /// Returns `false` if there is not enough space.
    #[must_use]
    pub fn try_write_slice(&mut self, items: &[T]) -> bool {
        let Some(len) = NonZeroUsize::new(items.len()) else {
            return true;
        };

        // SAFETY: if reservation succeeds, every reserved cell is written below.
        let mut batch = match unsafe { self.try_reserve_write_batch(len) } {
            Some(batch) => batch,
            None => return false,
        };

        for (index, item) in items.iter().copied().enumerate() {
            // SAFETY: `index` comes from enumerating exactly `len` items.
            unsafe { batch.write(index, item) };
        }
        true
    }

    /// Reserves a single cell for an in-place write, or `None` on backpressure.
    /// The cell becomes visible when the returned guard is dropped.
    ///
    /// # Safety
    /// - The caller must initialize the reserved cell before the guard is
    ///   dropped.
    #[must_use]
    pub unsafe fn try_reserve_write(&mut self) -> Option<WriteGuard<'_, T>> {
        let start = self.lane.try_reserve(NonZeroUsize::MIN)?;
        Some(WriteGuard {
            producer: self,
            start,
        })
    }

    /// Reserves `count` consecutive cells for in-place writes, or `None` on
    /// backpressure. The cells become visible when the returned batch is dropped.
    ///
    /// # Safety
    /// - The caller must initialize every reserved cell before the batch is
    ///   dropped.
    #[must_use]
    pub unsafe fn try_reserve_write_batch(
        &mut self,
        count: NonZeroUsize,
    ) -> Option<WriteBatch<'_, T>> {
        let start = self.lane.try_reserve(count)?;
        Some(WriteBatch {
            producer: self,
            start,
            count,
        })
    }
}

impl<T: Copy> Drop for Producer<T> {
    fn drop(&mut self) {
        self.lane.retire();
    }
}

// SAFETY: a producer is single-threaded (write ops take `&mut self`) but may be
// moved between threads.
unsafe impl<T: Copy + Send> Send for Producer<T> {}

/// A reservation of one cell in a producer's lane. Write it via
/// [`Self::write`]/[`Self::as_mut`], then drop the guard to publish it.
#[must_use]
pub struct WriteGuard<'a, T: Copy> {
    producer: &'a mut Producer<T>,
    start: usize,
}

impl<T: Copy> core::fmt::Debug for WriteGuard<'_, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WriteGuard")
            .field("lane_index", &self.producer.index())
            .field("producer_id", &self.producer.producer_id())
            .finish_non_exhaustive()
    }
}

impl<T: Copy> core::convert::AsMut<MaybeUninit<T>> for WriteGuard<'_, T> {
    /// Mutable reference to the reserved cell.
    fn as_mut(&mut self) -> &mut MaybeUninit<T> {
        let mut ptr = self.producer.lane.payload_ptr(self.start).cast();
        // SAFETY: forwarded; the cell is reserved for this producer.
        unsafe { ptr.as_mut() }
    }
}

impl<T: Copy> WriteGuard<'_, T> {
    /// Writes `value` into the reserved cell; the guard publishes it on drop.
    pub fn write(self, value: T) {
        let ptr = self.producer.lane.payload_ptr(self.start).cast();
        // SAFETY: the cell is reserved and not yet published; `T` is moved in.
        unsafe { ptr.write(value) };
    }
}

impl<T: Copy> Drop for WriteGuard<'_, T> {
    fn drop(&mut self) {
        self.producer.lane.publish(self.start, NonZeroUsize::MIN);
        self.producer.queue.wake();
    }
}

/// A reservation of `count` cells in a producer's lane. Write every cell via
/// [`Self::write`]/[`Self::as_mut`], then drop the batch to publish them.
#[must_use]
pub struct WriteBatch<'a, T: Copy> {
    producer: &'a mut Producer<T>,
    start: usize,
    count: NonZeroUsize,
}

impl<T: Copy> core::fmt::Debug for WriteBatch<'_, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WriteBatch")
            .field("lane_index", &self.producer.index())
            .field("producer_id", &self.producer.producer_id())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<T: Copy> WriteBatch<'_, T> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.count.get()
    }

    /// Mutable reference to the reserved cell at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn as_mut(&mut self, index: usize) -> &mut MaybeUninit<T> {
        debug_assert!(index < self.count.get());
        let mut ptr = self
            .producer
            .lane
            .payload_ptr(self.start.wrapping_add(index))
            .cast();
        // SAFETY: forwarded; the cell is reserved for this producer.
        unsafe { ptr.as_mut() }
    }

    /// Writes `value` into the reserved cell at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn write(&mut self, index: usize, value: T) {
        // SAFETY: forwarded; `index < len` and the cell is reserved.
        unsafe { self.as_mut(index).write(value) };
    }
}

impl<T: Copy> Drop for WriteBatch<'_, T> {
    fn drop(&mut self) {
        self.producer.lane.publish(self.start, self.count);
        self.producer.queue.wake();
    }
}

/// A lane with published values past this consumer's cursor: `sequence` is the
/// next unread sequence, `published` the publication that proved readability.
/// The unread values stay protected from overwrite until the consumer calls
/// [`ConsumerCore::advance`] for the lane.
#[derive(Clone, Copy)]
struct ReadableLane {
    lane: LaneIndex<InitializedLane>,
    sequence: usize,
    published: usize,
}

impl ReadableLane {
    /// Builds a readable lane from the publication load that proved the lane
    /// has an initialized, published value at `sequence`.
    fn try_new(lane: LaneIndex<UnverifiedLane>, sequence: usize, published: usize) -> Option<Self> {
        if published <= sequence {
            return None;
        }

        Some(Self {
            lane: LaneIndex {
                index: lane.get(),
                _state: PhantomData,
            },
            sequence,
            published,
        })
    }

    /// Number of consecutive published values to read starting at `sequence`,
    /// bounded by `max`.
    fn count(&self, max: NonZeroUsize) -> NonZeroUsize {
        let available = self.published.wrapping_sub(self.sequence);
        let count = available.min(max.get());
        NonZeroUsize::new(count).expect("readable lane has at least one value")
    }
}

/// Shared consumer machinery: owns a consumer index, tracks one cursor per
/// producer lane, and reserves raw payload pointers.
struct ConsumerCore {
    queue: SharedQueue,
    index: usize,
    /// One cached view per lane (avoids rebuilding it on every read).
    lanes: Box<[ProducerLane]>,
    /// Next sequence to read per lane (local cache of the published cursors).
    next_by_lane: Box<[usize]>,
    /// Lane to start the next round-robin scan from (rotates for fairness).
    scan_start_lane: usize,
}

impl ConsumerCore {
    fn from_queue(queue: SharedQueue) -> Result<Self, Error> {
        let index = queue.acquire_consumer_index()?;
        // Cache a view per lane (independent of `queue`) and join each at its
        // reservation frontier.
        let lanes: Box<[ProducerLane]> = queue.producer_lanes().collect();
        let next_by_lane = lanes
            .iter()
            .map(|lane| Self::join_lane(lane, index))
            .collect();
        queue.activate_consumer_index(index);
        Ok(Self {
            queue,
            index,
            lanes,
            next_by_lane,
            scan_start_lane: 0,
        })
    }

    fn recover_in_queue(queue: SharedQueue, index: usize) -> Result<Self, Error> {
        if index >= queue.consumer_slots() {
            return Err(Error::InvalidIndex);
        }
        let recovery_mode = queue.begin_consumer_recovery(index);
        let lanes: Box<[ProducerLane]> = queue.producer_lanes().collect();
        let next_by_lane = match recovery_mode {
            ConsumerRecoveryMode::Resume => lanes
                .iter()
                .map(|lane| Self::recover_lane(lane, index))
                .collect(),
            ConsumerRecoveryMode::RestartJoin => {
                let next_by_lane = lanes
                    .iter()
                    .map(|lane| Self::join_lane(lane, index))
                    .collect();
                queue.activate_consumer_index(index);
                next_by_lane
            }
        };
        Ok(Self {
            queue,
            index,
            lanes,
            next_by_lane,
            scan_start_lane: 0,
        })
    }

    fn force_release(queue: &SharedQueue, index: usize) -> Result<(), Error> {
        if index >= queue.consumer_slots() {
            return Err(Error::InvalidIndex);
        }
        for producer_lane in queue.producer_lanes() {
            producer_lane.consumer_state().release(index);
        }
        queue.release_consumer_index(index);
        Ok(())
    }

    fn join_lane(lane: &ProducerLane, index: usize) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.join(index, || lane.reserved())
    }

    fn recover_lane(lane: &ProducerLane, index: usize) -> usize {
        let consumer_state = lane.consumer_state();
        consumer_state.recover(index, || lane.reserved())
    }

    fn index(&self) -> usize {
        self.index
    }

    fn payload_size(&self) -> usize {
        self.queue.payload.size()
    }

    /// Returns metadata for a lane containing a published value.
    fn lane_metadata(&self, lane: LaneIndex<InitializedLane>) -> LaneMetadata<'_> {
        self.lane(lane.get()).metadata(lane)
    }

    #[inline]
    fn lane(&self, lane: usize) -> &ProducerLane {
        debug_assert!(lane < self.lanes.len());
        // SAFETY: every caller passes a lane produced by this consumer's
        // round-robin state or a guard/batch that was created from it.
        unsafe { self.lanes.get_unchecked(lane) }
    }

    #[inline]
    fn next_for_lane(&self, lane: usize) -> usize {
        debug_assert!(lane < self.next_by_lane.len());
        // SAFETY: `next_by_lane` is built with one entry per producer lane, and
        // every caller passes a valid lane index.
        unsafe { *self.next_by_lane.get_unchecked(lane) }
    }

    #[inline]
    fn set_next_for_lane(&mut self, lane: usize, next: usize) {
        debug_assert!(lane < self.next_by_lane.len());
        // SAFETY: same invariant as `next_for_lane`.
        unsafe { *self.next_by_lane.get_unchecked_mut(lane) = next };
    }

    /// Finds the next readable lane, scanning round-robin from
    /// `scan_start_lane`. A lane is readable when its publication is ahead of
    /// this consumer's cursor. The returned publication is the same load that
    /// proved readability.
    fn next_readable(&self) -> Option<ReadableLane> {
        let producer_slots = self.queue.producer_slots();
        let mut lane = self.scan_start_lane;
        for _ in 0..producer_slots {
            let sequence = self.next_for_lane(lane);
            let published = self.lane(lane).published();
            // Cursor wrap is not supported - simple comparison works here. An
            // observed publication also proves that lane acquisition completed.
            let lane_index = LaneIndex::new(lane);

            if let Some(readable) = ReadableLane::try_new(lane_index, sequence, published) {
                return Some(readable);
            }
            lane = lane.wrapping_add(1);
            if lane == producer_slots {
                lane = 0;
            }
        }
        None
    }

    /// Pointer to the published cell at `readable`'s next unread sequence.
    fn payload_ptr(&self, readable: ReadableLane) -> NonNull<u8> {
        self.lane(readable.lane.get())
            .payload_ptr(readable.sequence)
    }

    /// Advances this consumer's cursor on `lane` by `count` consumed values,
    /// publishing the progress and rotating the scan start. The consumer is the
    /// sole reader of its own cursor (`&mut self`), so the cursor only moves here
    /// and advancing is a plain increment.
    fn advance(&mut self, lane: LaneIndex<InitializedLane>, count: NonZeroUsize) {
        let lane_index = lane.get();
        let next = self.next_for_lane(lane_index).wrapping_add(count.get());
        self.set_next_for_lane(lane_index, next);
        self.lane(lane_index)
            .consumer_state()
            .set_cursor(self.index, next);
        // `lane < producer_slots`, so the wrap is a conditional subtract.
        let mut scan_start_lane = lane_index.wrapping_add(1);
        if scan_start_lane == self.queue.producer_slots() {
            scan_start_lane = 0;
        }
        self.scan_start_lane = scan_start_lane;
    }

    /// Blocks until [`Self::next_readable`] would succeed, sleeping on the
    /// queue's global wake counter (a publish on any lane wakes it).
    fn wait_until_readable(&self, timeout: Duration) -> Result<ReadableLane, WaitError> {
        self.queue.wait_for(timeout, || self.next_readable())
    }
}

impl Drop for ConsumerCore {
    fn drop(&mut self) {
        // Drop each lane's reserve limit first, then the global ownership, so no
        // limit lingers for an index another consumer could reclaim.
        for lane in self.lanes.iter() {
            lane.consumer_state().release(self.index);
        }
        self.queue.release_consumer_index(self.index);
    }
}

// SAFETY: core consumer state is single-threaded by its public wrappers, but
// may be moved between threads.
unsafe impl Send for ConsumerCore {}

/// A consumer: owns one consumer index and reads every lane round-robin,
/// starting at each lane's reservation frontier. Single-threaded use (`&mut
/// self`).
pub struct Consumer<T: Copy> {
    core: ConsumerCore,
    _marker: PhantomData<T>,
    _invariant: PhantomData<fn(T) -> T>,
}

impl<T: Copy> core::fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Consumer")
            .field("consumer_index", &self.index())
            .finish_non_exhaustive()
    }
}

impl<T: Copy> Consumer<T> {
    /// Creates a broadcast queue in `file` and joins as a consumer.
    ///
    /// # Safety
    /// - Same as [`Producer::create`]: `file` must be initialized as a queue
    ///   exactly once (the consumer may be the initializer), and all typed
    ///   handles must use the same `T` and layout with values valid in every
    ///   process that reads them.
    pub unsafe fn create(file: &File, config: BroadcastConfig) -> Result<Self, Error> {
        // SAFETY: the caller upholds the Broadcast::create requirements.
        let broadcast = unsafe { Broadcast::<T>::create(file, config) }?;
        broadcast.consumer()
    }

    /// Joins an existing broadcast queue in `file` as a consumer, starting at
    /// each lane's reservation frontier. Values already reserved are skipped,
    /// even if they have not yet been published.
    ///
    /// # Safety
    /// - Same as [`Producer::join`]: live queue, same `T` across all handles.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        // SAFETY: the caller upholds the Broadcast::join requirements.
        let broadcast = unsafe { Broadcast::<T>::join(file) }?;
        broadcast.consumer()
    }

    fn from_queue(queue: SharedQueue) -> Result<Self, Error> {
        Ok(Self {
            core: ConsumerCore::from_queue(queue)?,
            _marker: PhantomData,
            _invariant: PhantomData,
        })
    }

    /// The consumer index this handle owns. Record it so a replacement can
    /// [`recover`](Self::recover) it if this consumer's process dies.
    pub fn index(&self) -> usize {
        self.core.index()
    }

    /// Returns a lane-free handle that shares this consumer's queue mapping.
    pub fn broadcast_handle(&self) -> Broadcast<T> {
        Broadcast::from_queue(self.core.queue.clone())
    }

    /// Takes over a consumer index whose owner died. A fully joined consumer
    /// **resumes where the dead owner left off** on each lane — its unread items
    /// are still pinned by its reserve limit, so they are delivered. If the
    /// owner died before joining every lane, recovery instead restarts every
    /// lane at its current reservation frontier. To unconditionally restart
    /// fresh, [`force_release`](Self::force_release) the index and `join` it.
    ///
    /// # Safety
    /// - All of [`Self::join`]'s requirements, plus: the consumer that owned
    ///   `index` must be dead and no other live handle may use it — two consumers
    ///   sharing an index corrupts each other's cursor.
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    pub unsafe fn recover(file: &File, index: usize) -> Result<Self, Error> {
        // SAFETY: the caller upholds the shared queue join requirements.
        let broadcast = unsafe { Broadcast::<T>::join(file) }?;
        // SAFETY: the caller guarantees the previous owner is dead and
        // serializes recovery.
        unsafe { broadcast.recover_consumer(index) }
    }

    /// # Safety
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    fn recover_in_queue(queue: SharedQueue, index: usize) -> Result<Self, Error> {
        Ok(Self {
            core: ConsumerCore::recover_in_queue(queue, index)?,
            _marker: PhantomData,
            _invariant: PhantomData,
        })
    }

    /// Force-releases a consumer index whose owner died, returning it to the free
    /// pool: the lanes are un-wedged and a later [`join`](Self::join) can reclaim
    /// it fresh. Use this (rather than [`recover`](Self::recover)) when you want
    /// to drop the dead consumer's unread values and choose a new start position.
    ///
    /// # Safety
    /// - As [`Self::join`], plus: the consumer that owned `index` must be dead and
    ///   no other live handle may use it.
    /// - Force-release must be serialized externally, with the same restrictions
    ///   as [`Self::recover`].
    pub unsafe fn force_release(file: &File, index: usize) -> Result<(), Error> {
        // SAFETY: the caller upholds the Broadcast::join requirements.
        let broadcast = unsafe { Broadcast::<T>::join(file) }?;
        // SAFETY: the caller guarantees the previous owner is dead and
        // serializes force-release.
        unsafe { broadcast.force_release(index) }
    }

    /// Reads the next available value by copying it out, or `None` if every lane
    /// is caught up.
    pub fn try_read(&mut self) -> Option<T> {
        let readable = self.core.next_readable()?;
        let payload = self.core.payload_ptr(readable);
        // SAFETY: `sequence < publication`, so the cell is published (initialized);
        // this consumer's cursor still protects it from being overwritten until we
        // advance below. The copy happens before the cursor advances.
        let value = unsafe { payload.cast().read() };
        self.core.advance(readable.lane, NonZeroUsize::MIN);
        Some(value)
    }

    /// Reserves the next available value in place without copying, or `None` if
    /// every lane is caught up. The returned guard holds this consumer's cursor
    /// at the value's sequence, so the producer cannot overwrite it until the
    /// guard is dropped, which advances past it.
    #[must_use]
    pub fn try_reserve_read(&mut self) -> Option<ReadGuard<'_, T>> {
        Some(self.read_guard_for(self.core.next_readable()?))
    }

    fn read_guard_for(&mut self, readable: ReadableLane) -> ReadGuard<'_, T> {
        let payload = self.core.payload_ptr(readable);
        ReadGuard {
            consumer: &mut self.core,
            lane: readable.lane,
            payload: payload.cast(),
        }
    }

    /// Reserves up to `max` consecutive published values from the next readable
    /// lane, or `None` if every lane is caught up. A batch reads from a single
    /// lane, so its length is bounded by that lane's available run as well as by
    /// `max`. The returned guard holds this consumer's cursor at the batch start,
    /// so the producer cannot overwrite any of the batched cells until the guard
    /// is dropped, which advances past the whole batch.
    #[must_use]
    pub fn try_reserve_read_batch(&mut self, max: NonZeroUsize) -> Option<ReadBatch<'_, T>> {
        Some(self.read_batch_for(self.core.next_readable()?, max))
    }

    fn read_batch_for(&mut self, readable: ReadableLane, max: NonZeroUsize) -> ReadBatch<'_, T> {
        ReadBatch {
            consumer: &mut self.core,
            lane: readable.lane,
            start: readable.sequence,
            count: readable.count(max),
            _marker: PhantomData,
        }
    }

    /// Blocks until any lane has an unread value or `timeout` elapses, then
    /// returns a [`ReadGuard`] for it; `Err(Timeout)` if none arrived in time.
    pub fn reserve_read_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<ReadGuard<'_, T>, WaitError> {
        Ok(self.read_guard_for(self.core.wait_until_readable(timeout)?))
    }

    /// Blocks until any lane has unread values or `timeout` elapses, then returns
    /// a [`ReadBatch`] of up to `max` of them; `Err(Timeout)` if none arrived.
    pub fn reserve_read_batch_timeout(
        &mut self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<ReadBatch<'_, T>, WaitError> {
        Ok(self.read_batch_for(self.core.wait_until_readable(timeout)?, max))
    }

    /// Blocks until any lane has an unread value or `timeout` elapses, then
    /// copies it out; `Err(Timeout)` if none arrived in time.
    pub fn read_timeout(&mut self, timeout: Duration) -> Result<T, WaitError> {
        let guard = self.reserve_read_timeout(timeout)?;
        Ok(guard.read())
    }
}

// SAFETY: a consumer is single-threaded (read ops take `&mut self`) but may be
// moved between threads.
unsafe impl<T: Copy + Send> Send for Consumer<T> {}

/// An in-place borrow of one published value. The consumer's cursor stays at
/// this value's sequence while the guard lives (so the producer cannot overwrite
/// it); dropping the guard advances past it.
#[must_use]
pub struct ReadGuard<'a, T: Copy> {
    consumer: &'a mut ConsumerCore,
    lane: LaneIndex<InitializedLane>,
    payload: NonNull<T>,
}

impl<T: Copy + core::fmt::Debug> core::fmt::Debug for ReadGuard<'_, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let metadata = self.lane_metadata();
        // SAFETY: the cell is published and held by this consumer's cursor.
        let value = unsafe { self.payload.read() };
        f.debug_struct("ReadGuard")
            .field("lane_index", &metadata.lane())
            .field("producer_id", &metadata.producer_id())
            .field("value", &value)
            .finish_non_exhaustive()
    }
}

impl<T: Copy> ReadGuard<'_, T> {
    /// Metadata for the producer lane this value was published on.
    pub fn lane_metadata(&self) -> LaneMetadata<'_> {
        self.consumer.lane_metadata(self.lane)
    }

    /// Copies the value out; the guard advances past it on drop.
    pub fn read(self) -> T {
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { self.payload.read() }
    }
}

impl<T: Copy + Sync> AsRef<T> for ReadGuard<'_, T> {
    fn as_ref(&self) -> &T {
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { self.payload.as_ref() }
    }
}

impl<T: Copy> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        self.consumer.advance(self.lane, NonZeroUsize::MIN);
    }
}

/// An in-place borrow of `count` consecutive published values from one lane. The
/// consumer's cursor stays at the batch start while the guard lives (so the
/// producer cannot overwrite any of them); dropping the guard advances past the
/// whole batch.
#[must_use]
pub struct ReadBatch<'a, T: Copy> {
    consumer: &'a mut ConsumerCore,
    lane: LaneIndex<InitializedLane>,
    start: usize,
    count: NonZeroUsize,
    _marker: PhantomData<T>,
}

impl<T: Copy> core::fmt::Debug for ReadBatch<'_, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let metadata = self.lane_metadata();
        f.debug_struct("ReadBatch")
            .field("lane_index", &metadata.lane())
            .field("producer_id", &metadata.producer_id())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl<T: Copy> ReadBatch<'_, T> {
    /// Metadata for the producer lane every value in this batch was published on.
    pub fn lane_metadata(&self) -> LaneMetadata<'_> {
        self.consumer.lane_metadata(self.lane)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.count.get()
    }

    /// Reference to the value at `index`.
    ///
    /// # Safety
    /// - `index < len`
    pub unsafe fn as_ref(&self, index: usize) -> &T
    where
        T: Sync,
    {
        debug_assert!(index < self.count.get());
        let ptr = self.consumer.lanes[self.lane.get()]
            .payload_ptr(self.start.wrapping_add(index))
            .cast();
        // SAFETY: the cell is published and held by this consumer's
        // cursor.
        unsafe { ptr.as_ref() }
    }

    /// Copies the value at `index` out.
    ///
    /// # Safety
    /// - `index < len`
    pub unsafe fn read(&self, index: usize) -> T {
        debug_assert!(index < self.count.get());
        let ptr = self.consumer.lanes[self.lane.get()]
            .payload_ptr(self.start.wrapping_add(index))
            .cast();
        // SAFETY: the cell is published and held by this consumer's
        // cursor.
        unsafe { ptr.read() }
    }

    /// Returns the values in logical read order as two slices.
    ///
    /// The second slice is empty unless the batch wraps around the end of the
    /// lane's ring buffer. This borrows the values without consuming the batch.
    pub fn as_slices(&self) -> (&[T], &[T])
    where
        T: Sync,
    {
        let lane = self.consumer.lane(self.lane.get());
        let start = lane.mask(self.start);
        let first_len = self.len().min(lane.capacity().wrapping_sub(start));
        let second_len = self.len().wrapping_sub(first_len);

        let first = lane.payload_ptr(self.start).cast::<T>();
        let first = NonNull::slice_from_raw_parts(first, first_len);
        // SAFETY: The first part of the reservation is initialized, contiguous,
        // and remains protected from overwrite for the lifetime of the slice.
        let first = unsafe { first.as_ref() };
        let second = lane.payload_ptr(0).cast::<T>();
        let second = NonNull::slice_from_raw_parts(second, second_len);
        // SAFETY: The wrapped part of the reservation starts at the ring base,
        // is initialized and contiguous, and remains protected from overwrite
        // for the lifetime of the slice.
        let second = unsafe { second.as_ref() };
        (first, second)
    }
}

impl<T: Copy> Drop for ReadBatch<'_, T> {
    fn drop(&mut self) {
        self.consumer.advance(self.lane, self.count);
    }
}

/// An untyped consumer that reads each broadcast payload as a byte slice.
///
/// `SliceConsumer` joins an existing queue without knowing its Rust payload
/// type. The payload size is discovered from the queue header, and every read
/// returns a guard exposing the payload bytes. Dropping the guard advances the
/// consumer cursor, just like [`ReadGuard`].
///
/// # Safety
///
/// Every byte in each published payload must be initialized before it is
/// exposed as a byte slice. Ordinary typed writes guarantee this for types
/// without padding, such as `[u8; N]`, but not for arbitrary `Copy` structs
/// with implicit padding.
pub struct SliceConsumer {
    core: ConsumerCore,
}

impl core::fmt::Debug for SliceConsumer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SliceConsumer")
            .field("consumer_index", &self.index())
            .field("payload_size", &self.payload_size())
            .finish_non_exhaustive()
    }
}

impl SliceConsumer {
    /// Joins an existing broadcast queue in `file` as an untyped consumer,
    /// starting at each lane's reservation frontier. Values already reserved
    /// are skipped, even if they have not yet been published.
    ///
    /// # Safety
    /// - `file` must refer to a live broadcast queue, not resized while joined.
    /// - The payload must satisfy [`SliceConsumer`]'s full-byte initialization
    ///   requirement.
    /// - Payload bytes must be meaningful to the caller without relying on Rust
    ///   type validation.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        // SAFETY: the caller upholds the Broadcast::join_untyped requirements.
        let broadcast = unsafe { Broadcast::join_untyped(file) }?;
        // SAFETY: the caller upholds the full-byte initialization requirement.
        unsafe { broadcast.slice_consumer() }
    }

    /// Returns an untyped, lane-free handle that shares this consumer's queue
    /// mapping.
    pub fn broadcast_handle(&self) -> Broadcast<UnknownType> {
        Broadcast::from_queue(self.core.queue.clone())
    }

    fn from_queue(queue: SharedQueue) -> Result<Self, Error> {
        Ok(Self {
            core: ConsumerCore::from_queue(queue)?,
        })
    }

    /// The consumer index this handle owns. Record it so a replacement can
    /// [`recover`](Self::recover) it if this consumer's process dies.
    pub fn index(&self) -> usize {
        self.core.index()
    }

    /// Number of bytes in each payload cell, discovered from the queue header.
    pub fn payload_size(&self) -> usize {
        self.core.payload_size()
    }

    /// Takes over a consumer index whose owner died. A fully joined consumer
    /// resumes where the dead owner left off on each lane. If the owner died
    /// before joining every lane, recovery instead restarts every lane at its
    /// current reservation frontier.
    ///
    /// # Safety
    /// - All of [`Self::join`]'s requirements, plus: the consumer that owned
    ///   `index` must be dead and no other live handle may use it.
    /// - Recovery must be serialized externally; it must not race with other
    ///   recovery/force-release operations or with producer/consumer joins or
    ///   drops on the same queue.
    pub unsafe fn recover(file: &File, index: usize) -> Result<Self, Error> {
        // SAFETY: the caller upholds the untyped shared queue join requirements.
        let broadcast = unsafe { Broadcast::join_untyped(file) }?;
        // SAFETY: the caller guarantees the previous owner is dead and
        // serializes recovery.
        unsafe { broadcast.recover_slice_consumer(index) }
    }

    fn recover_in_queue(queue: SharedQueue, index: usize) -> Result<Self, Error> {
        Ok(Self {
            core: ConsumerCore::recover_in_queue(queue, index)?,
        })
    }

    /// Force-releases a consumer index whose owner died, returning it to the
    /// free pool.
    ///
    /// # Safety
    /// - As [`Self::join`], plus: the consumer that owned `index` must be dead
    ///   and no other live handle may use it.
    /// - Force-release must be serialized externally, with the same restrictions
    ///   as [`Self::recover`].
    pub unsafe fn force_release(file: &File, index: usize) -> Result<(), Error> {
        // SAFETY: the caller upholds the untyped shared queue join requirements.
        let broadcast = unsafe { Broadcast::join_untyped(file) }?;
        // SAFETY: the caller guarantees the previous owner is dead and
        // serializes force-release.
        unsafe { broadcast.force_release(index) }
    }

    /// Reserves the next available payload and exposes it as bytes, or `None` if
    /// every lane is caught up. The returned guard holds this consumer's cursor
    /// until it is dropped.
    #[must_use]
    pub fn try_read(&mut self) -> Option<SliceReadGuard<'_>> {
        self.try_reserve_read()
    }

    /// Alias for [`Self::try_read`].
    #[must_use]
    pub fn try_reserve_read(&mut self) -> Option<SliceReadGuard<'_>> {
        Some(self.read_guard_for(self.core.next_readable()?))
    }

    fn read_guard_for(&mut self, readable: ReadableLane) -> SliceReadGuard<'_> {
        let payload = self.core.payload_ptr(readable);
        let len = self.core.payload_size();
        SliceReadGuard {
            consumer: &mut self.core,
            lane: readable.lane,
            payload,
            len,
        }
    }

    /// Reserves up to `max` consecutive published payloads from the next
    /// readable lane, or `None` if every lane is caught up.
    #[must_use]
    pub fn try_reserve_read_batch(&mut self, max: NonZeroUsize) -> Option<SliceReadBatch<'_>> {
        Some(self.read_batch_for(self.core.next_readable()?, max))
    }

    fn read_batch_for(&mut self, readable: ReadableLane, max: NonZeroUsize) -> SliceReadBatch<'_> {
        let payload_size = self.core.payload_size();
        SliceReadBatch {
            consumer: &mut self.core,
            lane: readable.lane,
            start: readable.sequence,
            count: readable.count(max),
            payload_size,
        }
    }

    /// Blocks until any lane has an unread payload or `timeout` elapses, then
    /// returns a [`SliceReadGuard`] for it; `Err(Timeout)` if none arrived.
    pub fn read_timeout(&mut self, timeout: Duration) -> Result<SliceReadGuard<'_>, WaitError> {
        self.reserve_read_timeout(timeout)
    }

    /// Blocks until any lane has an unread payload or `timeout` elapses, then
    /// returns a [`SliceReadGuard`] for it; `Err(Timeout)` if none arrived.
    pub fn reserve_read_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<SliceReadGuard<'_>, WaitError> {
        Ok(self.read_guard_for(self.core.wait_until_readable(timeout)?))
    }

    /// Blocks until any lane has unread payloads or `timeout` elapses, then
    /// returns a [`SliceReadBatch`] of up to `max` of them.
    pub fn reserve_read_batch_timeout(
        &mut self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<SliceReadBatch<'_>, WaitError> {
        Ok(self.read_batch_for(self.core.wait_until_readable(timeout)?, max))
    }
}

// SAFETY: a slice consumer is single-threaded (read ops take `&mut self`) but
// may be moved between threads.
unsafe impl Send for SliceConsumer {}

/// An in-place borrow of one published payload as bytes.
#[must_use]
pub struct SliceReadGuard<'a> {
    consumer: &'a mut ConsumerCore,
    lane: LaneIndex<InitializedLane>,
    payload: NonNull<u8>,
    len: usize,
}

impl core::fmt::Debug for SliceReadGuard<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let metadata = self.lane_metadata();
        f.debug_struct("SliceReadGuard")
            .field("lane_index", &metadata.lane())
            .field("producer_id", &metadata.producer_id())
            .field("len", &self.len())
            .finish_non_exhaustive()
    }
}

impl SliceReadGuard<'_> {
    /// Metadata for the producer lane this payload was published on.
    pub fn lane_metadata(&self) -> LaneMetadata<'_> {
        self.consumer.lane_metadata(self.lane)
    }

    /// Byte length of the payload.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the payload bytes.
    pub fn as_slice(&self) -> &[u8] {
        let payload = NonNull::slice_from_raw_parts(self.payload, self.len);
        // SAFETY: the cell is published and held by this consumer's cursor.
        unsafe { payload.as_ref() }
    }
}

impl AsRef<[u8]> for SliceReadGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for SliceReadGuard<'_> {
    fn drop(&mut self) {
        self.consumer.advance(self.lane, NonZeroUsize::MIN);
    }
}

/// An in-place borrow of consecutive published payloads from one lane as bytes.
#[must_use]
pub struct SliceReadBatch<'a> {
    consumer: &'a mut ConsumerCore,
    lane: LaneIndex<InitializedLane>,
    start: usize,
    count: NonZeroUsize,
    payload_size: usize,
}

impl core::fmt::Debug for SliceReadBatch<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let metadata = self.lane_metadata();
        f.debug_struct("SliceReadBatch")
            .field("lane_index", &metadata.lane())
            .field("producer_id", &metadata.producer_id())
            .field("len", &self.len())
            .field("payload_size", &self.payload_size())
            .finish_non_exhaustive()
    }
}

impl SliceReadBatch<'_> {
    /// Metadata for the producer lane every payload in this batch was published on.
    pub fn lane_metadata(&self) -> LaneMetadata<'_> {
        self.consumer.lane_metadata(self.lane)
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.count.get()
    }

    /// Byte length of each payload in the batch.
    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    /// Byte slice for the payload at `index`.
    ///
    /// # Safety
    /// - `index < len`.
    pub unsafe fn as_slice(&self, index: usize) -> &[u8] {
        debug_assert!(index < self.count.get());
        let payload = self
            .consumer
            .lane(self.lane.get())
            .payload_ptr(self.start.wrapping_add(index));
        let payload = NonNull::slice_from_raw_parts(payload, self.payload_size);
        // SAFETY: forwarded; the cell is published and held by this consumer's
        // cursor until the batch is dropped.
        unsafe { payload.as_ref() }
    }
}

impl Drop for SliceReadBatch<'_> {
    fn drop(&mut self) {
        self.consumer.advance(self.lane, self.count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(miri))]
    use crate::shmem::create_temp_shmem_file;

    fn assert_debug<T: core::fmt::Debug>() {}

    #[test]
    fn public_debug_trait_bounds() {
        #[derive(Clone, Copy)]
        struct CopyOnly;
        #[derive(Debug, Clone, Copy)]
        struct DebugAndCopy;

        assert_debug::<BroadcastConfig>();
        assert_debug::<Broadcast<CopyOnly>>();
        assert_debug::<UnknownType>();
        assert_debug::<ProducerId>();
        assert_debug::<LaneMetadata<'static>>();
        assert_debug::<Producer<CopyOnly>>();
        assert_debug::<WriteGuard<'static, CopyOnly>>();
        assert_debug::<WriteBatch<'static, CopyOnly>>();
        assert_debug::<Consumer<CopyOnly>>();
        assert_debug::<ReadBatch<'static, CopyOnly>>();
        assert_debug::<SliceConsumer>();
        assert_debug::<SliceReadGuard<'static>>();
        assert_debug::<SliceReadBatch<'static>>();
        assert_debug::<ReadGuard<'static, DebugAndCopy>>();
    }

    type Payload = u64;

    /// Placeholder id for tests that don't assert on producer metadata.
    const BOGUS_ID: ProducerId = ProducerId::new(1111);

    type CreateProducer = fn(BroadcastConfig) -> Producer<Payload>;
    type CreateIdentifiedProducer = fn(BroadcastConfig, ProducerId) -> Producer<Payload>;

    /// In-process (heap-backed) producer with a caller-chosen id. The
    /// `Producer` keeps the region alive via its `SharedQueue`'s `Arc<Region>`.
    fn create_identified_heap_producer(
        config: BroadcastConfig,
        id: ProducerId,
    ) -> Producer<Payload> {
        let size = QueueLayout::new::<Payload>(&config).expect("layout").total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).expect("alloc");
        // SAFETY: freshly allocated region, initialized exactly once here.
        let queue = unsafe {
            SharedQueue::create_in_region::<Payload>(&region, &config, DEFAULT_QUEUE_IDENTIFIER)
        }
        .unwrap();
        Producer::from_queue(queue, id).unwrap()
    }

    /// Heap-backed producer with a placeholder id.
    fn create_heap_producer(config: BroadcastConfig) -> Producer<Payload> {
        create_identified_heap_producer(config, BOGUS_ID)
    }

    /// File-backed producer (mmap) with a caller-chosen id. Not run
    /// under miri (no mmap).
    #[cfg(not(miri))]
    fn create_identified_file_backed_producer(
        config: BroadcastConfig,
        id: ProducerId,
    ) -> Producer<Payload> {
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: a fresh temp file, initialized exactly once here.
        unsafe { Producer::create(&file, config, id) }.expect("create")
    }

    /// File-backed producer with a placeholder id.
    #[cfg(not(miri))]
    fn create_file_backed_producer(config: BroadcastConfig) -> Producer<Payload> {
        create_identified_file_backed_producer(config, BOGUS_ID)
    }

    /// Every behavioral test runs against both backings (heap always; file-backed
    /// when not under miri), matching the other queues.
    fn producer_creators() -> &'static [CreateProducer] {
        &[
            create_heap_producer,
            #[cfg(not(miri))]
            create_file_backed_producer,
        ]
    }

    /// Backings for tests that pin a specific producer id at creation.
    fn identified_producer_creators() -> &'static [CreateIdentifiedProducer] {
        &[
            create_identified_heap_producer,
            #[cfg(not(miri))]
            create_identified_file_backed_producer,
        ]
    }

    #[cfg(not(miri))]
    #[test]
    fn queue_identifier_returns_supplied_zero() {
        assert_queue_identifier(0);
    }

    #[cfg(not(miri))]
    #[test]
    fn queue_identifier_returns_supplied_nonzero() {
        assert_queue_identifier(42);
    }

    #[cfg(not(miri))]
    #[test]
    fn queue_identifier_returns_supplied_max() {
        assert_queue_identifier(u64::MAX);
    }

    #[cfg(not(miri))]
    fn assert_queue_identifier(identifier: u64) {
        let file = create_temp_shmem_file().expect("temp file");
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        // SAFETY: fresh file, initialized once with process-portable u64 payloads.
        let broadcast =
            unsafe { Broadcast::<u64>::create_with_identifier(&file, config, identifier) }.unwrap();
        assert_eq!(broadcast.queue_identifier(), identifier);

        // SAFETY: the file contains a live broadcast queue with u64 payloads.
        let joined = unsafe { Broadcast::<u64>::join(&file) }.unwrap();
        assert_eq!(joined.queue_identifier(), identifier);
        // SAFETY: u64 has fully initialized, process-portable payload bytes.
        let untyped = unsafe { Broadcast::join_untyped(&file) }.unwrap();
        assert_eq!(untyped.queue_identifier(), identifier);
    }

    #[cfg(not(miri))]
    #[test]
    fn queue_identifier_defaults_to_zero() {
        let file = create_temp_shmem_file().expect("temp file");
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        // SAFETY: fresh file, initialized once with process-portable u64 payloads.
        let broadcast = unsafe { Broadcast::<u64>::create(&file, config) }.unwrap();
        assert_eq!(broadcast.queue_identifier(), 0);

        // SAFETY: the file contains a live broadcast queue with u64 payloads.
        let joined = unsafe { Broadcast::<u64>::join(&file) }.unwrap();
        assert_eq!(joined.queue_identifier(), 0);
        // SAFETY: u64 has fully initialized, process-portable payload bytes.
        let untyped = unsafe { Broadcast::join_untyped(&file) }.unwrap();
        assert_eq!(untyped.queue_identifier(), 0);
    }

    #[test]
    fn clones_until_lanes_exhausted() {
        for create in producer_creators() {
            let p0 = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let broadcast = p0.broadcast_handle();
            // Two lanes total: the original plus one binding from a cloned
            // broadcast exhausts them.
            let p1 = broadcast.producer(BOGUS_ID).unwrap();
            assert!(matches!(
                broadcast.producer(BOGUS_ID),
                Err(Error::ProducerSlotsExhausted)
            ));
            drop(p1);
            // The dropped lane is retired, not returned to the pool.
            assert!(matches!(
                broadcast.producer(BOGUS_ID),
                Err(Error::ProducerSlotsExhausted)
            ));
        }
    }

    #[test]
    fn try_write_publishes() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            for value in 0..5u64 {
                assert!(p.try_write(value * 10).is_ok());
            }
            for value in 0..5u64 {
                assert_eq!(c.try_read(), Some(value * 10));
            }
        }
    }

    #[test]
    fn write_batch_publishes_on_drop() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            {
                // SAFETY: every reserved slot is initialized below.
                let mut batch =
                    unsafe { p.try_reserve_write_batch(NonZeroUsize::new(3).unwrap()) }.unwrap();
                assert_eq!(batch.len(), 3);
                for index in 0..3usize {
                    // SAFETY: index < len.
                    unsafe { batch.write(index, (index as u64) + 1) };
                }
            }
            for value in 1..=3u64 {
                assert_eq!(c.try_read(), Some(value));
            }
        }
    }

    #[test]
    fn try_write_slice_publishes_all_items() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            assert!(p.try_write_slice(&[]));
            assert!(p.try_write_slice(&[1, 2, 3]));

            for value in 1..=3u64 {
                assert_eq!(c.try_read(), Some(value));
            }
            assert_eq!(c.try_read(), None);
        }
    }

    #[test]
    fn backpressure_without_consumers_is_only_capacity() {
        // No consumers: the producer overwrites freely past one revolution.
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            for value in 0..16u64 {
                assert!(p.try_write(value).is_ok());
            }
        }
    }

    #[test]
    fn zero_consumer_slots_lets_producer_run_free() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 0,
            });
            // No consumer can ever constrain the lane, so writes never block.
            for value in 0..16u64 {
                assert!(p.try_write(value).is_ok());
            }
            // And no consumer can join when there are no consumer slots.
            assert!(matches!(
                p.broadcast_handle().consumer(),
                Err(Error::ConsumerSlotsExhausted)
            ));
        }
    }

    /// Only a file can be opened before it holds a valid queue (e.g. by another
    /// process); a heap region is always initialized in-process before it is
    /// joined, so there is no uninitialized heap case to reject.
    #[cfg(not(miri))]
    #[test]
    fn join_rejects_uninitialized_file() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let size = QueueLayout::new::<Payload>(&config).unwrap().total;
        let file = create_temp_shmem_file().expect("temp file");
        file.set_len(size as u64).expect("set_len");
        // SAFETY: sized-but-zeroed file → magic mismatch.
        let err = unsafe { SharedQueue::join::<Payload>(&file) };
        assert!(matches!(err, Err(Error::InvalidMagic)));
    }

    #[test]
    fn header_records_payload_layout() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let size = QueueLayout::new::<Payload>(&config).expect("layout").total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).expect("alloc");
        // SAFETY: freshly allocated region, initialized exactly once.
        let queue = unsafe {
            SharedQueue::create_in_region::<Payload>(&region, &config, DEFAULT_QUEUE_IDENTIFIER)
        }
        .unwrap();

        assert_eq!(queue.header().payload_size, size_of::<Payload>());
        assert_eq!(queue.header().payload_align, align_of::<Payload>());
    }

    #[test]
    fn typed_join_rejects_payload_layout_mismatch() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let size = QueueLayout::new::<u64>(&config).expect("layout").total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).expect("alloc");
        // SAFETY: freshly allocated region, initialized exactly once.
        unsafe { SharedQueue::create_in_region::<u64>(&region, &config, DEFAULT_QUEUE_IDENTIFIER) }
            .unwrap();

        // Same payload size as `u64`, but different alignment.
        // SAFETY: the region is a live broadcast queue; validation should fail.
        let err = unsafe { SharedQueue::join_region::<[u8; 8]>(&region) };
        assert!(matches!(err, Err(Error::InvalidBufferSize)));

        // Different payload size.
        // SAFETY: the region is a live broadcast queue; validation should fail.
        let err = unsafe { SharedQueue::join_region::<[u8; 4]>(&region) };
        assert!(matches!(err, Err(Error::InvalidBufferSize)));
    }

    #[test]
    fn slice_consumer_reads_payload_bytes() {
        let mut producer = create_heap_producer(BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        });
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();
        assert_eq!(consumer.payload_size(), size_of::<Payload>());

        let value = 0x0102_0304_0506_0708u64;
        assert!(producer.try_write(value).is_ok());

        let guard = consumer.try_read().expect("readable");
        assert_eq!(guard.len(), size_of::<Payload>());
        assert_eq!(guard.as_slice(), value.to_ne_bytes());
        drop(guard);

        assert!(consumer.try_read().is_none());
    }

    #[test]
    fn slice_consumer_batch_reads_payload_bytes() {
        let mut producer = create_heap_producer(BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        });
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();

        assert!(producer.try_write_slice(&[11, 12, 13]));
        {
            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .expect("readable batch");
            assert_eq!(batch.len(), 2);
            assert_eq!(batch.payload_size(), size_of::<Payload>());
            // SAFETY: index 0 < batch.len().
            unsafe { assert_eq!(batch.as_slice(0), 11u64.to_ne_bytes()) };
            // SAFETY: index 1 < batch.len().
            unsafe { assert_eq!(batch.as_slice(1), 12u64.to_ne_bytes()) };
        }

        let guard = consumer.try_read().expect("remaining item");
        assert_eq!(guard.as_slice(), 13u64.to_ne_bytes());
    }

    #[cfg(not(miri))]
    #[test]
    fn slice_consumer_joins_file_without_payload_type() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: a fresh temp file, initialized exactly once here.
        let mut producer = unsafe { Producer::<Payload>::create(&file, config, BOGUS_ID) }.unwrap();
        // SAFETY: the file now contains a live broadcast queue.
        let mut consumer = unsafe { SliceConsumer::join(&file) }.unwrap();
        assert_eq!(consumer.payload_size(), size_of::<Payload>());

        let value = 0xfeed_face_cafe_beefu64;
        assert!(producer.try_write(value).is_ok());

        let guard = consumer.read_timeout(Duration::ZERO).expect("readable");
        assert_eq!(guard.as_slice(), value.to_ne_bytes());
    }

    #[test]
    fn layout_rejects_oversized_capacity_without_panicking() {
        let config = BroadcastConfig {
            capacity: usize::MAX,
            producer_slots: 1,
            consumer_slots: 1,
        };
        assert!(QueueLayout::new::<Payload>(&config).is_err());
    }

    #[test]
    fn layout_rejects_zero_capacity() {
        let config = BroadcastConfig {
            capacity: 0,
            producer_slots: 1,
            consumer_slots: 1,
        };
        assert!(QueueLayout::new::<Payload>(&config).is_err());
    }

    #[test]
    fn every_consumer_observes_every_item_in_order() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 2,
            });
            // Both consumers join before anything is published, so they start at 0.
            let mut c0 = p.broadcast_handle().consumer().unwrap();
            let mut c1 = p.broadcast_handle().consumer().unwrap();
            // A third consumer would exhaust the slots.
            assert!(matches!(
                p.broadcast_handle().consumer(),
                Err(Error::ConsumerSlotsExhausted)
            ));

            for value in 0..5u64 {
                assert!(p.try_write(value * 10).is_ok());
            }
            for value in 0..5u64 {
                assert_eq!(c0.try_read(), Some(value * 10));
                assert_eq!(c1.try_read(), Some(value * 10));
            }
            assert_eq!(c0.try_read(), None);
            assert_eq!(c1.try_read(), None);

            // A released consumer's index can be reclaimed.
            drop(c1);
            assert!(p.broadcast_handle().consumer().is_ok());
        }
    }

    #[cfg(not(miri))]
    #[test]
    fn threaded_producer_publish_is_visible_to_all_consumers() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const ITEMS: usize = 257;
        const BATCH: usize = 7;

        for create in producer_creators() {
            let mut producer = create(BroadcastConfig {
                capacity: 32,
                producer_slots: 1,
                consumer_slots: 2,
            });
            let consumers = [
                producer.broadcast_handle().consumer().unwrap(),
                producer.broadcast_handle().consumer().unwrap(),
            ];
            let start = Arc::new(Barrier::new(consumers.len() + 1));

            let consumer_threads: Vec<_> = consumers
                .into_iter()
                .map(|mut consumer| {
                    let start = Arc::clone(&start);
                    thread::spawn(move || {
                        start.wait();
                        let mut seen = Vec::with_capacity(ITEMS);
                        for _ in 0..ITEMS {
                            seen.push(
                                consumer
                                    .read_timeout(Duration::from_secs(1))
                                    .expect("published value"),
                            );
                        }
                        assert!(matches!(
                            consumer.read_timeout(Duration::ZERO),
                            Err(WaitError::Timeout)
                        ));
                        seen
                    })
                })
                .collect();

            let producer_start = Arc::clone(&start);
            let producer_thread = thread::spawn(move || {
                producer_start.wait();
                let mut next = 0usize;
                while next < ITEMS {
                    let count = (ITEMS - next).min(BATCH);
                    let mut batch = [0u64; BATCH];
                    for (offset, slot) in batch[..count].iter_mut().enumerate() {
                        *slot = (next + offset) as u64;
                    }
                    if producer.try_write_slice(&batch[..count]) {
                        next += count;
                    } else {
                        thread::yield_now();
                    }
                }
            });

            producer_thread.join().expect("producer thread");
            let expected: Vec<_> = (0..ITEMS as u64).collect();
            for consumer_thread in consumer_threads {
                assert_eq!(consumer_thread.join().expect("consumer thread"), expected);
            }
        }
    }

    #[test]
    fn slow_consumer_backpressures_producer() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();

            // Fill the ring; the consumer has read nothing, so the producer blocks.
            for value in 0..4u64 {
                assert!(p.try_write(value).is_ok());
            }
            assert!(p.try_write(99).is_err());

            // The consumer reads one; one cell frees up.
            assert_eq!(c.try_read(), Some(0));
            assert!(p.try_write(99).is_ok());
            assert!(p.try_write(100).is_err());
        }
    }

    #[test]
    fn read_guard_holds_cell_until_dropped() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            assert!(p.try_write(10).is_ok());

            let guard = c.try_reserve_read().expect("readable");
            assert_eq!(*guard.as_ref(), 10);

            // The producer can fill the rest of the ring but cannot overwrite the
            // cell the guard still holds (sequence 0).
            for value in 11..14u64 {
                assert!(p.try_write(value).is_ok());
            }
            assert!(p.try_write(14).is_err());
            assert_eq!(*guard.as_ref(), 10);

            // Dropping the guard commits, advancing past the cell and freeing it.
            drop(guard);
            assert!(p.try_write(14).is_ok());
        }
    }

    #[test]
    fn write_guard_publishes_on_drop_and_read_guard_reads() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();

            // A single write guard publishes its cell when dropped.
            {
                // SAFETY: the reserved slot is initialized before `guard` is dropped.
                let mut guard = unsafe { p.try_reserve_write() }.unwrap();
                guard.as_mut().write(42);
            }
            // `write` consumes the guard and publishes on drop too.
            // SAFETY: `write` initializes the reserved slot before publishing.
            unsafe { p.try_reserve_write() }.unwrap().write(43);

            assert_eq!(c.try_reserve_read().unwrap().read(), 42);
            assert_eq!(c.try_reserve_read().unwrap().read(), 43);
            assert!(c.try_reserve_read().is_none());
        }
    }

    #[test]
    fn read_batch_is_bounded_and_commits_on_drop() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            for value in 0..5u64 {
                assert!(p.try_write(value).is_ok());
            }

            // Bounded by `max` (3) below the available run (5); drop commits it.
            {
                let batch = c
                    .try_reserve_read_batch(NonZeroUsize::new(3).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 3);
                let (first, second) = batch.as_slices();
                assert_eq!(first, &[0, 1, 2]);
                assert!(second.is_empty());
                for index in 0..3 {
                    // SAFETY: `index < len`; `Payload` is `u64`.
                    assert_eq!(unsafe { batch.read(index) }, index as u64);
                }
            }
            // Now bounded by the available run (2), not `max`.
            {
                let batch = c
                    .try_reserve_read_batch(NonZeroUsize::new(10).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 2);
                for index in 0..2 {
                    // SAFETY: `index < len`; `Payload` is `u64`.
                    assert_eq!(unsafe { *batch.as_ref(index) }, (index as u64) + 3);
                }
            }
            assert!(c
                .try_reserve_read_batch(NonZeroUsize::new(1).unwrap())
                .is_none());
        }
    }

    #[test]
    fn read_batch_holds_cells_until_dropped() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            for value in 0..4u64 {
                assert!(p.try_write(value).is_ok());
            }

            // The batch holds the whole ring; the producer is fully blocked.
            {
                let batch = c
                    .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                    .unwrap();
                assert_eq!(batch.len(), 4);
                assert!(p.try_write(99).is_err());
            }
            // Dropping the batch commits all four cells, freeing them.
            for value in 99..103u64 {
                assert!(p.try_write(value).is_ok());
            }
        }
    }

    #[test]
    fn read_batch_as_slices_supports_wrapping() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            for value in 0..4u64 {
                assert!(p.try_write(value).is_ok());
            }
            assert_eq!(c.try_read(), Some(0));
            assert_eq!(c.try_read(), Some(1));
            assert!(p.try_write(4).is_ok());
            assert!(p.try_write(5).is_ok());

            let batch = c
                .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                .unwrap();
            let (first, second) = batch.as_slices();
            assert_eq!(first, &[2, 3]);
            assert_eq!(second, &[4, 5]);
        }
    }

    #[test]
    fn consumer_joining_late_skips_earlier_items() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 8,
                producer_slots: 1,
                consumer_slots: 1,
            });
            for value in 0..3u64 {
                assert!(p.try_write(value).is_ok());
            }
            // Joins at the current reservation (3), so it sees only later items.
            let mut c = p.broadcast_handle().consumer().unwrap();
            assert!(p.try_write(99).is_ok());
            assert_eq!(c.try_read(), Some(99));
            assert_eq!(c.try_read(), None);
        }
    }

    #[test]
    fn consumer_joining_during_unpublished_reservation_skips_it() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        let mut producer = Producer::from_queue(queue.clone(), BOGUS_ID).unwrap();

        // SAFETY: the reserved slot is initialized before the guard is dropped.
        let mut guard = unsafe { producer.try_reserve_write() }.unwrap();
        let mut consumer = Consumer::from_queue(queue.clone()).unwrap();

        assert!(consumer.try_reserve_read().is_none());
        guard.as_mut().write(42);
        drop(guard);

        assert_eq!(consumer.try_read(), None);

        assert!(producer.try_write(99).is_ok());
        assert_eq!(consumer.try_read(), Some(99));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn reserve_read_timeout_times_out_then_observes_publication() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();

            // Nothing published on any lane yet: a zero timeout reports `Timeout`
            // rather than blocking.
            assert!(matches!(
                c.reserve_read_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            // A publish on any lane satisfies the (already-elapsed) wait.
            assert!(p.try_write(42).is_ok());
            let guard = c.reserve_read_timeout(Duration::ZERO).expect("readable");
            assert_eq!(*guard.as_ref(), 42);
        }
    }

    /// Exercises the real `FUTEX_WAIT` syscall (not just the elapsed-deadline
    /// short-circuit): with nothing published, a bounded wait blocks in the
    /// kernel on the wake counter and returns `Timeout`.
    #[cfg(not(miri))]
    #[test]
    fn reserve_read_timeout_blocks_in_futex_then_times_out() {
        for create in producer_creators() {
            let p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();
            assert!(matches!(
                c.reserve_read_timeout(Duration::from_millis(5)),
                Err(WaitError::Timeout)
            ));
        }
    }

    #[test]
    fn read_and_batch_timeout_observe_publication() {
        for create in producer_creators() {
            let mut p = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let mut c = p.broadcast_handle().consumer().unwrap();

            assert!(matches!(
                c.read_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));
            assert!(matches!(
                c.reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            for value in 0..2u64 {
                assert!(p.try_write(value).is_ok());
            }
            // The batch sees both published values; dropping it commits them.
            {
                let batch = c
                    .reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO)
                    .expect("readable");
                assert_eq!(batch.len(), 2);
            }
            assert!(matches!(
                c.reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO),
                Err(WaitError::Timeout)
            ));
        }
    }

    #[test]
    fn broadcast_reports_the_configured_lane_count() {
        for create in producer_creators() {
            let producer = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });

            let producer_slots = producer.broadcast_handle().producer_slots();

            assert_eq!(producer_slots, 2);
        }
    }

    #[test]
    fn producer_reports_its_configured_id() {
        for create in identified_producer_creators() {
            let producer_id = ProducerId::new(42);
            let producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 1,
                    consumer_slots: 1,
                },
                producer_id,
            );

            let reported_id = producer.producer_id();

            assert_eq!(reported_id, producer_id);
        }
    }

    #[test]
    fn owned_lane_metadata_reports_the_producer_id() {
        for create in identified_producer_creators() {
            let producer_id = ProducerId::new(42);
            let producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 1,
                    consumer_slots: 1,
                },
                producer_id,
            );
            let broadcast = producer.broadcast_handle();

            let metadata = broadcast
                .lane_metadata(producer.index())
                .expect("owned lane has metadata");

            assert_eq!(producer_id, metadata.producer_id());
        }
    }

    #[test]
    fn owned_lane_metadata_reports_the_lane() {
        for create in identified_producer_creators() {
            let producer_id = ProducerId::new(42);
            let producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 1,
                    consumer_slots: 1,
                },
                producer_id,
            );
            let broadcast = producer.broadcast_handle();
            let producer_lane = producer.index();

            let metadata = broadcast
                .lane_metadata(producer.index())
                .expect("owned lane has metadata");

            assert_eq!(metadata.lane(), producer_lane);
        }
    }

    #[test]
    fn owned_lane_metadata_starts_with_no_rejected_items() {
        for create in producer_creators() {
            let producer = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let broadcast = producer.broadcast_handle();

            let rejected_items = broadcast
                .lane_metadata(producer.index())
                .expect("owned lane has metadata")
                .rejected_items();

            assert_eq!(rejected_items, 0);
        }
    }

    #[test]
    fn never_owned_lane_has_no_metadata() {
        for create in producer_creators() {
            let producer = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let broadcast = producer.broadcast_handle();

            let metadata = broadcast.lane_metadata(1 - producer.index());

            assert!(metadata.is_none());
        }
    }

    #[test]
    fn rejected_items_count_writes_refused_by_backpressure() {
        for create in producer_creators() {
            let mut producer = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let broadcast = producer.broadcast_handle();
            let _consumer = broadcast.consumer().unwrap();
            for value in 0..4u64 {
                producer.try_write(value).expect("ring has capacity");
            }

            let _ = producer.try_write(99);
            let _ = producer.try_write_slice(&[1, 2]);

            assert_eq!(
                broadcast
                    .lane_metadata(producer.index())
                    .unwrap()
                    .rejected_items(),
                3
            );
        }
    }

    #[test]
    fn read_guard_reports_the_source_lane() {
        for create in identified_producer_creators() {
            let idle_producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 2,
                    consumer_slots: 1,
                },
                ProducerId::new(41),
            );
            let broadcast = idle_producer.broadcast_handle();
            let mut publishing_producer = broadcast.producer(ProducerId::new(42)).unwrap();
            let mut consumer = broadcast.consumer().unwrap();
            publishing_producer.try_write(7).expect("ring has capacity");

            let guard = consumer.try_reserve_read().expect("readable");
            let source_lane = guard.lane_metadata().lane();
            drop(guard);

            assert_eq!(source_lane, publishing_producer.index());
        }
    }

    #[test]
    fn read_guard_reports_the_source_producer_id() {
        for create in identified_producer_creators() {
            let idle_producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 2,
                    consumer_slots: 1,
                },
                ProducerId::new(41),
            );
            let broadcast = idle_producer.broadcast_handle();
            let mut publishing_producer = broadcast.producer(ProducerId::new(42)).unwrap();
            let mut consumer = broadcast.consumer().unwrap();
            publishing_producer.try_write(7).expect("ring has capacity");

            let guard = consumer.try_reserve_read().expect("readable");
            let source_producer_id = guard.lane_metadata().producer_id();
            drop(guard);

            assert_eq!(source_producer_id, publishing_producer.producer_id());
        }
    }

    #[test]
    fn lane_metadata_is_none_for_an_out_of_range_index() {
        for create in producer_creators() {
            let producer = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 2,
                consumer_slots: 1,
            });
            let broadcast = producer.broadcast_handle();

            let metadata = broadcast.lane_metadata(2);

            assert!(metadata.is_none());
        }
    }

    #[test]
    fn lane_metadata_remains_available_after_producer_drop() {
        for create in identified_producer_creators() {
            let producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 1,
                    consumer_slots: 1,
                },
                ProducerId::new(42),
            );
            let broadcast = producer.broadcast_handle();
            let lane = producer.index();

            drop(producer);

            assert!(broadcast.lane_metadata(lane).is_some());
        }
    }

    #[test]
    fn borrowed_lane_metadata_keeps_the_producer_id_after_drop() {
        for create in identified_producer_creators() {
            let producer_id = ProducerId::new(42);
            let producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 1,
                    consumer_slots: 1,
                },
                producer_id,
            );
            let broadcast = producer.broadcast_handle();
            let metadata = broadcast
                .lane_metadata(producer.index())
                .expect("owned lane has metadata");

            drop(producer);

            assert_eq!(metadata.producer_id(), producer_id);
        }
    }

    #[test]
    fn retired_lane_metadata_keeps_the_rejected_items_count() {
        for create in producer_creators() {
            let mut producer = create(BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            });
            let broadcast = producer.broadcast_handle();
            let _consumer = broadcast.consumer().unwrap();
            for value in 0..4u64 {
                producer.try_write(value).expect("ring has capacity");
            }
            let _ = producer.try_write(99);
            let lane = producer.index();

            drop(producer);

            let metadata = broadcast
                .lane_metadata(lane)
                .expect("retired lane retains metadata");
            assert_eq!(metadata.rejected_items(), 1);
        }
    }

    #[test]
    fn borrowed_lane_metadata_remains_usable_after_a_read() {
        for create in identified_producer_creators() {
            let idle_producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 2,
                    consumer_slots: 1,
                },
                ProducerId::new(41),
            );
            let broadcast = idle_producer.broadcast_handle();
            let mut publishing_producer = broadcast.producer(ProducerId::new(42)).unwrap();
            let mut consumer = broadcast.consumer().unwrap();
            let metadata = broadcast
                .lane_metadata(publishing_producer.index())
                .expect("owned lane has metadata");
            publishing_producer.try_write(7).expect("ring has capacity");

            let guard = consumer.try_reserve_read().expect("readable");
            drop(guard);

            assert_eq!(metadata.producer_id(), publishing_producer.producer_id());
        }
    }

    #[test]
    fn read_batch_reports_the_source_lane() {
        for create in identified_producer_creators() {
            let idle_producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 2,
                    consumer_slots: 1,
                },
                ProducerId::new(41),
            );
            let broadcast = idle_producer.broadcast_handle();
            let mut publishing_producer = broadcast.producer(ProducerId::new(42)).unwrap();
            let mut consumer = broadcast.consumer().unwrap();
            let _ = publishing_producer.try_write_slice(&[8, 9]);

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .expect("readable batch");
            let source_lane = batch.lane_metadata().lane();
            drop(batch);

            assert_eq!(source_lane, publishing_producer.index());
        }
    }

    #[test]
    fn read_batch_reports_the_source_producer_id() {
        for create in identified_producer_creators() {
            let idle_producer = create(
                BroadcastConfig {
                    capacity: 4,
                    producer_slots: 2,
                    consumer_slots: 1,
                },
                ProducerId::new(41),
            );
            let broadcast = idle_producer.broadcast_handle();
            let mut publishing_producer = broadcast.producer(ProducerId::new(42)).unwrap();
            let mut consumer = broadcast.consumer().unwrap();
            let _ = publishing_producer.try_write_slice(&[8, 9]);

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .expect("readable batch");
            let source_producer_id = batch.lane_metadata().producer_id();
            drop(batch);

            assert_eq!(source_producer_id, publishing_producer.producer_id());
        }
    }

    #[test]
    fn untyped_broadcast_exposes_lane_metadata() {
        let producer = create_identified_heap_producer(
            BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            },
            ProducerId::new(42),
        );
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();
        let broadcast: Broadcast<UnknownType> = consumer.broadcast_handle();

        let metadata = broadcast.lane_metadata(producer.index());

        assert!(metadata.is_some());
    }

    #[test]
    fn slice_read_guard_reports_the_source_lane() {
        let mut producer = create_identified_heap_producer(
            BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            },
            ProducerId::new(42),
        );
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();
        producer.try_write(1).expect("ring has capacity");

        let guard = consumer.try_read().expect("readable");
        let source_lane = guard.lane_metadata().lane();
        drop(guard);

        assert_eq!(source_lane, producer.index());
    }

    #[test]
    fn slice_read_guard_reports_the_source_producer_id() {
        let mut producer = create_identified_heap_producer(
            BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            },
            ProducerId::new(42),
        );
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();
        producer.try_write(1).expect("ring has capacity");

        let guard = consumer.try_read().expect("readable");
        let source_producer_id = guard.lane_metadata().producer_id();
        drop(guard);

        assert_eq!(source_producer_id, producer.producer_id());
    }

    #[test]
    fn slice_read_batch_reports_the_source_lane() {
        let mut producer = create_identified_heap_producer(
            BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            },
            ProducerId::new(42),
        );
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();
        let _ = producer.try_write_slice(&[2, 3]);

        let batch = consumer
            .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
            .expect("readable batch");
        let source_lane = batch.lane_metadata().lane();
        drop(batch);

        assert_eq!(source_lane, producer.index());
    }

    #[test]
    fn slice_read_batch_reports_the_source_producer_id() {
        let mut producer = create_identified_heap_producer(
            BroadcastConfig {
                capacity: 4,
                producer_slots: 1,
                consumer_slots: 1,
            },
            ProducerId::new(42),
        );
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut consumer = unsafe { producer.broadcast_handle().slice_consumer() }.unwrap();
        let _ = producer.try_write_slice(&[2, 3]);

        let batch = consumer
            .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
            .expect("readable batch");
        let source_producer_id = batch.lane_metadata().producer_id();
        drop(batch);

        assert_eq!(source_producer_id, producer.producer_id());
    }

    /// Allocates a heap-backed queue and returns the shared handle (recovery
    /// tests need the queue directly to simulate a crashed handle).
    fn recovery_queue(config: &BroadcastConfig) -> SharedQueue {
        let size = QueueLayout::new::<Payload>(config).expect("layout").total;
        let region = Region::alloc(NonZeroUsize::new(size).unwrap()).expect("alloc");
        // SAFETY: freshly allocated region, initialized exactly once.
        unsafe {
            SharedQueue::create_in_region::<Payload>(&region, config, DEFAULT_QUEUE_IDENTIFIER)
        }
        .unwrap()
    }

    #[test]
    fn dropped_producer_lane_stays_readable() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        let mut consumer = Consumer::from_queue(queue.clone()).unwrap();

        // A producer publishes two items and drops, retiring its lane.
        {
            let mut producer = Producer::from_queue(queue.clone(), BOGUS_ID).unwrap();
            assert!(producer.try_write(1).is_ok());
            assert!(producer.try_write(2).is_ok());
        }

        // The retired lane is never handed to a new producer.
        assert!(matches!(
            Producer::<Payload>::from_queue(queue.clone(), BOGUS_ID),
            Err(Error::ProducerSlotsExhausted)
        ));

        // The consumer still drains what the dead producer published.
        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(consumer.try_read(), None);
    }

    #[test]
    fn recover_consumer_resumes_from_last_position() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);

        // A consumer joins at sequence 0, then its process "crashes" with a read
        // position recorded (next-to-read = 1). Build that state directly (claim
        // the index, join, record the cursor) so nothing releases the slot.
        let index = queue.acquire_consumer_index().unwrap();
        let lane = queue.producer_lanes().next().unwrap();
        ConsumerCore::join_lane(&lane, index);
        queue.activate_consumer_index(index);

        let mut producer = Producer::from_queue(queue.clone(), BOGUS_ID).unwrap();
        for value in 0..3u64 {
            assert!(producer.try_write(value).is_ok());
        }
        lane.consumer_state().set_cursor(index, 1);

        // Recovery resumes at the recorded position: it reads the unread values
        // (items 1, 2), never re-reading item 0.
        let mut recovered = Consumer::recover_in_queue(queue.clone(), index).unwrap();
        assert_eq!(recovered.index(), index);
        assert_eq!(recovered.try_read(), Some(1));
        assert_eq!(recovered.try_read(), Some(2));
        assert_eq!(recovered.try_read(), None);
    }

    #[test]
    fn recover_consumer_restarts_an_interrupted_join() {
        let config = BroadcastConfig {
            capacity: 4,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        let index = queue.acquire_consumer_index().unwrap();
        let lane = queue.producer_lanes().next().unwrap();
        let mut producer = Producer::from_queue(queue.clone(), BOGUS_ID).unwrap();

        // The consumer sampled reservation 0, then the producer filled the ring
        // before the consumer published its initial limit. Simulate a crash after
        // that initial limit store but before the second reservation sample. The
        // global ownership slot deliberately remains JOINING.
        for value in 0..4u64 {
            assert!(producer.try_write(value).is_ok());
        }
        lane.consumer_state().set_cursor(index, 0);

        // Recovery must restart the incomplete join at reservation 4 rather than
        // interpreting the temporary limit as a completed cursor at sequence 0.
        let mut recovered = Consumer::recover_in_queue(queue.clone(), index).unwrap();
        assert_eq!(recovered.try_read(), None);

        assert!(producer.try_write(99).is_ok());
        assert_eq!(recovered.try_read(), Some(99));
        assert_eq!(recovered.try_read(), None);
    }

    #[test]
    fn force_release_consumer_frees_the_index_for_a_fresh_join() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);

        // The only consumer index is claimed and the lane's limit recorded, then
        // its owner "crashes" (no release).
        let index = queue.acquire_consumer_index().unwrap();
        let lane = queue.producer_lanes().next().unwrap();
        ConsumerCore::join_lane(&lane, index);
        queue.activate_consumer_index(index);
        let mut producer = Producer::from_queue(queue.clone(), BOGUS_ID).unwrap();
        for value in 0..3u64 {
            assert!(producer.try_write(value).is_ok());
        }
        lane.consumer_state().set_cursor(index, 1);

        // A fresh join can't proceed — the index is still owned.
        assert!(matches!(
            Consumer::<Payload>::from_queue(queue.clone()),
            Err(Error::ConsumerSlotsExhausted)
        ));

        // Force-release frees it (clear limits + free the index); a fresh join
        // then reclaims it at the current reservation frontier.
        for lane in queue.producer_lanes() {
            lane.consumer_state().release(index);
        }
        queue.release_consumer_index(index);
        let mut fresh = Consumer::from_queue(queue.clone()).unwrap();
        assert_eq!(fresh.index(), index);
        assert_eq!(fresh.try_read(), None);

        assert!(producer.try_write(99).is_ok());
        assert_eq!(fresh.try_read(), Some(99));
        assert_eq!(fresh.try_read(), None);
    }

    #[test]
    fn recover_rejects_out_of_range_index() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 1,
        };
        let queue = recovery_queue(&config);
        assert!(matches!(
            Consumer::<Payload>::recover_in_queue(queue.clone(), 1),
            Err(Error::InvalidIndex)
        ));
    }

    #[cfg(not(miri))]
    #[test]
    fn broadcast_create_clone_and_join_share_all_lanes() {
        let config = BroadcastConfig {
            capacity: 8,
            producer_slots: 2,
            consumer_slots: 1,
        };
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: a fresh temp file, initialized exactly once here.
        let creator = unsafe { Broadcast::<Payload>::create(&file, config) }.unwrap();

        let mut p0 = creator.producer(ProducerId::new(1)).unwrap();
        // SAFETY: the file now holds a live queue with the same `T` and layout.
        let joiner = unsafe { Broadcast::<Payload>::join(&file) }.unwrap();
        let mut p1 = joiner.producer(ProducerId::new(2)).unwrap();
        assert!(matches!(
            creator.producer(ProducerId::new(3)),
            Err(Error::ProducerSlotsExhausted)
        ));

        let mut consumer = creator.consumer().unwrap();
        assert!(p0.try_write(1).is_ok());
        assert!(p1.try_write(2).is_ok());
        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
    }

    #[test]
    fn every_endpoint_exposes_its_broadcast() {
        let mut p0 = create_heap_producer(BroadcastConfig {
            capacity: 8,
            producer_slots: 2,
            consumer_slots: 2,
        });
        let mut consumer = p0.broadcast_handle().consumer().unwrap();
        let mut p1 = consumer.broadcast_handle().producer(BOGUS_ID).unwrap();
        // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
        let mut slice_consumer = unsafe { p1.broadcast_handle().slice_consumer() }.unwrap();

        let untyped: Broadcast<UnknownType> = slice_consumer.broadcast_handle();
        // Both consumer slots are already claimed.
        assert!(matches!(
            // SAFETY: `Payload` is `u64`, whose entire representation is initialized.
            unsafe { untyped.slice_consumer() },
            Err(Error::ConsumerSlotsExhausted)
        ));

        assert!(p0.try_write(1).is_ok());
        assert!(p1.try_write(2).is_ok());
        assert_eq!(consumer.try_read(), Some(1));
        assert_eq!(consumer.try_read(), Some(2));
        assert_eq!(
            slice_consumer.try_read().unwrap().as_slice(),
            1u64.to_ne_bytes()
        );
        assert_eq!(
            slice_consumer.try_read().unwrap().as_slice(),
            2u64.to_ne_bytes()
        );
    }
}
