//! DPDK-style bounded MPMC ring queue

use crate::{
    checked_queue_size,
    error::{Error, WaitError},
    futex::{Waiters, SPIN_ATTEMPTS},
    shmem::Region,
    CacheAlignedAtomicSize, DEFAULT_QUEUE_IDENTIFIER, VERSION,
};
use core::{
    iter::FusedIterator,
    marker::PhantomData,
    mem::{ManuallyDrop, MaybeUninit},
    ops::{Index, Range},
    ptr::NonNull,
    sync::atomic::Ordering,
};
use std::{
    fs::File,
    num::NonZeroUsize,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

/// Magic signature for MPMC queues in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqmpmc");

pub struct Producer<T> {
    queue: Arc<SharedQueue<T>>,
}

impl<T> Producer<T> {
    /// Returns the identifier of the queue.
    ///
    /// Returns `0` when created without an explicit identifier.
    pub fn queue_identifier(&self) -> u64 {
        // SAFETY: the queue keeps the initialized header mapping alive.
        unsafe { self.queue.header.as_ref() }.identifier
    }

    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: the caller upholds the same requirements as create_with_identifier.
        unsafe { Self::create_with_identifier(file, file_size, DEFAULT_QUEUE_IDENTIFIER) }
    }

    /// Creates a shared queue in `file` with a caller chosen identifier.
    /// The identifier is not enforced to be unique.
    ///
    /// # Safety
    /// The same requirements as [`Self::create`] apply.
    pub unsafe fn create_with_identifier(
        file: &File,
        file_size: usize,
        queue_identifier: u64,
    ) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer, so initializing the queue header for
        // this mapping happens exactly once.
        let (region, header) =
            unsafe { SharedQueueHeader::create::<T>(file, file_size, queue_identifier) }?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing producer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Consumer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    pub fn join_as_consumer(&self) -> Consumer<T> {
        Consumer {
            queue: Arc::clone(&self.queue),
        }
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(region, header) }?,
        })
    }

    /// Writes item into the queue or returns it if there is not enough space.
    pub fn try_write(&self, item: T) -> Result<(), T> {
        // SAFETY: On successful reservation the item is written below.
        let guard = match unsafe { self.try_reserve_write() } {
            Some(guard) => guard,
            None => return Err(item),
        };
        guard.write(item);
        Ok(())
    }

    /// Writes items from a slice into the queue.
    ///
    /// Returns `false` if there is not enough space.
    pub fn try_write_slice(&self, items: &[T]) -> bool
    where
        T: Copy,
    {
        let Some(len) = NonZeroUsize::new(items.len()) else {
            return true;
        };

        // SAFETY: if successful we write all items below.
        let mut guard = match unsafe { self.try_reserve_write_batch(len) } {
            Some(guard) => guard,
            None => return false,
        };

        for (index, item) in items.iter().copied().enumerate() {
            // SAFETY: index is not out of bounds.
            unsafe { guard.write(index, item) };
        }
        true
    }

    /// Reserves a slot for writing.
    /// The slot is committed when the guard is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes must be
    /// published in order they were reserved. Holding a [`WriteGuard`] should
    /// be treated similarly to holding a lock on a critical section.
    ///
    /// The slot is logically uninitialized when reserved. Raw writes to an
    /// already initialized slot overwrite the previous value without running
    /// its destructor.
    ///
    /// # Safety
    /// - The caller must leave the reserved slot initialized with a valid `T`
    ///   before the guard is dropped.
    #[must_use]
    pub unsafe fn try_reserve_write(&self) -> Option<WriteGuard<'_, T>> {
        self.queue
            .reserve_write()
            .map(|(cell, position)| WriteGuard {
                header: self.queue.header,
                cell,
                start: position,
                _marker: PhantomData,
            })
    }

    /// Reserves exactly `count` slots for writing.
    /// The slots are committed when the batch is dropped.
    ///
    /// Other [`Producer`]s may write in parallel, but writes must be
    /// published in the order they were reserved. Holding a [`WriteBatch`]
    /// should be treated similarly to holding a lock on a critical section.
    ///
    /// Every slot is logically uninitialized when reserved. Raw writes to an
    /// already initialized slot overwrite the previous value without running
    /// its destructor.
    ///
    /// # Safety
    /// - The caller must leave every reserved slot initialized with a valid `T`
    ///   before the batch is dropped.
    #[must_use]
    pub unsafe fn try_reserve_write_batch(&self, count: NonZeroUsize) -> Option<WriteBatch<'_, T>> {
        let start = self.queue.reserve_write_batch(count)?;
        Some(WriteBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start,
            count,
            buffer_mask: self.queue.buffer_mask,
            _marker: PhantomData,
        })
    }

    /// Abandons all reserved-but-unpublished writes left behind by a previous
    /// producer process.
    ///
    /// This rolls `producer_reservation` back to `producer_publication`,
    /// making capacity consumed by reservations whose guards were lost without
    /// running `Drop` reusable without exposing their slots to consumers.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   producer process is still live.
    /// - This must only be used when joining as the sole producer process for
    ///   the shared queue.
    /// - Racing with any live producer process or thread may corrupt the queue.
    pub unsafe fn recover_as_exclusive(&self) {
        // SAFETY: `self.queue.header` points to a valid shared queue header.
        let header = unsafe { self.queue.header.as_ref() };
        let publication = header.producer_publication.load(Ordering::Acquire);
        header
            .producer_reservation
            .store(publication, Ordering::Release);
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

// SAFETY: All payload access is synchronized through the atomic reservation
// and publication cursors in the shared header, so `T` is only touched by one
// endpoint at a time; sending/sharing across threads is sound when `T: Send`.
unsafe impl<T: Send> Send for Producer<T> {}
// SAFETY: See the `Send` impl above; the queue protocol synchronizes all access.
unsafe impl<T: Send> Sync for Producer<T> {}

pub struct Consumer<T> {
    queue: Arc<SharedQueue<T>>,
}

impl<T> Consumer<T> {
    /// Returns the identifier of the queue.
    ///
    /// Returns `0` when created without an explicit identifier.
    pub fn queue_identifier(&self) -> u64 {
        // SAFETY: the queue keeps the initialized header mapping alive.
        unsafe { self.queue.header.as_ref() }.identifier
    }

    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    pub unsafe fn create(file: &File, file_size: usize) -> Result<Self, Error> {
        // SAFETY: the caller upholds the same requirements as create_with_identifier.
        unsafe { Self::create_with_identifier(file, file_size, DEFAULT_QUEUE_IDENTIFIER) }
    }

    /// Creates a shared queue in `file` with a caller chosen identifier.
    /// The identifier is not enforced to be unique.
    ///
    /// # Safety
    /// The same requirements as [`Self::create`] apply.
    pub unsafe fn create_with_identifier(
        file: &File,
        file_size: usize,
        queue_identifier: u64,
    ) -> Result<Self, Error> {
        // SAFETY: caller guarantees this process or thread is the externally
        // designated sole initializer, so initializing the queue header for
        // this mapping happens exactly once.
        let (region, header) =
            unsafe { SharedQueueHeader::create::<T>(file, file_size, queue_identifier) }?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Joins an existing consumer for the shared queue in the provided file.
    ///
    /// # Safety
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Producer`]s that are joined with
    ///   the same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Producer that shares the same memory mapping.
    pub fn join_as_producer(&self) -> Producer<T> {
        Producer {
            queue: Arc::clone(&self.queue),
        }
    }

    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - allocation backing `region` must be of sufficient size.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        Ok(Self {
            // SAFETY:
            // - `header` is non-null and aligned properly.
            // - allocation at `header` is large enough to hold the header and the buffer.
            queue: unsafe { SharedQueue::from_header(region, header) }?,
        })
    }

    /// Attempts to read a value from the queue.
    /// Returns `None` if there are no values available.
    pub fn try_read(&self) -> Option<T> {
        self.try_reserve_read().map(ReadGuard::into_inner)
    }

    /// Attempts to read a value from the queue, waiting up to `timeout` for
    /// a producer to publish data.
    pub fn read_timeout(&self, timeout: Duration) -> Result<T, WaitError> {
        self.reserve_read_timeout(timeout)
            .map(ReadGuard::into_inner)
    }

    /// Attempts to reserve a value from the queue, returning a guard.
    /// Reading the guard moves the value out; dropping it unread drops the
    /// value. Either path releases the slot back to producers.
    ///
    /// Other [`Consumer`]s may read in parallel, but reads must be
    /// released in order they were reserved. Holding a [`ReadGuard`] should
    /// be treated similarly to holding a lock on a critical section.
    #[must_use]
    pub fn try_reserve_read(&self) -> Option<ReadGuard<'_, T>> {
        self.queue.reserve_read().map(|(cell, position)| ReadGuard {
            header: self.queue.header,
            cell,
            start: position,
            _marker: PhantomData,
        })
    }

    /// Attempts to reserve a value from the queue, waiting up to `timeout` for
    /// a producer to publish data.
    pub fn reserve_read_timeout(&self, timeout: Duration) -> Result<ReadGuard<'_, T>, WaitError> {
        self.wait_for_read(timeout, || self.try_reserve_read())
    }

    /// Attempts to reserve up to `max` values from the queue.
    /// The slots are released back to producers when the batch is dropped.
    ///
    /// Other [`Consumer`]s may read in parallel, but reads must be
    /// released in order they were reserved. Holding a [`ReadBatch`] should
    /// be treated similarly to holding a lock on a critical section.
    #[must_use]
    pub fn try_reserve_read_batch(&self, max: NonZeroUsize) -> Option<ReadBatch<'_, T>> {
        // SAFETY: ReadBatch drops all values that are not moved out through its
        // consuming iterator before the raw reservation is released.
        let raw = unsafe { self.try_reserve_read_batch_raw(max) }?;
        Some(ReadBatch { raw })
    }

    /// Attempts to reserve up to `max` values from the queue.
    ///
    /// The slots are released back to producers when the batch is dropped.
    /// For non-`Copy` `T`, the caller is responsible for moving out or dropping
    /// every value before then; otherwise those values are leaked. Leak-free
    /// behavior during unwinding requires the caller to arrange cleanup of the
    /// remaining values.
    ///
    /// # Safety
    /// - A non-`Copy` value may be moved out at most once.
    /// - A slot must not be referenced after its value has been moved out.
    #[must_use]
    pub unsafe fn try_reserve_read_batch_raw(
        &self,
        max: NonZeroUsize,
    ) -> Option<RawReadBatch<'_, T>> {
        let (start, count) = self.queue.reserve_read_batch(max)?;
        Some(RawReadBatch {
            header: self.queue.header,
            buffer: self.queue.buffer,
            start,
            count,
            buffer_mask: self.queue.buffer_mask,
            _marker: PhantomData,
        })
    }

    /// Attempts to reserve up to `max` values from the queue, waiting up to
    /// `timeout` for a producer to publish data.
    ///
    /// Returns `Err(WaitError::Timeout)` if no values are available before the
    /// timeout elapses.
    pub fn reserve_read_batch_timeout(
        &self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<ReadBatch<'_, T>, WaitError> {
        self.wait_for_read(timeout, || self.try_reserve_read_batch(max))
    }

    /// Attempts to reserve up to `max` values, waiting up to `timeout` for a
    /// producer to publish data.
    ///
    /// Returns `Err(WaitError::Timeout)` if no values are available before the
    /// timeout elapses.
    ///
    /// # Safety
    /// - The caller must satisfy [`Self::try_reserve_read_batch_raw`]'s safety
    ///   requirements for a successfully returned batch.
    pub unsafe fn reserve_read_batch_raw_timeout(
        &self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<RawReadBatch<'_, T>, WaitError> {
        self.wait_for_read(timeout, || {
            // SAFETY: The caller accepts the raw batch ownership contract.
            unsafe { self.try_reserve_read_batch_raw(max) }
        })
    }

    fn wait_for_read<R>(
        &self,
        timeout: Duration,
        check: impl FnMut() -> Option<R>,
    ) -> Result<R, WaitError> {
        // SAFETY: `self.queue.header` points to this consumer's live shared
        // queue header.
        let header = unsafe { self.queue.header.as_ref() };
        header
            .waiters
            .wait_for(&header.producer_publication, SPIN_ATTEMPTS, timeout, check)
    }

    /// Makes reserved-but-not-released reads left behind by a previous
    /// consumer process available to be read again.
    ///
    /// This rolls `consumer_reservation` back to `consumer_release`, making
    /// previously claimed items readable again by the new consumer process
    /// after their guards were lost without running `Drop`.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   consumer process is still live.
    /// - This must only be used when joining as the sole consumer process for
    ///   the shared queue.
    /// - Racing with any live consumer process or thread may corrupt the queue.
    /// - If `T` requires freeing of memory or other resources, this may cause
    ///   double-free if the previous consumer had processed some items but not
    ///   released them before crashing.
    pub unsafe fn recover_as_exclusive(&self) {
        // SAFETY: `self.queue.header` points to a valid shared queue header.
        let header = unsafe { self.queue.header.as_ref() };
        let release = header.consumer_release.load(Ordering::Acquire);
        header
            .consumer_reservation
            .store(release, Ordering::Release);
    }

    /// Discards all reserved-but-not-released reads left behind by a previous
    /// consumer process.
    ///
    /// This advances `consumer_release` up to `consumer_reservation`,
    /// discarding items already claimed by the previous consumer process after
    /// their guards were lost without running `Drop` and making their capacity
    /// reusable by producers.
    ///
    /// Payload destructors are not run. Discarding values that own resources
    /// may therefore leak those resources.
    ///
    /// # Safety
    /// - This must only be called when the caller can prove that no other
    ///   consumer process is still live.
    /// - This must only be used when joining as the sole consumer process for
    ///   the shared queue.
    /// - Racing with any live consumer process or thread may corrupt the queue.
    pub unsafe fn recover_as_exclusive_lossy(&self) {
        // SAFETY: `self.queue.header` points to a valid shared queue header.
        let header = unsafe { self.queue.header.as_ref() };
        let reservation = header.consumer_reservation.load(Ordering::Acquire);
        header
            .consumer_release
            .store(reservation, Ordering::Release);
    }
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
        }
    }
}

// SAFETY: All payload access is synchronized through the atomic reservation
// and publication cursors in the shared header, so `T` is only touched by one
// endpoint at a time; sending/sharing across threads is sound when `T: Send`.
unsafe impl<T: Send> Send for Consumer<T> {}
// SAFETY: See the `Send` impl above; the queue protocol synchronizes all access.
unsafe impl<T: Send> Sync for Consumer<T> {}

/// Calculates the minimum file size required for a queue with given capacity.
/// Note that file size MAY need to be increased beyond this to account for
/// page-size requirements.
///
/// # Panics
/// Panics if capacity normalization or byte-size arithmetic overflows.
/// Use [`try_minimum_file_size`] to handle overflow as an error.
pub const fn minimum_file_size<T>(capacity: usize) -> usize {
    checked_queue_size(
        capacity,
        core::mem::size_of::<T>(),
        SharedQueueHeader::buffer_offset::<T>(),
    )
    .expect("queue size overflow")
}

/// Equivalent to [`minimum_file_size`], including its overflow panic behavior.
pub const fn minimum_region_size<T>(capacity: usize) -> usize {
    minimum_file_size::<T>(capacity)
}

/// Calculates the minimum file size using checked arithmetic.
///
/// Nonzero capacities round up to a power of two; zero returns the header size.
/// Page-size rounding remains the caller's responsibility.
///
/// # Errors
/// Returns [`Error::InvalidBufferSize`] on arithmetic overflow. Success does not
/// guarantee a supported queue capacity or available memory.
pub const fn try_minimum_file_size<T>(capacity: usize) -> Result<usize, Error> {
    match checked_queue_size(
        capacity,
        core::mem::size_of::<T>(),
        SharedQueueHeader::buffer_offset::<T>(),
    ) {
        Some(size) => Ok(size),
        None => Err(Error::InvalidBufferSize),
    }
}

/// Equivalent to [`try_minimum_file_size`], including its normalization and errors.
pub const fn try_minimum_region_size<T>(capacity: usize) -> Result<usize, Error> {
    try_minimum_file_size::<T>(capacity)
}

/// Creates a new in-process MPMC queue pair backed by a heap allocation.
///
/// Buffered values are dropped with the last endpoint. Values held by
/// forgotten reservations may be leaked.
pub fn pair<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), Error> {
    let region_size = try_minimum_region_size::<T>(capacity)?;
    SharedQueueHeader::calculate_buffer_size_in_items::<T>(region_size)?;
    let region = Region::alloc(NonZeroUsize::new(region_size).ok_or(Error::InvalidBufferSize)?)?;
    // SAFETY: `region` is freshly allocated and used only for this queue.
    let header =
        unsafe { SharedQueueHeader::create_in_region::<T>(&region, DEFAULT_QUEUE_IDENTIFIER) }?;
    // SAFETY: `header` was just created in `region`, so it is valid, aligned,
    // and paired with that region.
    let producer = unsafe { Producer::from_header(region, header) }?;
    let consumer = producer.join_as_consumer();
    Ok((producer, consumer))
}

struct SharedQueue<T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    buffer_mask: usize,
    // `NonNull<T>` is covariant, but paired endpoints must not be independently
    // coerced to different payload lifetimes. The function input/output marker
    // makes `T` invariant without affecting layout or auto traits.
    _invariant: PhantomData<fn(T) -> T>,

    // NB: Region must be declared last so it remains alive throughout Drop.
    region: Arc<Region>,
}

impl<T> Drop for SharedQueue<T> {
    fn drop(&mut self) {
        if !self.region.is_heap() || !core::mem::needs_drop::<T>() {
            return;
        }

        // SAFETY: SharedQueue is dropped by its Arc after the last endpoint
        // disappears, so no endpoint can access the header during cleanup.
        let header = unsafe { self.header.as_ref() };
        let mut position = header.consumer_reservation.load(Ordering::Acquire);
        let publication = header.producer_publication.load(Ordering::Acquire);

        while position != publication {
            // SAFETY: Mask keeps the index within the allocated buffer.
            let slot = unsafe { self.buffer.add(position & self.buffer_mask) };
            // SAFETY: Positions from the consumer reservation cursor through
            // producer publication contain initialized, unclaimed values.
            unsafe { slot.drop_in_place() };
            position = position.wrapping_add(1);
        }
    }
}

const NON_ZERO_USIZE_ONE: NonZeroUsize = NonZeroUsize::new(1).unwrap();

impl<T> SharedQueue<T> {
    #[inline]
    fn capacity(&self) -> usize {
        self.buffer_mask.wrapping_add(1)
    }

    fn reserve_write(&self) -> Option<(NonNull<T>, usize)> {
        let position = self.reserve_write_batch(NON_ZERO_USIZE_ONE)?;
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, position))
    }

    fn reserve_read(&self) -> Option<(NonNull<T>, usize)> {
        let (position, _) = self.reserve_read_batch(NON_ZERO_USIZE_ONE)?;
        let cell_index = position & self.buffer_mask;
        // SAFETY: Mask ensures index is in bounds.
        let cell = unsafe { self.buffer.add(cell_index) };
        Some((cell, position))
    }

    fn reserve_write_batch(&self, count: NonZeroUsize) -> Option<usize> {
        let capacity = self.capacity();
        if count.get() > capacity {
            return None;
        }

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut producer_reservation = header.producer_reservation.load(Ordering::Relaxed);

        loop {
            let consumer_release = header.consumer_release.load(Ordering::Acquire);
            let used = producer_reservation.wrapping_sub(consumer_release);
            // Other producers' writes were consumed past our stale reservation
            // cursor, which would read as full: retry if it moved, else the
            // header is inconsistent and stays full
            if used > capacity {
                let current = header.producer_reservation.load(Ordering::Relaxed);
                if current == producer_reservation {
                    return None;
                }
                producer_reservation = current;
                continue;
            }
            let limit = capacity.wrapping_sub(count.get());
            if used > limit {
                return None;
            }
            let new_reservation = producer_reservation.wrapping_add(count.get());
            match header.producer_reservation.compare_exchange_weak(
                producer_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(producer_reservation);
                }
                Err(current) => {
                    producer_reservation = current;
                }
            }
        }
    }

    fn reserve_read_batch(&self, max: NonZeroUsize) -> Option<(usize, NonZeroUsize)> {
        let capacity = self.capacity();
        let max = max.get().min(capacity);

        // SAFETY: Header is non-null valid pointer, never accessed mutably elsewhere.
        let header = unsafe { self.header.as_ref() };
        let mut consumer_reservation = header.consumer_reservation.load(Ordering::Relaxed);

        loop {
            let producer_publication = header.producer_publication.load(Ordering::Acquire);
            let available = producer_publication.wrapping_sub(consumer_reservation);
            // Other consumers reserved past our stale reservation cursor, which
            // would read as more than capacity: retry if it moved, else the
            // header is inconsistent and stays empty
            if available > capacity {
                let current = header.consumer_reservation.load(Ordering::Relaxed);
                if current == consumer_reservation {
                    return None;
                }
                consumer_reservation = current;
                continue;
            }
            if available == 0 {
                return None;
            }

            let count = available.min(max);
            let new_reservation = consumer_reservation.wrapping_add(count);
            match header.consumer_reservation.compare_exchange_weak(
                consumer_reservation,
                new_reservation,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // SAFETY: unwrap is safe here because count is guaranteed to be non-zero:
                    //         `max` is non-zero by type.
                    //         `available` is checked to be non-zero.
                    return Some((consumer_reservation, NonZeroUsize::new(count).unwrap()));
                }
                Err(current) => {
                    consumer_reservation = current;
                }
            }
        }
    }

    /// Creates a new shared queue from a header pointer and region.
    ///
    /// # Safety
    /// - `region` must back the allocation at `header`.
    /// - `header` must be non-null and properly aligned.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Arc<Self>, Error> {
        // SAFETY: caller guarantees `header` is non-null, aligned, and valid.
        let header_ref = unsafe { header.as_ref() };
        let buffer_mask = header_ref.buffer_mask as usize;
        let buffer_size_in_items = buffer_mask.wrapping_add(1);
        if !buffer_size_in_items.is_power_of_two()
            || buffer_size_in_items == 0
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(region.size())?
                != buffer_size_in_items
        {
            return Err(Error::InvalidBufferSize);
        }

        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - allocation at `header` is large enough to hold the header and the buffer.
        let buffer = unsafe { Self::buffer_from_header(header) };
        Ok(Arc::new(Self {
            header,
            buffer,
            buffer_mask,
            _invariant: PhantomData,
            region,
        }))
    }

    /// Gets a pointer to the buffer following the header.
    ///
    /// # Safety
    /// - The header must be non-null and properly aligned.
    /// - The allocation at `header` must be of sufficient size to hold the
    ///   header and padding bytes to align the trailing buffer of `T`.
    unsafe fn buffer_from_header(header: NonNull<SharedQueueHeader>) -> NonNull<T> {
        let buffer_offset = SharedQueueHeader::buffer_offset::<T>();

        // SAFETY:
        // - buffer_offset will not overflow isize.
        // - header allocation is large enough to accommodate the alignment.
        let aligned_ptr = unsafe { header.byte_add(buffer_offset) };
        aligned_ptr.cast()
    }
}

#[repr(C)]
struct SharedQueueHeader {
    // Cold metadata cacheline.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,
    identifier: u64,

    /// Producer reservation cursor.
    ///
    /// Producers atomically advance this with CAS to claim slots, but claimed
    /// writes are not visible to consumers until `producer_publication` is
    /// advanced.
    producer_reservation: CacheAlignedAtomicSize,
    /// Producer publication cursor.
    ///
    /// Producers advance this in-order after filling reserved slots. Consumers
    /// use it to determine how many initialized items are readable.
    producer_publication: CacheAlignedAtomicSize,
    /// Consumer reservation cursor.
    ///
    /// Consumers atomically advance this with CAS to claim readable slots, but
    /// reclaimed capacity is not visible to producers until `consumer_release`
    /// is advanced.
    consumer_reservation: CacheAlignedAtomicSize,
    /// Consumer release cursor.
    ///
    /// Consumers advance this in-order after dropping/reading claimed slots.
    /// Producers use it to determine how much free space is available.
    consumer_release: CacheAlignedAtomicSize,
    /// Consumer wait/wake coordination.
    waiters: Waiters,
}

impl SharedQueueHeader {
    /// Creates and initializes a new shared queue header in `file`.
    ///
    /// # Safety
    /// - The mapping created for `file` must be used to initialize at most one
    ///   queue header.
    /// - The returned `region` must not be passed to any other queue-header
    ///   initialization routine.
    unsafe fn create<T>(
        file: &File,
        size: usize,
        identifier: u64,
    ) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        Self::calculate_buffer_size_in_items::<T>(size)?;
        file.set_len(size as u64)?;

        let region = Region::map_file(file, size)?;
        // SAFETY: caller guarantees this mapping is initialized exactly once.
        let header = unsafe { Self::create_in_region::<T>(&region, identifier) }?;
        Ok((region, header))
    }

    /// Initializes a shared queue header in `region`.
    ///
    /// # Safety
    /// - This function must be called at most once for a given `region`.
    unsafe fn create_in_region<T>(
        region: &Arc<Region>,
        identifier: u64,
    ) -> Result<NonNull<Self>, Error> {
        let buffer_size_in_items = Self::calculate_buffer_size_in_items::<T>(region.size())?;
        let header = region.addr().cast();
        // SAFETY: The header is non-null and aligned properly.
        //         Alignment is guaranteed because mmap ensures that the
        //         memory is aligned to the page size, which is sufficient for the
        //         alignment of `SharedQueueHeader`.
        //         Access is exclusive because the caller guarantees this region
        //         is initialized at most once.
        unsafe { Self::initialize(header, buffer_size_in_items, identifier) };
        Ok(header)
    }

    const fn buffer_offset<T>() -> usize {
        const {
            assert!(
                core::mem::align_of::<T>() <= crate::shmem::MINIMUM_REGION_ALIGNMENT,
                "types with alignment > MINIMUM_REGION_ALIGNMENT are not supported"
            )
        }

        core::mem::size_of::<Self>().next_multiple_of(core::mem::align_of::<T>())
    }

    const fn calculate_buffer_size_in_items<T>(file_size: usize) -> Result<usize, Error> {
        const {
            assert!(
                core::mem::size_of::<T>() > 0,
                "zero-sized types are not supported"
            )
        }

        let buffer_offset = Self::buffer_offset::<T>();
        if file_size < buffer_offset {
            return Err(Error::InvalidBufferSize);
        }

        // The buffer size (in units of T) must be a power of two.
        let buffer_size_in_bytes = file_size - buffer_offset;
        let buffer_size_in_items = buffer_size_in_bytes / core::mem::size_of::<T>();
        if buffer_size_in_items == 0 {
            return Err(Error::InvalidBufferSize);
        }
        // Round down without first rounding up, which can overflow.
        let buffer_size_in_items = buffer_size_in_items.isolate_highest_one();

        // The buffer mask is stored as u32, so the capacity must fit.
        if buffer_size_in_items > u32::MAX as usize + 1 {
            return Err(Error::InvalidBufferSize);
        }

        Ok(buffer_size_in_items)
    }

    /// Initializes the shared queue header.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `header` allocation must be large enough to hold the header and the buffer.
    /// - `access` to `header` must be unique when this is called.
    unsafe fn initialize(
        mut header_ptr: NonNull<Self>,
        buffer_size_in_items: usize,
        identifier: u64,
    ) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header_ptr.as_mut() };
        header.producer_reservation.store(0, Ordering::Release);
        header.producer_publication.store(0, Ordering::Release);
        header.consumer_reservation.store(0, Ordering::Release);
        header.consumer_release.store(0, Ordering::Release);
        header.waiters.initialize();
        header.buffer_mask = u32::try_from(buffer_size_in_items - 1).unwrap();
        header.version = VERSION;
        header.identifier = identifier;
        header.magic.store(MAGIC, Ordering::Release);
    }

    fn join<T>(file: &File) -> Result<(Arc<Region>, NonNull<Self>), Error> {
        let file_size = file.metadata()?.len() as usize;
        Self::calculate_buffer_size_in_items::<T>(file_size)?;
        let region = Region::map_file(file, file_size)?;
        let header = Self::join_region::<T>(&region)?;
        Ok((region, header))
    }

    fn join_region<T>(region: &Arc<Region>) -> Result<NonNull<Self>, Error> {
        let header = region.addr().cast::<Self>();
        {
            // SAFETY: The header is non-null and aligned properly.
            //         Alignment is guaranteed because mmap ensures that the
            //         memory is aligned to the page size, which is sufficient for the
            //         alignment of `SharedQueueHeader`.
            let header = unsafe { header.as_ref() };
            if header.magic.load(Ordering::Acquire) != MAGIC {
                return Err(Error::InvalidMagic);
            }
            if header.version != VERSION {
                return Err(Error::InvalidVersion {
                    expected: VERSION,
                    actual: header.version,
                });
            }
            let buffer_size_in_items = (header.buffer_mask as usize).wrapping_add(1);
            if buffer_size_in_items != Self::calculate_buffer_size_in_items::<T>(region.size())? {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok(header)
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this producer.
    unsafe fn publish_producer_publication(
        header_ptr: NonNull<Self>,
        start: usize,
        count: NonZeroUsize,
    ) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.producer_publication.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        // Release publication; `wake` supplies the fence that pairs it with
        // a registering waiter and must be called unconditionally; see the
        // `futex` module docs.
        header
            .producer_publication
            .store(start.wrapping_add(count.get()), Ordering::Release);
        header
            .waiters
            .wake(&header.producer_publication, count.get());
    }

    /// # Safety
    /// - `start..start+count` must be reserved by this consumer.
    unsafe fn publish_consumer_release(
        header_ptr: NonNull<Self>,
        start: usize,
        count: NonZeroUsize,
    ) {
        // SAFETY: `header_ptr` is a valid shared-memory header.
        let header = unsafe { header_ptr.as_ref() };
        while header.consumer_release.load(Ordering::Acquire) != start {
            core::hint::spin_loop();
        }
        header
            .consumer_release
            .store(start.wrapping_add(count.get()), Ordering::Release);
    }
}

#[must_use]
/// A reservation for one logically uninitialized producer slot.
///
/// Initialize the slot with [`Self::write`] or through [`AsMut::as_mut`]
/// before dropping the guard. Initializing it through both paths overwrites
/// the first value without running its destructor.
pub struct WriteGuard<'a, T> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<T> core::convert::AsMut<MaybeUninit<T>> for WriteGuard<'_, T> {
    /// Mutable reference to the reserved cell.
    fn as_mut(&mut self) -> &mut MaybeUninit<T> {
        let mut cell = self.cell.cast();
        // SAFETY: The cell was reserved for writing.
        unsafe { cell.as_mut() }
    }
}

impl<'a, T> WriteGuard<'a, T> {
    /// Initializes the reserved slot.
    ///
    /// This performs a raw write. If the slot was already initialized through
    /// [`AsMut::as_mut`], the previous value is overwritten without running
    /// its destructor.
    pub fn write(self, value: T) {
        // SAFETY: The cell was reserved for writing.
        unsafe { self.cell.write(value) };
    }
}

impl<'a, T> Drop for WriteGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This guard owns one reserved producer slot.
        unsafe {
            SharedQueueHeader::publish_producer_publication(
                self.header,
                self.start,
                NON_ZERO_USIZE_ONE,
            );
        }
    }
}

#[must_use]
/// A reservation for one initialized consumer slot.
///
/// [`Self::into_inner`] moves the value out of the queue. Dropping the guard
/// without taking it drops the queued value. Either path releases the slot
/// back to producers.
pub struct ReadGuard<'a, T> {
    header: NonNull<SharedQueueHeader>,
    cell: NonNull<T>,
    start: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> ReadGuard<'a, T> {
    fn release(&self) {
        // SAFETY: A ReadGuard owns one reserved consumer slot. Both call sites
        // invoke this helper exactly once after moving the value out.
        unsafe {
            SharedQueueHeader::publish_consumer_release(
                self.header,
                self.start,
                NON_ZERO_USIZE_ONE,
            );
        }
    }

    /// Moves the reserved value out of the queue and releases its slot.
    pub fn into_inner(self) -> T {
        let guard = ManuallyDrop::new(self);
        // SAFETY: The cell was reserved for reading and holds an initialized value.
        let value = unsafe { guard.cell.read() };
        guard.release();
        value
    }
}

impl<'a, T> AsRef<T> for ReadGuard<'a, T> {
    /// Returns a shared reference to the reserved slot.
    fn as_ref(&self) -> &T {
        // SAFETY: The cell was reserved for reading and is initialized.
        unsafe { self.cell.as_ref() }
    }
}

impl<'a, T> Drop for ReadGuard<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This guard owns an initialized value that was not moved out.
        let value = unsafe { self.cell.read() };
        // The value now lives outside the shared cell, so the cell can be
        // released before running its destructor. This ordering lets producers
        // reuse the cell and prevents a panicking destructor from wedging the
        // consumer-release cursor.
        self.release();
        drop(value);
    }
}

#[must_use]
/// A reservation for logically uninitialized producer slots.
///
/// Every slot must contain a valid `T` before the batch is dropped. Repeatedly
/// initializing one slot overwrites the previous value without running its
/// destructor unless the caller first moves out or drops that value.
pub struct WriteBatch<'a, T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    count: NonZeroUsize,
    buffer_mask: usize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T> WriteBatch<'a, T> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.count.get()
    }

    /// Returns a mutable reference to the reserved slot.
    ///
    /// Writing through the returned [`MaybeUninit`] does not drop a value
    /// already in the slot.
    ///
    /// # Safety
    /// - `index` must be less than [`Self::len`].
    /// - The caller must leave the slot initialized with a valid `T` before
    ///   the batch is dropped.
    pub unsafe fn as_mut(&mut self, index: usize) -> &mut MaybeUninit<T> {
        debug_assert!(index < self.count.get());
        let position = self.start.wrapping_add(index);
        // SAFETY: Mask keeps the index within the reserved buffer.
        let slot = unsafe { self.buffer.add(position & self.buffer_mask) };
        // SAFETY: The position was reserved for writing.
        unsafe { slot.cast().as_mut() }
    }

    /// Writes a value into the slot at index.
    ///
    /// This performs a raw write and does not drop a value already in the slot.
    ///
    /// # Safety
    /// - `index` must be less than [`Self::len`].
    pub unsafe fn write(&mut self, index: usize, value: T) {
        debug_assert!(index < self.count.get());
        let position = self.start.wrapping_add(index);
        // SAFETY: Mask keeps the index within the reserved buffer.
        let slot = unsafe { self.buffer.add(position & self.buffer_mask) };
        // SAFETY: The position was reserved for writing.
        unsafe { slot.write(value) }
    }
}

impl<'a, T> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved producer slots.
        unsafe {
            SharedQueueHeader::publish_producer_publication(self.header, self.start, self.count);
        }
    }
}

#[must_use]
/// A raw reservation for consecutive initialized consumer slots.
///
/// The caller is responsible for moving out or dropping every non-`Copy` value
/// before the batch is dropped. Dropping this guard only releases the slots; it
/// does not run payload destructors.
pub struct RawReadBatch<'a, T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    start: usize,
    count: NonZeroUsize,
    buffer_mask: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T> RawReadBatch<'a, T> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.count.get()
    }

    /// Returns a reference to the reserved slot.
    ///
    /// # Safety
    /// - `index` must be less than [`Self::len`].
    /// - The value at `index` must not have previously been moved out or
    ///   dropped.
    /// - A reference returned by this method must not be used after the value
    ///   is moved out or dropped by any means.
    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        debug_assert!(index < self.count.get());
        let position = self.start.wrapping_add(index);
        // SAFETY: Mask keeps the index within the buffer.
        let slot = unsafe { self.buffer.add(position & self.buffer_mask) };
        // SAFETY: The position was reserved for reading and is initialized.
        unsafe { slot.as_ref() }
    }

    /// Reads the value at `index`.
    ///
    /// # Safety
    /// - `index` must be less than [`Self::len`].
    /// - For non-`Copy` `T`, the value must not have previously been moved out
    ///   or dropped. It may be moved out exactly once.
    /// - No reference to the slot may be used after moving out its value.
    pub unsafe fn get_owned_unchecked(&self, index: usize) -> T {
        debug_assert!(index < self.count.get());
        let position = self.start.wrapping_add(index);
        // SAFETY: Mask keeps the index within the buffer.
        let slot = unsafe { self.buffer.add(position & self.buffer_mask) };
        // SAFETY: The position was reserved for reading.
        unsafe { slot.read() }
    }
}

impl<'a, T> Drop for RawReadBatch<'a, T> {
    fn drop(&mut self) {
        // SAFETY: This batch owns `count` reserved consumer slots.
        unsafe {
            SharedQueueHeader::publish_consumer_release(self.header, self.start, self.count);
        }
    }
}

#[must_use]
/// A destructor-safe reservation for consecutive initialized consumer slots.
///
/// The batch keeps all of its slots reserved while it lives. It may be
/// inspected without consuming values or converted into a sequential consuming
/// iterator. Dropping the batch without consuming it drops every value before
/// releasing the reservation.
pub struct ReadBatch<'a, T> {
    raw: RawReadBatch<'a, T>,
}

impl<'a, T> ReadBatch<'a, T> {
    fn raw(&self) -> &RawReadBatch<'a, T> {
        &self.raw
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.raw().len()
    }

    /// Returns a shared reference to the value at `index`, or `None` if it is
    /// outside the batch.
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len() {
            return None;
        }
        // SAFETY: The bounds check establishes that the initialized slot is in
        // the reservation, and a safe batch has not moved out any values.
        Some(unsafe { self.raw().get_unchecked(index) })
    }

    /// Returns the values in logical read order as two slices.
    ///
    /// The second slice is empty unless the batch wraps around the end of the
    /// queue buffer. This borrows the values without consuming the batch.
    pub fn as_slices(&self) -> (&[T], &[T]) {
        let start = self.raw.start & self.raw.buffer_mask;
        let capacity = self.raw.buffer_mask.wrapping_add(1);
        let first_len = self.len().min(capacity.wrapping_sub(start));
        let second_len = self.len().wrapping_sub(first_len);

        // SAFETY: `start` is within the queue buffer.
        let first = unsafe { self.raw.buffer.add(start) };
        let first = NonNull::slice_from_raw_parts(first, first_len);
        // SAFETY: The first part of the reservation is initialized, contiguous,
        // and remains reserved for the lifetime of the returned slice.
        let first = unsafe { first.as_ref() };
        let second = NonNull::slice_from_raw_parts(self.raw.buffer, second_len);
        // SAFETY: The wrapped part of the reservation starts at the buffer base,
        // is initialized and contiguous, and remains reserved for the lifetime
        // of the returned slice.
        let second = unsafe { second.as_ref() };
        (first, second)
    }

    /// Iterates over shared references without consuming any values.
    ///
    /// The batch reservation remains held for the iterator's lifetime.
    pub fn iter(&self) -> ReadBatchIter<'_, 'a, T> {
        self.into_iter()
    }
}

impl<'batch, 'queue, T> IntoIterator for &'batch ReadBatch<'queue, T> {
    type Item = &'batch T;
    type IntoIter = ReadBatchIter<'batch, 'queue, T>;

    fn into_iter(self) -> Self::IntoIter {
        ReadBatchIter {
            batch: self,
            range: 0..self.len(),
        }
    }
}

/// An iterator over shared references to the values in a read batch.
#[must_use]
pub struct ReadBatchIter<'batch, 'queue, T> {
    batch: &'batch ReadBatch<'queue, T>,
    range: Range<usize>,
}

impl<'batch, T> Iterator for ReadBatchIter<'batch, '_, T> {
    type Item = &'batch T;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch.get(self.range.next()?)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.range.size_hint()
    }
}

impl<T> DoubleEndedIterator for ReadBatchIter<'_, '_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.batch.get(self.range.next_back()?)
    }
}

impl<T> ExactSizeIterator for ReadBatchIter<'_, '_, T> {}
impl<T> FusedIterator for ReadBatchIter<'_, '_, T> {}

impl<'a, T> IntoIterator for ReadBatch<'a, T> {
    type Item = T;
    type IntoIter = ReadBatchIntoIter<'a, T>;

    /// Converts the batch into a sequential consuming iterator.
    ///
    /// The returned iterator keeps the complete batch reservation held.
    /// Dropping it before exhaustion drops all values that have not yet been
    /// yielded.
    fn into_iter(self) -> Self::IntoIter {
        let batch = ManuallyDrop::new(self);
        // SAFETY: `batch` is not dropped, so this moves its raw guard exactly once.
        let raw = unsafe { core::ptr::read(&batch.raw) };
        ReadBatchIntoIter { raw, next: 0 }
    }
}

impl<'a, T: Copy> ReadBatch<'a, T> {
    /// Copies the value at `index`, or returns `None` if it is outside the
    /// batch.
    pub fn get_owned(&self, index: usize) -> Option<T> {
        self.get(index).copied()
    }
}

impl<T> Index<usize> for ReadBatch<'_, T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).expect("read batch index out of bounds")
    }
}

impl<T> Drop for ReadBatch<'_, T> {
    fn drop(&mut self) {
        if !core::mem::needs_drop::<T>() {
            return;
        }

        for index in 0..self.raw.len() {
            // SAFETY: An intact safe batch has not moved out any values.
            let value = unsafe { self.raw.get_owned_unchecked(index) };
            drop(value);
        }
    }
}

#[must_use]
/// A sequential consuming iterator over a reserved read batch.
///
/// This iterator keeps the complete batch reservation held until it is
/// dropped. Dropping it before exhaustion drops the unconsumed suffix before
/// releasing the reservation.
pub struct ReadBatchIntoIter<'a, T> {
    raw: RawReadBatch<'a, T>,
    next: usize,
}

impl<T> Iterator for ReadBatchIntoIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.raw.len() {
            return None;
        }
        let index = self.next;
        // Advance before returning ownership so Drop only considers the suffix.
        self.next += 1;
        // SAFETY: The cursor visits every reserved index at most once.
        Some(unsafe { self.raw.get_owned_unchecked(index) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.raw.len() - self.next;
        (remaining, Some(remaining))
    }
}

impl<T> ExactSizeIterator for ReadBatchIntoIter<'_, T> {}
impl<T> FusedIterator for ReadBatchIntoIter<'_, T> {}

impl<T> Drop for ReadBatchIntoIter<'_, T> {
    fn drop(&mut self) {
        if !core::mem::needs_drop::<T>() {
            return;
        }

        while self.next < self.raw.len() {
            let index = self.next;
            self.next += 1;
            // SAFETY: The cursor visits every reserved index at most once.
            let value = unsafe { self.raw.get_owned_unchecked(index) };
            drop(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(miri))]
    use crate::shmem::create_temp_shmem_file;
    use core::cell::Cell;
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::atomic::AtomicUsize,
    };

    type Item = u64;
    const BUFFER_CAPACITY: usize = 512;

    type CreateQueue<T> = fn(usize) -> (Producer<T>, Consumer<T>);

    #[cfg(not(miri))]
    #[test]
    fn invalid_file_sizes_are_rejected_before_io() {
        let file = create_temp_shmem_file().unwrap();
        let offset = SharedQueueHeader::buffer_offset::<u8>();
        for size in [0, offset - 1, offset, usize::MAX] {
            // SAFETY: this is a fresh file with no other users.
            let result = unsafe { Producer::<u8>::create(&file, size) };
            assert!(matches!(result, Err(Error::InvalidBufferSize)));
            assert_eq!(file.metadata().unwrap().len(), 0);
        }
        file.set_len(offset as u64).unwrap();
        assert!(matches!(
            SharedQueueHeader::join::<u8>(&file),
            Err(Error::InvalidBufferSize)
        ));
    }

    fn create_heap_test_queue<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
        pair(capacity).expect("failed to create heap-backed queue pair")
    }

    #[cfg(not(miri))]
    fn create_file_backed_test_queue<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
        let file = create_temp_shmem_file().expect("failed to create temp file");
        let file_size = try_minimum_file_size::<T>(capacity).expect("invalid queue size");
        // SAFETY: test-only; sole producer for a freshly created file.
        let producer =
            unsafe { Producer::create(&file, file_size) }.expect("failed to create producer");
        // SAFETY: test-only; sole consumer for this file.
        let consumer = unsafe { Consumer::join(&file) }.expect("failed to join consumer");

        (producer, consumer)
    }

    fn test_queue_creators<T: Send>() -> &'static [CreateQueue<T>] {
        &[
            create_heap_test_queue::<T>,
            #[cfg(not(miri))]
            create_file_backed_test_queue::<T>,
        ]
    }

    fn expect_wait_ok<T>(result: Result<T, WaitError>) -> T {
        match result {
            Ok(value) => value,
            Err(WaitError::Timeout) => panic!("wait timed out"),
        }
    }

    #[test]
    fn endpoints_are_send_and_sync_for_send_not_sync_items() {
        fn assert_send_and_sync<T: Send + Sync>() {}

        assert_send_and_sync::<Producer<Cell<u64>>>();
        assert_send_and_sync::<Consumer<Cell<u64>>>();
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
        let size = minimum_file_size::<u64>(4);
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: fresh file, initialized once with one producer and u64 payloads.
        let producer =
            unsafe { Producer::<u64>::create_with_identifier(&file, size, identifier) }.unwrap();
        // SAFETY: the file contains a u64 queue with no consumer yet.
        let consumer = unsafe { Consumer::<u64>::join(&file) }.unwrap();
        assert_eq!(producer.queue_identifier(), identifier);
        assert_eq!(consumer.queue_identifier(), identifier);

        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: fresh file, initialized once with one consumer and u64 payloads.
        let consumer =
            unsafe { Consumer::<u64>::create_with_identifier(&file, size, identifier) }.unwrap();
        // SAFETY: the file contains a u64 queue with no producer yet.
        let producer = unsafe { Producer::<u64>::join(&file) }.unwrap();
        assert_eq!(producer.queue_identifier(), identifier);
        assert_eq!(consumer.queue_identifier(), identifier);
    }

    #[cfg(not(miri))]
    #[test]
    fn queue_identifier_defaults_to_zero() {
        let size = minimum_file_size::<u64>(4);
        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: fresh file, initialized once with one producer and u64 payloads.
        let producer = unsafe { Producer::<u64>::create(&file, size) }.unwrap();
        // SAFETY: the file contains a u64 queue with no consumer yet.
        let consumer = unsafe { Consumer::<u64>::join(&file) }.unwrap();
        assert_eq!(producer.queue_identifier(), 0);
        assert_eq!(consumer.queue_identifier(), 0);

        let file = create_temp_shmem_file().expect("temp file");
        // SAFETY: fresh file, initialized once with one consumer and u64 payloads.
        let consumer = unsafe { Consumer::<u64>::create(&file, size) }.unwrap();
        // SAFETY: the file contains a u64 queue with no producer yet.
        let producer = unsafe { Producer::<u64>::join(&file) }.unwrap();
        assert_eq!(producer.queue_identifier(), 0);
        assert_eq!(consumer.queue_identifier(), 0);
    }

    #[test]
    fn test_producer_consumer() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);
            let capacity = BUFFER_CAPACITY;

            for i in 0..capacity {
                assert_eq!(producer.try_write(i as Item), Ok(()));
            }
            assert!(producer.try_write(999).is_err());

            for i in 0..capacity {
                assert_eq!(consumer.try_read(), Some(i as Item));
            }
            assert_eq!(consumer.try_read(), None);
        }
    }

    #[test]
    fn test_read_timeout_observes_publication() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            assert!(matches!(
                consumer.read_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            producer.try_write(42).unwrap();

            assert_eq!(expect_wait_ok(consumer.read_timeout(Duration::ZERO)), 42);
            assert_eq!(consumer.try_read(), None);
        }
    }

    #[test]
    fn test_reserve_read_timeout_observes_publication() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            assert!(matches!(
                consumer.reserve_read_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            producer.try_write(7).unwrap();

            let guard = expect_wait_ok(consumer.reserve_read_timeout(Duration::ZERO));
            assert_eq!(*guard.as_ref(), 7);
        }
    }

    #[test]
    fn test_reserve_read_batch_timeout_observes_publication() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            assert!(matches!(
                consumer.reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            assert!(producer.try_write_slice(&[1, 2, 3]));

            let batch = expect_wait_ok(
                consumer.reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO),
            );
            assert_eq!(batch.len(), 3);
            for (index, expected) in [1, 2, 3].into_iter().enumerate() {
                assert_eq!(batch.get_owned(index), Some(expected));
            }
        }
    }

    #[test]
    fn test_reserve_and_try_read_ptr() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            // SAFETY: test-only; slot is initialized below before the guard drops.
            let mut guard = unsafe { producer.try_reserve_write() }.expect("reserve failed");
            guard.as_mut().write(42);
            drop(guard);

            let guard = consumer.try_reserve_read().expect("try_read_ptr failed");
            assert_eq!(*guard.as_ref(), 42);
            assert_eq!(guard.into_inner(), 42);
        }
    }

    #[test]
    fn test_reserve_batch_and_try_read_batch() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            let batch_size = NonZeroUsize::new(4).unwrap();
            // SAFETY: test-only; every slot is initialized below before drop.
            let mut batch = unsafe { producer.try_reserve_write_batch(batch_size) }
                .expect("reserve_batch failed");
            for index in 0..batch.len() {
                // SAFETY: test-only; index < batch.len().
                unsafe {
                    batch.as_mut(index).write(index as u64);
                }
            }
            drop(batch);

            let batch = consumer
                .try_reserve_read_batch(batch_size)
                .expect("try_read_batch failed");
            for index in 0..batch.len() {
                assert_eq!(batch.get(index), Some(&(index as u64)));
                assert_eq!(batch[index], index as u64);
                assert_eq!(batch.get_owned(index), Some(index as u64));
            }
            assert_eq!(batch.get(batch.len()), None);
            assert_eq!(batch.get_owned(batch.len()), None);
            let (first, second) = batch.as_slices();
            assert_eq!(first, &[0, 1, 2, 3]);
            assert!(second.is_empty());
            let mut borrowed = Vec::new();
            for value in &batch {
                borrowed.push(*value);
            }
            assert_eq!(borrowed, vec![0, 1, 2, 3]);
        }
    }

    #[test]
    fn test_batch_write_exact_read_upto_max() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            // SAFETY: test-only; reservation fails so no slot is left uninitialized.
            unsafe {
                assert!(producer
                    .try_reserve_write_batch(NonZeroUsize::new(BUFFER_CAPACITY + 1).unwrap())
                    .is_none());
            }

            for i in 0..4 {
                assert_eq!(producer.try_write(i as Item), Ok(()));
            }
            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(5).unwrap())
                .expect("try_read_batch up-to failed");
            assert_eq!(batch.len(), 4);
            for index in 0..batch.len() {
                assert_eq!(batch[index], index as u64);
            }
        }
    }

    struct CountedItem {
        value: u64,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for CountedItem {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn write_counted(producer: &Producer<CountedItem>, value: u64, drops: &Arc<AtomicUsize>) {
        assert!(producer
            .try_write(CountedItem {
                value,
                drops: Arc::clone(drops),
            })
            .is_ok());
    }

    #[test]
    fn safe_read_batch_drop_drops_all_values() {
        for create_queue in test_queue_creators::<CountedItem>() {
            let drops = Arc::new(AtomicUsize::new(0));
            let (producer, consumer) = create_queue(4);
            for value in 0..3 {
                write_counted(&producer, value, &drops);
            }

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                .expect("read batch");
            assert_eq!(
                batch.iter().map(|item| item.value).collect::<Vec<_>>(),
                vec![0, 1, 2]
            );
            assert_eq!(batch.get(1).map(|item| item.value), Some(1));
            assert_eq!(batch[2].value, 2);
            assert!(batch.get(3).is_none());
            drop(batch);

            assert_eq!(drops.load(Ordering::Relaxed), 3);
            for value in 3..7 {
                write_counted(&producer, value, &drops);
            }
            for _ in 3..7 {
                drop(consumer.try_read().expect("replacement value"));
            }
            assert_eq!(drops.load(Ordering::Relaxed), 7);
        }
    }

    #[test]
    fn read_batch_into_iter_drop_drops_only_unconsumed_values() {
        for create_queue in test_queue_creators::<CountedItem>() {
            let drops = Arc::new(AtomicUsize::new(0));
            let (producer, consumer) = create_queue(4);
            for value in 0..3 {
                write_counted(&producer, value, &drops);
            }

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                .expect("read batch");
            let mut iter = batch.into_iter();
            assert_eq!(iter.len(), 3);
            let first = iter.next().expect("first value");
            assert_eq!(first.value, 0);
            assert_eq!(iter.len(), 2);
            drop(first);
            assert_eq!(drops.load(Ordering::Relaxed), 1);

            drop(iter);
            assert_eq!(drops.load(Ordering::Relaxed), 3);
            assert!(consumer.try_read().is_none());
            for value in 3..7 {
                write_counted(&producer, value, &drops);
            }
            for _ in 3..7 {
                drop(consumer.try_read().expect("replacement value"));
            }
            assert_eq!(drops.load(Ordering::Relaxed), 7);
        }
    }

    #[test]
    fn copy_read_batch_supports_wrapped_random_access_and_into_iter() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(4);
            assert!(producer.try_write_slice(&[0, 1, 2]));
            assert_eq!(consumer.try_read(), Some(0));
            assert_eq!(consumer.try_read(), Some(1));
            assert!(producer.try_write_slice(&[3, 4, 5]));

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                .expect("wrapped read batch");
            assert_eq!(batch.get_owned(3), Some(5));
            assert_eq!(batch.get_owned(0), Some(2));
            assert_eq!(batch.get_owned(3), Some(5));
            let (first, second) = batch.as_slices();
            assert_eq!(first, &[2, 3]);
            assert_eq!(second, &[4, 5]);
            assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![2, 3, 4, 5]);
            let mut values = Vec::new();
            for value in batch {
                values.push(value);
            }
            assert_eq!(values, vec![2, 3, 4, 5]);
        }
    }

    #[test]
    fn raw_read_batch_supports_manual_random_access_cleanup() {
        for create_queue in test_queue_creators::<CountedItem>() {
            let drops = Arc::new(AtomicUsize::new(0));
            let (producer, consumer) = create_queue(4);
            for value in 0..3 {
                write_counted(&producer, value, &drops);
            }

            // SAFETY: Every value is read and dropped exactly once below.
            let batch = unsafe {
                consumer
                    .try_reserve_read_batch_raw(NonZeroUsize::new(4).unwrap())
                    .expect("raw read batch")
            };
            // SAFETY: test-only; index 2 is in bounds and not yet moved out.
            assert_eq!(unsafe { batch.get_unchecked(2) }.value, 2);
            for index in [2, 0, 1] {
                // SAFETY: Each in-bounds index is moved out exactly once.
                drop(unsafe { batch.get_owned_unchecked(index) });
            }
            drop(batch);

            assert_eq!(drops.load(Ordering::Relaxed), 3);
            assert!(consumer.try_read().is_none());
        }
    }

    struct PanicOnDrop(bool);

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            assert!(!self.0, "requested drop panic");
        }
    }

    #[test]
    fn panicking_batch_element_drop_still_releases_reservation() {
        for create_queue in test_queue_creators::<PanicOnDrop>() {
            let (producer, consumer) = create_queue(2);
            assert!(producer.try_write(PanicOnDrop(true)).is_ok());
            assert!(producer.try_write(PanicOnDrop(false)).is_ok());
            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .expect("read batch");

            assert!(catch_unwind(AssertUnwindSafe(|| drop(batch))).is_err());

            assert!(producer.try_write(PanicOnDrop(false)).is_ok());
            assert!(producer.try_write(PanicOnDrop(false)).is_ok());
            let replacement = consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .expect("replacement read batch");
            drop(replacement);
        }
    }

    #[test]
    fn test_try_write_slice() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            assert!(producer.try_write_slice(&[]));

            let values = [10, 11, 12, 13];
            assert!(producer.try_write_slice(&values));
            for value in values {
                assert_eq!(consumer.try_read(), Some(value));
            }
            assert_eq!(consumer.try_read(), None);
        }
    }

    #[test]
    fn test_capacity_rounds_up() {
        for create_queue in test_queue_creators::<u64>() {
            let (producer, consumer) = create_queue(3);

            assert_eq!(producer.queue.capacity(), 4);
            assert_eq!(consumer.queue.capacity(), 4);
        }
    }

    #[test]
    fn test_inconsistent_cursors_read_as_full_and_empty() {
        for create_queue in test_queue_creators::<u64>() {
            let (producer, consumer) = create_queue(4);
            // SAFETY: The header lives as long as the queue.
            let header = unsafe { producer.queue.header.as_ref() };

            // Released past reserved, which no consumer can do
            header.consumer_release.store(100, Ordering::Release);
            assert_eq!(producer.try_write(1), Err(1));

            // Published more than capacity past reserved, which no producer can do
            header.producer_publication.store(100, Ordering::Release);
            assert_eq!(consumer.try_read(), None);
        }
    }

    #[test]
    fn test_multiple_producers_consumers() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);
            let producer2 = producer.clone();
            let consumer2 = consumer.clone();

            let capacity = BUFFER_CAPACITY;
            for i in 0..(capacity / 2) {
                assert_eq!(producer.try_write((i * 2) as Item), Ok(()));
                assert_eq!(producer2.try_write((i * 2 + 1) as Item), Ok(()));
            }

            let mut values = Vec::with_capacity(capacity);
            while values.len() < capacity {
                let mut progressed = false;
                if let Some(value) = consumer.try_read() {
                    values.push(value);
                    progressed = true;
                }
                if let Some(value) = consumer2.try_read() {
                    values.push(value);
                    progressed = true;
                }
                if !progressed {
                    break;
                }
            }

            assert_eq!(values.len(), capacity);
            values.sort_unstable();
            for (i, value) in values.iter().enumerate() {
                assert_eq!(*value, i as Item);
            }
        }
    }

    #[test]
    fn test_clone_producer() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);
            let producer2 = producer.clone();

            producer.try_write(10).unwrap();
            producer2.try_write(20).unwrap();

            let mut values = Vec::new();
            while let Some(v) = consumer.try_read() {
                values.push(v);
            }
            values.sort_unstable();
            assert_eq!(values, vec![10, 20]);
        }
    }

    #[test]
    fn test_cross_role_joins() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer1, consumer1) = create_queue(BUFFER_CAPACITY);
            let consumer2 = producer1.join_as_consumer();
            let producer2 = consumer2.join_as_producer();

            // Write two values.
            producer1.try_write(100).unwrap();
            producer2.try_write(200).unwrap();

            // Read two values.
            assert_eq!(consumer2.try_read().unwrap(), 100);
            assert_eq!(consumer1.try_read().unwrap(), 200);
        }
    }

    #[test]
    fn test_drop_original_region_stays_alive() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, _consumer) = create_queue(BUFFER_CAPACITY);
            let consumer = producer.join_as_consumer();
            let producer2 = producer.clone();

            // Drop the original producer; the shared region stays alive via Arc.
            drop(producer);

            producer2.try_write(42).unwrap();
            assert_eq!(consumer.try_read(), Some(42));
        }
    }

    #[test]
    fn test_heap_pair_drops_buffered_value_with_last_endpoint() {
        let value = Arc::new(());
        let (producer, consumer) = pair(4).expect("failed to create queue");
        let producer2 = producer.clone();
        let consumer2 = consumer.clone();

        producer.try_write(Arc::clone(&value)).unwrap();
        drop(producer);
        drop(consumer);
        drop(producer2);
        assert_eq!(Arc::strong_count(&value), 2);

        drop(consumer2);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn test_clone_roles() {
        for create_queue in test_queue_creators::<u64>() {
            let (producer, consumer) = create_queue(64);
            let producer2 = producer.clone();
            let consumer2 = consumer.clone();

            producer.try_write(1).expect("write failed");
            producer2.try_write(2).expect("write failed");

            let mut values = Vec::new();
            loop {
                let mut progressed = false;
                if let Some(value) = consumer.try_read() {
                    values.push(value);
                    progressed = true;
                }
                if let Some(value) = consumer2.try_read() {
                    values.push(value);
                    progressed = true;
                }
                if !progressed {
                    break;
                }
            }

            values.sort_unstable();
            assert_eq!(values, vec![1, 2]);
        }
    }

    #[test]
    fn test_consumer_recover_as_exclusive_lossy() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            for i in 0..4 {
                producer.try_write(i).unwrap();
            }

            let guard = consumer.try_reserve_read().expect("reserve read");
            assert_eq!(*guard.as_ref(), 0);
            core::mem::forget(guard);

            // SAFETY: test-only; sole consumer, no other live consumer.
            unsafe {
                consumer.recover_as_exclusive_lossy();
            }

            assert_eq!(consumer.try_read(), Some(1));
            assert_eq!(consumer.try_read(), Some(2));
            assert_eq!(consumer.try_read(), Some(3));
            assert_eq!(consumer.try_read(), None);
        }
    }

    #[test]
    fn test_consumer_recover_as_exclusive() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            for i in 0..4 {
                producer.try_write(i).unwrap();
            }

            let guard = consumer.try_reserve_read().expect("reserve read");
            assert_eq!(*guard.as_ref(), 0);
            core::mem::forget(guard);

            // SAFETY: test-only; sole consumer, no other live consumer.
            unsafe {
                consumer.recover_as_exclusive();
            }

            assert_eq!(consumer.try_read(), Some(0));
            assert_eq!(consumer.try_read(), Some(1));
            assert_eq!(consumer.try_read(), Some(2));
            assert_eq!(consumer.try_read(), Some(3));
            assert_eq!(consumer.try_read(), None);
        }
    }

    #[test]
    fn test_producer_recover_as_exclusive() {
        for create_queue in test_queue_creators::<Item>() {
            let (producer, consumer) = create_queue(BUFFER_CAPACITY);

            producer.try_write(10).unwrap();

            // SAFETY: test-only; slot is initialized below before it is forgotten.
            let mut guard = unsafe { producer.try_reserve_write() }.expect("reserve write");
            guard.as_mut().write(99);
            core::mem::forget(guard);

            // SAFETY: test-only; sole producer, no other live producer.
            unsafe {
                producer.recover_as_exclusive();
            }

            producer.try_write(20).unwrap();

            assert_eq!(consumer.try_read(), Some(10));
            assert_eq!(consumer.try_read(), Some(20));
            assert_eq!(consumer.try_read(), None);
        }
    }
}
