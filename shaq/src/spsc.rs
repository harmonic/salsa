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
};
use std::{
    fs::File,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

/// Magic signature for SPSC queues in shared memory.
const MAGIC: u64 = u64::from_be_bytes(*b"shaqspsc");

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

/// Creates a new in-process SPSC queue pair backed by a heap allocation.
///
/// Buffered values are dropped with the last endpoint. Values in uncommitted
/// writes may be leaked.
pub fn pair<T: Send>(capacity: usize) -> Result<(Producer<T>, Consumer<T>), Error> {
    let region_size = try_minimum_region_size::<T>(capacity)?;
    SharedQueueHeader::calculate_buffer_size_in_items::<T>(region_size)?;
    let region = Region::alloc(NonZeroUsize::new(region_size).ok_or(Error::InvalidBufferSize)?)?;
    // SAFETY: `region` is freshly allocated and used only for this queue.
    let header =
        unsafe { SharedQueueHeader::create_in_region::<T>(&region, DEFAULT_QUEUE_IDENTIFIER) }?;
    // SAFETY: `header` was just created in `region`, so it is valid for the queue.
    let producer = unsafe { Producer::from_header(Arc::clone(&region), header) }?;
    // SAFETY: The region is fresh and has no other consumer.
    let consumer = unsafe { producer.join_as_consumer() }?;

    Ok((producer, consumer))
}

/// Producer side of the SPSC shared queue.
pub struct Producer<T> {
    queue: SharedQueue<T>,
}

impl<T> Producer<T> {
    /// Returns the identifier of the queue.
    ///
    /// Returns `0` when created without an explicit identifier.
    pub fn queue_identifier(&self) -> u64 {
        self.queue.header().identifier
    }

    /// Creates a new producer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - This queue permits exactly one [`Producer`]. If initialization is
    ///   performed as a [`Producer`], no other [`Producer`] may join it.
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
    /// - This queue permits exactly one [`Producer`]. No other [`Producer`]
    ///   may have created or joined the same file.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Consumer`] that is joined with the
    ///   same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Consumer that shares the same memory mapping.
    ///
    /// # Safety
    /// - The caller must ensure this is the unique Consumer for this queue.
    pub unsafe fn join_as_consumer(&self) -> Result<Consumer<T>, Error> {
        Ok(Consumer {
            queue: SharedQueue::from_shared(Arc::clone(&self.queue.shared)),
        })
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

    /// Return the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Return the current length of the queue.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Start a batched write.
    pub fn write_batch(&mut self) -> WriteBatch<'_, T> {
        self.sync();
        WriteBatch { producer: self }
    }

    /// Writes item into the queue or returns it if there is not enough space.
    ///
    /// When writing multiple items, prefer [`Self::write_batch`] to amortize
    /// synchronization and publication across the batch.
    pub fn try_write(&mut self, item: T) -> Result<(), T> {
        self.sync();
        self.try_write_inner(item)?;
        self.commit();
        Ok(())
    }

    /// Writes item into the queue or returns it if there is not enough space.
    /// Does not perform synchronization of cached cursors.
    fn try_write_inner(&mut self, item: T) -> Result<(), T> {
        // SAFETY: pointer is written below if successfully reserved.
        match unsafe { self.reserve() } {
            Some(p) => {
                // SAFETY: `reserve` returns a properly aligned ptr with enough
                //         space to write T.
                unsafe { p.write(item) };
                Ok(())
            }
            None => Err(item),
        }
    }

    /// Reserves a position, and increments the cached write position.
    /// Returns `None` if the queue is full.
    /// Returns a pointer to the reserved position.
    ///
    /// # Safety
    /// All reserved positions must be fully initialized before calling `commit`.
    /// Pointers should be dropped before calling `commit`.
    unsafe fn reserve(&mut self) -> Option<NonNull<T>> {
        // If write is > read + buffer_mask, the queue is written one iteration
        // ahead of the consumer, and we cannot reserve more space.
        if self.queue.cached_write.wrapping_sub(self.queue.cached_read) > self.queue.buffer_mask {
            return None;
        }

        let reserved_index = self.queue.mask(self.queue.cached_write);
        // SAFETY: The reserved index is guaranteed to be within bounds given the mask.
        let reserved_ptr = unsafe { self.queue.buffer.add(reserved_index) };
        self.queue.cached_write = self.queue.cached_write.wrapping_add(1);

        Some(reserved_ptr)
    }

    /// Commits the reserved position, making it visible to the consumer.
    fn commit(&self) {
        let header = self.queue.header();
        // Release publication; `wake` supplies the fence that pairs it with
        // a registering waiter and must be called unconditionally; see the
        // `futex` module docs.
        header
            .write
            .store(self.queue.cached_write, Ordering::Release);
        header.waiters.wake(&header.write, 1);
    }

    /// Synchronize the producer's cached read position with the queue's read
    /// position.
    fn sync(&mut self) {
        self.queue.load_read();
    }
}

// SAFETY: The producer owns the write side exclusively and access to the
// shared buffer is synchronized by the queue protocol, so it is safe to move
// to another thread when `T: Send`.
unsafe impl<T: Send> Send for Producer<T> {}

/// A batch of writes published on drop.
#[must_use]
pub struct WriteBatch<'a, T> {
    producer: &'a mut Producer<T>,
}

impl<'a, T> WriteBatch<'a, T> {
    /// If the next sequence number is available, writes the item and returns Ok(()).
    /// Otherwise, returns an error with the item.
    pub fn try_write(&mut self, item: T) -> Result<(), T> {
        self.producer.try_write_inner(item)
    }

    /// Returns a mutable reference to the next reserved position if one is available.
    ///
    /// # Safety
    /// If this returns `Some`, the caller must initialize the reserved slot with a
    /// valid `T` before the batch is dropped and publishes it. This requirement
    /// also applies if control exits by unwinding.
    pub unsafe fn try_as_mut(&mut self) -> Option<&mut MaybeUninit<T>> {
        // SAFETY: The reserved slot belongs exclusively to this producer.
        let mut reserved = unsafe { self.producer.reserve() }?.cast();
        // SAFETY: The mutable reference is tied to the borrow of this batch.
        Some(unsafe { reserved.as_mut() })
    }
}

impl<'a, T> Drop for WriteBatch<'a, T> {
    fn drop(&mut self) {
        // Commit any written items
        self.producer.commit();
    }
}

/// Consumer side of the SPSC shared queue.
pub struct Consumer<T> {
    queue: SharedQueue<T>,
}

impl<T> Consumer<T> {
    /// Returns the identifier of the queue.
    ///
    /// Returns `0` when created without an explicit identifier.
    pub fn queue_identifier(&self) -> u64 {
        self.queue.header().identifier
    }

    /// Creates a new consumer for the shared queue in the provided file with
    /// the given size.
    ///
    /// # Safety
    /// - The file must be created and initialized exactly once.
    /// - Initialization may be performed by either a [`Producer`] or a
    ///   [`Consumer`], but that process or thread must be designated
    ///   externally as the sole initializer.
    /// - This queue permits exactly one [`Consumer`]. If initialization is
    ///   performed as a [`Consumer`], no other [`Consumer`] may join it.
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
    /// - This queue permits exactly one [`Consumer`]. No other [`Consumer`]
    ///   may have created or joined the same file.
    /// - The queue does not validate `T` across processes.
    /// - If a process may read, dereference, mutate, or drop a queued value,
    ///   that operation must be valid for that value in that process.
    /// - The same `T` must be used by the [`Producer`] that is joined with the
    ///   same file.
    pub unsafe fn join(file: &File) -> Result<Self, Error> {
        let (region, header) = SharedQueueHeader::join::<T>(file)?;
        // SAFETY: `header` is non-null and aligned properly and allocated with
        //         size of `file_size`.
        unsafe { Self::from_header(region, header) }
    }

    /// Creates a Producer that shares the same memory mapping.
    ///
    /// # Safety
    /// - The caller must ensure this is the unique Producer for this queue.
    pub unsafe fn join_as_producer(&self) -> Result<Producer<T>, Error> {
        Ok(Producer {
            queue: SharedQueue::from_shared(Arc::clone(&self.queue.shared)),
        })
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

    /// Return the capacity of the queue in items.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Return the current length of the queue.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Returns true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Attempts to read a value from the queue, synchronizing with the
    /// producer first.
    ///
    /// Returns `None` if there are no values available. Consumed capacity is
    /// released before this method returns.
    ///
    /// When reading multiple items, prefer [`Self::try_reserve_read_batch`] to
    /// amortize synchronization and capacity release across the batch.
    pub fn try_read(&mut self) -> Option<T> {
        self.sync();
        let item = self.try_read_inner();
        self.finalize();
        item
    }

    /// Attempts to reserve up to `max` values from the queue.
    ///
    /// The consumer synchronizes with the producer before reserving. Dropping
    /// the returned batch drops any values that were not moved out through its
    /// [`IntoIterator`] implementation and releases the complete reservation.
    #[must_use]
    pub fn try_reserve_read_batch(&mut self, max: NonZeroUsize) -> Option<ReadBatch<'_, T>> {
        self.sync();
        let reservation = self.try_reserve_read_range(max)?;
        Some(self.read_batch_for(reservation))
    }

    fn try_read_inner(&mut self) -> Option<T> {
        if self.queue.cached_read == self.queue.cached_write {
            return None; // Queue is empty
        }

        let read_index = self.queue.mask(self.queue.cached_read);
        // SAFETY: read_index is guaranteed to be within bounds given the mask.
        let read_ptr = unsafe { self.queue.buffer.add(read_index) };
        self.queue.cached_read = self.queue.cached_read.wrapping_add(1);

        // SAFETY: `read_ptr` is the initialized position just reserved by this
        // consumer, and ownership has not previously been taken from it.
        Some(unsafe { read_ptr.read() })
    }

    fn try_reserve_read_range(&mut self, max: NonZeroUsize) -> Option<(usize, NonZeroUsize)> {
        let count = NonZeroUsize::new(self.queue.len().min(max.get()))?;
        let start = self.queue.cached_read;
        self.queue.cached_read = self.queue.cached_read.wrapping_add(count.get());
        Some((start, count))
    }

    fn read_batch_for(&mut self, (start, count): (usize, NonZeroUsize)) -> ReadBatch<'_, T> {
        ReadBatch {
            reservation: ReadReservation {
                consumer: self,
                start,
                count,
            },
        }
    }

    fn finalize(&mut self) {
        self.queue
            .header()
            .read
            .store(self.queue.cached_read, Ordering::Release);
    }

    fn sync(&mut self) {
        self.queue.load_write();
    }

    /// Blocks until at least one committed item is readable or `timeout` elapses.
    pub fn wait_readable_timeout(&mut self, timeout: Duration) -> Result<(), WaitError> {
        let header = self.queue.header;
        // SAFETY: `header` points to this consumer's live shared queue header.
        let header = unsafe { header.as_ref() };
        header
            .waiters
            .wait_for(&header.write, SPIN_ATTEMPTS, timeout, || {
                self.queue.load_write();
                if !self.queue.is_empty() {
                    Some(())
                } else {
                    None
                }
            })
    }

    /// Blocks until ownership of a committed item can be taken or `timeout`
    /// elapses. Consumed capacity is released before this method returns.
    pub fn read_timeout(&mut self, timeout: Duration) -> Result<T, WaitError> {
        let batch = self.reserve_read_batch_timeout(NonZeroUsize::MIN, timeout)?;
        Ok(batch
            .into_iter()
            .next()
            .expect("a successful one-item reservation is non-empty"))
    }

    /// Attempts to reserve up to `max` values, waiting up to `timeout` for a
    /// producer to publish data.
    ///
    /// Returns `Err(WaitError::Timeout)` if no values are available before the
    /// timeout elapses. The method returns as soon as at least one value can be
    /// reserved; it does not wait for `max` values.
    pub fn reserve_read_batch_timeout(
        &mut self,
        max: NonZeroUsize,
        timeout: Duration,
    ) -> Result<ReadBatch<'_, T>, WaitError> {
        let header = self.queue.header;
        // SAFETY: `header` points to this consumer's live shared queue header.
        let header = unsafe { header.as_ref() };
        let reservation = header
            .waiters
            .wait_for(&header.write, SPIN_ATTEMPTS, timeout, || {
                self.queue.load_write();
                self.try_reserve_read_range(max)
            })?;
        Ok(self.read_batch_for(reservation))
    }
}

// SAFETY: The consumer owns the read side exclusively and access to the
// shared buffer is synchronized by the queue protocol, so it is safe to move
// to another thread when `T: Send`.
unsafe impl<T: Send> Send for Consumer<T> {}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        self.finalize();
    }
}

struct ReadReservation<'a, T> {
    consumer: &'a mut Consumer<T>,
    start: usize,
    count: NonZeroUsize,
}

impl<T> ReadReservation<'_, T> {
    fn len(&self) -> usize {
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
    unsafe fn get_unchecked(&self, index: usize) -> &T {
        debug_assert!(index < self.len());
        let position = self.start.wrapping_add(index);
        // SAFETY: Masking the reserved position produces an index within the
        // queue buffer.
        let value = unsafe {
            self.consumer
                .queue
                .buffer
                .add(position & self.consumer.queue.buffer_mask)
        };
        // SAFETY: The position was reserved for reading and is initialized.
        unsafe { value.as_ref() }
    }

    /// Reads the value at `index`.
    ///
    /// # Safety
    /// - `index` must be less than [`Self::len`].
    /// - The value must not have previously been moved out or dropped.
    /// - No reference to the slot may be used after moving out its value.
    unsafe fn get_owned_unchecked(&self, index: usize) -> T {
        debug_assert!(index < self.len());
        let position = self.start.wrapping_add(index);
        // SAFETY: Masking the reserved position produces an index within the
        // queue buffer.
        let value = unsafe {
            self.consumer
                .queue
                .buffer
                .add(position & self.consumer.queue.buffer_mask)
        };
        // SAFETY: The position was reserved for reading and is initialized.
        unsafe { value.read() }
    }
}

impl<T> Drop for ReadReservation<'_, T> {
    fn drop(&mut self) {
        self.consumer.finalize();
    }
}

/// A destructor-safe reservation for consecutive initialized consumer slots.
///
/// The batch keeps all of its slots reserved while it lives. It may be
/// inspected without consuming values or converted into a sequential consuming
/// iterator. Dropping the batch without consuming it drops every value before
/// releasing the reservation.
#[must_use]
pub struct ReadBatch<'a, T> {
    reservation: ReadReservation<'a, T>,
}

impl<'a, T> ReadBatch<'a, T> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.reservation.len()
    }

    /// Returns a shared reference to the value at `index`, or `None` if it is
    /// outside the batch.
    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.len() {
            return None;
        }
        // SAFETY: The bounds check establishes that the initialized slot is in
        // the reservation, and a safe batch has not moved out any values.
        Some(unsafe { self.reservation.get_unchecked(index) })
    }

    /// Returns the values in logical read order as two slices.
    ///
    /// The second slice is empty unless the batch wraps around the end of the
    /// queue buffer. This borrows the values without consuming the batch.
    pub fn as_slices(&self) -> (&[T], &[T]) {
        let queue = &self.reservation.consumer.queue;
        let start = self.reservation.start & queue.buffer_mask;
        let first_len = self.len().min(queue.capacity().wrapping_sub(start));
        let second_len = self.len().wrapping_sub(first_len);

        // SAFETY: `start` is within the queue buffer.
        let first = unsafe { queue.buffer.add(start) };
        let first = NonNull::slice_from_raw_parts(first, first_len);
        // SAFETY: The first part of the reservation is initialized, contiguous,
        // and remains reserved for the lifetime of the returned slice.
        let first = unsafe { first.as_ref() };
        let second = NonNull::slice_from_raw_parts(queue.buffer, second_len);
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
        // SAFETY: `batch` is not dropped, so this moves its reservation exactly once.
        let reservation = unsafe { core::ptr::read(&batch.reservation) };
        ReadBatchIntoIter {
            reservation,
            next: 0,
        }
    }
}

impl<T: Copy> ReadBatch<'_, T> {
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

        for index in 0..self.reservation.len() {
            // SAFETY: An intact safe batch has not moved out any values.
            let value = unsafe { self.reservation.get_owned_unchecked(index) };
            drop(value);
        }
    }
}

/// A sequential consuming iterator over a reserved read batch.
///
/// This iterator keeps the complete batch reservation held until it is
/// dropped. Dropping it before exhaustion drops the unconsumed suffix before
/// releasing the reservation.
#[must_use]
pub struct ReadBatchIntoIter<'a, T> {
    reservation: ReadReservation<'a, T>,
    next: usize,
}

impl<T> Iterator for ReadBatchIntoIter<'_, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.reservation.len() {
            return None;
        }
        let index = self.next;
        // Advance before returning ownership so Drop only considers the suffix.
        self.next += 1;
        // SAFETY: The cursor visits every reserved index at most once.
        Some(unsafe { self.reservation.get_owned_unchecked(index) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.reservation.len() - self.next;
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

        while self.next < self.reservation.len() {
            let index = self.next;
            self.next += 1;
            // SAFETY: The cursor visits every reserved index at most once.
            let value = unsafe { self.reservation.get_owned_unchecked(index) };
            drop(value);
        }
    }
}

struct SharedQueue<T> {
    cached_write: usize,
    cached_read: usize,
    shared: Arc<SharedQueueInner<T>>,
}

struct SharedQueueInner<T> {
    header: NonNull<SharedQueueHeader>,
    buffer: NonNull<T>,
    buffer_mask: usize,
    // `NonNull<T>` is covariant, but paired endpoints must not be independently
    // coerced to different payload lifetimes. The function input/output marker
    // makes `T` invariant without affecting layout or auto traits.
    _invariant: PhantomData<fn(T) -> T>,

    // NB: Region must be declared last so it is dropped last ensuring `header` and
    // `buffer` remain valid for their entire lifetime.
    region: Arc<Region>,
}

impl<T> core::ops::Deref for SharedQueue<T> {
    type Target = SharedQueueInner<T>;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl<T> Drop for SharedQueueInner<T> {
    fn drop(&mut self) {
        if !self.region.is_heap() || !core::mem::needs_drop::<T>() {
            return;
        }

        // SharedQueueInner is dropped by its Arc after both endpoints
        // disappear, so the published cursors are final. `header` remains
        // valid because `region` is still alive and is dropped last.
        // SAFETY: `header` points into the live region for this queue.
        let header = unsafe { self.header.as_ref() };
        let mut position = header.read.load(Ordering::Acquire);
        let write = header.write.load(Ordering::Acquire);

        while position != write {
            // SAFETY: Masking the position produces an index within the buffer.
            let value = unsafe { self.buffer.add(position & self.buffer_mask) };
            // SAFETY: Published positions not yet released by the consumer
            // contain initialized values still in the queue.
            unsafe { value.drop_in_place() };
            position = position.wrapping_add(1);
        }
    }
}

impl<T> SharedQueue<T> {
    /// Creates a new shared queue from a header pointer and region.
    ///
    /// # Safety
    /// - `header` must be non-null and properly aligned.
    /// - `region` must back the allocation at `header`.
    unsafe fn from_header(
        region: Arc<Region>,
        header: NonNull<SharedQueueHeader>,
    ) -> Result<Self, Error> {
        // SAFETY: `header` is non-null and aligned properly.
        let size = unsafe { (header.as_ref().buffer_mask as usize).wrapping_add(1) };

        if !size.is_power_of_two()
            || SharedQueueHeader::calculate_buffer_size_in_items::<T>(region.size())? != size
        {
            return Err(Error::InvalidBufferSize);
        }

        // SAFETY: `header` is non-null and aligned properly with allocation
        //         of sufficient size.
        let buffer = unsafe { Self::buffer_from_header(header) };

        let shared = Arc::new(SharedQueueInner {
            region,
            header,
            buffer,
            buffer_mask: size - 1,
            _invariant: PhantomData,
        });

        Ok(Self::from_shared(shared))
    }

    fn from_shared(shared: Arc<SharedQueueInner<T>>) -> Self {
        let mut queue = Self {
            cached_write: 0,
            cached_read: 0,
            shared,
        };

        queue.load_write();
        queue.load_read();

        queue
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

    fn capacity(&self) -> usize {
        self.buffer_mask + 1
    }

    fn len(&self) -> usize {
        self.cached_write.wrapping_sub(self.cached_read)
    }

    fn is_empty(&self) -> bool {
        self.cached_write == self.cached_read
    }

    fn mask(&self, index: usize) -> usize {
        index & self.buffer_mask
    }

    #[inline]
    fn header(&self) -> &SharedQueueHeader {
        // SAFETY: See safety on `from_header`. `header` is non-null and aligned.
        unsafe { self.header.as_ref() }
    }

    #[inline]
    fn load_write(&mut self) {
        self.cached_write = self.header().write.load(Ordering::Acquire);
    }

    #[inline]
    fn load_read(&mut self) {
        self.cached_read = self.header().read.load(Ordering::Acquire);
    }
}

/// Header in shared memory for the queue.
#[repr(C)]
struct SharedQueueHeader {
    // Cold cache line.
    magic: AtomicU64,
    version: u32,
    buffer_mask: u32,
    identifier: u64,

    // Hot cache lines.
    write: CacheAlignedAtomicSize,
    read: CacheAlignedAtomicSize,
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

        (core::mem::size_of::<Self>() + core::mem::align_of::<T>() - 1)
            & !(core::mem::align_of::<T>() - 1)
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
    unsafe fn initialize(mut header: NonNull<Self>, buffer_size_in_items: usize, identifier: u64) {
        // SAFETY:
        // - `header` is non-null and aligned properly.
        // - `access` to `header` is unique.
        let header = unsafe { header.as_mut() };
        header.write.store(0, Ordering::Release);
        header.read.store(0, Ordering::Release);
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
            if (header.buffer_mask as usize).wrapping_add(1)
                != Self::calculate_buffer_size_in_items::<T>(region.size())?
            {
                return Err(Error::InvalidBufferSize);
            }
        }

        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(miri))]
    use crate::shmem::create_temp_shmem_file;
    use std::{
        panic::{catch_unwind, AssertUnwindSafe},
        sync::atomic::{AtomicU64, AtomicUsize},
        time::Duration,
    };

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
        // SAFETY: fresh temp file sized for the queue, no other users.
        let producer =
            unsafe { Producer::create(&file, file_size) }.expect("failed to create producer");
        // SAFETY: file was just initialized by the producer above.
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
        type Item = AtomicU64;
        const BUFFER_CAPACITY: usize = 1024;
        for create_queue in test_queue_creators::<Item>() {
            let (mut producer, mut consumer) = create_queue(BUFFER_CAPACITY);

            assert_eq!(producer.capacity(), BUFFER_CAPACITY);
            assert_eq!(consumer.capacity(), BUFFER_CAPACITY);

            // SAFETY: single producer over a freshly created queue.
            let spot = unsafe { producer.reserve() }.expect("Failed to reserve");
            // SAFETY: spot is a valid reserved slot.
            unsafe { spot.as_ref() }.store(42, Ordering::Release);
            assert!(consumer.try_read().is_none()); // not committed yet
            producer.commit();
            let item = consumer.try_read().expect("Failed to read item");
            assert_eq!(item.load(Ordering::Acquire), 42);
            assert!(consumer.try_read().is_none()); // no more items to read

            // Ensure we can push up to the capacity.
            {
                let mut batch = producer.write_batch();
                for _ in 0..BUFFER_CAPACITY {
                    assert!(batch.try_write(AtomicU64::new(1)).is_ok());
                }
                assert!(batch.try_write(AtomicU64::new(1)).is_err());
            }
            {
                let batch = consumer
                    .try_reserve_read_batch(NonZeroUsize::new(BUFFER_CAPACITY).unwrap())
                    .expect("Failed to reserve read batch");
                let mut iter = batch.into_iter();
                for _ in 0..BUFFER_CAPACITY {
                    let item = iter.next().expect("Failed to read item");
                    assert_eq!(item.load(Ordering::Acquire), 1);
                }
                assert!(iter.next().is_none()); // no more items to read
            }

            // Ensure we can write again after the batch releases its reads.
            producer.try_write(AtomicU64::new(2)).unwrap();
            let item = consumer
                .try_read()
                .expect("Failed to read item after batch");
            assert_eq!(item.load(Ordering::Acquire), 2);
        }
    }

    #[test]
    fn test_join_producer_as_consumer() {
        const BUFFER_CAPACITY: usize = 64;
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, consumer) = create_queue(BUFFER_CAPACITY);
            drop(consumer);
            // SAFETY: this is the unique consumer for this queue.
            let mut consumer = unsafe { producer.join_as_consumer() }.expect("join failed");

            producer.try_write(42).unwrap();
            let val = consumer.try_read().expect("read failed");
            assert_eq!(val, 42);
        }
    }

    #[test]
    fn test_owned_read_non_copy() {
        let (mut producer, mut consumer) = pair(1).expect("failed to create queue");
        let value = Arc::new(42);

        producer.try_write(Arc::clone(&value)).unwrap();

        let received = consumer.try_read().expect("read failed");
        assert!(Arc::ptr_eq(&received, &value));
        assert_eq!(Arc::strong_count(&value), 2);
        drop(received);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn test_heap_pair_drops_buffered_value_with_last_endpoint() {
        let value = Arc::new(());
        let (mut producer, consumer) = pair(1).expect("failed to create queue");

        producer.try_write(Arc::clone(&value)).unwrap();
        drop(producer);
        assert_eq!(Arc::strong_count(&value), 2);

        drop(consumer);
        assert_eq!(Arc::strong_count(&value), 1);
    }

    #[test]
    fn test_join_consumer_as_producer() {
        const BUFFER_CAPACITY: usize = 64;
        for create_queue in test_queue_creators::<u64>() {
            let (producer, mut consumer) = create_queue(BUFFER_CAPACITY);
            drop(producer);
            // SAFETY: this is the unique producer for this queue.
            let mut producer = unsafe { consumer.join_as_producer() }.expect("join failed");

            producer.try_write(99).unwrap();
            let val = consumer.try_read().expect("read failed");
            assert_eq!(val, 99);
        }
    }

    #[test]
    fn test_drop_order_independent() {
        const BUFFER_CAPACITY: usize = 64;
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, consumer) = create_queue(BUFFER_CAPACITY);
            drop(consumer);
            // SAFETY: this is the unique consumer for this queue.
            let mut consumer = unsafe { producer.join_as_consumer() }.expect("join failed");

            // Write a message then drop.
            producer.try_write(7).unwrap();
            drop(producer);

            // Can still read the message from the shared consumer
            let val = consumer.try_read().expect("read after producer drop");
            assert_eq!(val, 7);
        }
    }

    #[test]
    fn test_capacity_rounds_up() {
        for create_queue in test_queue_creators::<u64>() {
            let (producer, _consumer) = create_queue(3);
            assert_eq!(producer.capacity(), 4);
        }
    }

    #[test]
    fn test_read_timeout_observes_commit() {
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, mut consumer) = create_queue(64);

            // SAFETY: single producer over a freshly created queue.
            let spot = unsafe { producer.reserve() }.expect("reserve failed");
            // SAFETY: spot is a valid reserved slot.
            unsafe { spot.write(42) };

            assert!(matches!(
                consumer.wait_readable_timeout(Duration::ZERO),
                Err(WaitError::Timeout)
            ));

            producer.commit();

            let value = match consumer.read_timeout(Duration::ZERO) {
                Ok(value) => value,
                Err(WaitError::Timeout) => panic!("read timed out after commit"),
            };
            assert_eq!(value, 42);
        }
    }

    #[test]
    fn test_wait_readable_max_timeout_does_not_panic() {
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, mut consumer) = create_queue(64);

            producer.try_write(1).unwrap();

            // `Duration::MAX` overflows `Instant`; the deadline must saturate
            // instead of panicking. Data is already committed so this returns
            // immediately.
            consumer
                .wait_readable_timeout(Duration::MAX)
                .expect("wait failed");
        }
    }

    #[test]
    fn test_wait_readable_timeout_cleans_waiter() {
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, mut consumer) = create_queue(64);

            assert!(matches!(
                consumer.wait_readable_timeout(Duration::from_millis(1)),
                Err(WaitError::Timeout)
            ));

            assert!(matches!(
                consumer.read_timeout(Duration::from_millis(1)),
                Err(WaitError::Timeout)
            ));

            producer.try_write(9).unwrap();

            let value = match consumer.read_timeout(Duration::ZERO) {
                Ok(value) => value,
                Err(WaitError::Timeout) => panic!("read timed out after commit"),
            };
            assert_eq!(value, 9);
        }
    }

    #[test]
    fn read_batch_is_bounded_and_releases_on_drop() {
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, mut consumer) = create_queue(4);
            for value in 0..4 {
                producer.try_write(value).unwrap();
            }

            {
                let batch = consumer
                    .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                    .expect("read batch");
                assert_eq!(batch.len(), 2);
                assert_eq!(batch.get(0), Some(&0));
                assert_eq!(batch[1], 1);
                assert_eq!(batch.get_owned(1), Some(1));
                assert_eq!(batch.get(2), None);
                let (first, second) = batch.as_slices();
                assert_eq!(first, &[0, 1]);
                assert!(second.is_empty());
                assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![0, 1]);
                let mut borrowed = Vec::new();
                for value in &batch {
                    borrowed.push(*value);
                }
                assert_eq!(borrowed, vec![0, 1]);
                assert!(producer.try_write(4).is_err());
            }

            assert!(producer.try_write(4).is_ok());
            let batch = consumer
                .reserve_read_batch_timeout(NonZeroUsize::new(8).unwrap(), Duration::ZERO)
                .expect("remaining batch");
            assert_eq!(batch.into_iter().collect::<Vec<_>>(), vec![2, 3, 4]);
        }
    }

    #[test]
    fn read_batch_supports_wrapped_random_access() {
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, mut consumer) = create_queue(4);
            for value in 0..3 {
                producer.try_write(value).unwrap();
            }
            assert_eq!(consumer.try_read(), Some(0));
            assert_eq!(consumer.try_read(), Some(1));
            for value in 3..6 {
                producer.try_write(value).unwrap();
            }

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(4).unwrap())
                .expect("wrapped read batch");
            assert_eq!(batch.get_owned(3), Some(5));
            let (first, second) = batch.as_slices();
            assert_eq!(first, &[2, 3]);
            assert_eq!(second, &[4, 5]);
            assert_eq!(batch.iter().copied().collect::<Vec<_>>(), vec![2, 3, 4, 5]);
            assert_eq!(batch.into_iter().collect::<Vec<_>>(), vec![2, 3, 4, 5]);
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

    #[test]
    fn read_batch_drop_and_partial_iteration_drop_values() {
        for create_queue in test_queue_creators::<CountedItem>() {
            let drops = Arc::new(AtomicUsize::new(0));
            let (mut producer, mut consumer) = create_queue(4);
            for value in 0..3 {
                producer
                    .try_write(CountedItem {
                        value,
                        drops: Arc::clone(&drops),
                    })
                    .unwrap_or_else(|_| panic!("write failed"));
            }

            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(3).unwrap())
                .expect("read batch");
            assert_eq!(
                batch.iter().map(|item| item.value).collect::<Vec<_>>(),
                vec![0, 1, 2]
            );
            drop(batch);
            assert_eq!(drops.load(Ordering::Relaxed), 3);

            for value in 3..6 {
                producer
                    .try_write(CountedItem {
                        value,
                        drops: Arc::clone(&drops),
                    })
                    .unwrap_or_else(|_| panic!("write failed"));
            }
            let batch = consumer
                .try_reserve_read_batch(NonZeroUsize::new(3).unwrap())
                .expect("read batch");
            let mut iter = batch.into_iter();
            let first = iter.next().expect("first value");
            assert_eq!(first.value, 3);
            drop(first);
            drop(iter);
            assert_eq!(drops.load(Ordering::Relaxed), 6);
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
        let (mut producer, mut consumer) = pair(2).expect("failed to create queue");
        producer
            .try_write(PanicOnDrop(true))
            .unwrap_or_else(|_| panic!("write failed"));
        producer
            .try_write(PanicOnDrop(false))
            .unwrap_or_else(|_| panic!("write failed"));
        let batch = consumer
            .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
            .expect("read batch");

        assert!(catch_unwind(AssertUnwindSafe(|| drop(batch))).is_err());

        producer
            .try_write(PanicOnDrop(false))
            .unwrap_or_else(|_| panic!("write failed"));
        producer
            .try_write(PanicOnDrop(false))
            .unwrap_or_else(|_| panic!("write failed"));
        drop(
            consumer
                .try_reserve_read_batch(NonZeroUsize::new(2).unwrap())
                .expect("replacement read batch"),
        );
    }

    #[test]
    fn reserve_read_batch_timeout_waits_for_at_least_one_value() {
        for create_queue in test_queue_creators::<u64>() {
            let (mut producer, mut consumer) = create_queue(4);
            assert!(matches!(
                consumer.reserve_read_batch_timeout(
                    NonZeroUsize::new(4).unwrap(),
                    Duration::from_millis(1),
                ),
                Err(WaitError::Timeout)
            ));

            producer.try_write(7).unwrap();
            let batch = consumer
                .reserve_read_batch_timeout(NonZeroUsize::new(4).unwrap(), Duration::ZERO)
                .expect("read batch");
            assert_eq!(batch.len(), 1);
            assert_eq!(batch.into_iter().collect::<Vec<_>>(), vec![7]);
        }
    }
}
