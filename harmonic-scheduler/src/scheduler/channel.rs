//! Transaction stream channels, and the bundle batches they carry

use {
    crate::scheduler::MAX_BUNDLE_TXS,
    agave_scheduler_bindings::execution_message_flags,
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    anyhow::{Context, Result},
    arrayvec::ArrayVec,
    rtrb::{Consumer, RingBuffer},
    rts_alloc::{Allocator, MAX_ALLOC_SIZE},
    solana_clock::Slot,
    tracing::warn,
};

/// Successful pushes between reclaims of what the leader freed
const RECLAIM_INTERVAL: u32 = 8;

/// Execution flags of a revert-protected batch: all of it lands, or none
pub const REVERT_PROTECT: u16 =
    execution_message_flags::ALL_OR_NOTHING | execution_message_flags::DROP_ON_FAILURE;

/// Metadata the scheduler keeps with a batch it builds; the validator ignores it
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct BundleMeta {
    /// The leader slot a block bundle or tip crank is for; 0 for others
    pub slot: Slot,
    /// `execution_message_flags` the batch executes with: [`REVERT_PROTECT`] or 0
    pub flags: u16,
    /// The id the stage holding the bundle gave it; 0 until a stage takes it
    pub id: u32,
}

pub struct Channel<T> {
    producer: rtrb::Producer<T>,
    /// Reclaims what the leader freed only when asked, with `clean_remote_frees`
    pub allocator: Allocator,
    /// Successful pushes, to reclaim every `RECLAIM_INTERVAL`
    pushes: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushError {
    /// The queue is full
    ChannelFull,
    /// Shared memory is exhausted
    OutOfMemory,
    /// A transaction is empty
    EmptyTx,
    /// A transaction exceeds `MAX_ALLOC_SIZE` bytes
    OversizedTx,
    /// A bundle has more than `MAX_BUNDLE_TXS` transactions
    OversizedBundle,
}

/// Transactions a stream dropped since its last report, by cause
#[derive(Default, PartialEq)]
pub struct Metrics {
    channel_full: usize,
    out_of_memory: usize,
    empty_tx: usize,
    oversized_tx: usize,
    oversized_bundle: usize,
}

impl Metrics {
    /// Count `txs` transactions dropped for `error`
    pub fn add(&mut self, error: PushError, txs: usize) {
        let count = match error {
            PushError::ChannelFull => &mut self.channel_full,
            PushError::OutOfMemory => &mut self.out_of_memory,
            PushError::EmptyTx => &mut self.empty_tx,
            PushError::OversizedTx => &mut self.oversized_tx,
            PushError::OversizedBundle => &mut self.oversized_bundle,
        };
        *count = count.saturating_add(txs);
    }

    /// Warn of `stream`'s drops since the last report, if any
    pub fn report(&mut self, stream: &'static str) {
        let metrics = std::mem::take(self);
        if metrics == Self::default() {
            return;
        }
        let Self {
            channel_full,
            out_of_memory,
            empty_tx,
            oversized_tx,
            oversized_bundle,
        } = metrics;
        warn!(
            stream,
            channel_full,
            out_of_memory,
            empty_tx,
            oversized_tx,
            oversized_bundle,
            "dropped transactions"
        );
    }
}

impl<T> Channel<T> {
    pub fn new(allocator: &Allocator, capacity: usize) -> Result<(Self, Consumer<T>)> {
        let (producer, consumer) = RingBuffer::new(capacity);
        let allocator = Allocator::join_from_existing(allocator).context("join allocator")?;
        Ok((
            Self {
                producer,
                allocator,
                pushes: 0,
            },
            consumer,
        ))
    }
}

impl Channel<TransactionPtr> {
    pub fn push(&mut self, packet: &[u8]) -> Result<(), PushError> {
        if packet.is_empty() {
            return Err(PushError::EmptyTx);
        }
        if packet.len() > MAX_ALLOC_SIZE {
            return Err(PushError::OversizedTx);
        }
        // Make sure there is space in the queue
        let entry = self
            .producer
            .write_chunk_uninit(1)
            .map_err(|_| PushError::ChannelFull)?;
        let tx = TransactionPtr::from_slice(packet, &self.allocator)
            .or_else(|| {
                // Out of memory only once what the leader freed is reclaimed
                self.allocator.clean_remote_frees();
                TransactionPtr::from_slice(packet, &self.allocator)
            })
            .ok_or(PushError::OutOfMemory)?;
        entry.fill_from_iter([tx]);
        self.pushes = self.pushes.wrapping_add(1);
        if self.pushes.is_multiple_of(RECLAIM_INTERVAL) {
            self.allocator.clean_remote_frees();
        }
        Ok(())
    }
}

impl Channel<TransactionPtrBatch<(), BundleMeta>> {
    pub fn push<'a>(
        &mut self,
        bundle: impl IntoIterator<Item = &'a [u8]>,
        meta: BundleMeta,
    ) -> Result<(), PushError> {
        let mut txs = ArrayVec::<&[u8], MAX_BUNDLE_TXS>::new();
        for tx in bundle {
            if tx.is_empty() {
                return Err(PushError::EmptyTx);
            }
            if tx.len() > MAX_ALLOC_SIZE {
                return Err(PushError::OversizedTx);
            }
            if txs.try_push(tx).is_err() {
                return Err(PushError::OversizedBundle);
            }
        }
        // Make sure there is space in the queue
        let entry = self
            .producer
            .write_chunk_uninit(1)
            .map_err(|_| PushError::ChannelFull)?;
        let batch = copy_batch(&txs, meta, &self.allocator)
            .or_else(|| {
                // Out of memory only once what the leader freed is reclaimed
                self.allocator.clean_remote_frees();
                copy_batch(&txs, meta, &self.allocator)
            })
            .ok_or(PushError::OutOfMemory)?;
        entry.fill_from_iter([batch]);
        self.pushes = self.pushes.wrapping_add(1);
        if self.pushes.is_multiple_of(RECLAIM_INTERVAL) {
            self.allocator.clean_remote_frees();
        }
        Ok(())
    }
}

/// Copy `bundle` into one batch with `meta`; None, with nothing allocated, if memory runs out
fn copy_batch(
    bundle: &ArrayVec<&[u8], MAX_BUNDLE_TXS>,
    meta: BundleMeta,
    allocator: &Allocator,
) -> Option<TransactionPtrBatch<(), BundleMeta>> {
    let txs: ArrayVec<TransactionPtr, MAX_BUNDLE_TXS> = bundle
        .iter()
        .map_while(|data| TransactionPtr::from_slice(data, allocator))
        .collect();
    if txs.len() != bundle.len() {
        for tx in txs {
            // SAFETY: Allocated above by this allocator and in no batch, so ours alone
            unsafe { tx.free(allocator) };
        }
        return None;
    }
    // SAFETY: Allocated above by this allocator; the batch takes them over,
    // so they stay allocated while it is in use
    let batch = unsafe {
        TransactionPtrBatch::from_transactions(txs.iter().map(|tx| (tx, ())), meta, allocator)
    };
    if batch.is_none() {
        for tx in txs {
            // SAFETY: Allocated above by this allocator and in no batch, so ours alone
            unsafe { tx.free(allocator) };
        }
    }
    batch
}
