//! The check workers: CHECKs out, and their replies back to whoever sent them

use {
    crate::scheduler::CHECK_QUEUE_CAPACITY,
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, MAX_TRANSACTIONS_PER_MESSAGE, PackToCheckWorkerMessage,
        SharableTransactionRegion, processed_codes, worker_message_types::CheckResponse,
    },
    agave_scheduling_utils::{
        pubkeys_ptr::PubkeysPtr, responses_region::CheckResponsesPtr,
        transaction_ptr::TransactionPtrBatch,
    },
    anyhow::{Result, anyhow, bail},
    rts_alloc::{Allocator, MAX_ALLOC_SIZE},
    solana_pubkey::Pubkey,
    std::{marker::PhantomData, num::NonZeroUsize},
};

/// Most transactions in one CHECK: its responses must fit rts-alloc's largest allocation
pub const MAX_CHECK_TXS: usize = MAX_ALLOC_SIZE / size_of::<CheckResponse>();
const _: () = assert!(MAX_CHECK_TXS > 0 && MAX_CHECK_TXS <= MAX_TRANSACTIONS_PER_MESSAGE);

/// Metadata on every transaction of a CHECK the scheduler sends
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct CheckMeta {
    /// The sender's id for the transaction
    pub id: u32,
    /// How many of the transaction's lookup-table keys are writable
    pub writable: u16,
}

/// One transaction of a CHECK reply
pub struct CheckResult<'r, M> {
    pub meta: M,
    pub response: &'r CheckResponse,
    /// The lookup-table keys the check resolved, if it loaded them
    pub keys: Option<&'r [Pubkey]>,
}

/// Sends CHECKs and hands back their replies
pub struct Checker<'a, M> {
    requests: shaq::mpmc::Producer<PackToCheckWorkerMessage>,
    replies: shaq::mpmc::Consumer<CheckWorkerToPackMessage>,
    allocator: &'a Allocator,
    /// CHECKs in flight; at most the reply queue's capacity, as a full one makes workers exit
    in_flight: usize,
    /// Every transaction of its CHECKs carries an `M`
    meta: PhantomData<M>,
}

impl<'a, M: Copy> Checker<'a, M> {
    pub fn new(
        requests: shaq::mpmc::Producer<PackToCheckWorkerMessage>,
        replies: shaq::mpmc::Consumer<CheckWorkerToPackMessage>,
        allocator: &'a Allocator,
    ) -> Self {
        Self {
            requests,
            replies,
            allocator,
            in_flight: 0,
            meta: PhantomData,
        }
    }

    /// CHECKs in flight
    pub fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Whether another CHECK can go out
    pub fn has_room(&self) -> bool {
        self.in_flight < CHECK_QUEUE_CAPACITY
    }

    /// Send one CHECK with `flags` over the front of `entries`; how many it took, 0 if no room
    ///
    /// # Safety
    ///
    /// Each region is a transaction in this mapping that stays allocated until its reply is polled
    pub unsafe fn send(
        &mut self,
        flags: u16,
        entries: &[(SharableTransactionRegion, M)],
    ) -> Result<usize> {
        let n = entries.len().min(MAX_CHECK_TXS);
        // The validator rejects an empty CHECK as invalid
        if n == 0 || !self.has_room() {
            return Ok(0);
        }
        let entries = entries.iter().take(n).copied();
        // SAFETY: The caller keeps every region's transaction allocated until its reply is polled.
        let batch = unsafe { TransactionPtrBatch::from_iter(entries, (), self.allocator) }
            .ok_or_else(|| anyhow!("no shared memory for a CHECK"))?;
        let message = PackToCheckWorkerMessage {
            flags,
            batch: batch.to_sharable_transaction_batch_region(),
        };
        if self.requests.try_write(message).is_err() {
            // SAFETY: Never sent, so ours; it does not own the transactions.
            unsafe { batch.free(self.allocator) };
            bail!("check request queue full with a CHECK slot free");
        }
        self.in_flight = self.in_flight.saturating_add(1);
        Ok(n)
    }

    /// Hand every transaction of every reply to `f`, then free the replies
    pub fn poll(&mut self, mut f: impl FnMut(CheckResult<'_, M>) -> Result<()>) -> Result<()> {
        let Some(replies) = self.replies.try_reserve_read_batch(NonZeroUsize::MAX) else {
            return Ok(());
        };
        for reply in replies {
            match reply.processed_code {
                processed_codes::PROCESSED => {}
                processed_codes::INVALID => bail!("check worker rejected a CHECK as invalid"),
                code => bail!("unexpected processed code {code} for a CHECK reply"),
            }
            self.in_flight = self
                .in_flight
                .checked_sub(1)
                .ok_or_else(|| anyhow!("CHECK reply with none in flight"))?;
            // SAFETY: a reply returns a batch this session sent with `M`,
            // over transactions their owners keep until it returns
            let batch = unsafe {
                TransactionPtrBatch::<M, ()>::from_sharable_transaction_batch_region(
                    &reply.batch,
                    self.allocator,
                )
            };
            // SAFETY: a processed reply from this session's check worker
            let responses = unsafe {
                CheckResponsesPtr::from_transaction_response_region(
                    &reply.responses,
                    self.allocator,
                )
            };
            for ((_, meta), response) in batch.iter().zip(responses.iter()) {
                // SAFETY: the reply's resolved keys are ours
                let keys = unsafe { PubkeysPtr::from_check_response(response, self.allocator) };
                // An error ends the session, and its shared memory with it, so
                // what this reply still holds is not freed
                f(CheckResult {
                    meta,
                    response,
                    keys: keys.as_ref().map(PubkeysPtr::as_slice),
                })?;
                if let Some(keys) = keys {
                    // SAFETY: freed once, here; `f` saw them only by reference
                    unsafe { keys.free(self.allocator) };
                }
            }
            // SAFETY: the reply returns its responses to us; freed once
            unsafe { responses.free(self.allocator) };
            // SAFETY: the reply returns the batch to us; it does not own its transactions
            unsafe { batch.free(self.allocator) };
        }
        Ok(())
    }
}
