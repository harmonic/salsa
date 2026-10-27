//! Execution: the executor that runs bundles on the validator's workers

use {
    crate::scheduler::PACK_TO_WORKER_CAPACITY,
    agave_scheduler_bindings::{
        ExecutionWorkerToPackMessage, MAX_TRANSACTIONS_PER_MESSAGE, PackToExecutionWorkerMessage,
        processed_codes, worker_message_types::ExecutionResponse,
    },
    agave_scheduler_handshake::{ClientWorkerSession, MAX_WORKERS},
    agave_scheduling_utils::{
        responses_region::ExecutionResponsesPtr, transaction_ptr::TransactionPtrBatch,
    },
    anyhow::{Result, anyhow, bail},
    arrayvec::ArrayVec,
    rts_alloc::{Allocator, MAX_ALLOC_SIZE},
    solana_clock::Slot,
    std::{marker::PhantomData, num::NonZeroUsize},
};

// The worker allocates a reply's responses as one array, which every EXECUTE's must fit
const _: () =
    assert!(MAX_TRANSACTIONS_PER_MESSAGE * size_of::<ExecutionResponse>() <= MAX_ALLOC_SIZE);

/// One EXECUTE reply
pub struct ExecuteResult<'r, M> {
    /// The executed batch, which its sender owns again
    pub batch: TransactionPtrBatch<(), M>,
    /// Each transaction's response; None if the slot ended before the worker reached it
    pub responses: Option<&'r ExecutionResponsesPtr>,
}

/// An execution worker's queues, and its EXECUTEs in flight
struct Worker {
    requests: shaq::spsc::Producer<PackToExecutionWorkerMessage>,
    replies: shaq::spsc::Consumer<ExecutionWorkerToPackMessage>,
    /// At most its reply queue's capacity, as a worker whose reply queue is full exits
    in_flight: usize,
}

/// Sends EXECUTEs and hands back their replies
pub struct Executor<'a, M> {
    workers: ArrayVec<Worker, MAX_WORKERS>,
    allocator: &'a Allocator,
    /// Every batch it executes carries an `M`
    meta: PhantomData<M>,
}

impl<'a, M> Executor<'a, M> {
    pub fn new(workers: Vec<ClientWorkerSession>, allocator: &'a Allocator) -> Self {
        let workers = workers
            .into_iter()
            .take(MAX_WORKERS)
            .map(|worker| Worker {
                requests: worker.pack_to_worker,
                replies: worker.worker_to_pack,
                in_flight: 0,
            })
            .collect();
        Self {
            workers,
            allocator,
            meta: PhantomData,
        }
    }

    /// EXECUTEs in flight
    pub fn in_flight(&self) -> usize {
        self.workers.iter().map(|worker| worker.in_flight).sum()
    }

    /// Whether some worker can take another EXECUTE
    pub fn has_room(&self) -> bool {
        self.workers
            .iter()
            .any(|worker| worker.in_flight < PACK_TO_WORKER_CAPACITY)
    }

    /// Send one EXECUTE to the least-loaded worker; hands `batch` back if none has room
    pub fn send(
        &mut self,
        flags: u16,
        slot: Slot,
        batch: TransactionPtrBatch<(), M>,
    ) -> Result<(), TransactionPtrBatch<(), M>> {
        let Some(worker) = self
            .workers
            .iter_mut()
            .min_by_key(|worker| worker.in_flight)
            .filter(|worker| worker.in_flight < PACK_TO_WORKER_CAPACITY)
        else {
            return Err(batch);
        };
        let message = PackToExecutionWorkerMessage {
            flags,
            max_working_slot: slot,
            batch: batch.to_sharable_transaction_batch_region(),
        };
        // A worker's queue holds every EXECUTE it may have in flight, so this fits
        if worker.requests.try_write(message).is_err() {
            return Err(batch);
        }
        worker.in_flight = worker.in_flight.saturating_add(1);
        Ok(())
    }

    /// Hand every reply to `f`, then free the replies
    pub fn poll(&mut self, mut f: impl FnMut(ExecuteResult<'_, M>) -> Result<()>) -> Result<()> {
        for worker in &mut self.workers {
            if worker.in_flight == 0 {
                continue;
            }
            let Some(replies) = worker.replies.try_reserve_read_batch(NonZeroUsize::MAX) else {
                continue;
            };
            for reply in replies {
                let responses = match reply.processed_code {
                    // SAFETY: a processed reply from this session's worker to an EXECUTE
                    processed_codes::PROCESSED => Some(unsafe {
                        ExecutionResponsesPtr::from_transaction_response_region(
                            &reply.responses,
                            self.allocator,
                        )
                    }),
                    // The slot ended before the worker reached it
                    processed_codes::MAX_WORKING_SLOT_EXCEEDED => None,
                    processed_codes::INVALID => {
                        bail!("execution worker rejected an EXECUTE as invalid")
                    }
                    code => bail!("unexpected processed code {code} for an EXECUTE reply"),
                };
                // SAFETY: a reply returns a batch this session sent, which its
                // sender owns again
                let batch = unsafe {
                    TransactionPtrBatch::from_sharable_transaction_batch_region(
                        &reply.batch,
                        self.allocator,
                    )
                };
                worker.in_flight = worker
                    .in_flight
                    .checked_sub(1)
                    .ok_or_else(|| anyhow!("EXECUTE reply with none in flight"))?;
                // An error ends the session, and its shared memory with it, so
                // the responses are not freed
                f(ExecuteResult {
                    batch,
                    responses: responses.as_ref(),
                })?;
                if let Some(responses) = responses {
                    // SAFETY: freed once, here; `f` saw them only by reference
                    unsafe { responses.free(self.allocator) };
                }
            }
        }
        Ok(())
    }
}
