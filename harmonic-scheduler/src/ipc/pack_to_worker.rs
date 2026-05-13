//! Pack-to-worker channel helpers

use agave_scheduler_bindings::PackToWorkerMessage;

/// Send a message to a worker.
/// On success the allocation ownership transfers to the worker.
/// Freeing the batch before the response arrives is a use-after-free
///
/// # Returns
/// `Err(message)` if the worker's queue is full
pub fn send(
    producer: &mut shaq::Producer<PackToWorkerMessage>,
    message: PackToWorkerMessage,
) -> Result<(), PackToWorkerMessage> {
    // Optimistic path: cached view may underestimate available space
    if let Err(message) = producer.try_write(message) {
        // Sync to check actual available space and retry
        producer.sync();
        producer.try_write(message)?;
    };
    producer.commit();
    Ok(())
}
