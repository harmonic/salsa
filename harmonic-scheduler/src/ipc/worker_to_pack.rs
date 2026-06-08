//! Worker-to-pack response channel

use agave_scheduler_bindings::WorkerToPackMessage;

/// Iterator over pending responses. Finalizes the ring on drop
/// Caller owns the returned allocations
pub fn iter(
    consumer: &mut shaq::spsc::Consumer<WorkerToPackMessage>,
) -> impl Iterator<Item = WorkerToPackMessage> + '_ {
    consumer.sync();
    Iter { consumer }
}

struct Iter<'a> {
    consumer: &'a mut shaq::spsc::Consumer<WorkerToPackMessage>,
}

impl Iterator for Iter<'_> {
    type Item = WorkerToPackMessage;
    fn next(&mut self) -> Option<Self::Item> {
        self.consumer.try_read().copied()
    }
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
        self.consumer.finalize();
    }
}
