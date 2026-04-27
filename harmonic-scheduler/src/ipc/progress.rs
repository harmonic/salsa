//! Validator progress feed

use agave_scheduler_bindings::ProgressMessage;
use rdtsc::Instant;
use solana_hash::Hash;

/// ProgressMessage channel timeout.
/// If no message arrives within this window we assume disconnected
const IPC_STALE_TIMEOUT_MS: u64 = 400;

pub struct ProgressTracker {
    consumer: shaq::Consumer<ProgressMessage>,
    last: ProgressMessage,
    last_received: Instant,
}

impl ProgressTracker {
    pub fn new(consumer: shaq::Consumer<ProgressMessage>) -> Self {
        Self {
            consumer,
            // SAFETY: ProgressMessage is repr(C) with only primitive fields; all-zero is a valid bit pattern
            last: unsafe { std::mem::zeroed() },
            last_received: Instant::now(),
        }
    }

    /// Drain pending messages, keep the latest, and return it.
    /// Returns `Err` after `IPC_STALE_TIMEOUT_MS` of silence
    pub fn poll(&mut self) -> Result<&ProgressMessage, &'static str> {
        self.consumer.sync();
        let mut last_ptr = None;
        while let Some(p) = self.consumer.try_read_ptr() {
            last_ptr = Some(p);
        }
        if let Some(msg) = last_ptr {
            // SAFETY: msg is valid until finalize() below.
            self.last = unsafe { *msg.as_ptr() };
            self.consumer.finalize();
            self.last_received = Instant::now();
        } else if self.last_received.elapsed_ms() > IPC_STALE_TIMEOUT_MS {
            return Err("IPC progress feed stale");
        }
        Ok(&self.last)
    }

    /// Cached latest message (without re-polling)
    pub fn last(&self) -> &ProgressMessage {
        &self.last
    }

    /// Cached latest blockhash
    pub fn blockhash(&self) -> Hash {
        Hash::new_from_array(self.last.latest_blockhash)
    }
}
