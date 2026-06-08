//! Validator progress feed

use agave_scheduler_bindings::ProgressMessage;
use anyhow::{Result, bail};
use rdtsc::Instant;
use solana_hash::Hash;

/// Timeout between IPC progress messages before assuming disconnect
const PROGRESS_MESSAGE_TIMEOUT_MS: u64 = 400;

pub struct ProgressTracker {
    consumer: shaq::spsc::Consumer<ProgressMessage>,
    last: ProgressMessage,
    last_received: Instant,
}

impl ProgressTracker {
    pub fn new(consumer: shaq::spsc::Consumer<ProgressMessage>) -> Self {
        Self {
            consumer,
            // SAFETY: ProgressMessage is repr(C) with only primitive fields; all-zero is a valid bit pattern
            last: unsafe { std::mem::zeroed() },
            last_received: Instant::now(),
        }
    }

    /// Drain pending messages, keep the latest, and return it
    pub fn poll(&mut self) -> Result<&ProgressMessage> {
        self.consumer.sync();
        let mut last_ptr = None;
        while let Some(p) = self.consumer.try_read_ptr() {
            last_ptr = Some(p);
        }
        if let Some(msg) = last_ptr {
            // SAFETY: msg is valid until finalize() below
            self.last = unsafe { *msg.as_ptr() };
            self.consumer.finalize();
            self.last_received = Instant::now();
        } else if self.last_received.elapsed_ms() > PROGRESS_MESSAGE_TIMEOUT_MS {
            bail!("IPC progress feed stale");
        }
        Ok(&self.last)
    }

    /// Get the latest cached message without re-polling
    pub fn last(&self) -> &ProgressMessage {
        &self.last
    }

    /// Get the latest cached blockhash without re-polling
    pub fn blockhash(&self) -> Hash {
        Hash::new_from_array(self.last.latest_blockhash)
    }
}
