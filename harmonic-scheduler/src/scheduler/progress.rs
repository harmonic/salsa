//! Validator progress feed

use {
    agave_scheduler_bindings::{NOT_LEADER, ProgressMessage},
    anyhow::{Result, bail},
    std::{
        num::NonZeroUsize,
        time::{Duration, Instant},
    },
};

/// Timeout between progress messages before considering the session ended
pub const PROGRESS_MESSAGE_TIMEOUT: Duration = Duration::from_secs(2);

pub struct Progress {
    consumer: shaq::spsc::Consumer<ProgressMessage>,
    last: ProgressMessage,
    last_received: Instant,
}

impl Progress {
    pub fn new(consumer: shaq::spsc::Consumer<ProgressMessage>) -> Self {
        Self {
            consumer,
            last: ProgressMessage {
                leader_state: NOT_LEADER,
                current_slot_progress: 0,
                epoch: 0,
                current_slot: 0,
                next_leader_slot: 0,
                leader_range_end: 0,
                remaining_cost_units: 0,
                remaining_allocated_accounts_data_size: 0,
                latest_blockhash: [0; 32],
                target_bank_time_ms: 0,
            },
            last_received: Instant::now(),
        }
    }

    /// Drain pending messages and return the latest; Err once the feed is silent too long
    pub fn poll(&mut self) -> Result<&ProgressMessage> {
        let latest = self
            .consumer
            .try_reserve_read_batch(NonZeroUsize::MAX)
            .and_then(|batch| batch.into_iter().last());
        if let Some(message) = latest {
            self.last = message;
            self.last_received = Instant::now();
        } else if self.last_received.elapsed() > PROGRESS_MESSAGE_TIMEOUT {
            bail!("validator progress feed stale");
        }
        Ok(&self.last)
    }

    /// The latest message, without polling
    pub fn last(&self) -> &ProgressMessage {
        &self.last
    }
}
