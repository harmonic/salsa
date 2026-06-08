//! Validator-side TPU packet feed

use agave_scheduler_bindings::{SharableTransactionRegion, TpuToPackMessage, tpu_message_flags};

/// A packet pulled from the TPU -> Pack ring
/// Caller owns the returned allocations
pub enum Packet {
    Vote(SharableTransactionRegion),
    Nonvote(SharableTransactionRegion),
}

/// Iterator over pending packets. Finalizes the ring on drop
pub fn iter(
    consumer: &mut shaq::spsc::Consumer<TpuToPackMessage>,
) -> impl Iterator<Item = Packet> + '_ {
    consumer.sync();
    Iter { consumer }
}

struct Iter<'a> {
    consumer: &'a mut shaq::spsc::Consumer<TpuToPackMessage>,
}

impl Iterator for Iter<'_> {
    type Item = Packet;
    fn next(&mut self) -> Option<Self::Item> {
        let msg = self.consumer.try_read()?;
        if msg.flags & tpu_message_flags::IS_SIMPLE_VOTE != 0 {
            Some(Packet::Vote(msg.transaction))
        } else {
            Some(Packet::Nonvote(msg.transaction))
        }
    }
}

impl Drop for Iter<'_> {
    fn drop(&mut self) {
        self.consumer.finalize();
    }
}
