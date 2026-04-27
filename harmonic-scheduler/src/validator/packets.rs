//! Ingest transactions from tpu_to_pack and remote_tpu; route into
//! vote/nonvote SPSC channels for the scheduler

use crate::ipc::shmem::{Free, allocate_transaction};
use crate::ipc::tpu_to_pack;
use crate::ipc::tpu_to_pack::Packet;
use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionRegion, TpuToPackMessage,
};
use log::warn;
use rts_alloc::Allocator;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct Packets<'a> {
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    allocator: Allocator,
    packet_rx: &'a mut rtrb::Consumer<Vec<u8>>,
    vote_tx: rtrb::Producer<SharableTransactionRegion>,
    nonvote_tx: rtrb::Producer<SharableTransactionRegion>,
    exit: Arc<AtomicBool>,
}

impl<'a> Packets<'a> {
    pub fn new(
        tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
        allocator: Allocator,
        packet_rx: &'a mut rtrb::Consumer<Vec<u8>>,
        vote_tx: rtrb::Producer<SharableTransactionRegion>,
        nonvote_tx: rtrb::Producer<SharableTransactionRegion>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        Self {
            tpu_to_pack,
            allocator,
            packet_rx,
            vote_tx,
            nonvote_tx,
            exit,
        }
    }

    pub fn run(&mut self) {
        while !self.exit.load(Ordering::Relaxed) {
            // Reclaim SHM remote-freed by scheduler/Agave
            self.allocator.clean_remote_free_lists();
            self.handle_tpu_packets();
            self.handle_remote_nonvotes();
        }
    }

    fn handle_tpu_packets(&mut self) {
        for packet in tpu_to_pack::iter(&mut self.tpu_to_pack).take(MAX_TRANSACTIONS_PER_MESSAGE) {
            let (transaction, producer, is_vote) = match packet {
                Packet::Vote(r) => (r, &mut self.vote_tx, true),
                Packet::Nonvote(r) => (r, &mut self.nonvote_tx, false),
            };
            if producer.push(transaction).is_err() {
                warn!(
                    "{} channel full, dropping packet",
                    if is_vote { "vote" } else { "nonvote" }
                );
                transaction.free(&self.allocator);
            }
        }
    }

    fn handle_remote_nonvotes(&mut self) {
        for _ in 0..MAX_TRANSACTIONS_PER_MESSAGE {
            let Ok(data) = self.packet_rx.pop() else {
                break;
            };
            let Some(region) = allocate_transaction(&data, &self.allocator) else {
                warn!("malformed transaction: {} bytes", data.len());
                continue;
            };
            if self.nonvote_tx.push(region).is_err() {
                warn!("nonvote channel full, dropping packet");
                region.free(&self.allocator);
            }
        }
    }
}
