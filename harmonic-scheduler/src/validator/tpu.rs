//! TPU packet ingestion into the scheduler vote/nonvote queues

use crate::consts::BATCH_SIZE;
use crate::ipc::shmem::{Free, Slice, allocate};
use crate::ipc::tpu_to_pack;
use crate::ipc::tpu_to_pack::Packet;
use crate::state::tpu_exit;
use agave_scheduler_bindings::{SharableTransactionRegion, TpuToPackMessage};
use bytes::Bytes;
use log::info;
use rdtsc::Instant;
use rtrb::chunks::ChunkError;
use rts_alloc::Allocator;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast;

pub fn run(
    mut tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    allocator: Allocator,
    packet_rx: &mut rtrb::Consumer<Bytes>,
    mut vote_tx: rtrb::Producer<SharableTransactionRegion>,
    mut nonvote_tx: rtrb::Producer<SharableTransactionRegion>,
    backrun_tx: broadcast::Sender<Bytes>,
    is_leader: Arc<AtomicBool>,
) {
    let mut stats = Stats::new();
    while !tpu_exit() {
        allocator.clean_remote_free_lists();
        stats.log();

        // service tpu_to_pack channel
        for packet in tpu_to_pack::iter(&mut tpu_to_pack).take(BATCH_SIZE) {
            match packet {
                Packet::Vote(tx) => {
                    if vote_tx.push(tx).is_err() {
                        tx.free(&allocator);
                        stats.dropped_votes += 1;
                    }
                }
                Packet::Nonvote(tx) => {
                    // While leader, mirror the validator's own incoming non-vote
                    // transactions to the connected strategy server.
                    if is_leader.load(Ordering::Relaxed) && backrun_tx.receiver_count() != 0 {
                        let _ = backrun_tx.send(Bytes::copy_from_slice(tx.slice(&allocator)));
                    }
                    if nonvote_tx.push(tx).is_err() {
                        tx.free(&allocator);
                        stats.dropped_nonvotes += 1;
                    }
                }
            };
        }

        // service remote_tpu channel
        let (read, n) = match packet_rx.read_chunk(BATCH_SIZE) {
            Ok(chunk) => (chunk, BATCH_SIZE),
            Err(ChunkError::TooFewSlots(n)) if n != 0 => (
                packet_rx
                    .read_chunk(n)
                    .expect("n slots should be available"),
                n,
            ),
            Err(_) => continue,
        };
        let Ok(write) = nonvote_tx.write_chunk_uninit(n) else {
            stats.dropped_nonvotes += n;
            read.commit_all();
            continue;
        };
        let stream = is_leader.load(Ordering::Relaxed) && backrun_tx.receiver_count() != 0;
        write.fill_from_iter(read.into_iter().map(|data| {
            // Relayer-forwarded transactions are still the validator's own
            // ingress; mirror them to the strategy while leader.
            if stream {
                let _ = backrun_tx.send(data.clone());
            }
            allocate(&data, &allocator)
        }));
    }
}

struct Stats {
    timer: Instant,
    pub dropped_votes: usize,
    pub dropped_nonvotes: usize,
}

impl Stats {
    pub fn new() -> Self {
        Stats {
            timer: Instant::now(),
            dropped_votes: 0,
            dropped_nonvotes: 0,
        }
    }

    pub fn log(&mut self) {
        if self.timer.elapsed_ms() > 1000 {
            if self.dropped_votes | self.dropped_nonvotes != 0 {
                info!(
                    "dropped_votes={} dropped_nonvotes={}",
                    self.dropped_votes, self.dropped_nonvotes
                );
                self.dropped_votes = 0;
                self.dropped_nonvotes = 0;
            }
            self.timer = rdtsc::Instant::now();
        }
    }
}
