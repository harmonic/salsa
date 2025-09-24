use solana_perf::packet::PacketBatch;

#[derive(Clone, Debug)]
pub struct PacketBundle {
    pub batch: PacketBatch,
    pub slot: u64,
}
