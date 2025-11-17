use {
    super::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        latest_validator_vote_packet::VoteSource,
    },
    agave_feature_set as feature_set,
    solana_runtime::bank::Bank,
    std::{collections::VecDeque, sync::Arc},
};

/// Maximum number of votes a single receive call will accept
const MAX_NUM_VOTES_RECEIVE: usize = 10_000;

#[derive(Default, Debug)]
pub(crate) struct VoteBatchInsertionMetrics {
    pub(crate) num_dropped_gossip: usize,
    pub(crate) num_dropped_tpu: usize,
}

impl VoteBatchInsertionMetrics {
    pub fn total_dropped_packets(&self) -> usize {
        self.num_dropped_gossip + self.num_dropped_tpu
    }

    pub fn dropped_gossip_packets(&self) -> usize {
        self.num_dropped_gossip
    }

    pub fn dropped_tpu_packets(&self) -> usize {
        self.num_dropped_tpu
    }
}

#[derive(Debug)]
pub struct VoteStorage {
    votes: VecDeque<Arc<ImmutableDeserializedPacket>>,
    deprecate_legacy_vote_ixs: bool,
}

impl VoteStorage {
    pub fn new(bank: &Bank) -> Self {
        Self {
            votes: VecDeque::new(),
            deprecate_legacy_vote_ixs: bank
                .feature_set
                .is_active(&feature_set::deprecate_legacy_vote_ixs::id()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.votes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.votes.len()
    }

    pub fn max_receive_size(&self) -> usize {
        MAX_NUM_VOTES_RECEIVE
    }

    #[allow(unused_variables)]
    pub(crate) fn insert_batch(
        &mut self,
        vote_source: VoteSource,
        deserialized_packets: impl Iterator<Item = ImmutableDeserializedPacket>,
    ) -> VoteBatchInsertionMetrics {
        self.votes.extend(deserialized_packets.map(Arc::new));
        VoteBatchInsertionMetrics {
            num_dropped_gossip: 0,
            num_dropped_tpu: 0,
        }
    }

    // Re-insert re-tryable packets.
    pub(crate) fn reinsert_packets(
        &mut self,
        packets: impl Iterator<Item = Arc<ImmutableDeserializedPacket>>,
    ) {
        self.votes.extend(packets);
    }

    pub fn clear(&mut self) {
        self.votes.clear();
    }

    pub fn pop(&mut self) -> Option<Arc<ImmutableDeserializedPacket>> {
        self.votes.pop_front()
    }

    pub fn cache_epoch_boundary_info(&mut self, bank: &Bank) {
        self.deprecate_legacy_vote_ixs = bank
            .feature_set
            .is_active(&feature_set::deprecate_legacy_vote_ixs::id());
    }
}
