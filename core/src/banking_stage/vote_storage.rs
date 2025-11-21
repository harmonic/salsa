use {
    super::{
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        latest_validator_vote_packet::VoteSource,
    },
    agave_feature_set as feature_set,
    lru::LruCache,
    solana_clock::MAX_PROCESSING_AGE,
    solana_runtime::bank::Bank,
    solana_signature::Signature,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::sync::Arc,
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
    votes: LruCache<Signature, Arc<ImmutableDeserializedPacket>>,
    deprecate_legacy_vote_ixs: bool,
}

impl VoteStorage {
    pub fn new(bank: &Bank) -> Self {
        Self {
            votes: LruCache::new(400_000),
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
        for vote in deserialized_packets.map(Arc::new) {
            self.votes.push(vote.signature().clone(), vote);
        }
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
        for vote in packets {
            self.votes.push(vote.signature().clone(), vote);
        }
    }

    pub fn clear(&mut self) {
        self.votes.clear();
    }

    pub fn pop(&mut self) -> Option<Arc<ImmutableDeserializedPacket>> {
        self.votes.pop_lru().map(|(_sig, vote)| vote)
    }

    pub fn cache_epoch_boundary_info(&mut self, bank: &Bank) {
        self.deprecate_legacy_vote_ixs = bank
            .feature_set
            .is_active(&feature_set::deprecate_legacy_vote_ixs::id());
    }

    pub fn cavey_clean(&mut self, bank: &Bank) {
        let lock_result = [Ok(()); 1];
        let mut to_remove = vec![];
        let ref mut error_counters = TransactionErrorMetrics::default();
        for (sig, vote) in self.votes.iter().rev() {
            let Some((sanitized_txn, _slot)) =
                vote.build_sanitized_transaction(true, bank, bank.get_reserved_account_keys())
            else {
                continue;
            };
            let check = bank.check_transactions(
                core::array::from_ref(&sanitized_txn),
                &lock_result,
                MAX_PROCESSING_AGE,
                error_counters,
            );
            if check[0].is_err() {
                to_remove.push(sig.clone());
            }
        }

        for ref sig in to_remove {
            let _ = self.votes.pop(sig);
        }
    }
}
