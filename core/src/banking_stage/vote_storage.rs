use {
    super::latest_validator_vote_packet::VoteSource,
    crate::banking_stage::transaction_scheduler::transaction_state_container::{
        RuntimeTransactionView, SharedBytes,
    },
    agave_feature_set as feature_set,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    lru::LruCache,
    solana_clock::MAX_PROCESSING_AGE,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_signature::Signature,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_transaction::SVMTransaction,
    solana_transaction::sanitized::MessageHash,
    std::collections::HashSet,
};

/// Maximum number of votes a single receive call will accept
const MAX_NUM_VOTES_RECEIVE: usize = 10_000;

/// High capacity LRU cache size for vote storage (400k entries)
const VOTE_STORAGE_CAPACITY: usize = 400_000;

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
    /// LRU cache storing resolved vote transactions ready for processing
    votes: LruCache<Signature, RuntimeTransactionView>,
    deprecate_legacy_vote_ixs: bool,
    /// Cached reserved account keys for vote resolution
    reserved_account_keys: HashSet<solana_pubkey::Pubkey>,
}

impl VoteStorage {
    pub fn new(bank: &Bank) -> Self {
        Self {
            votes: LruCache::new(VOTE_STORAGE_CAPACITY),
            deprecate_legacy_vote_ixs: bank
                .feature_set
                .is_active(&feature_set::deprecate_legacy_vote_ixs::id()),
            reserved_account_keys: bank.get_reserved_account_keys().clone(),
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
        votes: impl Iterator<Item = SanitizedTransactionView<SharedBytes>>,
    ) -> VoteBatchInsertionMetrics {
        for vote in votes {
            if let Some(resolved) = self.try_resolve_vote(vote) {
                let sig = *resolved.signature();
                self.votes.push(sig, resolved);
            }
        }
        VoteBatchInsertionMetrics {
            num_dropped_gossip: 0,
            num_dropped_tpu: 0,
        }
    }

    // Re-insert re-tryable votes.
    pub(crate) fn reinsert_votes(&mut self, votes: impl Iterator<Item = RuntimeTransactionView>) {
        for vote in votes {
            let sig = *vote.signature();
            self.votes.push(sig, vote);
        }
    }

    pub fn clear(&mut self) {
        self.votes.clear();
    }

    /// Pop the least recently used vote, ready for processing.
    pub fn pop(&mut self) -> Option<RuntimeTransactionView> {
        self.votes.pop_lru().map(|(_sig, vote)| vote)
    }

    pub fn cache_epoch_boundary_info(&mut self, bank: &Bank) {
        self.deprecate_legacy_vote_ixs = bank
            .feature_set
            .is_active(&feature_set::deprecate_legacy_vote_ixs::id());
        self.reserved_account_keys = bank.get_reserved_account_keys().clone();
    }

    /// Remove votes that can no longer be processed.
    pub fn cavey_clean(&mut self, bank: &Bank) {
        let lock_result = [Ok(()); 1];
        let error_counters = &mut TransactionErrorMetrics::default();
        let mut to_remove = vec![];

        for (sig, vote) in self.votes.iter().rev() {
            let check = bank.check_transactions(
                core::array::from_ref(vote),
                &lock_result,
                MAX_PROCESSING_AGE,
                error_counters,
            );
            if check[0].is_err() {
                to_remove.push(*sig);
            }
        }

        for sig in &to_remove {
            let _ = self.votes.pop(sig);
        }
    }

    /// Try to resolve a vote packet into a RuntimeTransactionView.
    fn try_resolve_vote(
        &self,
        packet: SanitizedTransactionView<SharedBytes>,
    ) -> Option<RuntimeTransactionView> {
        // Build RuntimeTransaction from the view
        let view = RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
            packet,
            MessageHash::Compute,
            None,
        )
        .ok()?;

        // Filter non-vote transactions
        if !view.is_simple_vote_transaction() {
            return None;
        }

        // Resolve the transaction (votes do not have LUTs)
        RuntimeTransactionView::try_from(view, None, &self.reserved_account_keys).ok()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        agave_transaction_view::transaction_view::SanitizedTransactionView,
        solana_clock::UnixTimestamp,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_perf::packet::{BytesPacket, PacketFlags},
        solana_runtime::genesis_utils::ValidatorVoteKeypairs,
        solana_signer::Signer,
        solana_vote::vote_transaction::new_tower_sync_transaction,
        solana_vote_program::vote_state::TowerSync,
        std::sync::Arc,
    };

    pub(crate) fn packet_from_slots(
        slots: Vec<(u64, u32)>,
        keypairs: &ValidatorVoteKeypairs,
        timestamp: Option<UnixTimestamp>,
    ) -> BytesPacket {
        let mut vote = TowerSync::from(slots);
        vote.timestamp = timestamp;
        let vote_tx = new_tower_sync_transaction(
            vote,
            Hash::new_unique(),
            &keypairs.node_keypair,
            &keypairs.vote_keypair,
            &keypairs.vote_keypair,
            None,
        );
        let mut packet = BytesPacket::from_data(None, vote_tx).unwrap();
        packet
            .meta_mut()
            .flags
            .set(PacketFlags::SIMPLE_VOTE_TX, true);

        packet
    }

    fn to_sanitized_view(packet: BytesPacket) -> SanitizedTransactionView<SharedBytes> {
        SanitizedTransactionView::try_new_sanitized(Arc::new(packet.buffer().to_vec()), false)
            .unwrap()
    }

    #[test]
    fn test_insert_and_pop() {
        let keypair_a = ValidatorVoteKeypairs::new_rand();

        let genesis_config = solana_runtime::genesis_utils::create_genesis_config_with_leader(
            100,
            &Keypair::new().pubkey(),
            200,
        )
        .genesis_config;
        let (bank, _bank_forks) =
            solana_runtime::bank::Bank::new_with_bank_forks_for_tests(&genesis_config);

        let mut vote_storage = VoteStorage::new(&bank);
        assert!(vote_storage.is_empty());

        // Insert a vote
        let vote = to_sanitized_view(packet_from_slots(vec![(1, 1)], &keypair_a, None));
        vote_storage.insert_batch(VoteSource::Tpu, std::iter::once(vote));
        assert_eq!(vote_storage.len(), 1);

        // Pop the vote
        let popped = vote_storage.pop();
        assert!(popped.is_some());
        assert!(vote_storage.is_empty());
    }

    #[test]
    fn test_reinsert_votes() {
        let keypair_a = ValidatorVoteKeypairs::new_rand();

        let genesis_config = solana_runtime::genesis_utils::create_genesis_config_with_leader(
            100,
            &Keypair::new().pubkey(),
            200,
        )
        .genesis_config;
        let (bank, _bank_forks) =
            solana_runtime::bank::Bank::new_with_bank_forks_for_tests(&genesis_config);

        let mut vote_storage = VoteStorage::new(&bank);

        // Insert a vote
        let vote = to_sanitized_view(packet_from_slots(vec![(1, 1)], &keypair_a, None));
        vote_storage.insert_batch(VoteSource::Tpu, std::iter::once(vote));
        assert_eq!(vote_storage.len(), 1);

        // Pop and reinsert
        let popped = vote_storage.pop().unwrap();
        assert!(vote_storage.is_empty());

        vote_storage.reinsert_votes(std::iter::once(popped));
        assert_eq!(vote_storage.len(), 1);
    }

    #[test]
    fn test_clear() {
        let keypair_a = ValidatorVoteKeypairs::new_rand();
        let keypair_b = ValidatorVoteKeypairs::new_rand();

        let genesis_config = solana_runtime::genesis_utils::create_genesis_config_with_leader(
            100,
            &Keypair::new().pubkey(),
            200,
        )
        .genesis_config;
        let (bank, _bank_forks) =
            solana_runtime::bank::Bank::new_with_bank_forks_for_tests(&genesis_config);

        let mut vote_storage = VoteStorage::new(&bank);

        let vote_a = to_sanitized_view(packet_from_slots(vec![(1, 1)], &keypair_a, None));
        let vote_b = to_sanitized_view(packet_from_slots(vec![(2, 1)], &keypair_b, None));

        vote_storage.insert_batch(VoteSource::Tpu, vec![vote_a, vote_b].into_iter());
        assert_eq!(vote_storage.len(), 2);

        vote_storage.clear();
        assert!(vote_storage.is_empty());
    }

    #[test]
    fn test_lru_eviction() {
        let genesis_config = solana_runtime::genesis_utils::create_genesis_config_with_leader(
            100,
            &Keypair::new().pubkey(),
            200,
        )
        .genesis_config;
        let (bank, _bank_forks) =
            solana_runtime::bank::Bank::new_with_bank_forks_for_tests(&genesis_config);

        // Create a small LRU for testing eviction
        let mut vote_storage = VoteStorage {
            votes: LruCache::new(2),
            deprecate_legacy_vote_ixs: true,
            reserved_account_keys: bank.get_reserved_account_keys().clone(),
        };

        let keypair_a = ValidatorVoteKeypairs::new_rand();
        let keypair_b = ValidatorVoteKeypairs::new_rand();
        let keypair_c = ValidatorVoteKeypairs::new_rand();

        let vote_a = to_sanitized_view(packet_from_slots(vec![(1, 1)], &keypair_a, None));
        let vote_b = to_sanitized_view(packet_from_slots(vec![(2, 1)], &keypair_b, None));
        let vote_c = to_sanitized_view(packet_from_slots(vec![(3, 1)], &keypair_c, None));

        // Insert 3 votes into capacity-2 LRU
        vote_storage.insert_batch(VoteSource::Tpu, vec![vote_a, vote_b, vote_c].into_iter());

        // Should only have 2 votes (oldest evicted)
        assert_eq!(vote_storage.len(), 2);
    }
}
