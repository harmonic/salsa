use {
    super::{consumer::Consumer, latest_validator_vote_packet::VoteSource},
    crate::banking_stage::transaction_scheduler::transaction_state_container::{
        RuntimeTransactionView, SharedBytes,
    },
    agave_feature_set as feature_set,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    lru::LruCache,
    solana_accounts_db::account_locks::validate_account_locks,
    solana_clock::MAX_PROCESSING_AGE,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_signature::Signature,
    solana_svm::{
        account_loader::TransactionCheckResult, transaction_error_metrics::TransactionErrorMetrics,
    },
    solana_svm_transaction::{svm_message::SVMMessage, svm_transaction::SVMTransaction},
    solana_transaction::sanitized::MessageHash,
    solana_transaction_error::TransactionError,
    std::collections::{HashSet, VecDeque},
};

/// Maximum number of votes a single receive call will accept
const MAX_NUM_VOTES_RECEIVE: usize = 10_000;

/// High capacity LRU cache size for vote storage (400k entries)
const VOTE_STORAGE_CAPACITY: usize = 400_000;

/// Cap entries in the blockhash-not-found defer deque (half of main vote LRU capacity).
const MAX_BNF_DEQUE_CAPACITY: usize = VOTE_STORAGE_CAPACITY / 2;

/// Max BNF votes we try to move to the main buffer per `flush_bnf_on_new_slot` call (bounded work).
/// If backlog remains, we leave `last_bnf_slot` unchanged and continue on subsequent calls in the
/// same slot; when the defer queue empties (or was already empty), `last_bnf_slot` is set to the
/// current bank slot.
const MAX_BNF_FLUSH_PER_NEW_SLOT: usize = 10_000;

#[derive(Default, Debug)]
pub(crate) struct VoteBatchInsertionMetrics {
    pub(crate) num_dropped_gossip: usize,
    pub(crate) num_dropped_tpu: usize,
    /// Votes moved from the BNF defer queue into the main buffer after the slot advanced.
    pub(crate) num_recovered_from_bnf: usize,
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
    /// Votes whose recent blockhash is not yet in this node's hash queue (e.g. peer advanced first).
    bnf_deque: VecDeque<RuntimeTransactionView>,
    /// Slot watermark for BNF retry (starts at `0`). Deferred inserts set this to `bank.slot()`;
    /// we flush when `bank.slot()` is strictly greater. When `bnf_deque` is empty we advance this
    /// to the current slot.
    last_bnf_slot: u64,
    deprecate_legacy_vote_ixs: bool,
    /// Cached reserved account keys for vote resolution
    reserved_account_keys: HashSet<solana_pubkey::Pubkey>,
}

enum InsertResolvedResult {
    InsertMain(RuntimeTransactionView),
    DeferredBnf(RuntimeTransactionView),
    Dropped,
}

impl VoteStorage {
    pub fn new(bank: &Bank) -> Self {
        Self {
            votes: LruCache::new(VOTE_STORAGE_CAPACITY),
            bnf_deque: VecDeque::new(),
            last_bnf_slot: 0,
            deprecate_legacy_vote_ixs: bank
                .feature_set
                .is_active(&feature_set::deprecate_legacy_vote_ixs::id()),
            reserved_account_keys: bank.get_reserved_account_keys().clone(),
        }
    }

    /// True when no votes are buffered anywhere (LRU **or** BNF defer queue).
    /// Used by the worker run loop to decide whether to skip a tick entirely; it must
    /// stay `false` while BNF entries exist so we still drive `flush_bnf_on_new_slot`
    /// from `process_buffered_packets`.
    pub fn is_empty(&self) -> bool {
        self.votes.is_empty() && self.bnf_deque.is_empty()
    }

    /// True when there is at least one vote ready to pop for execution. Returns `false`
    /// when only BNF-deferred votes remain (those are not poppable until the slot advances
    /// and `flush_bnf_on_new_slot` migrates them into the main LRU).
    pub fn has_processable_votes(&self) -> bool {
        !self.votes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.votes.len() + self.bnf_deque.len()
    }

    pub fn max_receive_size(&self) -> usize {
        MAX_NUM_VOTES_RECEIVE
    }

    /// When the working bank's slot advances past `last_bnf_slot`, retry inserting deferred votes.
    ///
    /// BNF batching: at most [`MAX_BNF_FLUSH_PER_NEW_SLOT`] dequeue attempts per call so we do not
    /// spend unbounded time moving votes when the slot boundary fires. If `bnf_deque` still has
    /// entries after that, **do not** move `last_bnf_slot` (caller can invoke again in the same
    /// slot—`bank.slot() > last_slot` still holds). When the deque becomes or stays empty,
    /// advance `last_bnf_slot` to the current bank slot.
    ///
    /// After a vote was deferred once, we only support the one-slot-ahead recovery case: a second
    /// `BlockhashNotFound` on flush **drops** the vote (enforced by the caller below).
    pub(crate) fn flush_bnf_on_new_slot(&mut self, bank: &Bank) -> usize {
        // No backlog: stay aligned with the working bank so we are not stuck on a stale watermark.
        if self.bnf_deque.is_empty() {
            self.last_bnf_slot = bank.slot();
            return 0;
        }

        // Wait until the bank has moved past the slot where votes entered BNF.
        if bank.slot() <= self.last_bnf_slot {
            return 0;
        }

        // Cap work per call; backlog may need several invocations in the same slot.
        let take = self.bnf_deque.len().min(MAX_BNF_FLUSH_PER_NEW_SLOT);
        let drained: Vec<RuntimeTransactionView> = self.bnf_deque.drain(..take).collect();

        let mut error_counters = TransactionErrorMetrics::default();
        let checks = batched_check_transactions(bank, &drained, &mut error_counters);

        let mut recovered = 0usize;
        for (vote, check) in drained.into_iter().zip(checks.iter()) {
            match classify_post_check(bank, vote, check, &mut error_counters) {
                // Successfully held onto fast vote!
                InsertResolvedResult::InsertMain(vote) => {
                    let sig = *vote.signature();
                    self.votes.push(sig, vote);
                    recovered += 1;
                }
                // One-slot-ahead policy: a second BNF means the vote is not just timing,
                // so drop it instead of re-queueing.
                InsertResolvedResult::DeferredBnf(_) | InsertResolvedResult::Dropped => {}
            }
        }

        // If backlog remains, keep `last_bnf_slot` so callers can flush another batch in the same
        // slot (still bank.slot() > last_bnf_slot). If we drained everything, advance the watermark.
        if self.bnf_deque.is_empty() {
            self.last_bnf_slot = bank.slot();
        }
        recovered
    }

    #[allow(unused_variables)]
    pub(crate) fn insert_batch(
        &mut self,
        bank: &Bank,
        vote_source: VoteSource,
        votes: impl Iterator<Item = SanitizedTransactionView<SharedBytes>>,
    ) -> VoteBatchInsertionMetrics {
        let mut metrics = VoteBatchInsertionMetrics::default();
        metrics.num_recovered_from_bnf += self.flush_bnf_on_new_slot(bank);

        let resolved: Vec<RuntimeTransactionView> =
            votes.filter_map(|v| self.try_resolve_vote(v)).collect();
        if resolved.is_empty() {
            return metrics;
        }

        // One bank check per receive batch: amortizes the blockhash-queue and status-cache locks
        // across all incoming votes.
        let mut error_counters = TransactionErrorMetrics::default();
        let checks = batched_check_transactions(bank, &resolved, &mut error_counters);

        for (vote, check) in resolved.into_iter().zip(checks.iter()) {
            match classify_post_check(bank, vote, check, &mut error_counters) {
                // Vote passed `check_transactions` and `validate_vote_for_processing`:
                // ready to be popped and executed against the working bank.
                InsertResolvedResult::InsertMain(vote) => {
                    let sig = *vote.signature();
                    self.votes.push(sig, vote);
                }
                // `BlockhashNotFound` on first sight: the peer is likely one slot ahead of us.
                // Park the vote in the BNF deque so a later `flush_bnf_on_new_slot` can recover it
                // once our bank advances. Drop (and count) only when the deque is full, to bound
                // memory and DoS exposure.
                InsertResolvedResult::DeferredBnf(v) => {
                    if self.bnf_deque.len() < MAX_BNF_DEQUE_CAPACITY {
                        self.last_bnf_slot = bank.slot();
                        self.bnf_deque.push_back(v);
                    } else {
                        match vote_source {
                            VoteSource::Gossip => metrics.num_dropped_gossip += 1,
                            VoteSource::Tpu => metrics.num_dropped_tpu += 1,
                        }
                    }
                }
                // Non-recoverable failure (bad fee payer, lock violation, status-cache hit, etc.).
                // Drop without polluting the LRU; bump the per-source dropped counter.
                InsertResolvedResult::Dropped => match vote_source {
                    VoteSource::Gossip => metrics.num_dropped_gossip += 1,
                    VoteSource::Tpu => metrics.num_dropped_tpu += 1,
                },
            }
        }

        metrics
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
        self.bnf_deque.clear();
        self.last_bnf_slot = 0;
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

/// Run `Bank::check_transactions` once for the whole batch, with `Ok(())` lock results.
fn batched_check_transactions(
    bank: &Bank,
    txs: &[RuntimeTransactionView],
    error_counters: &mut TransactionErrorMetrics,
) -> Vec<TransactionCheckResult> {
    let lock_results = vec![Ok(()); txs.len()];
    bank.check_transactions(txs, &lock_results, MAX_PROCESSING_AGE, error_counters)
}

/// Classify a vote whose `check_transactions` result is already known.
/// Callers decide what to do with `DeferredBnf`: `insert_batch` puts it on the BNF deque,
/// `flush_bnf_on_new_slot` drops it (one-slot-ahead policy).
fn classify_post_check(
    bank: &Bank,
    resolved: RuntimeTransactionView,
    check: &TransactionCheckResult,
    error_counters: &mut TransactionErrorMetrics,
) -> InsertResolvedResult {
    match check.as_ref().err() {
        Some(TransactionError::BlockhashNotFound) => InsertResolvedResult::DeferredBnf(resolved),
        Some(_) => InsertResolvedResult::Dropped,
        None => {
            if validate_vote_for_processing(bank, &resolved, error_counters) {
                InsertResolvedResult::InsertMain(resolved)
            } else {
                InsertResolvedResult::Dropped
            }
        }
    }
}

/// Validate a pre-resolved vote transaction against the current bank (fees, locks).
/// Blockhash recency is handled separately via `Bank::check_transactions` before this.
pub(crate) fn validate_vote_for_processing(
    bank: &Bank,
    vote: &RuntimeTransactionView,
    error_counters: &mut TransactionErrorMetrics,
) -> bool {
    // Check the number of locks and whether there are duplicates
    if validate_account_locks(
        vote.account_keys(),
        bank.get_transaction_account_lock_limit(),
    )
    .is_err()
    {
        return false;
    }

    // Loads fee payer account, validates balance covers fee + rent-exempt minimum.
    // Also catches AccountNotFound (zero-balance fee payer DOS).
    if Consumer::check_fee_payer_unlocked(bank, vote, error_counters).is_err() {
        return false;
    }

    true
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
        vote_storage.insert_batch(&bank, VoteSource::Tpu, std::iter::once(vote));
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
        vote_storage.insert_batch(&bank, VoteSource::Tpu, std::iter::once(vote));
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

        vote_storage.insert_batch(&bank, VoteSource::Tpu, vec![vote_a, vote_b].into_iter());
        assert_eq!(vote_storage.len(), 2);

        vote_storage.clear();
        assert!(vote_storage.is_empty());
    }

    #[test]
    fn test_bnf_defer_then_recover_after_slot_advance() {
        // Genesis funds `keypair.node_keypair` so the recovered vote also passes fee-payer
        // validation (`validate_vote_for_processing`) when it lands in the main LRU.
        let keypair = ValidatorVoteKeypairs::new_rand();
        let solana_runtime::genesis_utils::GenesisConfigInfo { genesis_config, .. } =
            solana_runtime::genesis_utils::create_genesis_config_with_vote_accounts(
                1_000_000_000,
                &[&keypair],
                vec![100],
            );
        let (bank_a, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        // Simulate "peer is one slot ahead": vote references a hash bank_a does not have.
        let future_blockhash = Hash::new_unique();
        let vote_tx = new_tower_sync_transaction(
            TowerSync::from(vec![(1, 1)]),
            future_blockhash,
            &keypair.node_keypair,
            &keypair.vote_keypair,
            &keypair.vote_keypair,
            None,
        );
        let mut packet = BytesPacket::from_data(None, vote_tx).unwrap();
        packet
            .meta_mut()
            .flags
            .set(PacketFlags::SIMPLE_VOTE_TX, true);
        let view = to_sanitized_view(packet);

        let mut storage = VoteStorage::new(&bank_a);
        storage.insert_batch(&bank_a, VoteSource::Tpu, std::iter::once(view));

        // Vote was deferred: BNF deque holds it; main LRU stays clean.
        assert_eq!(storage.votes.len(), 0);
        assert_eq!(storage.bnf_deque.len(), 1);
        assert!(
            storage.pop().is_none(),
            "BNF entries must not be poppable until recovered"
        );

        // Catch up: child bank with the previously-unknown hash now in its queue.
        let bank_b = Arc::new(solana_runtime::bank::Bank::new_from_parent(
            bank_a,
            &solana_pubkey::Pubkey::new_unique(),
            1,
        ));
        bank_b.register_recent_blockhash_for_test(&future_blockhash, None);

        // Migration moves the vote into the main LRU.
        let recovered = storage.flush_bnf_on_new_slot(&bank_b);
        assert_eq!(recovered, 1);
        assert_eq!(storage.votes.len(), 1);
        assert_eq!(storage.bnf_deque.len(), 0);
        assert!(storage.pop().is_some());
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
            bnf_deque: VecDeque::new(),
            last_bnf_slot: 0,
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
        vote_storage.insert_batch(
            &bank,
            VoteSource::Tpu,
            vec![vote_a, vote_b, vote_c].into_iter(),
        );

        // Should only have 2 votes (oldest evicted)
        assert_eq!(vote_storage.len(), 2);
    }
}
