//! The `block_stage` processes blocks, which are transactions intended for a specific slot.
//! Unlike bundles, blocks have no transaction limit and the uuid field contains the intended slot.

mod block_consumer;
mod devin_scheduler;
mod harmonic_block;
mod timer;

pub use block_consumer::BlockConsumer;
pub use devin_scheduler::DevinScheduler;
pub use harmonic_block::HarmonicBlock;
use solana_signer::Signer;
pub use timer::Timer;

use crate::banking_stage::decision_maker::{BufferedPacketsDecision, DecisionMaker};

use {
    crate::{
        banking_stage::{
            committer::Committer,
            scheduler_messages::MaxAge,
            transaction_scheduler::receive_and_buffer::{
                calculate_max_age, translate_to_runtime_view,
            },
        },
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        scheduler_synchronization,
        tip_manager::TipManager,
    },
    agave_transaction_view::resolved_transaction_view::ResolvedTransactionView,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    log::{info, warn},
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_poh::transaction_recorder::TransactionRecorder,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct BlockStage {
    block_thread: JoinHandle<()>,
}

impl BlockStage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<BankForks>>,
        transaction_recorder: TransactionRecorder,
        block_receiver: Receiver<HarmonicBlock>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        tip_manager: TipManager,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
    ) -> Self {
        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache.clone(),
        );

        let consumer =
            BlockConsumer::new(committer, transaction_recorder, log_messages_bytes_limit);

        let cluster_info = Arc::clone(&cluster_info);
        let block_thread = Builder::new()
            .name("solBlockStgTx".to_string())
            .spawn(move || {
                Self::process_loop(
                    bank_forks,
                    block_receiver,
                    consumer,
                    exit,
                    cluster_info,
                    tip_manager,
                    block_builder_fee_info,
                );
            })
            .unwrap();

        Self { block_thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.block_thread.join()
    }

    /// Max crank transactions (init tip distribution + change tip receiver/block builder)
    const MAX_CRANK_TXNS: usize = 3;
    /// Max serialized transaction size
    const MAX_TXN_SIZE: usize = 1232;

    /// Check if the current bank is past the delegation period (last 1/8 of slot).
    fn past_delegation_period(bank: &Bank) -> bool {
        let current_tick_height = bank.tick_height();
        let max_tick_height = bank.max_tick_height();
        let bank_ticks_per_slot = bank.ticks_per_slot();
        let start_tick = max_tick_height - bank_ticks_per_slot;
        let ticks_into_slot = current_tick_height.saturating_sub(start_tick);
        let delegation_period_length = bank_ticks_per_slot * 7 / 8;
        ticks_into_slot >= delegation_period_length
    }

    /// Process a single streamed message: translate packets, prepend crank if needed, and execute.
    /// Returns true on success, false on failure.
    #[allow(clippy::too_many_arguments)]
    fn process_single_message(
        consumer: &mut BlockConsumer,
        block: &HarmonicBlock,
        working_bank: &Bank,
        root_bank: &Bank,
        tip_manager: &TipManager,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        keypair: &Keypair,
        crank_buffer: &mut [[u8; Self::MAX_TXN_SIZE]; Self::MAX_CRANK_TXNS],
        crank_lens: &mut [usize; Self::MAX_CRANK_TXNS],
    ) -> bool {
        let (mut transactions, mut max_ages) = Self::translate_packets_to_transactions(
            block.transactions(),
            root_bank,
            working_bank,
        );

        Self::maybe_prepend_crank(
            working_bank,
            root_bank,
            tip_manager,
            block_builder_fee_info,
            keypair,
            crank_buffer,
            crank_lens,
            &mut transactions,
            &mut max_ages,
        );

        let output = consumer.process_and_record_block_transactions(
            working_bank,
            &transactions,
            &max_ages,
            block.intended_slot(),
        );

        output
            .execute_and_commit_transactions_output
            .commit_transactions_result
            .is_ok()
    }

    #[allow(clippy::too_many_arguments)]
    fn process_loop(
        bank_forks: Arc<RwLock<BankForks>>,
        block_receiver: Receiver<HarmonicBlock>,
        mut consumer: BlockConsumer,
        exit: Arc<AtomicBool>,
        cluster_info: Arc<ClusterInfo>,
        tip_manager: TipManager,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
    ) {
        // Reusable buffer for crank transaction bytes
        let mut crank_buffer = [[0u8; Self::MAX_TXN_SIZE]; Self::MAX_CRANK_TXNS];
        let mut crank_lens = [0usize; Self::MAX_CRANK_TXNS];

        while !exit.load(Ordering::Relaxed) {
            match block_receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(block) => {
                    info!("Received block from Block Engine");
                    let (root_bank, working_bank) = {
                        let bank_forks_guard = bank_forks.read().unwrap();
                        (
                            bank_forks_guard.root_bank(),
                            bank_forks_guard.working_bank(),
                        )
                    };

                    let current_slot = working_bank.slot();

                    // Check if this block is for the correct slot
                    if block.intended_slot() != current_slot {
                        info!(
                            "Block intended for slot {} but current slot is {}, dropping",
                            block.intended_slot(), current_slot
                        );
                        continue;
                    }

                    // Sanity check we are leader
                    let keypair = cluster_info.keypair();
                    if !keypair.pubkey().eq(working_bank.collector_id()) {
                        warn!("received block for which we are not leader");
                        continue;
                    }

                    // Sanity check block is not empty.
                    // Note this is intentionally done after checking this was intended for us
                    if block.transactions().is_empty() {
                        warn!(
                            "received empty block intended for our slot {}",
                            block.intended_slot()
                        );
                    }

                    // Attempt to claim the slot for block stage
                    let decision = BufferedPacketsDecision::Consume(working_bank);
                    if let BufferedPacketsDecision::Consume(working_bank) =
                        DecisionMaker::maybe_consume::<false>(decision)
                    {
                        // Claimed! Process blocks for this slot in a single loop.
                        let mut current_block = block;
                        let mut failed = false;

                        loop {
                            scheduler_synchronization::wait_for_votes_to_finish();

                            failed = !Self::process_single_message(
                                &mut consumer,
                                &current_block,
                                &working_bank,
                                &root_bank,
                                &tip_manager,
                                &block_builder_fee_info,
                                &keypair,
                                &mut crank_buffer,
                                &mut crank_lens,
                            );
                            if failed {
                                break;
                            }

                            if Self::past_delegation_period(&working_bank) {
                                break;
                            }

                            match block_receiver.recv_timeout(Duration::from_millis(1)) {
                                Ok(next_block) => {
                                    if next_block.intended_slot() != current_slot {
                                        info!(
                                            "Streaming: block for slot {} during slot {}, stopping stream",
                                            next_block.intended_slot(), current_slot
                                        );
                                        break;
                                    }
                                    current_block = next_block;
                                }
                                Err(RecvTimeoutError::Timeout) => continue,
                                Err(RecvTimeoutError::Disconnected) => break,
                            }
                        }

                        // Always restore vote limit after block execution attempt
                        working_bank.restore_vote_limit();

                        if failed {
                            info!(
                                "Block failed for slot {}, reverting to vanilla",
                                current_slot,
                            );
                            scheduler_synchronization::block_failed(current_slot);
                        } else {
                            scheduler_synchronization::block_execution_finished(current_slot);
                        }
                    } else {
                        // Failed to claim for this slot.
                        info!("block stage failed to claim slot {}", current_slot);
                        continue;
                    };
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Continue loop
                }
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    /// Translate packet bytes to RuntimeTransaction<ResolvedTransactionView> using
    /// zerocopy parsing instead of bincode deserialization.
    fn translate_packets_to_transactions<'a>(
        batch: &'a solana_perf::packet::PacketBatch,
        root_bank: &Bank,
        working_bank: &Bank,
    ) -> (
        Vec<RuntimeTransaction<ResolvedTransactionView<&'a [u8]>>>,
        Vec<MaxAge>,
    ) {
        let enable_static_instruction_limit = root_bank
            .feature_set
            .is_active(&agave_feature_set::static_instruction_limit::ID);
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();

        let mut transactions = Vec::with_capacity(batch.len());
        let mut max_ages = Vec::with_capacity(batch.len());

        for packet in batch.iter() {
            // Skip packets marked for discard or with no data
            if packet.meta().discard() {
                continue;
            }

            let Some(packet_data) = packet.data(..) else {
                continue;
            };

            // Use translate_to_runtime_view for zerocopy parsing
            match translate_to_runtime_view(
                packet_data,
                working_bank,
                root_bank,
                enable_static_instruction_limit,
                transaction_account_lock_limit,
            ) {
                Ok((view, deactivation_slot)) => {
                    let max_age =
                        calculate_max_age(root_bank.epoch(), deactivation_slot, root_bank.slot());
                    transactions.push(view);
                    max_ages.push(max_age);
                }
                Err(_) => {
                    // Skip packets that fail sanitization/translation
                    continue;
                }
            }
        }

        (transactions, max_ages)
    }

    /// Generate crank transactions and prepend to block if any exist.
    /// Uses pre-allocated buffer to avoid allocations.
    #[allow(clippy::too_many_arguments)]
    fn maybe_prepend_crank<'a>(
        working_bank: &Bank,
        root_bank: &Bank,
        tip_manager: &TipManager,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        keypair: &Keypair,
        crank_buffer: &'a mut [[u8; Self::MAX_TXN_SIZE]; Self::MAX_CRANK_TXNS],
        crank_lens: &mut [usize; Self::MAX_CRANK_TXNS],
        transactions: &mut Vec<RuntimeTransaction<ResolvedTransactionView<&'a [u8]>>>,
        max_ages: &mut Vec<MaxAge>,
    ) {
        use solana_runtime_transaction::transaction_with_meta::TransactionWithMeta;

        // Generate crank transactions
        let fee_info = block_builder_fee_info.lock().unwrap();
        let crank_txns =
            match tip_manager.get_tip_programs_crank_bundle(working_bank, keypair, &fee_info) {
                Ok(txns) => txns,
                Err(e) => {
                    warn!("Failed to generate crank transactions: {:?}", e);
                    return;
                }
            };
        drop(fee_info);

        if crank_txns.is_empty() {
            return;
        }

        // Check if payer has enough money for these
        let lamports_per_signature = working_bank.get_lamports_per_signature();
        let crank_signature_fee = (crank_txns.len() as u64) * lamports_per_signature;
        let rent_exempt_amount = working_bank.get_minimum_balance_for_rent_exemption(
            // recall that fee payers cannot carry data
            0,
        );
        let min_requred_balance = rent_exempt_amount + crank_signature_fee;
        let payer_balance = working_bank.get_balance(&keypair.pubkey());
        if payer_balance < min_requred_balance {
            warn!("payer does not have enough SOL to pay for crank transactions; balance {}; required {}", payer_balance, min_requred_balance);
            return;
        }

        // Serialize directly into pre-allocated buffer
        let mut crank_count = 0usize;
        for (i, tx) in crank_txns.iter().take(Self::MAX_CRANK_TXNS).enumerate() {
            let mut cursor = std::io::Cursor::new(&mut crank_buffer[i][..]);
            if bincode::serialize_into(&mut cursor, &tx.to_versioned_transaction()).is_ok() {
                crank_lens[i] = cursor.position() as usize;
                crank_count += 1;
            }
        }

        // Done if there are no cranks to prepend
        if crank_count == 0 {
            return;
        }

        // Convert to views using stack-allocated array
        let enable_static_instruction_limit = root_bank
            .feature_set
            .is_active(&agave_feature_set::static_instruction_limit::ID);
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();

        let mut crank_views: [Option<RuntimeTransaction<ResolvedTransactionView<&[u8]>>>;
            Self::MAX_CRANK_TXNS] = [const { None }; Self::MAX_CRANK_TXNS];
        let mut view_count = 0usize;

        for i in 0..crank_count {
            let data = &crank_buffer[i][..crank_lens[i]];
            if let Ok((view, _)) = translate_to_runtime_view(
                data,
                working_bank,
                root_bank,
                enable_static_instruction_limit,
                transaction_account_lock_limit,
            ) {
                crank_views[view_count] = Some(view);
                view_count += 1;
            }
        }

        // Done if no views were created.
        // Technically this should not happen because the crank txns are all valid...
        if view_count == 0 {
            log::warn!("no views were created from crank transactions");
            return;
        }

        info!(
            "Prepending {} crank transactions to block for slot {}",
            view_count,
            working_bank.slot()
        );

        // Reserve space and shift existing transactions to make room at front
        let crank_max_age = MaxAge {
            sanitized_epoch: working_bank.epoch(),
            alt_invalidation_slot: working_bank.slot(),
        };

        transactions.reserve(view_count);
        max_ages.reserve(view_count);

        // Insert crank transactions at the front (in reverse order to maintain order)
        for i in (0..view_count).rev() {
            if let Some(view) = crank_views[i].take() {
                transactions.insert(0, view);
                max_ages.insert(0, crank_max_age);
            }
        }
    }
}
