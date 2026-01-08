//! The `block_stage` processes blocks, which are transactions intended for a specific slot.
//! Unlike bundles, blocks have no transaction limit and the uuid field contains the intended slot.

mod block_consumer;
mod devin_scheduler;
mod harmonic_block;
mod timer;

pub use block_consumer::BlockConsumer;
pub use devin_scheduler::DevinScheduler;
pub use harmonic_block::HarmonicBlock;
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
        scheduler_synchronization,
    },
    agave_transaction_view::resolved_transaction_view::ResolvedTransactionView,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    log::{info, warn},
    solana_gossip::cluster_info::ClusterInfo,
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
            Arc, RwLock,
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
                Self::process_loop(bank_forks, block_receiver, consumer, exit, cluster_info);
            })
            .unwrap();

        Self { block_thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.block_thread.join()
    }

    #[allow(clippy::too_many_arguments)]
    fn start_block_thread(
        cluster_info: &Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<BankForks>>,
        transaction_recorder: TransactionRecorder,
        block_receiver: Receiver<HarmonicBlock>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_message_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
    ) -> Self {
        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache.clone(),
        );

        let consumer = BlockConsumer::new(committer, transaction_recorder, log_message_bytes_limit);

        let cluster_info = cluster_info.clone();
        let block_thread = Builder::new()
            .name("solBlockStgTx".to_string())
            .spawn(move || {
                Self::process_loop(bank_forks, block_receiver, consumer, exit, cluster_info);
            })
            .unwrap();

        Self { block_thread }
    }

    fn process_loop(
        bank_forks: Arc<RwLock<BankForks>>,
        block_receiver: Receiver<HarmonicBlock>,
        mut consumer: BlockConsumer,
        exit: Arc<AtomicBool>,
        cluster_info: Arc<ClusterInfo>,
    ) {
        while !exit.load(Ordering::Relaxed) {
            match block_receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(block) => {
                    let (root_bank, working_bank) = {
                        let bank_forks_guard = bank_forks.read().unwrap();
                        (
                            bank_forks_guard.root_bank(),
                            bank_forks_guard.working_bank(),
                        )
                    };

                    let intended_slot = block.intended_slot();
                    let current_slot = working_bank.slot();

                    // Check if this block is for the correct slot
                    if intended_slot != current_slot {
                        info!(
                            "Block intended for slot {} but current slot is {}, dropping",
                            intended_slot, current_slot
                        );
                        continue;
                    }

                    // Sanity check we are leader
                    if !cluster_info.id().eq(working_bank.collector_id()) {
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
                        // Claimed! Now record, execute, commit

                        // Translate packets to RuntimeTransaction<ResolvedTransactionView>
                        // using zerocopy TransactionView instead of bincode deserialization
                        let (transactions, max_ages) = Self::translate_packets_to_transactions(
                            &block.transactions(),
                            &root_bank,
                            &working_bank,
                        );

                        // Process blocks
                        let output = consumer.process_and_record_block_transactions(
                            &working_bank,
                            &transactions,
                            &max_ages,
                            intended_slot,
                        );

                        // Check if recording failed - if so, revert so vanilla can build fallback block
                        if let Err(e) = output
                            .execute_and_commit_transactions_output
                            .commit_transactions_result
                        {
                            info!(
                                "Block recording failed for slot {}, reverting to vanilla: {:?}",
                                current_slot, e
                            );
                            scheduler_synchronization::block_failed(current_slot);
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
}
