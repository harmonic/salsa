//! Block consumer processes block transactions and records them to PoH.
//!
//! Unlike banking_stage and bundle_stage, block_stage uses OPTIMISTIC RECORDING:
//! 1. First, record ALL transactions to PoH (broadcasts to cluster immediately)
//! 2. Lock the bank to prevent slot from ending
//! 3. Then execute the transactions using parallel workers with the Scheduler
//! 4. Commit the results
//!
//! This allows the cluster to start replaying the block alongside us.
//! The Scheduler ensures proper transaction ordering based on account locks.
//! Multiple worker threads execute non-conflicting chunks in parallel.

use {
    super::{DevinScheduler, Timer},
    crate::banking_stage::{
        committer::{CommitTransactionDetails, Committer},
        consumer::{
            ExecuteAndCommitTransactionsOutput, LeaderProcessedTransactionCounts,
            ProcessTransactionBatchOutput,
        },
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
        scheduler_messages::MaxAge,
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView, transaction_data::TransactionData,
    },
    itertools::Itertools,
    log::{debug, error, info},
    rayon::{prelude::*, ThreadPool, ThreadPoolBuilder},
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::{Slot, MAX_PROCESSING_AGE},
    solana_entry::entry::hash_transactions,
    solana_measure::measure_us,
    solana_poh::{
        poh_recorder::PohRecorderError,
        transaction_recorder::{RecordTransactionsTimings, TransactionRecorder},
    },
    solana_runtime::{
        account_saver::collect_accounts_to_store,
        bank::{Bank, LoadAndExecuteTransactionsOutput},
    },
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
        transaction_with_meta::TransactionWithMeta,
    },
    solana_svm::{
        account_overrides::AccountOverrides,
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig},
    },
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::sanitized::SanitizedTransaction,
    solana_transaction_error::TransactionError,
    std::{cmp::max, num::Saturating, ops::Range, sync::mpsc},
};

/// Number of worker threads for parallel execution
const NUM_THREADS: usize = 8;

/// Verify signatures for all transactions in parallel using rayon.
/// Returns true if all signatures are valid, false on first invalid signature (short-circuits).
fn verify_signatures_parallel<D: TransactionData + Sync>(
    transactions: &[RuntimeTransaction<ResolvedTransactionView<D>>],
    thread_pool: &ThreadPool,
) -> bool {
    thread_pool.install(|| {
        transactions.par_iter().all(|tx| {
            // Transaction is already sanitized (num_signatures validated)
            let signatures = tx.signatures();
            let pubkeys = tx.account_keys();
            let message_data = tx.message_data();

            signatures
                .iter()
                .zip(pubkeys.iter())
                .all(|(sig, pk)| sig.verify(pk.as_ref(), message_data))
        })
    })
}

/// Result from a worker thread after executing and committing a chunk
type WorkerResult = Result<
    (
        usize,                         // thread index
        Range<usize>,                  // transaction range
        Vec<CommitTransactionDetails>, // commit details
        u64,                           // start time us
        u64,                           // end time us
    ),
    TransactionError,
>;

pub struct BlockConsumer {
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    log_messages_bytes_limit: Option<usize>,
    scheduler: DevinScheduler,
    thread_pool: ThreadPool,
}

impl BlockConsumer {
    pub fn new(
        committer: Committer,
        transaction_recorder: TransactionRecorder,
        log_messages_bytes_limit: Option<usize>,
    ) -> Self {
        // Pre-initialize the timer calibration to avoid 2s stall on first block
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        super::timer::memoize_ticks_per_us_and_invariant_tsc_check();

        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(NUM_THREADS)
            .thread_name(|i| format!("solBlkExec{i}"))
            .build()
            .expect("Failed to create block execution thread pool");

        Self {
            committer,
            transaction_recorder,
            log_messages_bytes_limit,
            scheduler: DevinScheduler::new(),
            thread_pool,
        }
    }

    /// Process and record block transactions using OPTIMISTIC RECORDING.
    ///
    /// Flow:
    /// 1. Record ALL transactions to PoH first (broadcasts to cluster)
    /// 2. Lock the bank to prevent slot from ending
    /// 3. Execute the transactions in parallel using worker threads
    /// 4. Commit the results after each chunk
    ///
    /// Uses RuntimeTransaction<ResolvedTransactionView<D>> for zerocopy transaction parsing.
    pub fn process_and_record_block_transactions<D: TransactionData + Sync>(
        &mut self,
        bank: &Bank,
        transactions: &[RuntimeTransaction<ResolvedTransactionView<D>>],
        max_ages: &[MaxAge],
        intended_slot: Slot,
    ) -> ProcessTransactionBatchOutput {
        if transactions.is_empty() {
            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: 0,
                cost_model_us: 0,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    transaction_counts: LeaderProcessedTransactionCounts::default(),
                    retryable_transaction_indexes: vec![],
                    commit_transactions_result: Ok(vec![]),
                    execute_and_commit_timings: Default::default(),
                    error_counters: Default::default(),
                    min_prioritization_fees: 0,
                    max_prioritization_fees: 0,
                },
            };
        }

        // Step 0: Verify all signatures in parallel (fails fast on first invalid)
        let (sigverify_passed, sigverify_us) =
            measure_us!(verify_signatures_parallel(transactions, &self.thread_pool));

        if !sigverify_passed {
            error!(
                "Signature verification failed for block at slot {} (took {}us)",
                intended_slot, sigverify_us
            );
            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: 0,
                cost_model_us: 0,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    transaction_counts: LeaderProcessedTransactionCounts::default(),
                    retryable_transaction_indexes: vec![],
                    commit_transactions_result: Err(
                        PohRecorderError::HarmonicBlockInvalidSignature,
                    ),
                    execute_and_commit_timings: Default::default(),
                    error_counters: Default::default(),
                    min_prioritization_fees: 0,
                    max_prioritization_fees: 0,
                },
            };
        }

        debug!(
            "Verified {} signatures in {}us for slot {}",
            transactions.len(),
            sigverify_us,
            intended_slot
        );

        // Filter transactions based on max_age (check reserved keys, ALT expiration)
        let pre_results = transactions
            .iter()
            .zip(max_ages)
            .map(|(tx, max_age)| {
                if bank.epoch() != max_age.sanitized_epoch {
                    bank.check_reserved_keys(tx)?;
                }
                if bank.slot() > max_age.alt_invalidation_slot {
                    let (_addresses, _deactivation_slot) =
                        bank.load_addresses_from_ref(tx.message_address_table_lookups())?;
                }
                Ok(())
            })
            .collect_vec();

        let mut error_counters = TransactionErrorMetrics::default();
        let check_results = bank.check_transactions(
            transactions,
            &pre_results,
            MAX_PROCESSING_AGE,
            &mut error_counters,
        );

        // All transactions should pass check_transactions
        if let Some((txn, err)) = check_results
            .into_iter()
            .enumerate()
            .find(|(_i, r)| r.is_err())
            .map(|(i, r)| (&transactions[i], r.expect_err("filtered for is_err")))
        {
            info!(
                "block in slot {} has txn {} which failed check_transactions: {:?}",
                bank.slot(),
                txn.signatures()[0],
                &err
            );
            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: 0,
                cost_model_us: 0,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    execute_and_commit_timings: LeaderExecuteAndCommitTimings::default(),
                    error_counters,
                    min_prioritization_fees: 0,
                    max_prioritization_fees: 0,
                    transaction_counts: LeaderProcessedTransactionCounts::default(),
                    retryable_transaction_indexes: vec![],
                    commit_transactions_result: Err(
                        PohRecorderError::HarmonicBlockInvalidTransaction,
                    ),
                },
            };
        }

        // Step 1: OPTIMISTICALLY RECORD ALL TRANSACTIONS TO POH FIRST
        // This broadcasts the block to the cluster so they can replay alongside us
        let mut record_transactions_timings = RecordTransactionsTimings::default();

        // Hash each transaction individually
        let mut hashes = Vec::with_capacity(transactions.len());
        let mut batches = Vec::with_capacity(transactions.len());
        for tx in transactions.iter() {
            let batch = vec![tx.to_versioned_transaction()];
            let (hash, hash_us) = measure_us!(hash_transactions(&batch));
            record_transactions_timings.hash_us += hash_us;
            hashes.push(hash);
            batches.push(batch);
        }

        // Lock the bank to prevent the slot from ending after we record
        // Both locks are needed: freeze_lock prevents bank hash finalization,
        // blockhash_queue_lock prevents blockhash queue updates
        let freeze_lock = bank.freeze_lock();
        let blockhash_queue_lock = bank.blockhash_queue_lock();

        // Record all transactions - this is all-or-nothing for the entire block
        // Pass harmonic=true to trigger PoH speedrun after this block is recorded
        let (record_result, poh_record_us) =
            measure_us!(self
                .transaction_recorder
                .record(bank.bank_id(), hashes, batches, true));
        record_transactions_timings.poh_record_us = Saturating(poh_record_us);

        let starting_transaction_index = match record_result {
            Ok(starting_index) => {
                info!(
                    "Optimistically recorded block for slot {} with {} transactions",
                    intended_slot,
                    transactions.len()
                );
                starting_index
            }
            Err(e) => {
                // Recording failed - return early, vanilla scheduler can build fallback block
                debug!("Failed to record block for slot {}: {:?}", intended_slot, e);
                let error = match e {
                    solana_poh::record_channels::RecordSenderError::InactiveBankId
                    | solana_poh::record_channels::RecordSenderError::Shutdown => {
                        PohRecorderError::MaxHeightReached
                    }
                    solana_poh::record_channels::RecordSenderError::Full => {
                        PohRecorderError::ChannelFull
                    }
                    solana_poh::record_channels::RecordSenderError::Disconnected => {
                        PohRecorderError::ChannelDisconnected
                    }
                };
                return ProcessTransactionBatchOutput {
                    cost_model_throttled_transactions_count: 0,
                    cost_model_us: 0,
                    execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                        transaction_counts: LeaderProcessedTransactionCounts::default(),
                        retryable_transaction_indexes: vec![],
                        commit_transactions_result: Err(error),
                        execute_and_commit_timings: LeaderExecuteAndCommitTimings {
                            record_transactions_timings,
                            ..Default::default()
                        },
                        error_counters,
                        min_prioritization_fees: 0,
                        max_prioritization_fees: 0,
                    },
                };
            }
        };

        // Step 2: NOW EXECUTE THE TRANSACTIONS using parallel workers
        // Recording succeeded, so we're committed to this block - execute and commit
        let execute_and_commit_output = self.execute_and_commit_parallel(
            bank,
            transactions,
            starting_transaction_index,
            record_transactions_timings,
        );

        // Add actual executed costs to the cost tracker
        if let Ok(ref commit_details) = execute_and_commit_output.commit_transactions_result {
            let mut cost_tracker = bank.write_cost_tracker().unwrap();
            cost_tracker.add_executed_transaction_costs(
                transactions
                    .iter()
                    .zip(commit_details.iter())
                    .filter_map(|(tx, detail)| match detail {
                        CommitTransactionDetails::Committed {
                            compute_units,
                            loaded_accounts_data_size,
                            ..
                        } => Some((tx, *compute_units, *loaded_accounts_data_size)),
                        CommitTransactionDetails::NotCommitted(_) => None,
                    }),
            );
        }

        drop(freeze_lock);
        drop(blockhash_queue_lock);

        // Restore vote limit after block execution completes
        bank.restore_vote_limit();

        // Comprehensive timing log for profiling
        let timings = &execute_and_commit_output.execute_and_commit_timings;
        let committed_count = execute_and_commit_output
            .transaction_counts
            .processed_with_successful_result_count;
        info!(
            "Block slot={} txns={} committed={} | sigverify={}us hash={}us record={}us execute={}us | total={}us",
            intended_slot,
            transactions.len(),
            committed_count,
            sigverify_us,
            timings.record_transactions_timings.hash_us.0,
            timings.record_transactions_timings.poh_record_us.0,
            timings.load_execute_us,
            sigverify_us
                + timings.record_transactions_timings.hash_us.0
                + timings.record_transactions_timings.poh_record_us.0
                + timings.load_execute_us,
        );

        ProcessTransactionBatchOutput {
            cost_model_throttled_transactions_count: 0,
            cost_model_us: 0,
            execute_and_commit_transactions_output: execute_and_commit_output,
        }
    }

    /// Execute a single chunk of transactions with retry on AccountInUse errors.
    /// AccountInUse errors can occur due to contention with vote processing threads.
    fn execute_chunk<D: TransactionData>(
        bank: &Bank,
        transactions: &[RuntimeTransaction<ResolvedTransactionView<D>>],
        range: Range<usize>,
        account_overrides: &AccountOverrides,
        log_messages_bytes_limit: Option<usize>,
        transaction_status_sender_enabled: bool,
    ) -> LoadAndExecuteTransactionsOutput {
        let chunk = &transactions[range];

        loop {
            // Prepare batch for this chunk
            let batch = bank.prepare_sanitized_batch(chunk);

            // Execute transactions with account overrides to see state from previous chunks
            let output = bank.load_and_execute_transactions(
                &batch,
                MAX_PROCESSING_AGE,
                &mut solana_svm_timings::ExecuteTimings::default(),
                &mut TransactionErrorMetrics::default(),
                TransactionProcessingConfig {
                    account_overrides: Some(account_overrides),
                    check_program_modification_slot: bank.check_program_modification_slot(),
                    log_messages_bytes_limit,
                    limit_to_load_programs: true,
                    recording_config: ExecutionRecordingConfig::new_single_setting(
                        transaction_status_sender_enabled,
                    ),
                },
            );

            // If we get AccountInUse errors, retry execution
            // Most likely account contention from vote processing threads
            if output
                .processing_results
                .iter()
                .any(|r| matches!(r, Err(TransactionError::AccountInUse)))
            {
                debug!("AccountInUse error detected, retrying chunk execution");
                continue;
            }

            return output;
        }
    }

    /// Execute and commit transactions in parallel after they have been recorded to PoH.
    /// Uses multiple worker threads with the Scheduler for chunked parallel execution.
    fn execute_and_commit_parallel<D: TransactionData + Sync>(
        &mut self,
        bank: &Bank,
        transactions: &[RuntimeTransaction<ResolvedTransactionView<D>>],
        starting_transaction_index: Option<usize>,
        record_transactions_timings: RecordTransactionsTimings,
    ) -> ExecuteAndCommitTransactionsOutput {
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        execute_and_commit_timings.record_transactions_timings = record_transactions_timings;

        // Calculate prioritization fees
        let min_max = transactions
            .iter()
            .filter_map(|transaction| {
                transaction
                    .compute_budget_instruction_details()
                    .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
                    .ok()
                    .map(|limits| limits.compute_unit_price)
            })
            .minmax();
        let (min_prioritization_fees, max_prioritization_fees) =
            min_max.into_option().unwrap_or_default();

        let transaction_status_sender_enabled = self.committer.transaction_status_sender_enabled();
        let log_messages_bytes_limit = self.log_messages_bytes_limit;

        // Thread-safe account overrides shared between all workers (uses scc::HashMap)
        let account_overrides = AccountOverrides::default();

        // Collect all commit details across chunks
        let mut all_commit_details: Vec<CommitTransactionDetails> = vec![
            CommitTransactionDetails::NotCommitted(TransactionError::AccountNotFound);
            transactions.len()
        ];
        let mut execution_error: Option<TransactionError> = None;

        // Timer for tracking execution time
        let start_time = Timer::new();

        // Execute in parallel using thread pool
        self.thread_pool.in_place_scope(|scope| {
            // Channel for workers to send results back
            let (finish_tx, finish_rx) = mpsc::channel::<WorkerResult>();

            // Channels for sending work to each worker
            let (work_senders, work_receivers): (Vec<_>, Vec<_>) = (0..NUM_THREADS)
                .map(|_| mpsc::channel::<Range<usize>>())
                .unzip();

            // Spawn worker threads
            for (thread_idx, work_rx) in work_receivers.into_iter().enumerate() {
                let finish_tx = finish_tx.clone();
                let account_overrides = &account_overrides;
                let committer = &self.committer;
                let start_time = &start_time;

                scope.spawn(move |_| {
                    while let Ok(range) = work_rx.recv() {
                        let worker_start = start_time.elapsed_us();

                        // Execute chunk with shared account overrides (with retry on AccountInUse)
                        let load_and_execute_output = Self::execute_chunk(
                            bank,
                            transactions,
                            range.clone(),
                            account_overrides,
                            log_messages_bytes_limit,
                            transaction_status_sender_enabled,
                        );

                        let LoadAndExecuteTransactionsOutput {
                            processing_results,
                            processed_counts,
                            balance_collector,
                        } = load_and_execute_output;

                        // Commit this chunk
                        let chunk = &transactions[range.clone()];
                        let batch = bank.prepare_sanitized_batch(chunk);

                        // Cache accounts in account_overrides BEFORE commit so next iterations
                        // can load cached state instead of using AccountsDB (which may be stale)
                        // This matches the audited implementation pattern.
                        let (accounts_to_cache, _) = collect_accounts_to_store(
                            chunk,
                            &None::<Vec<SanitizedTransaction>>,
                            &processing_results,
                        );
                        for (pubkey, account) in accounts_to_cache {
                            if account.lamports() == 0 {
                                account_overrides
                                    .set_account(pubkey, Some(AccountSharedData::default()));
                            } else {
                                account_overrides.set_account(pubkey, Some(account.clone()));
                            }
                        }

                        let chunk_starting_index =
                            starting_transaction_index.map(|start| start + range.start);

                        let commit_transaction_statuses =
                            if processed_counts.processed_transactions_count != 0 {
                                let (_, statuses) = committer.commit_transactions(
                                    &batch,
                                    processing_results,
                                    chunk_starting_index,
                                    bank,
                                    balance_collector,
                                    &mut LeaderExecuteAndCommitTimings::default(),
                                    &processed_counts,
                                );
                                statuses
                            } else {
                                processing_results
                                    .into_iter()
                                    .map(|r| match r {
                                        Ok(_) => unreachable!("processed count is 0"),
                                        Err(err) => CommitTransactionDetails::NotCommitted(err),
                                    })
                                    .collect()
                            };

                        let worker_end = start_time.elapsed_us();

                        let _ = finish_tx.send(Ok((
                            thread_idx,
                            range,
                            commit_transaction_statuses,
                            worker_start,
                            worker_end,
                        )));
                    }
                });
            }

            // Initialize scheduler
            self.scheduler.init(transactions);

            let mut next_worker = 0usize;
            let mut queue_depth = 0usize;
            let mut max_queue_depth = 0usize;
            let mut per_thread_count = [0usize; NUM_THREADS];
            let mut per_thread_execution_times: [Vec<(Range<usize>, u64, u64)>; NUM_THREADS] =
                Default::default();

            // Main scheduling loop - matches audited implementation
            while !self.scheduler.finished {
                // Schedule transactions for execution
                while let Some(range) = self.scheduler.pop(transactions, bank) {
                    // Send work to next available worker (round-robin)
                    if work_senders[next_worker].send(range.clone()).is_ok() {
                        per_thread_count[next_worker] += range.len();
                        queue_depth += 1;
                        max_queue_depth = max(max_queue_depth, queue_depth);
                    }
                    next_worker = (next_worker + 1) % NUM_THREADS;
                }

                // Finish any completed transactions (non-blocking)
                if let Ok(result) = finish_rx.try_recv() {
                    queue_depth -= 1;
                    match result {
                        Ok((thread_idx, range, commit_details, start_us, end_us)) => {
                            // Track per-thread execution times
                            per_thread_execution_times[thread_idx].push((
                                range.clone(),
                                start_us,
                                end_us,
                            ));
                            // Store commit details
                            for (i, status) in commit_details.into_iter().enumerate() {
                                all_commit_details[range.start + i] = status;
                            }
                            // Mark chunk as finished in scheduler (may unblock more work)
                            self.scheduler.finish(range, transactions);
                        }
                        Err(e) => {
                            error!("Block execution failed: {:?}", e);
                            execution_error = Some(e);
                            break;
                        }
                    }
                }
            }

            // Drop senders to signal workers to exit
            drop(work_senders);

            // Update timing metrics
            execute_and_commit_timings.load_execute_us = start_time.elapsed_us();

            // Log per-thread execution times for profiling
            for (thread_idx, times) in per_thread_execution_times.iter().enumerate() {
                if !times.is_empty() {
                    debug!(
                        "Thread {} execution times: {:?}",
                        thread_idx,
                        times
                            .iter()
                            .map(|(r, s, e)| (r.clone(), e - s))
                            .collect::<Vec<_>>()
                    );
                }
            }
        });

        // Count successes
        let total_processed_count = all_commit_details
            .iter()
            .filter(|d| matches!(d, CommitTransactionDetails::Committed { .. }))
            .count() as u64;

        let transaction_counts = LeaderProcessedTransactionCounts {
            processed_count: total_processed_count,
            processed_with_successful_result_count: total_processed_count,
            attempted_processing_count: transactions.len() as u64,
        };

        ExecuteAndCommitTransactionsOutput {
            transaction_counts,
            retryable_transaction_indexes: vec![],
            commit_transactions_result: if execution_error.is_some() {
                Ok(all_commit_details) // Still return partial results
            } else {
                Ok(all_commit_details)
            },
            execute_and_commit_timings,
            error_counters: TransactionErrorMetrics::default(),
            min_prioritization_fees,
            max_prioritization_fees,
        }
    }
}
