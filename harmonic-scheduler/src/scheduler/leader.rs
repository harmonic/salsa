//! Leader-slot state machine

use {
    crate::scheduler::{
        LeaderNotification, TIP_CRANK_TIMEOUT,
        block_stage::BlockStage,
        channel::BundleMeta,
        check::{CheckMeta, Checker},
        execute::Executor,
        fallback_stage::FallbackStage,
        progress::Progress,
        utils::free_batch,
    },
    agave_scheduler_bindings::{LEADER_READY, LEADER_STARTING, NOT_LEADER, ProgressMessage},
    agave_scheduler_handshake::ClientSession,
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    anyhow::Result,
    rts_alloc::Allocator,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::time::{Duration, Instant, SystemTime},
    tokio::sync::watch,
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

/// Slot % after which a fallback block replaces the block engine's, which comes early if at all
const BLOCK_STAGE_TIMEOUT_PERCENT: u8 = 50;

/// Incoming transaction channels
pub struct Inputs {
    pub remote_tpu: rtrb::Consumer<TransactionPtr>,
    pub packets: rtrb::Consumer<TransactionPtr>,
    pub blocks: rtrb::Consumer<TransactionPtrBatch<(), BundleMeta>>,
    pub bundles: rtrb::Consumer<TransactionPtrBatch<(), BundleMeta>>,
    pub tip_cranks: rtrb::Consumer<TransactionPtrBatch<(), BundleMeta>>,
}

struct Leader<'a> {
    token: &'a CancellationToken,
    progress: Progress,
    executor: Executor<'a, BundleMeta>,
    checker: Checker<'a, CheckMeta>,
    allocator: &'a Allocator,
    tip_cranks: rtrb::Consumer<TransactionPtrBatch<(), BundleMeta>>,
    notifications: &'a watch::Sender<Option<LeaderNotification>>,
    block: BlockStage<'a>,
    fallback: FallbackStage<'a>,
    slot: Slot,
}

/// Run the leader on the validator `session` until cancelled outside a leader window
pub fn run(
    token: &CancellationToken,
    session: ClientSession,
    inputs: Inputs,
    notifications: &watch::Sender<Option<LeaderNotification>>,
    tip_program: Pubkey,
) -> Result<()> {
    let ClientSession {
        allocator,
        tpu_to_pack,
        progress_tracker,
        pack_to_check_worker,
        check_worker_to_pack,
        workers,
    } = session;
    let Inputs {
        remote_tpu,
        packets,
        blocks,
        bundles,
        tip_cranks,
    } = inputs;
    let num_workers = workers.len();
    Leader {
        token,
        progress: Progress::new(progress_tracker),
        executor: Executor::new(workers, &allocator),
        checker: Checker::new(pack_to_check_worker, check_worker_to_pack, &allocator),
        allocator: &allocator,
        tip_cranks,
        notifications,
        block: BlockStage::new(tip_program, blocks, &allocator),
        fallback: FallbackStage::new(
            num_workers,
            tip_program,
            tpu_to_pack,
            remote_tpu,
            packets,
            bundles,
            &allocator,
        ),
        slot: 0,
    }
    .run()
}

impl Leader<'_> {
    /// Run until cancelled outside a leader window
    fn run(&mut self) -> Result<()> {
        self.progress.poll()?;
        loop {
            self.slot = self.progress.last().current_slot;
            if self.progress.last().leader_state == NOT_LEADER {
                if self.token.is_cancelled() {
                    return Ok(());
                }
                self.not_leader()?;
            } else {
                self.leader_starting()?;
                self.leader_ready()?;
                // Wait for the next slot's bank to avoid a brief not_leader
                while self.progress.poll()?.current_slot == self.slot {}
            }
        }
    }

    /// Keep the store fresh until the leader state changes
    fn not_leader(&mut self) -> Result<()> {
        let progress = self.progress.last();
        // u64::MAX until the validator finds our next slot in its schedule
        let next_leader_slot =
            (progress.next_leader_slot != u64::MAX).then_some(progress.next_leader_slot);
        info!(
            current_slot = progress.current_slot,
            ?next_leader_slot,
            "waiting for leader slot"
        );
        while self.progress.poll()?.leader_state == NOT_LEADER && !self.token.is_cancelled() {
            let progress = self.progress.last();
            self.block
                .drop_stale(progress.current_slot, progress.leader_range_end);
            self.fallback.poll(&mut self.checker)?;
        }
        self.fallback.drain(&mut self.checker, &mut self.progress)
    }

    /// Wait for the bank of our slot, or for the window to move on
    fn leader_starting(&mut self) -> Result<()> {
        info!(slot = self.slot, "starting leader slot");
        while self.progress.poll()?.leader_state == LEADER_STARTING
            && self.progress.last().current_slot == self.slot
        {}
        Ok(())
    }

    /// Run a leader slot with its bank
    fn leader_ready(&mut self) -> Result<()> {
        let progress = *self.progress.last();
        if progress.leader_state != LEADER_READY || progress.current_slot != self.slot {
            debug!(
                leader_state = progress.leader_state,
                slot = self.slot,
                current_slot = progress.current_slot,
                "skipping leader_ready"
            );
            return Ok(());
        }
        // A bank already closing can land nothing
        if progress.current_slot_progress >= 100 {
            return Ok(());
        }
        // The block engine goes with the session, so no block or crank will come
        if self.token.is_cancelled() {
            return self.fallback_stage(None);
        }
        self.send_leader_notification(&progress);
        let crank = self.tip_crank();
        while self.progress.poll()?.current_slot_progress < BLOCK_STAGE_TIMEOUT_PERCENT
            && self.progress.last().current_slot == self.slot
        {
            if self.block.arrived(self.slot, progress.leader_range_end) {
                return self.block_stage(crank);
            }
        }
        self.fallback_stage(crank)
    }

    /// Tell the block engine we lead this slot, and when its bank stops recording
    fn send_leader_notification(&self, progress: &ProgressMessage) {
        // Tenths of a percent of the bank time used. Progress is rounded down to
        // a 5% step, so assume the middle of it, 25 tenths on: at most half a
        // step off, and early when the bank has just started, as it usually has
        let used = u64::from(progress.current_slot_progress)
            .saturating_mul(10)
            .saturating_add(25);
        // A tenth of a percent of a millisecond is a microsecond, so this is exact
        let time_left = Duration::from_micros(
            u64::from(progress.target_bank_time_ms).saturating_mul(1000u64.saturating_sub(used)),
        );
        let start_time = SystemTime::now();
        self.notifications.send_replace(Some(LeaderNotification {
            slot: self.slot,
            start_time,
            end_time: start_time.checked_add(time_left).unwrap_or(start_time),
        }));
    }

    /// Replay the block engine's block, then a fallback block if the stream breaks
    fn block_stage(&mut self, crank: Option<TransactionPtrBatch<(), BundleMeta>>) -> Result<()> {
        info!(slot = self.slot, "entering block stage");
        let fallback = self.block.run(
            &mut self.executor,
            &mut self.checker,
            &mut self.progress,
            self.slot,
            crank,
        )?;
        // Our own block for the rest of the slot
        if fallback {
            return self.fallback_stage(None);
        }
        Ok(())
    }

    /// Build a block from `crank`, the bundles, and the store
    fn fallback_stage(&mut self, crank: Option<TransactionPtrBatch<(), BundleMeta>>) -> Result<()> {
        info!(slot = self.slot, "entering fallback stage");
        self.fallback.run(
            &mut self.executor,
            &mut self.checker,
            &mut self.progress,
            self.slot,
            crank,
        )
    }

    /// This slot's tip crank, if the validator signs it within [`TIP_CRANK_TIMEOUT`]
    fn tip_crank(&mut self) -> Option<TransactionPtrBatch<(), BundleMeta>> {
        let timer = Instant::now();
        while timer.elapsed() < TIP_CRANK_TIMEOUT {
            let Ok(crank) = self.tip_cranks.pop() else {
                continue;
            };
            let current = crank.batch_meta().slot == self.slot;
            if current && !crank.is_empty() {
                return Some(crank);
            }
            // An earlier slot's crank came too late; this slot's is empty when
            // none is needed or signing failed
            // SAFETY: Popped from its queue, so no worker holds it.
            unsafe { free_batch(crank, self.allocator) };
            if current {
                return None;
            }
        }
        warn!(slot = self.slot, "timed out waiting for tip crank");
        None
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::scheduler::channel::REVERT_PROTECT,
        agave_scheduler_bindings::{
            CheckWorkerToPackMessage, ExecutionWorkerToPackMessage, ProgressMessage,
            execution_message_flags, processed_codes,
            worker_message_types::{
                CheckResponse, ExecutionResponse, not_included_reasons, status_check_flags,
            },
        },
        agave_scheduler_handshake::{
            AgaveCheckWorkerSession, AgaveWorkerSession, ClientLogon, ClientSession,
            setup_local_session,
        },
        agave_scheduling_utils::{
            responses_region::{execution_responses_from_iter, resolve_responses_from_iter},
            transaction_ptr::TransactionPtrBatch,
        },
        agave_transaction_view::transaction_data::TransactionData,
        solana_instruction::{AccountMeta, Instruction},
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        std::time::Duration,
    };

    const LEADER_SLOT: u64 = 10;
    /// A revert-protected bundle of the leader slot's block, as the tip crank is
    const BLOCK: BundleMeta = BundleMeta {
        slot: LEADER_SLOT,
        flags: REVERT_PROTECT,
        id: 0,
    };
    /// A bundle from the bundle stream
    const BUNDLE: BundleMeta = BundleMeta {
        slot: 0,
        flags: REVERT_PROTECT,
        id: 0,
    };

    /// Flags and first signatures of an executed batch
    type Executed = (u16, Vec<[u8; 64]>);

    fn transaction() -> Vec<u8> {
        let ix = Instruction::new_with_bytes(
            Pubkey::new_unique(),
            &[],
            vec![AccountMeta::new(Pubkey::new_unique(), false)],
        );
        let mut tx = solana_transaction::Transaction::new_unsigned(Message::new(
            &[ix],
            Some(&Pubkey::new_unique()),
        ));
        tx.signatures[0] = Signature::new_unique();
        bincode::serialize(&tx).unwrap()
    }

    fn sig(tx: &[u8]) -> [u8; 64] {
        let tx: solana_transaction::Transaction = bincode::deserialize(tx).unwrap();
        *tx.signatures[0].as_array()
    }

    /// A batch of `txs` with `meta`, as the streams' channels build them
    fn copy_batch<T: AsRef<[u8]>>(
        txs: impl IntoIterator<Item = T>,
        meta: BundleMeta,
        allocator: &Allocator,
    ) -> TransactionPtrBatch<(), BundleMeta> {
        let txs: Vec<_> = txs
            .into_iter()
            .map(|tx| TransactionPtr::from_slice(tx.as_ref(), allocator).unwrap())
            .collect();
        // SAFETY: Allocated above, and never freed by the tests that use them.
        unsafe {
            TransactionPtrBatch::from_transactions(txs.iter().map(|tx| (tx, ())), meta, allocator)
        }
        .unwrap()
    }

    /// The validator side of a local session, answering EXECUTEs and CHECKs
    struct FakeValidator {
        progress: shaq::spsc::Producer<ProgressMessage>,
        worker: AgaveWorkerSession,
        checker: AgaveCheckWorkerSession,
        /// Every executed batch, in order
        executed: Vec<Executed>,
    }

    impl FakeValidator {
        fn progress(&mut self, leader_state: u8, slot: u64, progress: u8) {
            // The leader drains the feed; a full one only loses a duplicate update
            self.progress
                .try_write(ProgressMessage {
                    leader_state,
                    current_slot_progress: progress,
                    epoch: 0,
                    current_slot: slot,
                    next_leader_slot: if slot < LEADER_SLOT {
                        LEADER_SLOT
                    } else {
                        LEADER_SLOT + 100
                    },
                    leader_range_end: LEADER_SLOT + 3,
                    remaining_cost_units: 0,
                    remaining_allocated_accounts_data_size: 0,
                    latest_blockhash: [1; 32],
                    target_bank_time_ms: if leader_state == LEADER_READY { 200 } else { 0 },
                })
                .ok();
        }

        fn serve(&mut self) {
            let alloc = &self.worker.allocator;
            while let Some(message) = self.worker.pack_to_worker.try_read() {
                let n = message.batch.num_transactions;
                // SAFETY: an EXECUTE batch of a bundle, with `BundleMeta`
                let batch = unsafe {
                    TransactionPtrBatch::<(), BundleMeta>::from_sharable_transaction_batch_region(
                        &message.batch,
                        alloc,
                    )
                };
                let sigs = batch.iter().map(|(tx, ())| sig(tx.data())).collect();
                self.executed.push((message.flags, sigs));
                let responses = execution_responses_from_iter(
                    alloc,
                    (0..n).map(|_| ExecutionResponse {
                        execution_slot: LEADER_SLOT,
                        not_included_reason: not_included_reasons::NONE,
                        cost_units: 0,
                        fee_payer_balance: 0,
                    }),
                )
                .unwrap();
                self.worker
                    .worker_to_pack
                    .try_write(ExecutionWorkerToPackMessage {
                        batch: message.batch,
                        processed_code: processed_codes::PROCESSED,
                        responses,
                    })
                    .unwrap();
            }
            while let Some(message) = self.checker.pack_to_check_worker.try_read() {
                let responses = resolve_responses_from_iter(
                    &self.checker.allocator,
                    (0..message.batch.num_transactions).map(|_| CheckResponse {
                        status_check_flags: status_check_flags::REQUESTED
                            | status_check_flags::PERFORMED,
                        // SAFETY: plain data; all-zero is valid
                        ..unsafe { std::mem::zeroed() }
                    }),
                )
                .unwrap();
                self.checker
                    .check_worker_to_pack
                    .try_write(CheckWorkerToPackMessage {
                        batch: message.batch,
                        processed_code: processed_codes::PROCESSED,
                        responses,
                    })
                    .unwrap();
            }
        }
    }

    struct Feeds {
        tpu: Vec<Vec<u8>>,
        block_engine_packet: Vec<u8>,
        bundle: Vec<Vec<u8>>,
        /// The block's bundles, each with whether it is revert protected
        block: Vec<(Vec<Vec<u8>>, bool)>,
        /// A bundle for a slot we don't lead, queued ahead of the block
        stray: Option<Vec<u8>>,
        tip_crank: Vec<u8>,
    }

    /// A local session and the fake validator on its other side
    fn session() -> (FakeValidator, ClientSession) {
        let (mut agave, client) = setup_local_session(ClientLogon {
            worker_count: 1,
            check_worker_count: 1,
            allocator_size: 256 * 1024 * 1024,
            allocator_handles: 2,
            tpu_to_pack_capacity: 1024,
            progress_tracker_capacity: 1024,
            pack_to_worker_capacity: 64,
            worker_to_pack_capacity: 128,
            pack_to_check_worker_capacity: 256,
            check_worker_to_pack_capacity: 256,
            flags: 0,
        })
        .unwrap();
        let validator = FakeValidator {
            progress: agave.progress_tracker,
            worker: agave.workers.pop().unwrap(),
            checker: agave.check_workers.pop().unwrap(),
            executed: vec![],
        };
        (validator, client)
    }

    /// Run one leader slot against a fake validator; the executed batches and the notification
    fn run_leader_slot(
        feeds: &Feeds,
        leader_progress: u8,
    ) -> (Vec<Executed>, Option<LeaderNotification>) {
        let (mut validator, client) = session();
        let feeder = Allocator::join_from_existing(&client.allocator).unwrap();
        let (mut remote_tpu_tx, remote_tpu_rx) = rtrb::RingBuffer::new(1024);
        let (mut packet_tx, packet_rx) = rtrb::RingBuffer::new(1024);
        let (mut block_tx, block_rx) = rtrb::RingBuffer::new(16);
        let (mut bundle_tx, bundle_rx) = rtrb::RingBuffer::new(16);
        let (leader_tx, leader_rx) = watch::channel(None);
        let token = CancellationToken::new();
        let (mut tip_crank_tx, tip_crank_rx) = rtrb::RingBuffer::new(1);
        tip_crank_tx
            .push(copy_batch([&feeds.tip_crank], BLOCK, &feeder))
            .unwrap();
        for tx in &feeds.tpu {
            remote_tpu_tx
                .push(TransactionPtr::from_slice(tx, &feeder).unwrap())
                .unwrap();
        }
        packet_tx
            .push(TransactionPtr::from_slice(&feeds.block_engine_packet, &feeder).unwrap())
            .unwrap();
        bundle_tx
            .push(copy_batch(&feeds.bundle, BUNDLE, &feeder))
            .unwrap();

        let executed = std::thread::scope(|scope| {
            let leader = scope.spawn(|| {
                run(
                    &token,
                    client,
                    Inputs {
                        remote_tpu: remote_tpu_rx,
                        packets: packet_rx,
                        blocks: block_rx,
                        bundles: bundle_rx,
                        tip_cranks: tip_crank_rx,
                    },
                    &leader_tx,
                    Pubkey::new_unique(),
                )
            });

            let step = |validator: &mut FakeValidator, state, slot, progress, ms| {
                for _ in 0..ms {
                    validator.progress(state, slot, progress);
                    validator.serve();
                    std::thread::sleep(Duration::from_millis(1));
                }
            };
            step(&mut validator, NOT_LEADER, LEADER_SLOT - 1, 50, 100);
            if let Some(tx) = &feeds.stray {
                let meta = BundleMeta {
                    slot: u64::MAX,
                    ..BLOCK
                };
                block_tx.push(copy_batch([tx], meta, &feeder)).unwrap();
            }
            for (bundle, revert_protected) in &feeds.block {
                let flags = if *revert_protected { REVERT_PROTECT } else { 0 };
                let meta = BundleMeta { flags, ..BLOCK };
                block_tx.push(copy_batch(bundle, meta, &feeder)).unwrap();
            }
            step(
                &mut validator,
                LEADER_READY,
                LEADER_SLOT,
                leader_progress,
                200,
            );
            token.cancel();
            step(&mut validator, NOT_LEADER, LEADER_SLOT + 1, 0, 100);
            leader.join().unwrap().unwrap();
            validator.executed
        });
        (executed, *leader_rx.borrow())
    }

    fn fixture() -> Feeds {
        Feeds {
            tpu: (0..3).map(|_| transaction()).collect(),
            block_engine_packet: transaction(),
            bundle: (0..2).map(|_| transaction()).collect(),
            block: vec![],
            stray: None,
            tip_crank: transaction(),
        }
    }

    const PROTECTED: u16 =
        execution_message_flags::ALL_OR_NOTHING | execution_message_flags::DROP_ON_FAILURE;

    /// Without a block: the tip crank, then bundles, then packets from every source in any order
    #[test]
    fn fallback_stage_order() {
        let feeds = fixture();
        let (crank, bundle) = (
            sig(&feeds.tip_crank),
            feeds.bundle.iter().map(|tx| sig(tx)).collect::<Vec<_>>(),
        );
        let mut packets: Vec<_> = feeds.tpu.iter().map(|tx| sig(tx)).collect();
        packets.push(sig(&feeds.block_engine_packet));
        packets.sort_unstable();
        let (executed, notification) = run_leader_slot(&feeds, 60);

        assert_eq!(executed.len(), 6, "{executed:?}");
        assert_eq!(executed[0], (PROTECTED, vec![crank]), "tip crank first");
        assert_eq!(executed[1], (PROTECTED, bundle));
        let mut rest: Vec<_> = executed[2..]
            .iter()
            .map(|(flags, sigs)| {
                assert_eq!(*flags, 0);
                sigs[0]
            })
            .collect();
        rest.sort_unstable();
        assert_eq!(rest, packets);
        let notification = notification.unwrap();
        assert_eq!(notification.slot, LEADER_SLOT);
        assert!(notification.end_time > notification.start_time);
    }

    /// With a block: the tip crank, then the block in order; nothing else
    #[test]
    fn block_stage_order() {
        let mut feeds = fixture();
        let block_txs: Vec<Vec<u8>> = (0..4).map(|_| transaction()).collect();
        feeds.block = vec![
            (vec![block_txs[0].clone()], false),
            (vec![block_txs[1].clone(), block_txs[2].clone()], true),
            (vec![block_txs[3].clone()], false),
        ];
        let crank = sig(&feeds.tip_crank);
        let (executed, _) = run_leader_slot(&feeds, 10);

        let sigs: Vec<_> = block_txs.iter().map(|tx| sig(tx)).collect();
        assert_eq!(executed.len(), 4, "{executed:?}");
        assert_eq!(executed[0], (PROTECTED, vec![crank]), "tip crank first");
        assert_eq!(executed[1], (0, vec![sigs[0]]));
        assert_eq!(executed[2], (PROTECTED, vec![sigs[1], sigs[2]]));
        assert_eq!(executed[3], (0, vec![sigs[3]]));
    }

    /// A bundle for a slot we don't lead is dropped, so it can't hold back the block behind it
    #[test]
    fn block_stage_skips_bundles_for_slots_we_dont_lead() {
        let mut feeds = fixture();
        let block_tx = transaction();
        feeds.block = vec![(vec![block_tx.clone()], false)];
        feeds.stray = Some(transaction());
        let crank = sig(&feeds.tip_crank);
        let (executed, _) = run_leader_slot(&feeds, 10);

        assert_eq!(executed.len(), 2, "{executed:?}");
        assert_eq!(executed[0], (PROTECTED, vec![crank]), "tip crank first");
        assert_eq!(executed[1], (0, vec![sig(&block_tx)]));
    }

    /// A block the stream ends early is abandoned at once, as on any broken stream, and a
    /// fallback block fills the slot
    #[test]
    fn block_stage_falls_back_when_the_block_ends_early() {
        let mut feeds = fixture();
        // A block bundle taken in the same pass as the end, so never sent
        feeds.block = vec![(vec![transaction()], false), (vec![], false)];
        let crank = sig(&feeds.tip_crank);
        let bundle: Vec<_> = feeds.bundle.iter().map(|tx| sig(tx)).collect();
        let (executed, _) = run_leader_slot(&feeds, 10);

        assert_eq!(executed.len(), 6, "{executed:?}");
        assert_eq!(executed[0], (PROTECTED, vec![crank]), "tip crank first");
        assert_eq!(executed[1], (PROTECTED, bundle), "then the fallback block");
        assert!(executed[2..].iter().all(|(flags, _)| *flags == 0));
    }
}
