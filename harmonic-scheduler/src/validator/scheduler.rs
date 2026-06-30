//! Transaction scheduler: drives the leader-slot state machine

use super::block_stage::BlockStage;
use super::drain;
use super::fallback_stage::FallbackStage;
use super::storage::Storage;
use crate::block_engine::LeaderNotification;
use crate::consts::{BATCH_SIZE, NONVOTE_STORAGE_CAPACITY, VOTE_STORAGE_CAPACITY};
use crate::ipc::shmem::{Free, Slice, allocate, allocate_batch};
use crate::ipc::{ProgressTracker, pack_to_worker, worker_to_pack};
use crate::state::scheduler_exit;
use agave_scheduler_bindings::pack_message_flags::read_flags;
use agave_scheduler_bindings::worker_message_types::{READ_RESPONSE, ReadResponse, read_results};
use agave_scheduler_bindings::{
    LEADER_READY, LEADER_STARTING, NOT_LEADER, PackToWorkerMessage, SharableTransactionRegion,
    WorkerToPackMessage, pack_message_flags, processed_codes,
};
use agave_scheduling_utils::handshake::ClientWorkerSession;
use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use bytes::Bytes;
use log::{debug, info, warn};
use rts_alloc::Allocator;
use smallvec::SmallVec;
use solana_clock::DEFAULT_MS_PER_SLOT;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tip_manager::{BlockBuilderFeeInfo, TipAccountData, TipManager};
use tokio::sync::watch;

/// Target wall-clock duration of a slot
const SLOT_DURATION: Duration = Duration::from_millis(DEFAULT_MS_PER_SLOT);
/// Slot % after which we abandon the block engine and fall back to nonvotes
const BLOCK_STAGE_TIMEOUT_PERCENT: u8 = 75;
/// Slot % at which block_stage yields to vote_stage (tail reserved for vote ingestion)
const VOTE_STAGE_START_PERCENT: u8 = 94;
/// Stop checking stored transactions within this many slots of our leader window
const PRE_LEADER_HOLD_SLOTS: u64 = 2;

/// Drives the leader-slot state machine and worker IPC
pub struct Scheduler<'a> {
    progress: ProgressTracker,
    pack_to_worker: Vec<shaq::spsc::Producer<PackToWorkerMessage>>,
    worker_to_pack: Vec<shaq::spsc::Consumer<WorkerToPackMessage>>,
    allocator: &'a Allocator,
    vote_rx: rtrb::Consumer<SharableTransactionRegion>,
    nonvote_rx: rtrb::Consumer<SharableTransactionRegion>,
    block_rx: &'a mut rtrb::Consumer<(u64, Vec<Bytes>)>,
    leader_tx: &'a watch::Sender<Option<LeaderNotification>>,
    block_stage: BlockStage<'a>,
    fallback_stage: FallbackStage<'a>,
    vote_store: Storage<'a>,
    nonvote_store: Storage<'a>,
    slot: u64,
    pending_tip_bundle: SmallVec<[Vec<u8>; 4]>,
    identity_rx: watch::Receiver<Arc<Keypair>>,
    tip_manager_rx: watch::Receiver<Arc<TipManager>>,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    dropped_timer: rdtsc::Instant,
}

impl<'a> Scheduler<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        progress: ProgressTracker,
        workers: Vec<ClientWorkerSession>,
        allocator: &'a Allocator,
        vote_rx: rtrb::Consumer<SharableTransactionRegion>,
        nonvote_rx: rtrb::Consumer<SharableTransactionRegion>,
        block_rx: &'a mut rtrb::Consumer<(u64, Vec<Bytes>)>,
        leader_tx: &'a watch::Sender<Option<LeaderNotification>>,
        identity_rx: watch::Receiver<Arc<Keypair>>,
        tip_manager_rx: watch::Receiver<Arc<TipManager>>,
        fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    ) -> Self {
        let num_workers = workers.len();
        let (pack_to_worker, worker_to_pack): (Vec<_>, Vec<_>) = workers
            .into_iter()
            .map(|s| (s.pack_to_worker, s.worker_to_pack))
            .unzip();
        Self {
            progress,
            pack_to_worker,
            worker_to_pack,
            allocator,
            vote_rx,
            nonvote_rx,
            block_rx,
            leader_tx,
            block_stage: BlockStage::new(num_workers, allocator),
            fallback_stage: FallbackStage::new(num_workers, allocator),
            vote_store: Storage::new(VOTE_STORAGE_CAPACITY, allocator),
            nonvote_store: Storage::new(NONVOTE_STORAGE_CAPACITY, allocator),
            slot: 0,
            pending_tip_bundle: SmallVec::new(),
            identity_rx,
            tip_manager_rx,
            fee_info,
            dropped_timer: rdtsc::Instant::now(),
        }
    }

    pub fn run(&mut self) -> Result<()> {
        // Wait for the first progress message
        while self.progress.poll()?.next_leader_slot == 0 {}
        info!(
            "waiting for leader slot: current_slot={}, next_leader_slot={}",
            self.progress.last().current_slot,
            self.progress.last().next_leader_slot,
        );

        loop {
            self.slot = self.progress.last().current_slot;
            if self.progress.last().leader_state == NOT_LEADER {
                // Wait for a non-leader slot to shutdown scheduler
                // This ensures there is no gap in vote processing when transferring scheduling
                if scheduler_exit() {
                    break;
                }
                self.not_leader()?;
            } else {
                self.leader_starting()?;
                self.leader_ready()?;
            }
        }
        Ok(())
    }

    /// Run a not leader slot
    fn not_leader(&mut self) -> Result<()> {
        self.vote_store.reset_cursor();
        self.nonvote_store.reset_cursor();
        let hold = self
            .progress
            .last()
            .next_leader_slot
            .saturating_sub(self.slot)
            <= PRE_LEADER_HOLD_SLOTS;

        while self.progress.poll()?.leader_state == NOT_LEADER
            && self.progress.last().current_slot == self.slot
        {
            self.allocator.clean_remote_free_lists();

            if self.dropped_timer.elapsed_ms() > 1000 {
                let dropped_votes = self.vote_store.dropped();
                let dropped_nonvotes = self.nonvote_store.dropped();
                if dropped_votes | dropped_nonvotes != 0 {
                    warn!(
                        "storage full, dropping packets: votes={dropped_votes} \
                         nonvotes={dropped_nonvotes}"
                    );
                }
                self.dropped_timer = rdtsc::Instant::now();
            }

            // Drain any stale blocks
            for (slot, txs) in drain(self.block_rx, usize::MAX) {
                warn!("discarded stale block: slot={slot} txs={}", txs.len());
            }

            // Store and dedup incoming transactions
            self.vote_store.insert(drain(&mut self.vote_rx, BATCH_SIZE));
            self.nonvote_store
                .insert(drain(&mut self.nonvote_rx, BATCH_SIZE));

            // Time-multiplex CHECK pipeline so resolve() never sees mixed responses
            let active_store = if !hold && self.vote_store.inflight() != 0 {
                &mut self.vote_store
            } else if self.nonvote_store.inflight() != 0 {
                &mut self.nonvote_store
            } else if !hold && self.vote_store.needs_check() {
                &mut self.vote_store
            } else {
                &mut self.nonvote_store
            };
            active_store.check(self.slot, &mut self.pack_to_worker);
            active_store.resolve(&mut self.worker_to_pack);
        }

        // Ensure any in flight CHECKs are resolved
        while self.vote_store.inflight() != 0 {
            self.progress.poll()?;
            self.vote_store.resolve(&mut self.worker_to_pack);
        }
        while self.nonvote_store.inflight() != 0 {
            self.progress.poll()?;
            self.nonvote_store.resolve(&mut self.worker_to_pack);
        }

        Ok(())
    }

    /// Start leader slot
    fn leader_starting(&mut self) -> Result<()> {
        info!("starting leader slot: slot={}", self.slot);
        // Back-date the start to account for how far into the slot we already are
        let elapsed = SLOT_DURATION * self.progress.last().current_slot_progress as u32 / 100;
        self.leader_tx.send(Some(LeaderNotification {
            slot: self.slot,
            start_time: SystemTime::now() - elapsed,
        }))?;
        // Wait for the bank for our slot, but bail if the window shifts under us
        while self.progress.poll()?.leader_state == LEADER_STARTING
            && self.progress.last().current_slot == self.slot
        {
            self.allocator.clean_remote_free_lists();
            self.vote_store.insert(drain(&mut self.vote_rx, BATCH_SIZE));
            self.nonvote_store
                .insert(drain(&mut self.nonvote_rx, BATCH_SIZE));
        }
        Ok(())
    }

    /// Run a full leader slot
    fn leader_ready(&mut self) -> Result<()> {
        if self.progress.last().leader_state != LEADER_READY
            || self.progress.last().current_slot != self.slot
        {
            debug!(
                "skipping leader_ready: leader_state={} slot={} current_slot={}",
                self.progress.last().leader_state,
                self.slot,
                self.progress.last().current_slot,
            );
            return Ok(());
        }

        if self.progress.last().current_slot_progress < BLOCK_STAGE_TIMEOUT_PERCENT
            && self.wait_for_block()?
        {
            self.block_stage()?;
            self.vote_stage()?;
        } else if self.progress.last().current_slot == self.slot {
            self.fallback_stage()?;
        }

        // Log our next leader slot if we are no longer going to be leader
        if self.progress.last().leader_state == NOT_LEADER {
            info!(
                "waiting for leader slot: current_slot={}, next_leader_slot={}",
                self.progress.last().current_slot,
                self.progress.last().next_leader_slot,
            );
        }
        Ok(())
    }

    /// Build init + crank tip bundle for the current slot and epoch
    fn build_tip_bundle(&mut self) -> Result<()> {
        debug!("building tip bundle for slot {}", self.slot);
        self.pending_tip_bundle.clear();
        let epoch = self.progress.last().epoch;

        // READ request for tip account data
        let tip_manager = self.tip_manager_rx.borrow();
        let message = PackToWorkerMessage {
            flags: pack_message_flags::READ | read_flags::LOAD_DATA,
            max_working_slot: self.slot,
            batch: allocate_batch(
                [
                    tip_manager.tip_payment_config_pubkey(),
                    tip_manager.tip_distribution_config_pubkey(),
                    tip_manager.get_my_tip_distribution_pda(epoch),
                ]
                .map(|pubkey| allocate(pubkey, self.allocator)),
                self.allocator,
            ),
        };

        debug!("submitting READ batch for tip accounts");
        pack_to_worker::send(&mut self.pack_to_worker[0], message)
            .expect("worker should have no inflight requests");

        debug!("waiting for tip account READ response");
        let message = loop {
            if let Some(response) = worker_to_pack::iter(&mut self.worker_to_pack[0]).next() {
                break response;
            };
            if self.progress.poll()?.current_slot_progress >= BLOCK_STAGE_TIMEOUT_PERCENT {
                // If we get here something is wrong with the worker, disconnect
                bail!("tip account READ timeout");
            }
            self.allocator.clean_remote_free_lists();
            self.vote_store.insert(drain(&mut self.vote_rx, BATCH_SIZE));
            self.nonvote_store
                .insert(drain(&mut self.nonvote_rx, BATCH_SIZE));
        };
        // The responses slice is undefined unless processed_code is PROCESSED
        assert_eq!(
            message.processed_code,
            processed_codes::PROCESSED,
            "tip account READ must be processed: processed_code={} slot={}",
            message.processed_code,
            self.slot,
        );
        assert_eq!(
            message.responses.tag, READ_RESPONSE,
            "expected READ response on worker[0]: got tag {}",
            message.responses.tag,
        );

        debug!("building tip bundle");
        let [payment, distribution, pda]: &[ReadResponse; 3] = message
            .responses
            .slice(self.allocator)
            .try_into()
            .expect("READ response count should match batch size of 3");
        let mut tip_account_data = TipAccountData {
            epoch,
            ..Default::default()
        };
        if payment.read_result == read_results::SUCCESS {
            tip_account_data.tip_payment_config_data =
                Some(payment.data.slice(self.allocator).to_vec());
            tip_account_data.tip_payment_config_owner = Some(Pubkey::new_from_array(payment.owner));
        }
        if distribution.read_result == read_results::SUCCESS {
            tip_account_data.tip_distribution_config_data =
                Some(distribution.data.slice(self.allocator).to_vec());
            tip_account_data.tip_distribution_config_owner =
                Some(Pubkey::new_from_array(distribution.owner));
        }
        if pda.read_result == read_results::SUCCESS {
            tip_account_data.tip_distribution_pda_data =
                Some(pda.data.slice(self.allocator).to_vec());
            tip_account_data.tip_distribution_pda_owner = Some(Pubkey::new_from_array(pda.owner));
        }
        message.free_full(self.allocator);

        let fee_info = self.fee_info.load();
        let blockhash = self.progress.blockhash();
        let identity = self.identity_rx.borrow();
        let tip_manager = self.tip_manager_rx.borrow();
        match tip_manager.get_initialize_tip_programs_bundle(
            &tip_account_data,
            blockhash,
            &identity,
        ) {
            Ok(txs) => self.pending_tip_bundle.extend(txs),
            Err(e) => warn!("get_initialize_tip_programs_bundle failed: {e}"),
        }
        match tip_manager.get_tip_programs_crank_bundle(
            &tip_account_data,
            &identity,
            &fee_info,
            blockhash,
        ) {
            Ok(txs) => self.pending_tip_bundle.extend(txs),
            Err(e) => warn!("get_tip_programs_crank_bundle failed: {e}"),
        }
        debug!(
            "built tip bundle with {} transactions",
            self.pending_tip_bundle.len()
        );
        Ok(())
    }

    fn wait_for_block(&mut self) -> Result<bool> {
        debug!("waiting for block: slot={}", self.slot);
        self.build_tip_bundle()?;

        while self.progress.poll()?.current_slot_progress < BLOCK_STAGE_TIMEOUT_PERCENT
            && self.progress.last().current_slot == self.slot
        {
            if let Ok((slot, _)) = self.block_rx.peek() {
                if *slot == self.slot {
                    return Ok(true);
                }
                let (slot, txs) = self
                    .block_rx
                    .pop()
                    .expect("peek confirmed slot is available");
                warn!("discarded stale block: slot={slot} txs={}", txs.len());
            }
            self.allocator.clean_remote_free_lists();
            self.vote_store.insert(drain(&mut self.vote_rx, BATCH_SIZE));
            self.nonvote_store
                .insert(drain(&mut self.nonvote_rx, BATCH_SIZE));
        }
        Ok(false)
    }

    fn block_stage(&mut self) -> Result<()> {
        info!("block received, entering block stage: slot={}", self.slot);

        let allocator = self.allocator;
        if !self.pending_tip_bundle.is_empty() {
            info!(
                "scheduling {} tip-program transactions: slot={}",
                self.pending_tip_bundle.len(),
                self.slot
            );
            self.block_stage.tick(
                self.slot,
                std::iter::once(
                    self.pending_tip_bundle
                        .drain(..)
                        .map(|tx| allocate(&tx, allocator)),
                ),
                &mut self.pack_to_worker,
                &mut self.worker_to_pack,
            );
        }

        while self.progress.poll()?.current_slot_progress < VOTE_STAGE_START_PERCENT
            && self.progress.last().current_slot == self.slot
        {
            self.allocator.clean_remote_free_lists();
            self.block_stage.tick(
                self.slot,
                drain(self.block_rx, usize::MAX)
                    .map(|(_, txs)| txs.into_iter().map(|tx| allocate(&tx, allocator))),
                &mut self.pack_to_worker,
                &mut self.worker_to_pack,
            );
        }
        Ok(())
    }

    fn fallback_stage(&mut self) -> Result<()> {
        info!(
            "no block received, building fallback block: slot={}",
            self.slot
        );
        while self.progress.poll()?.current_slot == self.slot && !self.fallback_stage.done() {
            self.allocator.clean_remote_free_lists();
            if !self.vote_store.is_empty() {
                self.fallback_stage.tick(
                    self.slot,
                    self.vote_store
                        .drain(self.pack_to_worker.len() * BATCH_SIZE),
                    &mut self.pack_to_worker,
                    &mut self.worker_to_pack,
                );
            } else if !self.vote_rx.is_empty() {
                self.fallback_stage.tick(
                    self.slot,
                    drain(&mut self.vote_rx, usize::MAX),
                    &mut self.pack_to_worker,
                    &mut self.worker_to_pack,
                );
            } else if !self.nonvote_store.is_empty() && !self.fallback_stage.backpressured() {
                self.fallback_stage.tick(
                    self.slot,
                    self.nonvote_store.drain(BATCH_SIZE),
                    &mut self.pack_to_worker,
                    &mut self.worker_to_pack,
                );
            } else if !self.nonvote_rx.is_empty() && !self.fallback_stage.backpressured() {
                self.fallback_stage.tick(
                    self.slot,
                    drain(&mut self.nonvote_rx, BATCH_SIZE),
                    &mut self.pack_to_worker,
                    &mut self.worker_to_pack,
                );
            } else {
                self.fallback_stage.tick(
                    self.slot,
                    std::iter::empty(),
                    &mut self.pack_to_worker,
                    &mut self.worker_to_pack,
                );
            }
        }

        self.fallback_stage.reset(&mut self.worker_to_pack)?;

        while self.progress.poll()?.current_slot == self.slot {
            self.allocator.clean_remote_free_lists();
            self.vote_store.insert(drain(&mut self.vote_rx, BATCH_SIZE));
            self.nonvote_store
                .insert(drain(&mut self.nonvote_rx, BATCH_SIZE));
        }
        Ok(())
    }

    fn vote_stage(&mut self) -> Result<()> {
        info!("entering vote stage: slot={}", self.slot);
        self.block_stage.vote_stage();
        while self.progress.poll()?.current_slot == self.slot {
            self.allocator.clean_remote_free_lists();
            self.block_stage.tick(
                self.slot,
                self.vote_store
                    .drain(BATCH_SIZE)
                    .chain(drain(&mut self.vote_rx, BATCH_SIZE))
                    .map(std::iter::once),
                &mut self.pack_to_worker,
                &mut self.worker_to_pack,
            );
        }

        self.vote_store
            .insert(self.block_stage.reset(&mut self.worker_to_pack)?);

        Ok(())
    }
}
