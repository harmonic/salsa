//! Transaction scheduler: drives the leader-slot state machine

use super::schedule::Schedule;
use crate::block_engine::{BlockBuilderFeeInfo, LeaderNotification};
use crate::ipc::shmem::{Free, Slice, allocate_batch, allocate_transaction};
use crate::ipc::{ProgressTracker, pack_to_worker, worker_to_pack};
use crate::tip_manager::{TipAccountData, TipManager};
use agave_scheduler_bindings::pack_message_flags::read_flags;
use agave_scheduler_bindings::worker_message_types::{ReadResponse, read_results};
use agave_scheduler_bindings::{
    LEADER_STARTING, MAX_TRANSACTIONS_PER_MESSAGE, NOT_LEADER, PackToWorkerMessage,
    SharableTransactionRegion, WorkerToPackMessage, pack_message_flags, processed_codes,
    worker_message_types,
};
use agave_scheduling_utils::handshake::client::ClientWorkerSession;
use arc_swap::ArcSwap;
use log::{debug, info, trace, warn};
use rts_alloc::Allocator;
use smallvec::SmallVec;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::SystemTime;
use tokio::sync::watch;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Slot % after which we abandon the block engine and fall back to nonvotes
const BLOCK_ENGINE_TIMEOUT_PERCENT: u8 = 50;
/// Slot % after which we only admit votes (tail reserved for vote ingestion)
const NONVOTE_CUTOFF_PERCENT: u8 = 87;

/// Drives the leader-slot state machine and worker IPC
pub struct Scheduler<'a> {
    progress: ProgressTracker,
    pack_to_worker: Vec<shaq::Producer<PackToWorkerMessage>>,
    worker_to_pack: Vec<shaq::Consumer<WorkerToPackMessage>>,
    allocator: &'a Allocator,
    vote_rx: rtrb::Consumer<SharableTransactionRegion>,
    nonvote_rx: rtrb::Consumer<SharableTransactionRegion>,
    block_rx: &'a mut rtrb::Consumer<Vec<u8>>,
    leader_tx: &'a watch::Sender<LeaderNotification>,
    schedule: Schedule<'a>,
    slot: u64,
    pending_tip_bundle: SmallVec<[Vec<u8>; 4]>,
    tip_manager: &'a TipManager,
    identity: Arc<Keypair>,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    exit: Arc<AtomicBool>,
}

impl<'a> Scheduler<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        progress: ProgressTracker,
        workers: Vec<ClientWorkerSession>,
        allocator: &'a Allocator,
        vote_rx: rtrb::Consumer<SharableTransactionRegion>,
        nonvote_rx: rtrb::Consumer<SharableTransactionRegion>,
        block_rx: &'a mut rtrb::Consumer<Vec<u8>>,
        leader_tx: &'a watch::Sender<LeaderNotification>,
        tip_manager: &'a TipManager,
        identity: Arc<Keypair>,
        fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
        exit: Arc<AtomicBool>,
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
            schedule: Schedule::new(num_workers, allocator),
            slot: 0,
            pending_tip_bundle: SmallVec::new(),
            tip_manager,
            identity,
            fee_info,
            exit,
        }
    }

    pub fn run(&mut self) -> Result<()> {
        // Wait for first progress message
        while self.progress.poll()?.current_slot == 0 {
            std::hint::spin_loop();
        }

        while !self.exit.load(Ordering::Relaxed) {
            self.not_leader()?;
            self.leader_starting()?;
            self.leader_ready()?;
        }
        Ok(())
    }

    /// Idle and service queues while not leader
    fn not_leader(&mut self) -> Result<()> {
        info!(
            "waiting for next leader slot {}",
            self.progress.last().next_leader_slot
        );
        while !self.exit.load(Ordering::Relaxed) && self.progress.poll()?.leader_state == NOT_LEADER
        {
            while self.block_rx.pop().is_ok() {}

            // TODO: more intelligent cacheing / checking of these transactions
            let p = self.progress.last();
            if p.next_leader_slot > p.current_slot.saturating_add(4) {
                for _ in 0..MAX_TRANSACTIONS_PER_MESSAGE {
                    let Ok(region) = self.vote_rx.pop() else {
                        break;
                    };
                    region.free(self.allocator);
                }
                for _ in 0..MAX_TRANSACTIONS_PER_MESSAGE {
                    let Ok(region) = self.nonvote_rx.pop() else {
                        break;
                    };
                    region.free(self.allocator);
                }
            }
        }
        if self.exit.load(Ordering::Relaxed) {
            Err("exit signal received".into())
        } else {
            Ok(())
        }
    }

    /// Start leader slot
    fn leader_starting(&mut self) -> Result<()> {
        self.slot = self.progress.last().current_slot;
        info!("starting leader slot {}", self.slot);
        self.leader_tx.send(LeaderNotification {
            slot: self.slot,
            start_time: SystemTime::now(),
        })?;
        self.schedule.reset(self.slot);
        self.build_tip_bundle()?;
        while !self.exit.load(Ordering::Relaxed)
            && self.progress.poll()?.leader_state == LEADER_STARTING
        {
            std::hint::spin_loop();
        }
        Ok(())
    }

    /// Build init + crank tip bundle for the current slot and epoch
    fn build_tip_bundle(&mut self) -> Result<()> {
        debug!("building tip bundle for slot {}", self.slot);
        self.pending_tip_bundle.clear();
        let epoch = self.progress.last().epoch;

        // Send READ request for tip account data
        let batch = allocate_batch(
            [
                self.tip_manager.tip_payment_config_pubkey(),
                self.tip_manager.tip_distribution_config_pubkey(),
                self.tip_manager.get_my_tip_distribution_pda(epoch),
            ]
            .map(|pk| allocate_transaction(pk, self.allocator).expect("transaction size is valid")),
            self.allocator,
        )
        .expect("batch size is valid");
        let message = PackToWorkerMessage {
            flags: pack_message_flags::READ | read_flags::LOAD_DATA,
            max_working_slot: self.slot,
            batch,
        };
        for p in self.pack_to_worker.iter_mut() {
            p.sync();
        }
        let worker = self
            .pack_to_worker
            .iter()
            .enumerate()
            .max_by_key(|(_, p)| p.capacity().saturating_sub(p.len()))
            .expect("pack_to_worker non-empty")
            .0;
        debug!("submitting READ batch for tip accounts to worker {worker}");
        if pack_to_worker::send(&mut self.pack_to_worker[worker], message).is_err() {
            warn!("failed to send READ batch for tip accounts");
            batch.free_full(self.allocator);
            return Ok(());
        }

        debug!("waiting for tip account data READ response");
        let mut response = None;
        let consumer = &mut self.worker_to_pack[worker];
        'wait: while !self.exit.load(Ordering::Relaxed)
            && self.progress.poll()?.current_slot_progress < BLOCK_ENGINE_TIMEOUT_PERCENT
        {
            self.allocator.clean_remote_free_lists();
            for message in worker_to_pack::iter(consumer).take(MAX_TRANSACTIONS_PER_MESSAGE) {
                if message.processed_code == processed_codes::PROCESSED
                    && message.responses.tag == worker_message_types::READ_RESPONSE
                {
                    response = Some(message);
                    break 'wait;
                }
                trace!(
                    "freeing stale response, processed_code={}",
                    message.processed_code
                );
                message.free_full(self.allocator);
            }
        }
        let Some(message) = response else {
            warn!("timed out waiting for tip account data");
            return Ok(());
        };

        let responses: &[ReadResponse] = message.responses.slice(self.allocator);
        let [payment, distribution, pda]: &[ReadResponse; 3] = responses
            .try_into()
            .expect("READ response count matches batch size of 3");

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
        match self.tip_manager.get_initialize_tip_programs_bundle(
            &tip_account_data,
            blockhash,
            &self.identity,
        ) {
            Ok(txs) => self.pending_tip_bundle.extend(txs),
            Err(e) => warn!("get_initialize_tip_programs_bundle failed: {e}"),
        }
        match self.tip_manager.get_tip_programs_crank_bundle(
            &tip_account_data,
            &self.identity,
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

    /// Run a full leader slot
    fn leader_ready(&mut self) -> Result<()> {
        if self.wait_for_block()? {
            self.block_stage()?;
        } else {
            self.fallback_stage()?;
        }
        self.vote_stage()?;
        Ok(())
    }

    fn wait_for_block(&mut self) -> Result<bool> {
        debug!("waiting for block for slot {}", self.slot);
        while self.progress.poll()?.current_slot_progress < BLOCK_ENGINE_TIMEOUT_PERCENT {
            self.process_responses();
            if self.block_rx.peek().is_ok() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn block_stage(&mut self) -> Result<()> {
        info!(
            "block received for slot {}, entering block stage",
            self.slot
        );
        info!(
            "scheduling {} tip-program init/crank transactions",
            self.pending_tip_bundle.len()
        );
        for tx in self.pending_tip_bundle.drain(..) {
            self.schedule.insert(
                allocate_transaction(&tx, self.allocator).expect("tip transaction is well-formed"),
            );
        }

        while self.progress.poll()?.current_slot_progress < NONVOTE_CUTOFF_PERCENT {
            self.process_responses();
            self.schedule.schedule(&mut self.pack_to_worker);
            if self.schedule.at_capacity() {
                continue;
            }
            for _ in 0..MAX_TRANSACTIONS_PER_MESSAGE {
                let Ok(tx) = self.block_rx.pop() else {
                    break;
                };
                let Some(tx_ref) = allocate_transaction(&tx, self.allocator) else {
                    warn!("dropping malformed block tx: {} bytes", tx.len());
                    continue;
                };
                self.schedule.insert(tx_ref);
            }
        }
        Ok(())
    }

    fn fallback_stage(&mut self) -> Result<()> {
        info!(
            "block timeout for slot {}, falling back to nonvotes",
            self.slot
        );
        while self.progress.poll()?.current_slot_progress < NONVOTE_CUTOFF_PERCENT {
            self.process_responses();
            self.schedule.schedule(&mut self.pack_to_worker);
            if self.schedule.at_capacity() {
                continue;
            }
            for _ in 0..MAX_TRANSACTIONS_PER_MESSAGE {
                let Ok(region) = self.nonvote_rx.pop() else {
                    break;
                };
                self.schedule.insert(region);
            }
        }
        Ok(())
    }

    fn vote_stage(&mut self) -> Result<()> {
        info!("entering vote stage for slot {}", self.slot);
        while self.progress.poll()?.current_slot == self.slot {
            self.process_responses();
            self.schedule.schedule(&mut self.pack_to_worker);
            if self.schedule.at_capacity() {
                continue;
            }
            for _ in 0..MAX_TRANSACTIONS_PER_MESSAGE {
                let Ok(region) = self.vote_rx.pop() else {
                    break;
                };
                self.schedule.insert(region);
            }
        }
        Ok(())
    }

    fn process_responses(&mut self) {
        self.allocator.clean_remote_free_lists();
        for (w, consumer) in self.worker_to_pack.iter_mut().enumerate() {
            for msg in worker_to_pack::iter(consumer).take(MAX_TRANSACTIONS_PER_MESSAGE) {
                self.schedule.resolve(w, &msg);
            }
        }
    }
}
