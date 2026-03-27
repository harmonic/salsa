use {
    crate::{
        auction_client::AuctionBlock,
        crank::CrankGenerator,
        fallback,
        ingest::Ingest,
        ipc::IpcClient,
        scheduling::SchedulingEngine,
        tpu_proxy_client::{ProxyTpuInfo, RelayerPacket},
    },
    agave_scheduler_bindings::{LEADER_READY, LEADER_STARTING, NOT_LEADER},
    log::{info, warn},
    std::time::{Duration, SystemTime},
};

// --- Constants ---

/// Non-vote scheduling stops at this slot progress percentage.
const NONVOTE_CUTOFF_PERCENT: u8 = 87;

/// If no auction block arrives by this slot progress percentage, switch to fallback.
const AUCTION_TIMEOUT_PERCENT: u8 = 50;

/// Control events from the relayer connection task.
pub enum ControlEvent {
    /// Relayer connected, providing proxy TPU info for the validator.
    RelayerConnected(ProxyTpuInfo),
    /// Relayer disconnected; restore original TPU on the validator.
    RelayerDisconnected,
}

/// Owns all scheduling state: IPC connection, scheduling engine, ingestion
/// pipeline, and channels. Methods on `self` decompose the main loop.
pub struct Scheduler {
    ipc: IpcClient,
    engine: SchedulingEngine,
    ingest: Ingest,
    packet_rx: crossbeam_channel::Receiver<Vec<RelayerPacket>>,
    block_rx: crossbeam_channel::Receiver<AuctionBlock>,
    control_rx: crossbeam_channel::Receiver<ControlEvent>,
    leader_tx: tokio::sync::mpsc::UnboundedSender<(u64, SystemTime)>,
    crank: Option<CrankGenerator>,
    /// Last leader slot that was notified to the auction client.
    last_notified_leader_slot: u64,
    /// Whether an auction block has been received for the current leader slot.
    received_auction_block: bool,
    /// The slot at which the current leader run started.
    leader_entry_slot: u64,
}

impl Scheduler {
    /// Create a new Scheduler. Blocks until IPC connects to the validator.
    pub fn new(
        validator_socket: &str,
        num_workers: usize,
        packet_rx: crossbeam_channel::Receiver<Vec<RelayerPacket>>,
        block_rx: crossbeam_channel::Receiver<AuctionBlock>,
        control_rx: crossbeam_channel::Receiver<ControlEvent>,
        leader_tx: tokio::sync::mpsc::UnboundedSender<(u64, SystemTime)>,
        crank: Option<CrankGenerator>,
    ) -> Self {
        let ipc = IpcClient::connect_blocking(validator_socket, num_workers);
        let engine = SchedulingEngine::new(num_workers);
        let ingest = Ingest::new();

        Self {
            ipc,
            engine,
            ingest,
            packet_rx,
            block_rx,
            control_rx,
            leader_tx,
            crank,
            last_notified_leader_slot: u64::MAX,
            received_auction_block: false,
            leader_entry_slot: 0,
        }
    }

    /// Run the scheduler loop forever. Does not return.
    pub fn run(mut self) -> ! {
        loop {
            self.handle_control_events();
            self.poll_validator();

            let leader_state = self.ipc.leader_state();
            match leader_state {
                NOT_LEADER => self.handle_not_leader(),
                LEADER_STARTING => self.handle_leader_starting(),
                LEADER_READY => self.handle_leader_slot(),
                _ => {
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Drain control events from the relayer connection task.
    fn handle_control_events(&mut self) {
        while let Ok(event) = self.control_rx.try_recv() {
            match event {
                ControlEvent::RelayerConnected(info) => {
                    info!("relayer connected, sending proxy TPU to validator");
                    self.ipc.send_set_proxy_tpu(&info);
                }
                ControlEvent::RelayerDisconnected => {
                    info!("relayer disconnected, restoring original TPU");
                    self.ipc.send_restore_tpu();
                }
            }
        }
    }

    /// Drain bank info and poll progress from the validator.
    fn poll_validator(&mut self) {
        self.ipc.poll_progress();
    }

    /// Handle NOT_LEADER state: discard tpu_to_pack, clear when leader range ends.
    fn handle_not_leader(&mut self) {
        let current_slot = self.ipc.current_slot();
        let leader_range_end = self.ipc.leader_range_end();
        let next_leader_slot = self.ipc.next_leader_slot();

        // Clear buffers when leader range ends.
        if current_slot > leader_range_end && leader_range_end != 0 && next_leader_slot == u64::MAX
        {
            self.engine.clear_and_free(self.ipc.scheduling_allocator());
            self.ingest.clear();
            self.drain_and_discard_channels();
        }

        // Free any TpuToPackMessage memory to prevent queue backup.
        self.ipc.drain_tpu_to_pack_discard();
        std::thread::sleep(Duration::from_micros(100));
    }

    /// Handle LEADER_STARTING state: notify auction of upcoming leader slot.
    fn handle_leader_starting(&mut self) {
        self.notify_auction_if_needed();
        std::thread::yield_now();
    }

    /// Handle LEADER_READY state: run the leader slot loop.
    fn handle_leader_slot(&mut self) {
        let entry_slot = self.ipc.current_slot();
        self.leader_entry_slot = entry_slot;
        self.received_auction_block = false;

        loop {
            self.handle_control_events();
            self.ipc.poll_progress();

            // Slot changed or leader lost.
            if self.ipc.current_slot() != entry_slot || self.ipc.leader_state() == NOT_LEADER {
                return;
            }

            // Process worker responses.
            {
                let (workers, allocator) = self.ipc.workers_and_allocator();
                self.engine.drain_worker_responses(workers, allocator);
            }

            let progress = self.ipc.current_slot_progress();

            // Vote phase (last 13% of slot).
            if progress >= NONVOTE_CUTOFF_PERCENT {
                self.handle_vote_phase();
                std::thread::yield_now();
                continue;
            }

            // Auction phase: try to receive block.
            if !self.received_auction_block {
                self.handle_auction_phase();
            }

            // If no auction block by timeout, switch to fallback.
            if !self.received_auction_block && progress >= AUCTION_TIMEOUT_PERCENT {
                self.received_auction_block = true;
            }

            // Fallback: ingest from relayer + TpuToPackMessage.
            if self.received_auction_block || progress >= AUCTION_TIMEOUT_PERCENT {
                self.handle_fallback_phase();
            }

            // Advance the engine pipeline.
            self.advance_pipeline();

            // Notify auction of upcoming leader slot.
            self.notify_auction_if_needed();

            std::thread::yield_now();
        }
    }

    /// Vote phase: wait for non-vote inflight to drain, then schedule votes.
    fn handle_vote_phase(&mut self) {
        if !self.engine.has_inflight() {
            let allocator = self.ipc.scheduling_allocator();
            self.ingest
                .process_relayer_votes(&self.packet_rx, &mut self.engine, allocator);
            self.engine.admit_resolved_prefix();
            let slot = self.leader_entry_slot;
            let (workers, alloc) = self.ipc.workers_and_allocator();
            self.engine.flush_ready_work(workers, alloc, slot);
        }
    }

    /// Auction phase: try to receive auction blocks.
    fn handle_auction_phase(&mut self) {
        while let Ok(block) = self.block_rx.try_recv() {
            self.received_auction_block = true;
            self.ingest_auction_block(&block);
        }
    }

    /// Fallback phase: ingest from relayer + TpuToPackMessage.
    fn handle_fallback_phase(&mut self) {
        let allocator = self.ipc.scheduling_allocator();
        self.ingest
            .process_relayer_nonvotes(&self.packet_rx, &mut self.engine, allocator);
        self.ingest
            .process_tpu_to_pack(&mut self.ipc, &mut self.engine);
    }

    /// Advance the engine pipeline: admit, resolve, flush.
    fn advance_pipeline(&mut self) {
        let slot = self.leader_entry_slot;
        self.engine.admit_resolved_prefix();
        {
            let (workers, allocator) = self.ipc.workers_and_allocator();
            self.engine.pump_resolve_stage(workers, allocator, slot);
        }
        {
            let (workers, allocator) = self.ipc.workers_and_allocator();
            self.engine.flush_ready_work(workers, allocator, slot);
        }
    }

    /// Notify the auction client of an upcoming leader slot if not already notified.
    fn notify_auction_if_needed(&mut self) {
        let next_leader_slot = self.ipc.next_leader_slot();
        if next_leader_slot != u64::MAX && next_leader_slot != self.last_notified_leader_slot {
            self.last_notified_leader_slot = next_leader_slot;
            let _ = self.leader_tx.send((next_leader_slot, SystemTime::now()));
        }
    }

    /// Ingest an auction block: prepend crank transactions, then block transactions.
    fn ingest_auction_block(&mut self, block: &AuctionBlock) {
        if block.slot != self.leader_entry_slot {
            warn!(
                "auction block slot {} != current slot {}, skipping",
                block.slot, self.leader_entry_slot
            );
            return;
        }

        // Prepend crank transactions if available.
        if let Some(crank) = &self.crank {
            let crank_txs =
                crank.generate_crank(self.leader_entry_slot, self.ipc.recent_blockhash());
            for tx_bytes in &crank_txs {
                if let Some(region) =
                    fallback::allocate_transaction(tx_bytes, self.ipc.scheduling_allocator())
                {
                    let _ = self.engine.submit(region, tx_bytes);
                }
            }
        }

        for tx_bytes in &block.transactions {
            if let Some(region) =
                fallback::allocate_transaction(tx_bytes, self.ipc.scheduling_allocator())
            {
                let _ = self.engine.submit(region, tx_bytes);
            }
        }
    }

    /// Drain all input channels, discarding their contents.
    fn drain_and_discard_channels(&self) {
        while self.packet_rx.try_recv().is_ok() {}
        while self.block_rx.try_recv().is_ok() {}
    }
}
