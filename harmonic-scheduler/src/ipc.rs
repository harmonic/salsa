//! IPC client for communicating with the Agave validator.
//!
//! Wraps the shared-memory queues (shaq) and allocators established
//! during the IPC handshake. Provides clean methods for polling
//! progress, reading bank info, managing TpuToPackMessage flow,
//! and sending control messages (e.g., proxy TPU updates).

use {
    crate::tpu_proxy_client::ProxyTpuInfo,
    agave_scheduler_bindings::{
        ControlMessage, NOT_LEADER, ProgressMessage, TpuToPackMessage, control_tags,
    },
    agave_scheduling_utils::handshake::{
        ClientLogon,
        client::{ClientWorkerSession, connect},
    },
    log::{error, info},
    rts_alloc::Allocator,
    std::time::Duration,
};

/// IPC connection to the Agave validator.
///
/// Owns all shared-memory queues and allocators. Provides methods
/// for polling validator state and sending control messages.
pub struct IpcClient {
    /// Allocator for batch regions and relayer transaction copies.
    scheduling_allocator: Allocator,
    /// Allocator for freeing validator-allocated TpuToPackMessage memory.
    tpu_to_pack_allocator: Allocator,
    /// Slot progress updates from the validator.
    progress_tracker: shaq::Consumer<ProgressMessage>,
    /// Transactions from the validator's TPU listener.
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    /// Control messages to the validator (proxy TPU updates).
    control: shaq::Producer<ControlMessage>,
    /// Per-worker IPC sessions.
    workers: Vec<ClientWorkerSession>,
    /// Last observed progress message.
    last_progress: ProgressMessage,
    /// Last observed recent blockhash.
    last_blockhash: [u8; 32],
    /// Slot of the last observed blockhash.
    last_blockhash_slot: u64,
}

impl IpcClient {
    /// Connect to the validator via IPC. Retries with backoff until successful.
    pub fn connect_blocking(socket_path: &str, num_workers: usize) -> Self {
        loop {
            match Self::try_connect(socket_path, num_workers) {
                Ok(client) => return client,
                Err(e) => {
                    error!("IPC connect failed: {e}, retrying in 5s");
                    std::thread::sleep(Duration::from_secs(5));
                }
            }
        }
    }

    fn try_connect(
        socket_path: &str,
        num_workers: usize,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let session = connect(
            socket_path,
            ClientLogon {
                worker_count: num_workers,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 2,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                control_capacity: 16,
            },
            Duration::from_secs(30),
        )?;

        let mut alloc_iter = session.allocators.into_iter();
        let scheduling_allocator = alloc_iter.next().ok_or("no scheduling allocator")?;
        let tpu_to_pack_allocator = alloc_iter.next().ok_or("no tpu_to_pack allocator")?;

        info!("IPC connected: {} workers", session.workers.len());

        Ok(Self {
            scheduling_allocator,
            tpu_to_pack_allocator,
            progress_tracker: session.progress_tracker,
            tpu_to_pack: session.tpu_to_pack,
            control: session.control,
            workers: session.workers,
            last_progress: ProgressMessage {
                leader_state: NOT_LEADER,
                current_slot: 0,
                next_leader_slot: u64::MAX,
                leader_range_end: u64::MAX,
                remaining_cost_units: 0,
                current_slot_progress: 0,
                recent_blockhash: [0u8; 32],
            },
            last_blockhash: [0u8; 32],
            last_blockhash_slot: 0,
        })
    }

    /// Poll the progress queue, caching the latest progress and blockhash.
    pub fn poll_progress(&mut self) -> ProgressMessage {
        self.progress_tracker.sync();
        while let Some(msg) = self.progress_tracker.try_read() {
            self.last_progress = *msg;
            if msg.recent_blockhash != [0u8; 32] {
                self.last_blockhash = msg.recent_blockhash;
                self.last_blockhash_slot = msg.current_slot;
            }
        }
        self.progress_tracker.finalize();
        self.last_progress
    }

    /// Send a SET_PROXY_TPU control message to the validator.
    pub fn send_set_proxy_tpu(&mut self, info: &ProxyTpuInfo) {
        if let Some((addr, port)) = info.proxy_tpu() {
            let msg = ControlMessage {
                tag: control_tags::SET_PROXY_TPU,
                proxy_tpu_address: addr,
                proxy_tpu_port: port,
            };
            self.control.sync();
            if self.control.try_write(msg).is_ok() {
                self.control.commit();
                info!("sent SET_PROXY_TPU to validator");
            }
        }
    }

    /// Send a RESTORE_ORIGINAL_TPU control message to the validator.
    pub fn send_restore_tpu(&mut self) {
        let msg = ControlMessage {
            tag: control_tags::RESTORE_ORIGINAL_TPU,
            proxy_tpu_address: [0; 16],
            proxy_tpu_port: 0,
        };
        self.control.sync();
        if self.control.try_write(msg).is_ok() {
            self.control.commit();
            info!("sent RESTORE_ORIGINAL_TPU to validator");
        }
    }

    /// Drain TpuToPackMessage queue, freeing all memory (used when not leader).
    pub fn drain_tpu_to_pack_discard(&mut self) {
        self.tpu_to_pack.sync();
        while let Some(msg) = self.tpu_to_pack.try_read() {
            unsafe {
                let ptr = self
                    .tpu_to_pack_allocator
                    .ptr_from_offset(msg.transaction.offset);
                self.tpu_to_pack_allocator.free(ptr);
            }
        }
        self.tpu_to_pack.finalize();
    }

    /// Returns mutable references to the TpuToPackMessage consumer and both
    /// allocators simultaneously, avoiding borrow-checker issues with split borrows.
    pub fn tpu_to_pack_refs(
        &mut self,
    ) -> (
        &mut shaq::Consumer<TpuToPackMessage>,
        &Allocator,
        &Allocator,
    ) {
        (
            &mut self.tpu_to_pack,
            &self.tpu_to_pack_allocator,
            &self.scheduling_allocator,
        )
    }

    /// Returns mutable worker sessions and an immutable reference to the
    /// scheduling allocator simultaneously, avoiding borrow-checker issues.
    pub fn workers_and_allocator(&mut self) -> (&mut [ClientWorkerSession], &Allocator) {
        (&mut self.workers, &self.scheduling_allocator)
    }

    // --- Accessors ---

    pub fn leader_state(&self) -> u8 {
        self.last_progress.leader_state
    }
    pub fn current_slot(&self) -> u64 {
        self.last_progress.current_slot
    }
    pub fn current_slot_progress(&self) -> u8 {
        self.last_progress.current_slot_progress
    }
    pub fn next_leader_slot(&self) -> u64 {
        self.last_progress.next_leader_slot
    }
    pub fn leader_range_end(&self) -> u64 {
        self.last_progress.leader_range_end
    }
    pub fn recent_blockhash(&self) -> &[u8; 32] {
        &self.last_blockhash
    }
    pub fn scheduling_allocator(&self) -> &Allocator {
        &self.scheduling_allocator
    }
}
