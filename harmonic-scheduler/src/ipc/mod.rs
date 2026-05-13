//! IPC client for the validator scheduling surface

pub mod pack_to_worker;
pub mod progress;
pub mod shmem;
pub mod tpu_to_pack;
pub mod worker_to_pack;

use crate::consts::{
    ALLOCATOR_SIZE, NUM_ALLOCATOR_HANDLES, PACK_TO_WORKER_CAPACITY, PROGRESS_TRACKER_CAPACITY,
    TPU_TO_PACK_CAPACITY, WORKER_TO_PACK_CAPACITY,
};
use agave_scheduling_utils::handshake::ClientLogon;
use agave_scheduling_utils::handshake::client::{
    ClientHandshakeError, ClientSession, connect as handshake_connect,
};
use log::{debug, info};
pub use progress::ProgressTracker;
use std::path::Path;
use std::time::Duration;

/// Timeout per IPC connection attempt
const IPC_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Connect to the validator via IPC
pub fn connect(
    socket_path: &Path,
    num_workers: usize,
) -> Result<ClientSession, ClientHandshakeError> {
    debug!("connecting to validator IPC at {}", socket_path.display());
    let session = handshake_connect(
        socket_path,
        ClientLogon {
            worker_count: num_workers,
            allocator_size: ALLOCATOR_SIZE,
            allocator_handles: NUM_ALLOCATOR_HANDLES,
            tpu_to_pack_capacity: TPU_TO_PACK_CAPACITY,
            progress_tracker_capacity: PROGRESS_TRACKER_CAPACITY,
            pack_to_worker_capacity: PACK_TO_WORKER_CAPACITY,
            worker_to_pack_capacity: WORKER_TO_PACK_CAPACITY,
            flags: 0, // no flags currently defined
        },
        IPC_HANDSHAKE_TIMEOUT,
    )?;
    info!("connected to validator IPC: workers={num_workers}");
    Ok(session)
}
