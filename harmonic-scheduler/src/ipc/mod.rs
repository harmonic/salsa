//! IPC client for the validator scheduling surface

pub mod pack_to_worker;
pub mod progress;
pub mod shmem;
pub mod tpu_to_pack;
pub mod worker_to_pack;

use crate::error::ErrorExt;
use agave_scheduling_utils::handshake::ClientLogon;
use agave_scheduling_utils::handshake::client::{ClientSession, connect as handshake_connect};
use log::{debug, info, warn};
pub use progress::ProgressTracker;
use std::net::SocketAddrV4;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::sleep;
use std::time::Duration;

/// Shared memory allocator size (1 GiB)
const ALLOCATOR_SIZE: usize = 1024 * 1024 * 1024;
/// tpu_to_pack ring buffer capacity
const TPU_TO_PACK_CAPACITY: usize = 8192;
/// Progress tracker ring buffer capacity
const PROGRESS_TRACKER_CAPACITY: usize = 64;
/// pack_to_worker depth: tight bound on dispatched-but-unexecuted work
const PACK_TO_WORKER_CAPACITY: usize = 8;
/// worker_to_pack depth: PACK_TO_WORKER_CAPACITY + 1 in-flight, rounded up to shaq's next power of two
const WORKER_TO_PACK_CAPACITY: usize = 16;
/// IPC handshake timeout
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Backoff between IPC connection attempts
const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// Connect to the validator via IPC, retrying on failure
pub fn connect(
    socket_path: &Path,
    num_workers: usize,
    num_allocator_handles: usize,
    tpu_override: SocketAddrV4,
    exit: &AtomicBool,
) -> Option<ClientSession> {
    while !exit.load(Ordering::Relaxed) {
        debug!("connecting to validator IPC at {}", socket_path.display());
        match handshake_connect(
            socket_path,
            ClientLogon {
                worker_count: num_workers,
                allocator_size: ALLOCATOR_SIZE,
                allocator_handles: num_allocator_handles,
                tpu_to_pack_capacity: TPU_TO_PACK_CAPACITY,
                progress_tracker_capacity: PROGRESS_TRACKER_CAPACITY,
                pack_to_worker_capacity: PACK_TO_WORKER_CAPACITY,
                worker_to_pack_capacity: WORKER_TO_PACK_CAPACITY,
                flags: 0, // no flags currently defined
                tpu_override_addr: tpu_override.ip().octets(),
                tpu_override_port: tpu_override.port(),
            },
            HANDSHAKE_TIMEOUT,
        ) {
            Ok(session) => {
                info!(
                    "IPC connected to {} with {num_workers} workers, tpu_override={tpu_override}",
                    socket_path.display(),
                );
                return Some(session);
            }
            Err(e) => {
                warn!("IPC connect failed: {}", e.chain());
                sleep(CONNECTION_BACKOFF);
            }
        }
    }
    None
}
