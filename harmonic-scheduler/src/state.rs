//! Subsystem session-liveness flags

use std::sync::atomic::{AtomicU64, Ordering};

static FLAGS: AtomicU64 = AtomicU64::new(0);

// Individual task / thread active bits
const ADMIN_RPC_ACTIVE: u64 = 1 << 0;
const REMOTE_TPU_ACTIVE: u64 = 1 << 1;
const BLOCK_ENGINE_ACTIVE: u64 = 1 << 2;
const TPU_ACTIVE: u64 = 1 << 3;
const SCHEDULER_ACTIVE: u64 = 1 << 4;

// Composite bitmasks: required-active bits for each composite predicate
const VALIDATOR_DEPS: u64 = ADMIN_RPC_ACTIVE | REMOTE_TPU_ACTIVE | BLOCK_ENGINE_ACTIVE;
const TPU_DEPS: u64 = SCHEDULER_ACTIVE;
const SCHEDULER_DEPS: u64 = VALIDATOR_DEPS | TPU_ACTIVE;
const TPU_OVERRIDE_DEPS: u64 =
    REMOTE_TPU_ACTIVE | BLOCK_ENGINE_ACTIVE | TPU_ACTIVE | SCHEDULER_ACTIVE;

/// Clears the named active bit when the guard is dropped
#[must_use = "the active bit clears immediately on drop"]
pub struct ActiveGuard(u64);

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        FLAGS.fetch_and(!self.0, Ordering::Release);
    }
}

/// Generate an RAII setter that sets `$bit` and clears it on drop
macro_rules! active {
    ($set:ident, $bit:expr) => {
        pub fn $set() -> ActiveGuard {
            FLAGS.fetch_or($bit, Ordering::Release);
            ActiveGuard($bit)
        }
    };
}

/// Generate a getter that returns true when any bit in `$bits` is unset
macro_rules! exit {
    ($name:ident, $bits:expr) => {
        pub fn $name() -> bool {
            FLAGS.load(Ordering::Acquire) & ($bits) != ($bits)
        }
    };
}

active!(admin_rpc_active, ADMIN_RPC_ACTIVE);
active!(remote_tpu_active, REMOTE_TPU_ACTIVE);
active!(block_engine_active, BLOCK_ENGINE_ACTIVE);
active!(tpu_active, TPU_ACTIVE);
active!(scheduler_active, SCHEDULER_ACTIVE);

exit!(validator_exit, VALIDATOR_DEPS);
exit!(tpu_exit, TPU_DEPS);
exit!(scheduler_exit, SCHEDULER_DEPS);
exit!(tpu_override_exit, TPU_OVERRIDE_DEPS);
