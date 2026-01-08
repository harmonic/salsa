//! Synchronize whole block and vanilla schedulers.
//!
//! Every slot, there are two stages: delegation and fallback.
//! During delegation stage, the block scheduler awaits a whole block.
//! If a whole block is received by the end of the stage, it is scheduled.
//! Otherwise, vanilla scheduling takes over during fallback stage.
//!
//! The state is stored in a single atomic u64:
//! - Top bit (bit 63): 1 = claimed by block, 0 = claimed by vanilla
//! - Lower 63 bits: slot number
//! - Sentinel value (u64::MAX) indicates no slot has been scheduled yet

use {
    log::info,
    std::sync::atomic::{AtomicU64, Ordering},
};

/// Top bit indicates block claimed (1) vs vanilla claimed (0)
const BLOCK_CLAIMED_BIT: u64 = 1 << 63;
/// Mask to extract the slot number (lower 63 bits)
const SLOT_MASK: u64 = !BLOCK_CLAIMED_BIT;
/// Sentinel value - all bits set, indicates no slot scheduled yet.
/// Note: get_slot(SENTINEL) = 0x7FFFFFFFFFFFFFFF which is far larger than any real slot.
const SENTINEL: u64 = u64::MAX;

/// Module private state. Shared with block & vanilla schedulers.
/// Encodes both the slot and who claimed it in a single atomic.
static SCHEDULER_STATE: AtomicU64 = AtomicU64::new(SENTINEL);

/// Extract the slot number from the combined state value.
#[inline]
fn get_slot(value: u64) -> u64 {
    value & SLOT_MASK
}

/// Check if the state indicates the slot was claimed by block.
#[inline]
fn is_block_claim(value: u64) -> bool {
    value != SENTINEL && value & BLOCK_CLAIMED_BIT != 0
}

/// Create a state value for a slot claimed by vanilla (top bit clear).
#[inline]
fn vanilla_claim(slot: u64) -> u64 {
    slot & SLOT_MASK
}

/// Create a state value for a slot claimed by block (top bit set).
#[inline]
fn block_claim(slot: u64) -> u64 {
    (slot & SLOT_MASK) | BLOCK_CLAIMED_BIT
}

/// If vanilla should schedule, the internal private atomic is
/// updated so that the block scheduler does not schedule.
///
/// Returns:
/// - None => not yet time to decide (still in delegation period and not yet claimed)
/// - Some(true) => yes, vanilla should schedule (claimed by vanilla)
/// - Some(false) => no, vanilla should not schedule (claimed by block)
pub fn vanilla_should_schedule(current_slot: u64, in_delegation_period: bool) -> Option<bool> {
    let state = SCHEDULER_STATE.load(Ordering::Acquire);

    // If slot is already claimed for current_slot, check who claimed it
    // This must be checked BEFORE in_delegation_period to handle:
    // 1. Multiple vanilla threads after one has claimed
    // 2. Test scenarios using force_vanilla_claim
    if state != SENTINEL && get_slot(state) == current_slot {
        // Check who claimed it - if vanilla claimed, all vanilla threads can consume
        // If block claimed, no vanilla thread should consume
        let claimed_by_vanilla = is_block_claim(state);
        return Some(!claimed_by_vanilla);
    }

    // If still in delegation period and slot not yet claimed, don't try to claim
    if in_delegation_period {
        return None;
    }

    // Try to claim the slot atomically
    let new_state = vanilla_claim(current_slot);
    let result = SCHEDULER_STATE.fetch_update(Ordering::Release, Ordering::Acquire, |old_state| {
        // Handle sentinel value
        if old_state == SENTINEL {
            return Some(new_state);
        }

        let old_slot = get_slot(old_state);
        match old_slot.cmp(&current_slot) {
            // Last slot scheduled was in the past => update
            std::cmp::Ordering::Less => Some(new_state),
            // Something has been scheduled for this slot => no update
            std::cmp::Ordering::Equal => None,
            // Weird edge case (slot went backwards?) => don't schedule
            std::cmp::Ordering::Greater => None,
        }
    });

    if result.is_ok() {
        info!("vanilla claimed slot {current_slot}");
        return Some(true);
    }

    // Failed to claim - slot was claimed while we were trying
    // Re-check: if it was claimed by another vanilla thread, we can still consume
    let state_now = SCHEDULER_STATE.load(Ordering::Acquire);
    if state_now != SENTINEL && get_slot(state_now) == current_slot {
        let claimed_by_block = is_block_claim(state_now);
        info!(
            "vanilla unable to claim {}, but slot is claimed, by_block={}",
            current_slot, claimed_by_block
        );
        return Some(!claimed_by_block);
    }

    info!("vanilla unable to claim slot {current_slot}");
    Some(false)
}

/// If block should schedule, the internal private atomic is
/// updated so that the vanilla scheduler does not schedule.
///
/// Returns:
/// - None => not in delegation period, can't schedule block
/// - Some(true) => yes, block should schedule (claimed successfully)
/// - Some(false) => no, block should not schedule (already claimed)
pub fn block_should_schedule(current_slot: u64, in_delegation_period: bool) -> Option<bool> {
    if !in_delegation_period {
        return None;
    }

    // Try to claim the slot atomically with block flag set
    let new_state = block_claim(current_slot);
    let did_claim = SCHEDULER_STATE
        .fetch_update(Ordering::Release, Ordering::Acquire, |old_state| {
            // Handle sentinel value
            if old_state == SENTINEL {
                return Some(new_state);
            }

            let old_slot = get_slot(old_state);
            match old_slot.cmp(&current_slot) {
                // Last slot scheduled was in the past => update
                std::cmp::Ordering::Less => Some(new_state),
                // Something has been scheduled for this slot => no update
                std::cmp::Ordering::Equal => {
                    info!("unexpectedly hit Equal branch in block_should_schedule");
                    None
                }
                // Weird edge case => don't schedule
                std::cmp::Ordering::Greater => None,
            }
        })
        .is_ok();

    if did_claim {
        info!("block claimed slot {current_slot}");
    }

    Some(did_claim)
}

/// If block failed, we should revert and give vanilla a chance.
/// This atomically clears the block claim and sets the slot to current_slot - 1
/// so that vanilla can claim the current slot.
pub fn block_failed(current_slot: u64) -> Option<bool> {
    // Atomically revert if we're still on the same slot with block claim
    let did_revert = SCHEDULER_STATE
        .fetch_update(Ordering::Release, Ordering::Acquire, |old_state| {
            // Only revert if currnt slot is claimed by block
            if old_state == SENTINEL {
                return None;
            }

            let old_slot = get_slot(old_state);
            if old_slot != current_slot {
                // Different slot, don't revert
                return None;
            }

            if !is_block_claim(old_state) {
                // Not claimed by block, don't revert
                return None;
            }

            // Revert to previous slot (vanilla claim, so vanilla can now claim current_slot)
            // Using wrapping_sub to handle slot 0 edge case
            let new_state = vanilla_claim(current_slot.wrapping_sub(1));
            Some(new_state)
        })
        .is_ok();

    info!("block_failed did_revert={did_revert} in slot={current_slot}");

    Some(did_revert)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reset the scheduler synchronization state. Used in tests to ensure
    /// a clean slate for each test.
    fn reset_for_tests() {
        SCHEDULER_STATE.store(SENTINEL, Ordering::Release);
    }

    /// Force claim a slot for vanilla scheduling. Used in tests to simulate
    /// being past the delegation period.
    fn force_vanilla_claim(slot: u64) {
        SCHEDULER_STATE.store(vanilla_claim(slot), Ordering::Release);
    }

    /// Returns the last slot that was scheduled (without the block/vanilla flag).
    fn last_slot_scheduled() -> u64 {
        get_slot(SCHEDULER_STATE.load(Ordering::Acquire))
    }

    /// Returns true if the current slot was claimed by block, false if by vanilla.
    fn is_slot_claimed_by_block() -> bool {
        is_block_claim(SCHEDULER_STATE.load(Ordering::Acquire))
    }

    #[test]
    fn test_encoding() {
        // Test vanilla claim
        let v = vanilla_claim(42);
        assert_eq!(get_slot(v), 42);
        assert!(!is_block_claim(v));

        // Test block claim
        let b = block_claim(42);
        assert_eq!(get_slot(b), 42);
        assert!(is_block_claim(b));

        // Test sentinel
        assert_eq!(get_slot(SENTINEL), SLOT_MASK); // Very large, not a real slot
        assert!(is_block_claim(SENTINEL)); // Top bit is set in u64::MAX
    }

    #[test]
    fn test_vanilla_claim_after_delegation() {
        reset_for_tests();

        // Not in delegation period, should be able to claim
        let result = vanilla_should_schedule(100, false);
        assert_eq!(result, Some(true));
        assert_eq!(last_slot_scheduled(), 100);
        assert!(!is_slot_claimed_by_block());
    }

    #[test]
    fn test_vanilla_during_delegation_unclaimed() {
        reset_for_tests();

        // In delegation period, unclaimed, should return None
        let result = vanilla_should_schedule(100, true);
        assert_eq!(result, None);
    }

    #[test]
    fn test_vanilla_during_delegation_claimed_by_vanilla() {
        reset_for_tests();
        force_vanilla_claim(100);

        // In delegation period but already claimed by vanilla
        let result = vanilla_should_schedule(100, true);
        assert_eq!(result, Some(true));
    }

    #[test]
    fn test_vanilla_during_delegation_claimed_by_block() {
        reset_for_tests();
        SCHEDULER_STATE.store(block_claim(100), Ordering::Release);

        // In delegation period, claimed by block
        let result = vanilla_should_schedule(100, true);
        assert_eq!(result, Some(false));
    }

    #[test]
    fn test_block_claim_during_delegation() {
        reset_for_tests();

        // In delegation period, should be able to claim
        let result = block_should_schedule(100, true);
        assert_eq!(result, Some(true));
        assert_eq!(last_slot_scheduled(), 100);
        assert!(is_slot_claimed_by_block());
    }

    #[test]
    fn test_block_outside_delegation() {
        reset_for_tests();

        // Not in delegation period, should return None
        let result = block_should_schedule(100, false);
        assert_eq!(result, None);
    }

    #[test]
    fn test_block_failed_reverts() {
        reset_for_tests();

        // Block claims slot 100
        block_should_schedule(100, true);
        assert!(is_slot_claimed_by_block());

        // Block fails, should revert
        let result = block_failed(100);
        assert_eq!(result, Some(true));

        // Now vanilla should be able to claim slot 100
        let result = vanilla_should_schedule(100, false);
        assert_eq!(result, Some(true));
        assert!(!is_slot_claimed_by_block());
    }

    #[test]
    fn test_block_failed_wrong_slot() {
        reset_for_tests();

        // Block claims slot 100
        block_should_schedule(100, true);

        // Try to revert slot 99 (wrong slot)
        let result = block_failed(99);
        assert_eq!(result, Some(false));

        // Slot 100 should still be claimed by block
        assert!(is_slot_claimed_by_block());
        assert_eq!(last_slot_scheduled(), 100);
    }
}
