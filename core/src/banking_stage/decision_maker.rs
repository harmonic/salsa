use {
    crate::scheduler_synchronization,
    log::debug,
    solana_clock::{
        DEFAULT_TICKS_PER_SLOT, FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
        HOLD_TRANSACTIONS_SLOT_OFFSET,
    },
    solana_poh::poh_recorder::{PohRecorder, SharedLeaderState},
    solana_runtime::bank::Bank,
    solana_unified_scheduler_pool::{BankingStageMonitor, BankingStageStatus},
    std::sync::{
        atomic::{AtomicBool, Ordering::Relaxed},
        Arc,
    },
};

#[derive(Debug, Clone)]
pub enum BufferedPacketsDecision {
    Consume(Arc<Bank>),
    Forward,
    ForwardAndHold,
    Hold,
}

impl BufferedPacketsDecision {
    /// Returns the `Bank` if the decision is `Consume`. Otherwise, returns `None`.
    pub fn bank(&self) -> Option<&Arc<Bank>> {
        match self {
            Self::Consume(bank) => Some(bank),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct DecisionMaker {
    shared_leader_state: SharedLeaderState,
}

impl std::fmt::Debug for DecisionMaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionMaker").finish()
    }
}

impl DecisionMaker {
    pub fn new(shared_leader_state: SharedLeaderState) -> Self {
        Self {
            shared_leader_state,
        }
    }

    pub(crate) fn make_consume_or_forward_decision(&self) -> BufferedPacketsDecision {
        let state = self.shared_leader_state.load();

        if let Some(working_bank) = state.working_bank() {
            BufferedPacketsDecision::Consume(working_bank.clone())
        } else if let Some(leader_first_tick_height) = state.leader_first_tick_height() {
            let current_tick_height = state.tick_height();
            let ticks_until_leader = leader_first_tick_height.saturating_sub(current_tick_height);
            if ticks_until_leader
                <= (FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET - 1) * DEFAULT_TICKS_PER_SLOT
            {
                BufferedPacketsDecision::Hold
            } else if ticks_until_leader < HOLD_TRANSACTIONS_SLOT_OFFSET * DEFAULT_TICKS_PER_SLOT {
                BufferedPacketsDecision::ForwardAndHold
            } else {
                BufferedPacketsDecision::Forward
            }
        } else {
            BufferedPacketsDecision::Forward
        }
    }

    /// Gate consume decisions based on scheduler synchronization.
    ///
    /// vanilla: consume if we are in fallback period with no external signal.
    ///          there are no other preconditions
    /// block: consume if we are in delegation period.
    ///        preconditions: there is a bundle (for this slot) to consume
    pub fn maybe_consume<const VANILLA: bool>(
        decision: BufferedPacketsDecision,
    ) -> BufferedPacketsDecision {
        debug!("maybe_consume VANILLA {VANILLA:?} decision {decision:?}");
        let BufferedPacketsDecision::Consume(bank) = decision else {
            return decision;
        };

        let current_tick_height = bank.tick_height();
        let max_tick_height = bank.max_tick_height();
        let bank_ticks_per_slot = bank.ticks_per_slot();
        let start_tick = max_tick_height - bank_ticks_per_slot;
        let ticks_into_slot = current_tick_height.saturating_sub(start_tick);
        let delegation_period_length = bank_ticks_per_slot * 15 / 16;
        let in_delegation_period = ticks_into_slot < delegation_period_length;

        debug!("maybe_consume current_tick_height {current_tick_height} max_tick_height {max_tick_height} bank_ticks_per_slot {bank_ticks_per_slot} start_tick {start_tick} ticks_into_slot {ticks_into_slot} delegation_period_length {delegation_period_length} in_delegation_period {in_delegation_period}");

        let current_slot = bank.slot();

        // Call the appropriate scheduler function
        // vanilla_should_schedule and block_should_schedule are now idempotent -
        // multiple threads calling for the same slot will get consistent results
        let should_schedule: fn(u64, bool) -> Option<bool> = if VANILLA {
            scheduler_synchronization::vanilla_should_schedule
        } else {
            scheduler_synchronization::block_should_schedule
        };

        match should_schedule(current_slot, in_delegation_period) {
            Some(true) => BufferedPacketsDecision::Consume(bank),
            Some(false) => BufferedPacketsDecision::Hold,
            None => BufferedPacketsDecision::Hold,
        }
    }
}

impl From<&PohRecorder> for DecisionMaker {
    fn from(poh_recorder: &PohRecorder) -> Self {
        Self::new(poh_recorder.shared_leader_state())
    }
}

#[derive(Debug)]
pub(crate) struct DecisionMakerWrapper {
    is_exited: Arc<AtomicBool>,
    decision_maker: DecisionMaker,
}

impl DecisionMakerWrapper {
    pub(crate) fn new(is_exited: Arc<AtomicBool>, decision_maker: DecisionMaker) -> Self {
        Self {
            is_exited,
            decision_maker,
        }
    }
}

impl BankingStageMonitor for DecisionMakerWrapper {
    fn status(&mut self) -> BankingStageStatus {
        if self.is_exited.load(Relaxed) {
            BankingStageStatus::Exited
        } else if matches!(
            self.decision_maker.make_consume_or_forward_decision(),
            BufferedPacketsDecision::Forward,
        ) {
            BankingStageStatus::Inactive
        } else {
            BankingStageStatus::Active
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_ledger::genesis_utils::create_genesis_config,
        solana_poh::poh_recorder::LeaderState, solana_runtime::bank::Bank,
    };

    #[test]
    fn test_buffered_packet_decision_bank() {
        let bank = Arc::new(Bank::default_for_tests());
        assert!(BufferedPacketsDecision::Consume(bank).bank().is_some());
        assert!(BufferedPacketsDecision::Forward.bank().is_none());
        assert!(BufferedPacketsDecision::ForwardAndHold.bank().is_none());
        assert!(BufferedPacketsDecision::Hold.bank().is_none());
    }

    #[test]
    fn test_make_consume_or_forward_decision() {
        let genesis_config = create_genesis_config(2).genesis_config;
        let (bank, _bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut shared_leader_state = SharedLeaderState::new(0, None, None);

        let decision_maker = DecisionMaker::new(shared_leader_state.clone());

        // No active bank, no leader first tick height.
        assert_matches!(
            decision_maker.make_consume_or_forward_decision(),
            BufferedPacketsDecision::Forward
        );

        // Active bank.
        shared_leader_state.store(Arc::new(LeaderState::new(
            Some(bank.clone()),
            0,
            None,
            None,
        )));
        assert_matches!(
            decision_maker.make_consume_or_forward_decision(),
            BufferedPacketsDecision::Consume(_)
        );
        shared_leader_state.store(Arc::new(LeaderState::new(None, 0, None, None)));

        // Will be leader shortly - Hold
        for next_leader_slot_offset in [0, 1].into_iter() {
            let next_leader_slot = bank.slot() + next_leader_slot_offset;
            shared_leader_state.store(Arc::new(LeaderState::new(
                None,
                0,
                Some(next_leader_slot * DEFAULT_TICKS_PER_SLOT),
                Some((next_leader_slot, next_leader_slot + 4)),
            )));

            let decision = decision_maker.make_consume_or_forward_decision();
            assert!(
                matches!(decision, BufferedPacketsDecision::Hold),
                "next_leader_slot_offset: {next_leader_slot_offset}",
            );
        }

        // Will be leader - ForwardAndHold
        for next_leader_slot_offset in [2, 19].into_iter() {
            let next_leader_slot = bank.slot() + next_leader_slot_offset;
            shared_leader_state.store(Arc::new(LeaderState::new(
                None,
                0,
                Some(next_leader_slot * DEFAULT_TICKS_PER_SLOT),
                Some((next_leader_slot, next_leader_slot + 4)),
            )));

            let decision = decision_maker.make_consume_or_forward_decision();
            assert!(
                matches!(decision, BufferedPacketsDecision::ForwardAndHold),
                "next_leader_slot_offset: {next_leader_slot_offset}",
            );
        }

        // Longer period until next leader - Forward
        let next_leader_slot = 20 + bank.slot();
        shared_leader_state.store(Arc::new(LeaderState::new(
            None,
            0,
            Some(next_leader_slot * DEFAULT_TICKS_PER_SLOT),
            Some((next_leader_slot, next_leader_slot + 4)),
        )));
        let decision = decision_maker.make_consume_or_forward_decision();
        assert!(
            matches!(decision, BufferedPacketsDecision::Forward),
            "next_leader_slot: {next_leader_slot}",
        );
    }
}
