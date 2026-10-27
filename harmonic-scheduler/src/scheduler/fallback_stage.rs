//! Fallback stage

use {
    crate::scheduler::{
        MAX_BUNDLE_TXS, PACK_TO_WORKER_CAPACITY, PASSES_PER_PROGRESS_POLL, STORAGE_CAPACITY,
        channel::BundleMeta,
        check::{CheckMeta, Checker},
        execute::{ExecuteResult, Executor},
        progress::Progress,
        storage::Storage,
        utils::{Accounts, free_batch, parse},
    },
    agave_scheduler_bindings::{
        LEADER_READY, TpuToPackMessage, worker_message_types::not_included_reasons,
    },
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    anyhow::{Result, anyhow, bail},
    rtrb::Consumer,
    rts_alloc::Allocator,
    rustc_hash::{FxBuildHasher, FxHashMap},
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    tracing::info,
};

pub struct FallbackStage<'a> {
    allocator: &'a Allocator,
    tip_program: Pubkey,
    /// Packets and bundles, checked and ranked
    store: Storage<'a>,
    locks: AccountLocks,
    next_id: u32,
    metrics: Metrics,
    /// A reply showed the slot over: the bank gone or the block full
    slot_over: bool,
}

/// The slot's transactions, by what became of them
#[derive(Default)]
struct Metrics {
    /// Executed and landed
    landed: usize,
    /// Sent, but reached a worker only after the bank was gone
    expired: usize,
    /// Executed and not landed, by `not_included_reason`
    failed: FxHashMap<u8, usize>,
}

impl<'a> FallbackStage<'a> {
    pub fn new(
        num_workers: usize,
        tip_program: Pubkey,
        tpu_to_pack: shaq::spsc::Consumer<TpuToPackMessage>,
        remote_tpu: Consumer<TransactionPtr>,
        packets: Consumer<TransactionPtr>,
        bundles: Consumer<TransactionPtrBatch<(), BundleMeta>>,
        allocator: &'a Allocator,
    ) -> Self {
        let executing = num_workers.saturating_mul(PACK_TO_WORKER_CAPACITY);
        Self {
            allocator,
            tip_program,
            store: Storage::new(
                STORAGE_CAPACITY,
                tip_program,
                tpu_to_pack,
                remote_tpu,
                packets,
                bundles,
                allocator,
            ),
            locks: AccountLocks {
                // 32 accounts per candidate, as the store holds inline
                locks: FxHashMap::with_capacity_and_hasher(
                    executing.saturating_mul(32),
                    FxBuildHasher,
                ),
                held: FxHashMap::with_capacity_and_hasher(executing, FxBuildHasher),
            },
            next_id: 0,
            metrics: Metrics::default(),
            slot_over: false,
        }
    }

    /// Take in and re-check the store between leader windows
    pub fn poll(&mut self, checker: &mut Checker<'_, CheckMeta>) -> Result<()> {
        // Intake first: the re-check sweep takes all the CHECK room it gets
        self.store.take(checker)?;
        self.store.dispatch(checker)?;
        checker.poll(|result| self.store.checked(&result))?;
        self.store.report();
        Ok(())
    }

    /// Wait for the store's CHECKs, so the block stage has the checker to itself
    pub fn drain(
        &mut self,
        checker: &mut Checker<'_, CheckMeta>,
        progress: &mut Progress,
    ) -> Result<()> {
        while checker.in_flight() != 0 {
            progress.poll()?;
            checker.poll(|result| self.store.checked(&result))?;
        }
        Ok(())
    }

    /// Run the fallback stage
    pub fn run(
        &mut self,
        executor: &mut Executor<'_, BundleMeta>,
        checker: &mut Checker<'_, CheckMeta>,
        progress: &mut Progress,
        slot: Slot,
        crank: Option<TransactionPtrBatch<(), BundleMeta>>,
    ) -> Result<()> {
        if let Some(crank) = crank {
            self.crank(crank, executor, slot)?;
        }
        'progress: while progress.poll()?.leader_state == LEADER_READY
            && progress.last().current_slot == slot
        {
            for _ in 0..PASSES_PER_PROGRESS_POLL {
                if self.slot_over {
                    break 'progress;
                }
                self.dispatch(executor, slot)?;
                self.store.take(checker)?;
                executor.poll(|result| self.executed(result))?;
                checker.poll(|result| self.store.checked(&result))?;
            }
        }
        self.finish(executor, checker, progress)
    }

    /// Handle a candidate's EXECUTE reply
    fn executed(&mut self, result: ExecuteResult<'_, BundleMeta>) -> Result<()> {
        let id = result.batch.batch_meta().id;
        let len = result.batch.len();
        // SAFETY: Its reply returned it, so the stage's alone.
        unsafe { free_batch(result.batch, self.allocator) };
        self.locks.unlock(id)?;
        // Not executed: the worker's bank had already moved past our slot
        let Some(responses) = result.responses else {
            self.metrics.expired = self.metrics.expired.saturating_add(len);
            self.slot_over = true;
            return Ok(());
        };
        let reasons: SmallVec<[u8; MAX_BUNDLE_TXS]> = responses
            .iter()
            .map(|response| response.not_included_reason)
            .collect();
        if reasons
            .iter()
            .all(|&reason| reason == not_included_reasons::NONE)
        {
            self.metrics.landed = self.metrics.landed.saturating_add(len);
        } else if reasons.contains(&not_included_reasons::BANK_NOT_AVAILABLE) {
            self.metrics.expired = self.metrics.expired.saturating_add(len);
            self.slot_over = true;
        } else {
            // Only a full block ends the slot; account and data limits leave room for others
            self.slot_over |=
                reasons.contains(&not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT);
            for reason in reasons {
                let count = self.metrics.failed.entry(reason).or_insert(0);
                *count = count.saturating_add(1);
            }
        }
        Ok(())
    }

    /// Send the store's best runnable entries while workers have room
    fn dispatch(&mut self, executor: &mut Executor<'_, BundleMeta>, slot: Slot) -> Result<()> {
        while executor.has_room()
            && let Some((batch, accounts)) = self.store.pop(|accounts| !self.locks.blocks(accounts))
        {
            self.send(batch, accounts, executor, slot)?;
        }
        Ok(())
    }

    /// Send the tip crank; tipping entries conflict with it, so they wait for it
    fn crank(
        &mut self,
        mut crank: TransactionPtrBatch<(), BundleMeta>,
        executor: &mut Executor<'_, BundleMeta>,
        slot: Slot,
    ) -> Result<()> {
        match parse(0, &mut crank, true, &self.tip_program, self.allocator) {
            Some((accounts, lookups)) if lookups.is_empty() => {
                self.send(crank, accounts, executor, slot)
            }
            _ => {
                // SAFETY: Handed in, so the stage's alone.
                unsafe { free_batch(crank, self.allocator) };
                Ok(())
            }
        }
    }

    /// Lock `accounts` for `batch` and send it
    fn send(
        &mut self,
        mut batch: TransactionPtrBatch<(), BundleMeta>,
        accounts: Accounts,
        executor: &mut Executor<'_, BundleMeta>,
        slot: Slot,
    ) -> Result<()> {
        let id = self.next_id;
        self.next_id = id.wrapping_add(1);
        let meta = BundleMeta {
            id,
            ..batch.batch_meta()
        };
        batch.set_batch_meta(meta);
        self.locks.lock(id, accounts);
        if executor.send(meta.flags, slot, batch).is_err() {
            bail!("executor refused a candidate it had room for");
        }
        Ok(())
    }

    /// End the slot: wait for in-flight work, then log the slot's counts
    fn finish(
        &mut self,
        executor: &mut Executor<'_, BundleMeta>,
        checker: &mut Checker<'_, CheckMeta>,
        progress: &mut Progress,
    ) -> Result<()> {
        while executor.in_flight() != 0 || checker.in_flight() != 0 {
            progress.poll()?;
            executor.poll(|result| self.executed(result))?;
            checker.poll(|result| self.store.checked(&result))?;
        }
        self.next_id = 0;
        self.slot_over = false;
        let Metrics {
            landed,
            expired,
            failed,
        } = std::mem::take(&mut self.metrics);
        info!(landed, expired, ?failed, "fallback stage");
        Ok(())
    }
}

/// Account locks of executing candidates
struct AccountLocks {
    locks: FxHashMap<Pubkey, u64>,
    /// Each holder's accounts, to release when it finishes
    held: FxHashMap<u32, Accounts>,
}

impl AccountLocks {
    /// The lock count of a written account
    const WRITE_LOCK: u64 = u64::MAX;

    /// Whether these locks block `accounts`
    fn blocks(&self, accounts: &[(Pubkey, bool)]) -> bool {
        accounts
            .iter()
            .any(|(account, is_write)| match self.locks.get(account) {
                Some(&n) => n == Self::WRITE_LOCK || *is_write,
                None => false,
            })
    }

    /// Lock `accounts` for candidate `id`
    fn lock(&mut self, id: u32, accounts: Accounts) {
        for (account, is_write) in &accounts {
            let n = self.locks.entry(*account).or_insert(0);
            *n = if *is_write {
                Self::WRITE_LOCK
            } else {
                n.saturating_add(1)
            };
        }
        self.held.insert(id, accounts);
    }

    /// Release candidate `id`'s locks
    fn unlock(&mut self, id: u32) -> Result<()> {
        let accounts = self
            .held
            .remove(&id)
            .ok_or_else(|| anyhow!("unlock for candidate {id} holding no locks"))?;
        for (account, is_write) in &accounts {
            let n = self
                .locks
                .get_mut(account)
                .ok_or_else(|| anyhow!("unlock of unlocked account {account}"))?;
            *n = n
                .checked_sub(if *is_write { Self::WRITE_LOCK } else { 1 })
                .ok_or_else(|| anyhow!("lock count underflow on account {account}"))?;
            if *n == 0 {
                self.locks.remove(account);
            }
        }
        Ok(())
    }
}
