//! Block stage

use {
    crate::scheduler::{
        MAX_BUNDLE_TXS, PASSES_PER_PROGRESS_POLL, STAGE_MAX_TASKS,
        channel::BundleMeta,
        check::{CheckMeta, CheckResult, Checker},
        execute::{ExecuteResult, Executor},
        progress::Progress,
        utils::{Accounts, add_resolved, free_batch, parse},
    },
    agave_scheduler_bindings::{
        LEADER_READY, SharableTransactionRegion, check_message_flags,
        worker_message_types::not_included_reasons,
    },
    agave_scheduling_utils::transaction_ptr::TransactionPtrBatch,
    anyhow::{Result, anyhow, bail},
    rtrb::Consumer,
    rts_alloc::Allocator,
    rustc_hash::{FxBuildHasher, FxHashMap},
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        cmp::{Ordering, Reverse},
        collections::{BinaryHeap, VecDeque},
    },
    tracing::{debug, error, info},
};

/// Bundles parsed per pass, few so replies aren't kept waiting
const BUNDLES_PER_PASS: usize = 16;

/// A block bundle in the dependency graph
#[derive(Default)]
struct Node {
    /// The bundle, until sent
    batch: Option<TransactionPtrBatch<(), BundleMeta>>,
    /// Accounts it uses, with whether it writes each
    accounts: Accounts,
    /// Lookup-table transactions still resolving
    resolving: u8,
    /// Unfinished earlier bundles it waits on
    conflicts: u32,
    /// Later bundles waiting on it
    successors: SmallVec<[u32; 4]>,
    /// Whether it landed
    finished: bool,
}

/// An account's last writer and its readers since
#[derive(Default)]
struct Users {
    writer: Option<u32>,
    readers: SmallVec<[u32; 4]>,
}

/// The slot's block transactions, by what became of them
#[derive(Default)]
struct Metrics {
    /// Executed and landed
    landed: usize,
    /// Sent, but reached a worker only after the bank was gone
    expired: usize,
    /// Never sent: their lookup tables, or an earlier bundle's, were resolving
    resolving: usize,
    /// Never sent: waiting on conflicting bundles
    blocked: usize,
    /// Never sent: ready, but no worker was free
    ready: usize,
}

pub struct BlockStage<'a> {
    allocator: &'a Allocator,
    tip_program: Pubkey,
    /// The block engine's block stream
    blocks: Consumer<TransactionPtrBatch<(), BundleMeta>>,
    /// The slot's bundles, by id
    nodes: Vec<Node>,
    /// Bundles in the graph: those with lower ids
    joined: u32,
    /// Bundles waiting on nothing, earliest first
    ready: BinaryHeap<Reverse<u32>>,
    /// Each account's users
    users: FxHashMap<Pubkey, Users>,
    /// Lookup-table CHECKs to send
    lookups: VecDeque<(SharableTransactionRegion, CheckMeta)>,
    /// The stream broke, so the rest of the slot needs a fallback block
    fallback: bool,
    /// A reply found the bank gone, so the slot is over
    bank_gone: bool,
    metrics: Metrics,
}

impl<'a> BlockStage<'a> {
    pub fn new(
        tip_program: Pubkey,
        blocks: Consumer<TransactionPtrBatch<(), BundleMeta>>,
        allocator: &'a Allocator,
    ) -> Self {
        Self {
            allocator,
            tip_program,
            blocks,
            nodes: Vec::with_capacity(STAGE_MAX_TASKS),
            joined: 0,
            ready: BinaryHeap::with_capacity(STAGE_MAX_TASKS),
            users: FxHashMap::with_capacity_and_hasher(STAGE_MAX_TASKS, FxBuildHasher),
            lookups: VecDeque::with_capacity(STAGE_MAX_TASKS.saturating_mul(MAX_BUNDLE_TXS)),
            fallback: false,
            bank_gone: false,
            metrics: Metrics::default(),
        }
    }

    /// Free bundles of slots before `slot`, or after `last`, our leader window's last slot
    pub fn drop_stale(&mut self, slot: Slot, last: Slot) {
        let (mut stale, mut later): (usize, usize) = (0, 0);
        while self
            .blocks
            .peek()
            .is_ok_and(|bundle| !(slot..=last).contains(&bundle.batch_meta().slot))
            && let Ok(bundle) = self.blocks.pop()
        {
            if bundle.batch_meta().slot < slot {
                stale = stale.saturating_add(bundle.len());
            } else {
                later = later.saturating_add(bundle.len());
            }
            // SAFETY: Popped from its queue, so the stage's alone.
            unsafe { free_batch(bundle, self.allocator) };
        }
        // Expected: a block's bundles can still arrive after its slot
        if stale != 0 {
            debug!(txs = stale, "discarded stale block transactions");
        }
        if later != 0 {
            error!(
                txs = later,
                last, "discarded block transactions for slots we don't lead"
            );
        }
    }

    /// Whether `slot`'s block has started arriving
    pub fn arrived(&mut self, slot: Slot, last: Slot) -> bool {
        self.drop_stale(slot, last);
        self.blocks
            .peek()
            .is_ok_and(|bundle| bundle.batch_meta().slot == slot)
    }

    /// Run the block stage; true if the rest of the slot needs a fallback block
    pub fn run(
        &mut self,
        executor: &mut Executor<'_, BundleMeta>,
        checker: &mut Checker<'_, CheckMeta>,
        progress: &mut Progress,
        slot: Slot,
        crank: Option<TransactionPtrBatch<(), BundleMeta>>,
    ) -> Result<bool> {
        if let Some(crank) = crank {
            self.push(crank, true);
        }
        'progress: while progress.poll()?.leader_state == LEADER_READY
            && progress.last().current_slot == slot
        {
            for _ in 0..PASSES_PER_PROGRESS_POLL {
                if self.fallback || self.bank_gone {
                    break 'progress;
                }
                self.dispatch(executor, checker, slot)?;
                self.take(slot);
                executor.poll(|result| self.executed(result))?;
                checker.poll(|result| self.checked(&result))?;
            }
        }
        self.finish(executor, checker, progress)?;
        Ok(std::mem::take(&mut self.fallback))
    }

    /// Handle a bundle's EXECUTE reply
    fn executed(&mut self, result: ExecuteResult<'_, BundleMeta>) -> Result<()> {
        let id = result.batch.batch_meta().id;
        let len = result.batch.len();
        // SAFETY: Its reply returned it, so the stage's alone.
        unsafe { free_batch(result.batch, self.allocator) };
        let node = self.node(id)?;
        if node.batch.is_some() || node.finished {
            bail!("reply for bundle {id}, which was not executing");
        }
        // Not executed: the worker's bank had already moved past our slot
        let Some(responses) = result.responses else {
            self.metrics.expired = self.metrics.expired.saturating_add(len);
            self.bank_gone = true;
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
            node.finished = true;
            let successors = std::mem::take(&mut node.successors);
            self.metrics.landed = self.metrics.landed.saturating_add(len);
            for successor in successors {
                let node = self.node(successor)?;
                node.conflicts = node.conflicts.saturating_sub(1);
                if node.conflicts == 0 {
                    self.ready.push(Reverse(successor));
                }
            }
        } else if reasons.contains(&not_included_reasons::BANK_NOT_AVAILABLE) {
            self.metrics.expired = self.metrics.expired.saturating_add(len);
            self.bank_gone = true;
        } else {
            self.fallback = true;
            error!(bundle = id, ?reasons, "block bundle did not land");
        }
        Ok(())
    }

    /// Handle a lookup-table CHECK reply
    fn checked(&mut self, checked: &CheckResult<'_, CheckMeta>) -> Result<()> {
        let id = checked.meta.id;
        let tip_program = self.tip_program;
        let node = self.node(id)?;
        node.resolving = node
            .resolving
            .checked_sub(1)
            .ok_or_else(|| anyhow!("lookup-table reply for bundle {id} with none outstanding"))?;
        if !add_resolved(&mut node.accounts, checked, &tip_program) {
            self.fallback = true;
            error!(
                bundle = id,
                "block bundle lookup tables failed to resolve or loaded the tip-payment program"
            );
        }
        self.join();
        Ok(())
    }

    /// Send queued lookup-table CHECKs, then ready bundles earliest first, while there is room
    fn dispatch(
        &mut self,
        executor: &mut Executor<'_, BundleMeta>,
        checker: &mut Checker<'_, CheckMeta>,
        slot: Slot,
    ) -> Result<()> {
        while checker.has_room() && !self.lookups.is_empty() {
            // `send` takes a slice and sends a CHECK's worth from its front
            let entries = self.lookups.make_contiguous();
            // SAFETY: A bundle is freed only after its CHECKs return.
            let sent =
                unsafe { checker.send(check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES, entries)? };
            self.lookups.drain(..sent);
        }
        while executor.has_room()
            && let Some(Reverse(id)) = self.ready.pop()
            && let Some(batch) = self
                .nodes
                .get_mut(id as usize)
                .and_then(|node| node.batch.take())
        {
            // The graph already ordered its conflicts
            let flags = batch.batch_meta().flags;
            if executor.send(flags, slot, batch).is_err() {
                bail!("executor refused a bundle it had room for");
            }
        }
        Ok(())
    }

    /// Take up to [`BUNDLES_PER_PASS`] of `slot`'s bundles
    fn take(&mut self, slot: Slot) {
        for _ in 0..BUNDLES_PER_PASS {
            if self.nodes.len() >= STAGE_MAX_TASKS {
                return;
            }
            let Ok(bundle) = self.blocks.pop() else {
                return;
            };
            match bundle.batch_meta().slot.cmp(&slot) {
                Ordering::Equal => self.push(bundle, false),
                Ordering::Less => {
                    debug!(txs = bundle.len(), "discarded stale block transactions");
                    // SAFETY: Popped from its queue, so the stage's alone.
                    unsafe { free_batch(bundle, self.allocator) };
                }
                // No later slot was announced yet
                Ordering::Greater => {
                    error!(
                        slot = bundle.batch_meta().slot,
                        "block bundle for a later slot"
                    );
                    // SAFETY: Popped from its queue, so the stage's alone.
                    unsafe { free_batch(bundle, self.allocator) };
                    self.fallback = true;
                }
            }
        }
    }

    /// Add a bundle; only a `trusted` one may reference the tip-payment program
    fn push(&mut self, mut bundle: TransactionPtrBatch<(), BundleMeta>, trusted: bool) {
        let Ok(id) = u32::try_from(self.nodes.len()) else {
            // SAFETY: Handed in, so the stage's alone.
            unsafe { free_batch(bundle, self.allocator) };
            return;
        };
        // The block stream's sign that the rest of the block will not come
        if bundle.is_empty() {
            // SAFETY: Handed in, so the stage's alone.
            unsafe { free_batch(bundle, self.allocator) };
            self.fallback = true;
            error!(bundle = id, "block stream ended the block early");
            return;
        }
        let Some((accounts, lookups)) =
            parse(id, &mut bundle, trusted, &self.tip_program, self.allocator)
        else {
            // SAFETY: Handed in, so the stage's alone.
            unsafe { free_batch(bundle, self.allocator) };
            self.fallback = true;
            error!(
                bundle = id,
                "block bundle does not parse or references the tip-payment program"
            );
            return;
        };
        self.nodes.push(Node {
            batch: Some(bundle),
            accounts,
            // A bundle has at most `MAX_BUNDLE_TXS` members
            resolving: u8::try_from(lookups.len()).unwrap_or(u8::MAX),
            ..Node::default()
        });
        self.lookups.extend(lookups);
        self.join();
    }

    /// Add waiting bundles to the graph, in block order
    fn join(&mut self) {
        while let Some((earlier, [node, ..])) =
            self.nodes.split_at_mut_checked(self.joined as usize)
            && node.resolving == 0
        {
            let id = self.joined;
            let mut conflicts: u32 = 0;
            for &(account, is_write) in &node.accounts {
                let users = self.users.entry(account).or_default();
                // A finished bundle would never release it
                let mut wait_on = |from: u32| {
                    if let Some(earlier) = earlier.get_mut(from as usize)
                        && !earlier.finished
                    {
                        earlier.successors.push(id);
                        conflicts = conflicts.saturating_add(1);
                    }
                };
                if let Some(writer) = users.writer {
                    wait_on(writer);
                }
                if is_write {
                    users.readers.drain(..).for_each(&mut wait_on);
                    users.writer = Some(id);
                } else {
                    users.readers.push(id);
                }
            }
            node.conflicts = conflicts;
            if conflicts == 0 {
                self.ready.push(Reverse(id));
            }
            self.joined = id.saturating_add(1);
        }
    }

    /// End the block: wait for in-flight work and free the rest
    fn finish(
        &mut self,
        executor: &mut Executor<'_, BundleMeta>,
        checker: &mut Checker<'_, CheckMeta>,
        progress: &mut Progress,
    ) -> Result<()> {
        while executor.in_flight() != 0 || checker.in_flight() != 0 {
            progress.poll()?;
            executor.poll(|result| self.executed(result))?;
            checker.poll(|result| self.checked(&result))?;
        }
        for (index, node) in self.nodes.drain(..).enumerate() {
            let Some(batch) = node.batch else {
                continue;
            };
            let unsent = if index >= self.joined as usize {
                &mut self.metrics.resolving
            } else if node.conflicts != 0 {
                &mut self.metrics.blocked
            } else {
                &mut self.metrics.ready
            };
            *unsent = unsent.saturating_add(batch.len());
            // SAFETY: Never sent, so the stage's alone.
            unsafe { free_batch(batch, self.allocator) };
        }
        self.joined = 0;
        self.ready.clear();
        self.users.clear();
        self.lookups.clear();
        self.bank_gone = false;
        let Metrics {
            landed,
            expired,
            resolving,
            blocked,
            ready,
        } = std::mem::take(&mut self.metrics);
        info!(landed, expired, resolving, blocked, ready, "block stage");
        Ok(())
    }

    fn node(&mut self, id: u32) -> Result<&mut Node> {
        self.nodes
            .get_mut(id as usize)
            .ok_or_else(|| anyhow!("unknown bundle {id}"))
    }
}
