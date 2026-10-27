//! Transaction storage

use {
    crate::scheduler::{
        CHECK_QUEUE_CAPACITY, MAX_BUNDLE_TXS,
        channel::BundleMeta,
        check::{CheckMeta, CheckResult, Checker, MAX_CHECK_TXS},
        utils::{Accounts, add_resolved, free_batch, static_accounts, union_extend},
    },
    agave_scheduler_bindings::{
        TpuToPackMessage, check_message_flags,
        worker_message_types::{
            CheckResponse, parsing_and_sanitization_flags, resolve_flags, scheduling_details_flags,
            status_check_flags,
        },
    },
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    agave_transaction_view::{
        transaction_data::TransactionData, transaction_view::UnsanitizedTransactionView,
    },
    anyhow::{Result, anyhow, bail},
    arrayvec::ArrayVec,
    rtrb::Consumer,
    rts_alloc::Allocator,
    rustc_hash::{FxBuildHasher, FxHashSet},
    slab::Slab,
    smallvec::SmallVec,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::BTreeSet,
        num::NonZeroUsize,
        ops::Bound,
        time::{Duration, Instant},
    },
    tracing::debug,
};

/// What the store's re-checks ask: whether a transaction can still land, and what it pays
const RECHECK_FLAGS: u16 =
    check_message_flags::STATUS_CHECKS | check_message_flags::CALCULATE_SCHEDULING_DETAILS;
/// What a new entry's check asks: also its lookup-table keys, resolved once
const INTAKE_FLAGS: u16 = RECHECK_FLAGS | check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES;
/// Bundles, and again packets, a pop looks past that conflict with executing work
const SKIPS_PER_POP: usize = 64;

/// How often the store logs its drop counts
const REPORT_INTERVAL: Duration = Duration::from_secs(1);
/// Most transactions the store's CHECKs hold at once
const MAX_IN_CHECKS: usize = CHECK_QUEUE_CAPACITY * MAX_CHECK_TXS;
/// TPU transactions read at once: one CHECK's worth, since a reservation is read whole
const TPU_TO_PACK_BATCH: NonZeroUsize = NonZeroUsize::new(MAX_CHECK_TXS).unwrap();
/// Bundles taken at once: as many as one CHECK holds
const BUNDLES_PER_CHECK: usize = MAX_CHECK_TXS / MAX_BUNDLE_TXS;
/// A packet's batch metadata: it executes alone, with no flags
const PACKET: BundleMeta = BundleMeta {
    slot: 0,
    flags: 0,
    id: 0,
};

/// Transaction storage
pub struct Storage<'a> {
    /// The tip-payment program, which only the crank may use
    tip_program: Pubkey,
    /// The validator's TPU transactions
    tpu_to_pack: shaq::spsc::Consumer<TpuToPackMessage>,
    /// The remote TPU's packets
    remote_tpu: Consumer<TransactionPtr>,
    /// The block engine's packets
    packets: Consumer<TransactionPtr>,
    /// The block engine's bundles
    bundles: Consumer<TransactionPtrBatch<(), BundleMeta>>,
    /// Stored packets, each a batch of one, and bundles
    entries: Slab<Entry>,
    /// First signature of every stored transaction
    signatures: FxHashSet<Signature>,
    /// Entries no CHECK holds, by rank then index, worst first
    ranked: BTreeSet<(bool, u64, usize)>,
    /// Transactions in `ranked`
    ranked_txs: usize,
    /// Where the re-check sweep resumes: below this key, or at the best if None
    recheck: Option<(bool, u64, usize)>,
    /// Rank of the next bundle; each ranks below the one before
    next_bundle: u64,
    /// Most ranked transactions held; past it the worst entries are dropped
    capacity: usize,
    metrics: Metrics,
    /// When `metrics` were last logged
    reported: Instant,
    allocator: &'a Allocator,
}

/// A stored packet or bundle
struct Entry {
    batch: TransactionPtrBatch<(), BundleMeta>,
    /// Each member's first signature
    signatures: SmallVec<[Signature; MAX_BUNDLE_TXS]>,
    /// Each member's writable lookup-table keys
    writable: SmallVec<[u16; MAX_BUNDLE_TXS]>,
    /// Each account once, with whether it writes it
    accounts: Accounts,
    /// Bundles rank above packets, older ones higher; packets by what they pay
    rank: (bool, u64),
    /// Members whose CHECK reply is still out
    checking: u8,
    /// A member's check found it can't land
    failed: bool,
}

/// The store's drop counts since they were last logged, in transactions
#[derive(Default, PartialEq)]
struct Metrics {
    /// Dropped when pushed because a member did not parse or uses the tip-payment program
    unparsable: usize,
    /// Dropped when pushed because a member with the same signature is stored
    duplicate: usize,
    /// Ranked, and dropped to keep the store within `capacity`
    dropped: usize,
    /// Dropped because a check found a member can't land or uses the tip-payment program
    pruned: usize,
}

impl<'a> Storage<'a> {
    pub fn new(
        capacity: usize,
        tip_program: Pubkey,
        tpu_to_pack: shaq::spsc::Consumer<TpuToPackMessage>,
        remote_tpu: Consumer<TransactionPtr>,
        packets: Consumer<TransactionPtr>,
        bundles: Consumer<TransactionPtrBatch<(), BundleMeta>>,
        allocator: &'a Allocator,
    ) -> Self {
        // Never outgrown: `capacity` ranked, plus what CHECKs hold
        let max = capacity.saturating_add(MAX_IN_CHECKS);
        Self {
            tip_program,
            tpu_to_pack,
            remote_tpu,
            packets,
            bundles,
            entries: Slab::with_capacity(max),
            signatures: FxHashSet::with_capacity_and_hasher(max, FxBuildHasher),
            ranked: BTreeSet::new(),
            ranked_txs: 0,
            recheck: None,
            next_bundle: u64::MAX,
            capacity,
            metrics: Metrics::default(),
            reported: Instant::now(),
            allocator,
        }
    }

    /// Take in up to a CHECK's worth from each source while the checker has room
    pub fn take(&mut self, checker: &mut Checker<'_, CheckMeta>) -> Result<()> {
        if checker.has_room() {
            let bundles: ArrayVec<_, BUNDLES_PER_CHECK> = self
                .bundles
                .read_chunk(BUNDLES_PER_CHECK.min(self.bundles.slots()))
                .into_iter()
                .flatten()
                .collect();
            self.push(checker, bundles)?;
        }
        if checker.has_room() {
            let allocator = self.allocator;
            let txs: ArrayVec<_, MAX_CHECK_TXS> = self
                .tpu_to_pack
                .try_reserve_read_batch(TPU_TO_PACK_BATCH)
                .into_iter()
                .flatten()
                .map(|message| {
                    // SAFETY: The validator hands us each TPU transaction, in our mapping.
                    unsafe {
                        TransactionPtr::from_sharable_transaction_region(
                            &message.transaction,
                            allocator,
                        )
                    }
                })
                .collect();
            self.push_packets(checker, txs)?;
        }
        if checker.has_room() {
            let txs: ArrayVec<_, MAX_CHECK_TXS> = self
                .remote_tpu
                .read_chunk(MAX_CHECK_TXS.min(self.remote_tpu.slots()))
                .into_iter()
                .flatten()
                .collect();
            self.push_packets(checker, txs)?;
        }
        if checker.has_room() {
            let txs: ArrayVec<_, MAX_CHECK_TXS> = self
                .packets
                .read_chunk(MAX_CHECK_TXS.min(self.packets.slots()))
                .into_iter()
                .flatten()
                .collect();
            self.push_packets(checker, txs)?;
        }
        Ok(())
    }

    /// Send re-checks while the checker has room, best first, wrapping after the worst
    pub fn dispatch(&mut self, checker: &mut Checker<'_, CheckMeta>) -> Result<()> {
        loop {
            // Next CHECK's worth below the bookmark, best first, so the top stays freshest
            let below = self.recheck.map_or(Bound::Unbounded, Bound::Excluded);
            let mut keys = ArrayVec::<(bool, u64, usize), MAX_CHECK_TXS>::new();
            let mut members: usize = 0;
            for &key in self.ranked.range((Bound::Unbounded, below)).rev() {
                let len = self.entries.get(key.2).map_or(0, |entry| entry.batch.len());
                members = members.saturating_add(len);
                if members > MAX_CHECK_TXS || keys.try_push(key).is_err() {
                    break;
                }
            }
            // Sweep end: wrap to the best; empty from the best means all are out in CHECKs
            if keys.is_empty() {
                if self.recheck.take().is_none() {
                    return Ok(());
                }
                continue;
            }
            let indices: ArrayVec<usize, MAX_CHECK_TXS> = keys.iter().map(|key| key.2).collect();
            if !self.send(checker, RECHECK_FLAGS, &indices)? {
                return Ok(());
            }
            // Out of reach while their CHECK is out; `checked` ranks them again
            for key in &keys {
                self.unrank(key);
            }
            self.recheck = keys.last().copied();
        }
    }

    /// Handle a store CHECK reply
    pub fn checked(&mut self, result: &CheckResult<'_, CheckMeta>) -> Result<()> {
        let index = usize::try_from(result.meta.id)?;
        let entry = self
            .entries
            .get_mut(index)
            .filter(|entry| entry.checking != 0)
            .ok_or_else(|| anyhow!("check reply for an entry with none outstanding"))?;
        entry.checking = entry.checking.saturating_sub(1);
        entry.failed |= check_failed(result.response);
        if result.response.resolve_flags & resolve_flags::REQUESTED != 0 {
            entry.failed =
                entry.failed || !add_resolved(&mut entry.accounts, result, &self.tip_program);
        }
        if !entry.rank.0 {
            entry.rank.1 = priority(result.response);
        }
        if entry.checking != 0 {
            return Ok(());
        }
        if entry.failed {
            if let Some(entry) = self.remove(index) {
                self.metrics.pruned = self.metrics.pruned.saturating_add(entry.batch.len());
                // SAFETY: Its last reply is back and it is not ranked, so the store's alone.
                unsafe { free_batch(entry.batch, self.allocator) };
            }
            return Ok(());
        }
        self.rank(index);
        Ok(())
    }

    /// Take out the best entry `runnable` accepts, skipping at most [`SKIPS_PER_POP`] per kind
    pub fn pop(
        &mut self,
        mut runnable: impl FnMut(&[(Pubkey, bool)]) -> bool,
    ) -> Option<(TransactionPtrBatch<(), BundleMeta>, Accounts)> {
        // Each kind gets its own skips, so bundles contending for tip accounts can't hide packets
        let bundles = self.ranked.range((true, 0, 0)..).rev();
        let packets = self.ranked.range(..(true, 0, 0)).rev();
        let key = bundles
            .take(SKIPS_PER_POP.saturating_add(1))
            .chain(packets.take(SKIPS_PER_POP.saturating_add(1)))
            .find(|key| {
                self.entries
                    .get(key.2)
                    .is_some_and(|entry| runnable(&entry.accounts))
            })
            .copied()?;
        self.unrank(&key);
        self.remove(key.2)
            .map(|entry| (entry.batch, entry.accounts))
    }

    /// Log and clear the drop counts, at most once per [`REPORT_INTERVAL`]
    pub fn report(&mut self) {
        if self.reported.elapsed() < REPORT_INTERVAL {
            return;
        }
        self.reported = Instant::now();
        let metrics = std::mem::take(&mut self.metrics);
        if metrics == Metrics::default() {
            return;
        }
        let Metrics {
            unparsable,
            duplicate,
            dropped,
            pruned,
        } = metrics;
        debug!(
            unparsable,
            duplicate, dropped, pruned, "transaction storage"
        );
    }

    /// Store `txs` as packets, each a batch of one, and check them
    fn push_packets(
        &mut self,
        checker: &mut Checker<'_, CheckMeta>,
        txs: impl IntoIterator<Item = TransactionPtr>,
    ) -> Result<()> {
        let mut batches = ArrayVec::<_, MAX_CHECK_TXS>::new();
        for tx in txs.into_iter().take(MAX_CHECK_TXS) {
            // SAFETY: Handed in, in this session's mapping; the batch owns it from here.
            let batch = unsafe {
                TransactionPtrBatch::from_transactions(
                    [(&tx, ())].into_iter(),
                    PACKET,
                    self.allocator,
                )
            };
            let Some(batch) = batch else {
                // SAFETY: Handed in, so the store's alone.
                unsafe { tx.free(self.allocator) };
                bail!("no shared memory for a packet's batch");
            };
            batches.push(batch);
        }
        self.push(checker, batches)
    }

    /// Store `batches`, whose members fit one CHECK, and send that CHECK
    fn push(
        &mut self,
        checker: &mut Checker<'_, CheckMeta>,
        batches: impl IntoIterator<Item = TransactionPtrBatch<(), BundleMeta>>,
    ) -> Result<()> {
        let mut indices = ArrayVec::<usize, MAX_CHECK_TXS>::new();
        for batch in batches {
            if let Some(index) = self.admit(batch) {
                indices
                    .try_push(index)
                    .map_err(|_| anyhow!("more entries than a CHECK holds"))?;
            }
        }
        if !self.send(checker, INTAKE_FLAGS, &indices)? {
            bail!("checker refused a CHECK of new transactions");
        }
        Ok(())
    }

    /// Store `batch` unranked, or free it if it is empty, unparsable, or a duplicate
    fn admit(&mut self, batch: TransactionPtrBatch<(), BundleMeta>) -> Option<usize> {
        let len = batch.len();
        let mut signatures = SmallVec::new();
        let mut writable = SmallVec::new();
        let mut accounts = SmallVec::new();
        // Parse: v1 transactions keep signatures at the end, so this is a full parse
        let parsed = !batch.is_empty()
            && batch.iter().all(|(tx, ())| {
                let Ok(view) = UnsanitizedTransactionView::try_new_unsanitized(tx.data()) else {
                    return false;
                };
                let Some(&signature) = view.signatures().first() else {
                    return false;
                };
                signatures.push(signature);
                writable.push(view.total_writable_lookup_accounts());
                union_extend(&mut accounts, static_accounts(&view));
                true
            });
        // Only the crank may use the tip-payment program
        if !parsed || accounts.iter().any(|(key, _)| *key == self.tip_program) {
            self.metrics.unparsable = self.metrics.unparsable.saturating_add(len);
            // SAFETY: Handed in, so the store's alone.
            unsafe { free_batch(batch, self.allocator) };
            return None;
        }
        if signatures
            .iter()
            .any(|signature| self.signatures.contains(signature))
        {
            self.metrics.duplicate = self.metrics.duplicate.saturating_add(len);
            // SAFETY: Handed in, so the store's alone.
            unsafe { free_batch(batch, self.allocator) };
            return None;
        }
        self.signatures.extend(signatures.iter().copied());
        let rank = if batch.batch_meta().flags == 0 {
            (false, 0)
        } else {
            let rank = self.next_bundle;
            self.next_bundle = rank.saturating_sub(1);
            (true, rank)
        };
        Some(self.entries.insert(Entry {
            batch,
            signatures,
            writable,
            accounts,
            rank,
            checking: 0,
            failed: false,
        }))
    }

    /// Send one CHECK over the entries at `indices`; false while the checker is full
    fn send(
        &mut self,
        checker: &mut Checker<'_, CheckMeta>,
        flags: u16,
        indices: &[usize],
    ) -> Result<bool> {
        let mut members = ArrayVec::<_, MAX_CHECK_TXS>::new();
        for &index in indices {
            let entry = self
                .entries
                .get(index)
                .ok_or_else(|| anyhow!("CHECK of an entry not in storage"))?;
            let id = u32::try_from(index)?;
            for ((tx, ()), &writable) in entry.batch.iter().zip(&entry.writable) {
                // SAFETY: Stored transactions are in the store's allocator's mapping.
                let region = unsafe { tx.to_sharable_transaction_region(self.allocator) };
                members
                    .try_push((region, CheckMeta { id, writable }))
                    .map_err(|_| anyhow!("more members than a CHECK holds"))?;
            }
        }
        if members.is_empty() {
            return Ok(true);
        }
        // SAFETY: An entry in a CHECK is neither ranked nor removed before its last reply.
        if unsafe { checker.send(flags, &members)? } == 0 {
            return Ok(false);
        }
        for &index in indices {
            if let Some(entry) = self.entries.get_mut(index) {
                entry.checking = u8::try_from(entry.batch.len()).unwrap_or(u8::MAX);
                entry.failed = false;
            }
        }
        Ok(true)
    }

    /// Rank the entry at `index`, dropping the worst entries past `capacity`
    fn rank(&mut self, index: usize) {
        let Some(entry) = self.entries.get(index) else {
            return;
        };
        self.ranked.insert((entry.rank.0, entry.rank.1, index));
        self.ranked_txs = self.ranked_txs.saturating_add(entry.batch.len());
        while self.ranked_txs > self.capacity
            && let Some(worst) = self.ranked.first().copied()
        {
            self.unrank(&worst);
            if let Some(entry) = self.remove(worst.2) {
                self.metrics.dropped = self.metrics.dropped.saturating_add(entry.batch.len());
                // SAFETY: Just unranked, so in no CHECK: the store's alone.
                unsafe { free_batch(entry.batch, self.allocator) };
            }
        }
    }

    /// Take the entry with `key` out of `ranked`
    fn unrank(&mut self, key: &(bool, u64, usize)) {
        if self.ranked.remove(key)
            && let Some(entry) = self.entries.get(key.2)
        {
            self.ranked_txs = self.ranked_txs.saturating_sub(entry.batch.len());
        }
    }

    /// Take the entry at `index` out of storage, for the caller to own
    fn remove(&mut self, index: usize) -> Option<Entry> {
        let entry = self.entries.try_remove(index)?;
        for signature in &entry.signatures {
            self.signatures.remove(signature);
        }
        Some(entry)
    }
}

/// Whether a checked transaction can't land
fn check_failed(response: &CheckResponse) -> bool {
    const MASK: u8 = status_check_flags::PERFORMED
        | status_check_flags::TOO_OLD
        | status_check_flags::ALREADY_PROCESSED
        | status_check_flags::INVALID_NONCE
        | status_check_flags::UNSUPPORTED_VERSION;
    response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0
        || response.status_check_flags & MASK != status_check_flags::PERFORMED
        // A compute budget the bank rejects fails only the scheduling details
        || response.scheduling_details_flags & scheduling_details_flags::FAILED != 0
}

/// What the leader earns per compute unit, in micro-lamports; 0 if the check couldn't tell
fn priority(response: &CheckResponse) -> u64 {
    if response.scheduling_details_flags & scheduling_details_flags::PERFORMED == 0 {
        return 0;
    }
    let reward = response
        .prioritization_fee
        // Half the base fee is burned
        .saturating_add(response.transaction_fee / 2);
    reward
        .saturating_mul(1_000_000)
        .checked_div(response.estimated_cost_units)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::scheduler::channel::REVERT_PROTECT,
        agave_scheduler_bindings::{
            CheckWorkerToPackMessage, PackToCheckWorkerMessage, processed_codes,
        },
        agave_scheduling_utils::responses_region::resolve_responses_from_iter,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_signer::Signer,
        solana_transaction::Transaction,
    };

    /// A fresh 128 MiB allocator for tests
    fn allocator() -> Allocator {
        // SAFETY: A fresh file, joined by no one else.
        unsafe {
            Allocator::create(
                &tempfile::tempfile().unwrap(),
                128 * 1024 * 1024,
                4,
                2 * 1024 * 1024,
            )
        }
        .unwrap()
    }

    /// A store of `capacity` whose sources stay empty; tests push directly
    fn storage(capacity: usize, allocator: &Allocator) -> Storage<'_> {
        let (_, tpu_to_pack) = shaq::spsc::pair::<TpuToPackMessage>(1).unwrap();
        let (_, remote_tpu) = rtrb::RingBuffer::new(1);
        let (_, packets) = rtrb::RingBuffer::new(1);
        let (_, bundles) = rtrb::RingBuffer::new(1);
        Storage::new(
            capacity,
            Pubkey::new_unique(),
            tpu_to_pack,
            remote_tpu,
            packets,
            bundles,
            allocator,
        )
    }

    /// A checker, and the check worker's ends of its queues
    fn checker(
        allocator: &Allocator,
    ) -> (
        Checker<'_, CheckMeta>,
        shaq::mpmc::Consumer<PackToCheckWorkerMessage>,
        shaq::mpmc::Producer<CheckWorkerToPackMessage>,
    ) {
        let (requests, worker_requests) =
            shaq::mpmc::pair::<PackToCheckWorkerMessage>(CHECK_QUEUE_CAPACITY).unwrap();
        let (worker_replies, replies) =
            shaq::mpmc::pair::<CheckWorkerToPackMessage>(CHECK_QUEUE_CAPACITY).unwrap();
        (
            Checker::new(requests, replies, allocator),
            worker_requests,
            worker_replies,
        )
    }

    fn tx(allocator: &Allocator) -> TransactionPtr {
        let payer = Keypair::new();
        let tx = Transaction::new_signed_with_payer(
            &[],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::default(),
        );
        TransactionPtr::from_slice(&bincode::serialize(&tx).unwrap(), allocator).unwrap()
    }

    fn sig(tx: &TransactionPtr) -> Signature {
        UnsanitizedTransactionView::try_new_unsanitized(tx.data())
            .unwrap()
            .signatures()[0]
    }

    /// A two-transaction bundle
    fn bundle(allocator: &Allocator) -> TransactionPtrBatch<(), BundleMeta> {
        let txs = [tx(allocator), tx(allocator)];
        let meta = BundleMeta {
            slot: 0,
            flags: REVERT_PROTECT,
            id: 0,
        };
        // SAFETY: Allocated above; the batch owns them from here.
        unsafe {
            TransactionPtrBatch::from_transactions(txs.iter().map(|tx| (tx, ())), meta, allocator)
        }
        .unwrap()
    }

    /// The first signature of a batch's first transaction
    fn first_sig(batch: &TransactionPtrBatch<(), BundleMeta>) -> Signature {
        sig(&batch.iter().next().unwrap().0)
    }

    /// A reply that a transaction can land, paying `fee` lamports for 1000 compute units
    fn landable(fee: u64) -> CheckResponse {
        CheckResponse {
            status_check_flags: status_check_flags::REQUESTED | status_check_flags::PERFORMED,
            scheduling_details_flags: scheduling_details_flags::REQUESTED
                | scheduling_details_flags::PERFORMED,
            prioritization_fee: fee,
            estimated_cost_units: 1000,
            // SAFETY: Plain data, so all-zero is valid.
            ..unsafe { std::mem::zeroed() }
        }
    }

    /// A reply that a transaction already landed
    fn landed() -> CheckResponse {
        CheckResponse {
            status_check_flags: status_check_flags::REQUESTED
                | status_check_flags::PERFORMED
                | status_check_flags::ALREADY_PROCESSED,
            // SAFETY: Plain data, so all-zero is valid.
            ..unsafe { std::mem::zeroed() }
        }
    }

    /// A reply that a transaction's compute budget is one the bank rejects
    fn bad_budget() -> CheckResponse {
        CheckResponse {
            scheduling_details_flags: scheduling_details_flags::REQUESTED
                | scheduling_details_flags::PERFORMED
                | scheduling_details_flags::FAILED,
            ..landable(1)
        }
    }

    /// Answer waiting CHECKs, the `i`th transaction with `reply(i)`; their signatures, in order
    fn answer(
        allocator: &Allocator,
        requests: &shaq::mpmc::Consumer<PackToCheckWorkerMessage>,
        replies: &shaq::mpmc::Producer<CheckWorkerToPackMessage>,
        mut reply: impl FnMut(usize) -> CheckResponse,
    ) -> Vec<Signature> {
        let mut checked = vec![];
        while let Some(message) = requests.try_read() {
            assert!(message.flags == INTAKE_FLAGS || message.flags == RECHECK_FLAGS);
            // SAFETY: A CHECK the store sent, over transactions it still holds.
            let batch = unsafe {
                TransactionPtrBatch::<CheckMeta, ()>::from_sharable_transaction_batch_region(
                    &message.batch,
                    allocator,
                )
            };
            let first = checked.len();
            checked.extend(batch.iter().map(|(tx, _)| sig(&tx)));
            let responses =
                resolve_responses_from_iter(allocator, (first..checked.len()).map(&mut reply))
                    .unwrap();
            replies
                .try_write(CheckWorkerToPackMessage {
                    batch: message.batch,
                    processed_code: processed_codes::PROCESSED,
                    responses,
                })
                .unwrap();
        }
        checked
    }

    /// Take every entry out, in order, and free it
    fn pop_all(storage: &mut Storage, allocator: &Allocator) -> Vec<Signature> {
        let mut popped = vec![];
        while let Some((batch, _)) = storage.pop(|_| true) {
            popped.push(first_sig(&batch));
            // SAFETY: Taken out of the store, so the test's alone.
            unsafe { free_batch(batch, allocator) };
        }
        popped
    }

    /// Checks drop what can't land and rank the rest, and `pop` takes the best first
    #[test]
    fn checks_prune_and_rank() {
        let allocator = allocator();
        let (mut checker, requests, replies) = checker(&allocator);
        let mut storage = storage(1024, &allocator);
        let txs: Vec<_> = (0..100).map(|_| tx(&allocator)).collect();
        let sigs: Vec<_> = txs.iter().map(sig).collect();
        let mut txs = txs.into_iter();
        while txs.len() != 0 {
            storage.push_packets(&mut checker, txs.by_ref()).unwrap();
        }
        assert!(
            storage.pop(|_| true).is_none(),
            "what a CHECK holds is out of reach"
        );

        // Of every five, one landed and one has a bad budget; the rest pay by arrival
        let sent = answer(&allocator, &requests, &replies, |i| match i % 5 {
            0 => landed(),
            1 => bad_budget(),
            _ => landable(i as u64),
        });
        assert_eq!(sent, sigs, "checked in arrival order");
        checker.poll(|result| storage.checked(&result)).unwrap();
        assert_eq!(storage.metrics.pruned, 40);

        let expected: Vec<_> = (0..100)
            .rev()
            .filter(|i| i % 5 > 1)
            .map(|i| sigs[i])
            .collect();
        assert_eq!(
            pop_all(&mut storage, &allocator),
            expected,
            "best first, the pruned gone"
        );
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    /// `pop` skips what `runnable` rejects and leaves it ranked
    #[test]
    fn pop_skips_rejected() {
        let allocator = allocator();
        let (mut checker, requests, replies) = checker(&allocator);
        let mut storage = storage(1024, &allocator);
        let txs: Vec<_> = (0..3).map(|_| tx(&allocator)).collect();
        let sigs: Vec<_> = txs.iter().map(sig).collect();
        storage.push_packets(&mut checker, txs).unwrap();
        answer(&allocator, &requests, &replies, |i| landable(i as u64 + 1));
        checker.poll(|result| storage.checked(&result)).unwrap();

        let mut first = true;
        let (batch, _) = storage.pop(|_| !std::mem::take(&mut first)).unwrap();
        assert_eq!(first_sig(&batch), sigs[1], "the best was skipped");
        // SAFETY: Taken out of the store, so the test's alone.
        unsafe { free_batch(batch, &allocator) };
        assert_eq!(
            pop_all(&mut storage, &allocator),
            [sigs[2], sigs[0]],
            "the skipped one is still first"
        );
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    /// A full store keeps its best: a better arrival evicts the worst, a worse one goes
    #[test]
    fn full_store_keeps_best() {
        let allocator = allocator();
        let (mut checker, requests, replies) = checker(&allocator);
        let mut storage = storage(2, &allocator);
        let rich = tx(&allocator);
        let rich_sig = sig(&rich);
        storage
            .push_packets(&mut checker, [tx(&allocator), rich])
            .unwrap();
        answer(&allocator, &requests, &replies, |i| {
            landable(if i == 0 { 10 } else { 1000 })
        });
        checker.poll(|result| storage.checked(&result)).unwrap();

        let better = tx(&allocator);
        let better_sig = sig(&better);
        storage.push_packets(&mut checker, [better]).unwrap();
        answer(&allocator, &requests, &replies, |_| landable(100));
        checker.poll(|result| storage.checked(&result)).unwrap();
        assert_eq!(storage.metrics.dropped, 1, "the worst made room");

        storage
            .push_packets(&mut checker, [tx(&allocator)])
            .unwrap();
        answer(&allocator, &requests, &replies, |_| landable(1));
        checker.poll(|result| storage.checked(&result)).unwrap();
        assert_eq!(storage.metrics.dropped, 2, "the worse arrival went");

        assert_eq!(pop_all(&mut storage, &allocator), [rich_sig, better_sig]);
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    /// Re-checks sweep from the best to the worst, then from the best again
    #[test]
    fn recheck_sweeps_best_first_and_wraps() {
        let allocator = allocator();
        let (mut checker, requests, replies) = checker(&allocator);
        let mut storage = storage(1024, &allocator);
        // More than one CHECK's worth
        let txs: Vec<_> = (0..=MAX_CHECK_TXS).map(|_| tx(&allocator)).collect();
        let sigs: Vec<_> = txs.iter().map(sig).collect();
        let mut txs = txs.into_iter();
        while txs.len() != 0 {
            storage.push_packets(&mut checker, txs.by_ref()).unwrap();
        }
        answer(&allocator, &requests, &replies, |i| landable(i as u64 + 1));
        checker.poll(|result| storage.checked(&result)).unwrap();

        storage.dispatch(&mut checker).unwrap();
        let rechecked = answer(&allocator, &requests, &replies, |i| {
            // Reverse the ranking
            landable(i as u64 + 1)
        });
        let best_first: Vec<_> = sigs.iter().rev().copied().collect();
        assert_eq!(rechecked, best_first, "best first");
        checker.poll(|result| storage.checked(&result)).unwrap();

        storage.dispatch(&mut checker).unwrap();
        let rechecked = answer(&allocator, &requests, &replies, |_| landable(1));
        assert_eq!(rechecked, sigs, "from the best again, as re-ranked");
        checker.poll(|result| storage.checked(&result)).unwrap();

        assert_eq!(pop_all(&mut storage, &allocator).len(), sigs.len());
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    /// A duplicate is freed before its CHECK, and counted
    #[test]
    fn push_dedups() {
        let allocator = allocator();
        let (mut checker, requests, replies) = checker(&allocator);
        let mut storage = storage(4, &allocator);
        let first = tx(&allocator);
        let copy = TransactionPtr::from_slice(first.data(), &allocator).unwrap();
        storage.push_packets(&mut checker, [first, copy]).unwrap();
        assert_eq!(
            answer(&allocator, &requests, &replies, |_| landable(1)).len(),
            1
        );
        checker.poll(|result| storage.checked(&result)).unwrap();
        assert_eq!(storage.metrics.duplicate, 1);
        assert_eq!(pop_all(&mut storage, &allocator).len(), 1);
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }

    /// Bundles come out before packets, oldest first; one with a failing member goes whole
    #[test]
    fn bundles_first_oldest_first() {
        let allocator = allocator();
        let (mut checker, requests, replies) = checker(&allocator);
        let mut storage = storage(1024, &allocator);
        let packet = tx(&allocator);
        let packet_sig = sig(&packet);
        storage.push_packets(&mut checker, [packet]).unwrap();
        let (older, newer, doomed) = (bundle(&allocator), bundle(&allocator), bundle(&allocator));
        let (older_sig, newer_sig) = (first_sig(&older), first_sig(&newer));
        storage.push(&mut checker, [older, newer, doomed]).unwrap();
        // The packet, then each bundle's two members; the doomed one's second already landed
        answer(&allocator, &requests, &replies, |i| {
            if i == 6 { landed() } else { landable(1000) }
        });
        checker.poll(|result| storage.checked(&result)).unwrap();
        assert_eq!(storage.metrics.pruned, 2, "the doomed bundle went whole");
        assert_eq!(
            pop_all(&mut storage, &allocator),
            [older_sig, newer_sig, packet_sig]
        );
        assert_eq!(allocator.outstanding_allocation_bytes(), 0);
    }
}
