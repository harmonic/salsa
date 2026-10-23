//! Tip program crank: points tips at this validator's tip distribution account
//! and the block builder, ahead of the block's other transactions. The validator
//! signs it so external schedulers never hold the identity key

use {
    solana_account::ReadableAccount,
    solana_clock::Epoch,
    solana_keypair::Keypair,
    solana_perf::packet::PACKET_DATA_SIZE,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_sdk_ids::{compute_budget, system_program},
    solana_signer::Signer,
    std::sync::Mutex,
    thiserror::Error,
};

/// Length of the memo the crank carries
pub const MEMO_LEN: usize = 3;

// Account indices. Accounts before the compute budget program are writable
const IDENTITY: usize = 0;
const TIP_PAYMENT_ACCOUNTS: [usize; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
const TIP_DISTRIBUTION_CONFIG: usize = 9;
const TIP_PAYMENT_CONFIG: usize = 10;
const OLD_TIP_RECEIVER: usize = 11;
const OLD_BLOCK_BUILDER: usize = 12;
const NEW_TIP_RECEIVER: usize = 13;
const NEW_BLOCK_BUILDER: usize = 14;
const VOTE_ACCOUNT: usize = 15;
const COMPUTE_BUDGET_PROGRAM: usize = 16;
const TIP_PAYMENT_PROGRAM: usize = 17;
const MEMO_PROGRAM: usize = 18;
const SYSTEM_PROGRAM: usize = 19;
const TIP_DISTRIBUTION_PROGRAM: usize = 20;

/// Accounts in the crank that only changes the tip payment config
const UPDATE_ACCOUNTS: usize = 19;
/// Accounts in the crank that also creates the tip distribution account
const CREATE_ACCOUNTS: usize = 21;

/// Start of the signed message, after the signature count and the signature
const MESSAGE_START: usize = 65;

// Anchor discriminators of the tip programs' config account and instructions
const CONFIG_ACCOUNT: [u8; 8] = [155, 12, 170, 224, 30, 250, 204, 130];
const CHANGE_TIP_RECEIVER: [u8; 8] = [69, 99, 22, 71, 11, 231, 86, 143];
const CHANGE_BLOCK_BUILDER: [u8; 8] = [134, 80, 38, 137, 165, 21, 114, 123];
const INITIALIZE_TIP_DISTRIBUTION_ACCOUNT: [u8; 8] = [120, 191, 25, 182, 111, 49, 179, 55];
/// Compute budget program instruction tag
const SET_COMPUTE_UNIT_LIMIT: u8 = 2;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum TipCrankError {
    #[error("the tip payment config account is missing or invalid")]
    InvalidConfig,
    #[error("the block builder is null or another account of the crank")]
    InvalidBlockBuilder,
    #[error("the identity, vote account, or tip program accounts collide")]
    DuplicateAccount,
}

pub struct TipCrankConfig {
    pub tip_payment_program_id: Pubkey,
    pub tip_distribution_program_id: Pubkey,
    /// The account with authority to upload merkle roots to this validator's tip distribution account
    pub merkle_root_upload_authority: Pubkey,
    /// This validator's commission on tips, in basis points
    pub commission_bps: u16,
}

pub struct TipCrank {
    /// The accounts every crank has, at their indices
    keys: [Pubkey; CREATE_ACCOUNTS],
    merkle_root_upload_authority: Pubkey,
    commission_bps: u16,
    /// The last vote account and epoch seen, with their tip distribution account and bump
    tip_receiver: Mutex<Option<(Pubkey, Epoch, Pubkey, u8)>>,
}

impl TipCrank {
    pub fn new(config: TipCrankConfig) -> Self {
        let TipCrankConfig {
            tip_payment_program_id,
            tip_distribution_program_id,
            merkle_root_upload_authority,
            commission_bps,
        } = config;
        let mut keys = [Pubkey::default(); CREATE_ACCOUNTS];
        for (i, index) in TIP_PAYMENT_ACCOUNTS.into_iter().enumerate() {
            let mut seed = *b"TIP_ACCOUNT_0";
            seed[12] += i as u8;
            keys[index] = Pubkey::find_program_address(&[&seed], &tip_payment_program_id).0;
        }
        keys[TIP_DISTRIBUTION_CONFIG] =
            Pubkey::find_program_address(&[b"CONFIG_ACCOUNT"], &tip_distribution_program_id).0;
        keys[TIP_PAYMENT_CONFIG] =
            Pubkey::find_program_address(&[b"CONFIG_ACCOUNT"], &tip_payment_program_id).0;
        keys[COMPUTE_BUDGET_PROGRAM] = compute_budget::id();
        keys[TIP_PAYMENT_PROGRAM] = tip_payment_program_id;
        keys[MEMO_PROGRAM] = spl_memo_interface::v3::id();
        keys[SYSTEM_PROGRAM] = system_program::id();
        keys[TIP_DISTRIBUTION_PROGRAM] = tip_distribution_program_id;
        Self {
            keys,
            merkle_root_upload_authority,
            commission_bps,
            tip_receiver: Mutex::default(),
        }
    }

    pub fn tip_payment_program_id(&self) -> Pubkey {
        self.keys[TIP_PAYMENT_PROGRAM]
    }

    /// `vote_account`'s tip distribution account and bump for `epoch`
    fn tip_receiver(&self, vote_account: Pubkey, epoch: Epoch) -> (Pubkey, u8) {
        let mut cached = self.tip_receiver.lock().unwrap();
        if let Some((cached_vote_account, cached_epoch, address, bump)) = *cached
            && (cached_vote_account, cached_epoch) == (vote_account, epoch)
        {
            return (address, bump);
        }
        let tip_receiver = Pubkey::find_program_address(
            &[
                b"TIP_DISTRIBUTION_ACCOUNT",
                vote_account.as_ref(),
                &epoch.to_le_bytes(),
            ],
            &self.keys[TIP_DISTRIBUTION_PROGRAM],
        );
        *cached = Some((vote_account, epoch, tip_receiver.0, tip_receiver.1));
        tip_receiver
    }

    /// The crank for the block `bank` is producing, signed by `keypair`. It
    /// points tips at the slot leader's tip distribution account, creating the
    /// account first if needed, and at `block_builder`. None if they already go there
    pub fn crank(
        &self,
        bank: &Bank,
        keypair: &Keypair,
        block_builder: &Pubkey,
        block_builder_commission: u64,
        memo: [u8; MEMO_LEN],
    ) -> Result<Option<Vec<u8>>, TipCrankError> {
        let vote_account = bank.leader().vote_address;
        let (tip_receiver, bump) = self.tip_receiver(vote_account, bank.epoch());
        let Some((old_tip_receiver, old_block_builder, old_commission)) = bank
            .get_account(&self.keys[TIP_PAYMENT_CONFIG])
            .filter(|config| *config.owner() == self.keys[TIP_PAYMENT_PROGRAM])
            .and_then(|config| parse_config(config.data()))
        else {
            return Err(TipCrankError::InvalidConfig);
        };
        // Anyone can fund the address, so only the owner shows it was created
        let create = bank
            .get_account(&tip_receiver)
            .is_none_or(|account| *account.owner() != self.keys[TIP_DISTRIBUTION_PROGRAM]);
        if !create
            && old_tip_receiver == tip_receiver
            && old_block_builder == *block_builder
            && old_commission == block_builder_commission
        {
            return Ok(None);
        }

        let (num_keys, mut num_instructions, compute_units): (usize, u8, u32) = if create {
            (CREATE_ACCOUNTS, 5, 130_000)
        } else {
            (UPDATE_ACCOUNTS, 4, 83_000)
        };
        let identity = keypair.pubkey();
        let mut keys = self.keys;
        keys[IDENTITY] = identity;
        keys[NEW_TIP_RECEIVER] = tip_receiver;
        keys[VOTE_ACCOUNT] = vote_account;
        let keys = &mut keys[..num_keys];
        // Unset slots are null, so this also rejects a null builder, the system program
        let new_block_builder = if *block_builder == identity {
            IDENTITY
        } else if keys.contains(block_builder) {
            return Err(TipCrankError::InvalidBlockBuilder);
        } else {
            keys[NEW_BLOCK_BUILDER] = *block_builder;
            NEW_BLOCK_BUILDER
        };
        // Anyone can make an old account the memo program, which the crank must then write, and
        // no transaction writes a program it calls: that crank goes without its memo
        let with_memo = ![old_tip_receiver, old_block_builder].contains(&keys[MEMO_PROGRAM]);
        if !with_memo {
            keys[MEMO_PROGRAM] = Pubkey::default();
            num_instructions = num_instructions.saturating_sub(1);
        }
        // The previous leader set the old accounts, so they may be any of the crank's
        let old_tip_receiver = place(keys, OLD_TIP_RECEIVER, old_tip_receiver);
        let old_block_builder = place(keys, OLD_BLOCK_BUILDER, old_block_builder);
        // The tip payment program writes them, and the programs the crank calls are read-only
        if old_tip_receiver.max(old_block_builder) >= COMPUTE_BUDGET_PROGRAM {
            return Err(TipCrankError::InvalidConfig);
        }
        for (slot, index) in [
            (OLD_TIP_RECEIVER, old_tip_receiver),
            (OLD_BLOCK_BUILDER, old_block_builder),
            (NEW_BLOCK_BUILDER, new_block_builder),
        ] {
            if slot != index {
                let filler = unused_key(keys);
                keys[slot] = filler;
            }
        }
        if !with_memo {
            keys[MEMO_PROGRAM] = unused_key(keys);
        }
        if (1..num_keys).any(|i| keys[..i].contains(&keys[i])) {
            return Err(TipCrankError::DuplicateAccount);
        }

        let mut tx = Vec::with_capacity(PACKET_DATA_SIZE);
        // One signature, written once the message is
        tx.push(1);
        tx.extend_from_slice(&[0; MESSAGE_START - 1]);
        // Message header: the identity signs and the programs are read-only
        tx.extend_from_slice(&[1, 0, (num_keys - COMPUTE_BUDGET_PROGRAM) as u8]);
        tx.push(num_keys as u8);
        for key in keys.iter() {
            tx.extend_from_slice(key.as_ref());
        }
        tx.extend_from_slice(bank.last_blockhash().as_ref());
        tx.push(num_instructions);
        instruction(
            &mut tx,
            COMPUTE_BUDGET_PROGRAM,
            &[],
            &[&[SET_COMPUTE_UNIT_LIMIT], &compute_units.to_le_bytes()],
        );
        if create {
            instruction(
                &mut tx,
                TIP_DISTRIBUTION_PROGRAM,
                &[&[
                    TIP_DISTRIBUTION_CONFIG,
                    NEW_TIP_RECEIVER,
                    VOTE_ACCOUNT,
                    IDENTITY,
                    SYSTEM_PROGRAM,
                ]],
                &[
                    &INITIALIZE_TIP_DISTRIBUTION_ACCOUNT,
                    self.merkle_root_upload_authority.as_ref(),
                    &self.commission_bps.to_le_bytes(),
                    &[bump],
                ],
            );
        }
        // The program pays the tips accrued so far to the old receiver and block builder
        instruction(
            &mut tx,
            TIP_PAYMENT_PROGRAM,
            &[
                &[
                    TIP_PAYMENT_CONFIG,
                    old_tip_receiver,
                    NEW_TIP_RECEIVER,
                    old_block_builder,
                ],
                &TIP_PAYMENT_ACCOUNTS,
                &[IDENTITY],
            ],
            &[&CHANGE_TIP_RECEIVER],
        );
        instruction(
            &mut tx,
            TIP_PAYMENT_PROGRAM,
            &[
                &[
                    TIP_PAYMENT_CONFIG,
                    NEW_TIP_RECEIVER,
                    old_block_builder,
                    new_block_builder,
                ],
                &TIP_PAYMENT_ACCOUNTS,
                &[IDENTITY],
            ],
            &[
                &CHANGE_BLOCK_BUILDER,
                &block_builder_commission.to_le_bytes(),
            ],
        );
        if with_memo {
            instruction(&mut tx, MEMO_PROGRAM, &[], &[&memo]);
        }
        let signature = keypair.sign_message(&tx[MESSAGE_START..]);
        tx[1..MESSAGE_START].copy_from_slice(signature.as_ref());
        Ok(Some(tx))
    }
}

/// The tip receiver, block builder, and block builder commission in a tip
/// payment config account. The program can't set a null account, which is the
/// system program, so one means the data is invalid
fn parse_config(data: &[u8]) -> Option<(Pubkey, Pubkey, u64)> {
    let (discriminator, data) = data.split_first_chunk::<8>()?;
    let (tip_receiver, data) = data.split_first_chunk::<32>()?;
    let (block_builder, data) = data.split_first_chunk::<32>()?;
    let (commission, _bumps) = data.split_first_chunk::<8>()?;
    (*discriminator == CONFIG_ACCOUNT && *tip_receiver != [0; 32] && *block_builder != [0; 32])
        .then(|| {
            (
                Pubkey::from(*tip_receiver),
                Pubkey::from(*block_builder),
                u64::from_le_bytes(*commission),
            )
        })
}

/// The index of `key`, which isn't null: where the crank already has it, else
/// `slot`. Unset slots are null, so `key` never matches one
fn place(keys: &mut [Pubkey], slot: usize, key: Pubkey) -> usize {
    match keys.iter().position(|k| *k == key) {
        Some(index) => index,
        None => {
            keys[slot] = key;
            slot
        }
    }
}

/// A key the crank doesn't use, for a slot whose account is at another index
fn unused_key(keys: &[Pubkey]) -> Pubkey {
    // More candidates than keys, so one is always free
    (1..=u8::MAX)
        .map(|byte| Pubkey::new_from_array([byte; 32]))
        .find(|key| !keys.contains(key))
        .unwrap_or_default()
}

/// Append a compiled instruction. Every count and length is under 128, so each
/// compact-u16 prefix is one byte
fn instruction(tx: &mut Vec<u8>, program: usize, accounts: &[&[usize]], data: &[&[u8]]) {
    let accounts = accounts.iter().copied().flatten();
    let data = data.iter().copied().flatten();
    tx.push(program as u8);
    tx.push(accounts.clone().count() as u8);
    tx.extend(accounts.map(|&index| index as u8));
    tx.push(data.clone().count() as u8);
    tx.extend(data);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_account::AccountSharedData,
        solana_clock::Slot,
        solana_leader_schedule::SlotLeader,
        solana_runtime::{bank_forks::BankForks, genesis_utils::create_genesis_config},
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
        std::{
            collections::HashSet,
            sync::{Arc, RwLock},
        },
    };

    /// An instruction with its accounts resolved: program, accounts, data
    type Ix = (Pubkey, Vec<Pubkey>, Vec<u8>);

    struct Setup {
        tip_crank: TipCrank,
        tip_payment_program: Pubkey,
        tip_distribution_program: Pubkey,
        merkle_root_upload_authority: Pubkey,
        keypair: Keypair,
        bank_forks: Arc<RwLock<BankForks>>,
    }

    fn pda(seeds: &[&[u8]], program_id: &Pubkey) -> Pubkey {
        Pubkey::find_program_address(seeds, program_id).0
    }

    fn account(data: &[u8], owner: &Pubkey) -> AccountSharedData {
        let mut account = AccountSharedData::new(1_000_000, 0, owner);
        account.set_data_from_slice(data);
        account
    }

    fn config(tip_receiver: Pubkey, block_builder: Pubkey, commission: u64) -> Vec<u8> {
        let commission = commission.to_le_bytes();
        [
            &CONFIG_ACCOUNT[..],
            tip_receiver.as_ref(),
            block_builder.as_ref(),
            &commission,
            &[0; 9],
        ]
        .concat()
    }

    impl Setup {
        fn new() -> Self {
            let tip_payment_program = Pubkey::new_unique();
            let tip_distribution_program = Pubkey::new_unique();
            let merkle_root_upload_authority = Pubkey::new_unique();
            let genesis_config = create_genesis_config(1_000_000).genesis_config;
            Self {
                tip_crank: TipCrank::new(TipCrankConfig {
                    tip_payment_program_id: tip_payment_program,
                    tip_distribution_program_id: tip_distribution_program,
                    merkle_root_upload_authority,
                    commission_bps: 800,
                }),
                tip_payment_program,
                tip_distribution_program,
                merkle_root_upload_authority,
                keypair: Keypair::new(),
                bank_forks: Bank::new_with_bank_forks_for_tests(&genesis_config).1,
            }
        }

        /// A bank for `slot` led by this validator with `vote_account`
        fn bank(&self, slot: Slot, vote_account: Pubkey) -> Arc<Bank> {
            let root = self.bank_forks.read().unwrap().root_bank();
            let leader = SlotLeader {
                id: self.keypair.pubkey(),
                vote_address: vote_account,
            };
            let bank = Bank::new_from_parent(root, leader, slot);
            let mut bank_forks = self.bank_forks.write().unwrap();
            bank_forks.insert(bank).clone_without_scheduler()
        }

        fn tip_payment_config(&self) -> Pubkey {
            pda(&[b"CONFIG_ACCOUNT"], &self.tip_payment_program)
        }

        fn tip_receiver(&self, bank: &Bank) -> (Pubkey, u8) {
            let vote_account = bank.leader().vote_address;
            let seeds: &[&[u8]] = &[
                b"TIP_DISTRIBUTION_ACCOUNT",
                vote_account.as_ref(),
                &bank.epoch().to_le_bytes(),
            ];
            Pubkey::find_program_address(seeds, &self.tip_distribution_program)
        }

        fn store_config(&self, bank: &Bank, data: &[u8]) {
            let config = account(data, &self.tip_payment_program);
            bank.store_account(&self.tip_payment_config(), &config);
        }

        fn create_tip_receiver(&self, bank: &Bank) {
            let tip_receiver = account(&[1], &self.tip_distribution_program);
            bank.store_account(&self.tip_receiver(bank).0, &tip_receiver);
        }

        fn crank(
            &self,
            bank: &Bank,
            block_builder: &Pubkey,
        ) -> Result<Option<Vec<u8>>, TipCrankError> {
            self.tip_crank
                .crank(bank, &self.keypair, block_builder, 5, *b"FBA")
        }

        /// The instructions that move tips from `old_tip_receiver` and
        /// `old_block_builder` to this validator and `block_builder`
        fn expected(
            &self,
            bank: &Bank,
            create: bool,
            old_tip_receiver: Pubkey,
            old_block_builder: Pubkey,
            block_builder: Pubkey,
        ) -> Vec<Ix> {
            let identity = self.keypair.pubkey();
            let (tip_receiver, bump) = self.tip_receiver(bank);
            let config = self.tip_payment_config();
            let tip_payment = |accounts: [Pubkey; 4]| {
                let tip_accounts = (0..8).map(|i| {
                    pda(
                        &[format!("TIP_ACCOUNT_{i}").as_bytes()],
                        &self.tip_payment_program,
                    )
                });
                accounts
                    .into_iter()
                    .chain(tip_accounts)
                    .chain([identity])
                    .collect()
            };
            let compute_units: u32 = if create { 130_000 } else { 83_000 };
            let compute_budget = [&[2][..], &compute_units.to_le_bytes()].concat();
            let mut instructions = vec![(compute_budget::id(), vec![], compute_budget)];
            if create {
                let accounts = vec![
                    pda(&[b"CONFIG_ACCOUNT"], &self.tip_distribution_program),
                    tip_receiver,
                    bank.leader().vote_address,
                    identity,
                    system_program::id(),
                ];
                let authority = self.merkle_root_upload_authority;
                let commission = 800u16.to_le_bytes();
                let data = [
                    &INITIALIZE_TIP_DISTRIBUTION_ACCOUNT[..],
                    authority.as_ref(),
                    &commission,
                    &[bump],
                ];
                instructions.push((self.tip_distribution_program, accounts, data.concat()));
            }
            let old_accounts = [config, old_tip_receiver, tip_receiver, old_block_builder];
            let new_accounts = [config, tip_receiver, old_block_builder, block_builder];
            let commission = [&CHANGE_BLOCK_BUILDER[..], &5u64.to_le_bytes()].concat();
            instructions.extend([
                (
                    self.tip_payment_program,
                    tip_payment(old_accounts),
                    CHANGE_TIP_RECEIVER.to_vec(),
                ),
                (
                    self.tip_payment_program,
                    tip_payment(new_accounts),
                    commission,
                ),
            ]);
            let memo_program = spl_memo_interface::v3::id();
            if ![old_tip_receiver, old_block_builder].contains(&memo_program) {
                instructions.push((memo_program, vec![], b"FBA".to_vec()));
            }
            instructions
        }

        /// The crank's instructions, after checking it is a valid transaction
        /// for `bank`, paid and signed by the identity, that can write every
        /// account it passes but the system program
        fn decode(&self, bank: &Bank, crank: &[u8]) -> Vec<Ix> {
            let tx: Transaction = bincode::deserialize(crank).unwrap();
            assert_eq!(bincode::serialize(&tx).unwrap(), crank);
            assert_eq!(tx.message.recent_blockhash, bank.last_blockhash());
            tx.verify().unwrap();
            let tx =
                SanitizedTransaction::try_from_legacy_transaction(tx, &HashSet::default()).unwrap();
            let message = tx.message();
            assert!(!message.has_duplicates());
            assert_eq!(*message.fee_payer(), self.keypair.pubkey());
            let keys = message.account_keys();
            let resolve = |index: &u8| {
                let index = usize::from(*index);
                assert_eq!(
                    message.is_writable(index),
                    keys[index] != system_program::id()
                );
                keys[index]
            };
            message
                .instructions()
                .iter()
                .map(|ix| {
                    let program = keys[usize::from(ix.program_id_index)];
                    (
                        program,
                        ix.accounts.iter().map(resolve).collect(),
                        ix.data.clone(),
                    )
                })
                .collect()
        }
    }

    #[test]
    fn crank_only_when_config_differs() {
        let setup = Setup::new();
        let bank = setup.bank(1, Pubkey::new_unique());
        setup.create_tip_receiver(&bank);
        let tip_receiver = setup.tip_receiver(&bank).0;
        let block_builder = Pubkey::new_unique();
        setup.store_config(&bank, &config(tip_receiver, block_builder, 5));
        assert_eq!(setup.crank(&bank, &block_builder), Ok(None));

        let (other_tip_receiver, other_block_builder) =
            (Pubkey::new_unique(), Pubkey::new_unique());
        for (old_tip_receiver, old_block_builder, commission) in [
            (other_tip_receiver, block_builder, 5),
            (tip_receiver, other_block_builder, 5),
            (tip_receiver, block_builder, 6),
        ] {
            setup.store_config(
                &bank,
                &config(old_tip_receiver, old_block_builder, commission),
            );
            let crank = setup.crank(&bank, &block_builder).unwrap().unwrap();
            let expected = setup.expected(
                &bank,
                false,
                old_tip_receiver,
                old_block_builder,
                block_builder,
            );
            assert_eq!(setup.decode(&bank, &crank), expected);
        }
    }

    #[test]
    fn crank_creates_the_tip_distribution_account() {
        let setup = Setup::new();
        let block_builder = Pubkey::new_unique();
        let vote_account = Pubkey::new_unique();
        let next_epoch = setup
            .bank(1, vote_account)
            .epoch_schedule()
            .get_first_slot_in_epoch(1);
        // Each epoch and vote account has its own
        for bank in [
            setup.bank(2, vote_account),
            setup.bank(next_epoch, vote_account),
            setup.bank(next_epoch + 1, Pubkey::new_unique()),
        ] {
            let tip_receiver = setup.tip_receiver(&bank).0;
            setup.store_config(&bank, &config(tip_receiver, block_builder, 5));
            // Funding the address doesn't create it
            bank.store_account(&tip_receiver, &account(&[], &system_program::id()));
            let crank = setup.crank(&bank, &block_builder).unwrap().unwrap();
            let expected = setup.expected(&bank, true, tip_receiver, block_builder, block_builder);
            assert_eq!(setup.decode(&bank, &crank), expected);
        }
    }

    #[test]
    fn crank_reuses_accounts_it_shares_with_the_old_config() {
        let setup = Setup::new();
        let vote_account = Pubkey::new_unique();
        let block_builder = Pubkey::new_unique();
        let (update, create) = (setup.bank(1, vote_account), setup.bank(2, vote_account));
        setup.create_tip_receiver(&update);
        let tip_receiver = setup.tip_receiver(&update).0;
        let memo_program = spl_memo_interface::v3::id();
        let shared = [
            vote_account,
            tip_receiver,
            block_builder,
            setup.tip_payment_config(),
            pda(&[b"TIP_ACCOUNT_3"], &setup.tip_payment_program),
            pda(&[b"CONFIG_ACCOUNT"], &setup.tip_distribution_program),
            setup.tip_distribution_program,
            setup.tip_payment_program,
            compute_budget::id(),
            memo_program,
            // Fillers for the unused slots
            Pubkey::new_from_array([1; 32]),
            Pubkey::new_from_array([2; 32]),
        ];
        for (bank, create) in [(&update, false), (&create, true)] {
            let old_configs = shared.iter().flat_map(|&key| {
                [
                    (key, Pubkey::new_unique()),
                    (Pubkey::new_unique(), key),
                    (key, key),
                ]
            });
            // The memo program beside a key the crank would otherwise pick as a filler
            let old_configs = old_configs.chain([(Pubkey::new_from_array([1; 32]), memo_program)]);
            for (old_tip_receiver, old_block_builder) in old_configs {
                setup.store_config(bank, &config(old_tip_receiver, old_block_builder, 4));
                let result = setup.crank(bank, &block_builder);
                // The crank can't write a program it calls
                let called = [setup.tip_payment_program, compute_budget::id()]
                    .into_iter()
                    .chain(create.then_some(setup.tip_distribution_program))
                    .any(|program| program == old_tip_receiver || program == old_block_builder);
                if called {
                    assert_eq!(result, Err(TipCrankError::InvalidConfig));
                    continue;
                }
                let expected = setup.expected(
                    bank,
                    create,
                    old_tip_receiver,
                    old_block_builder,
                    block_builder,
                );
                assert_eq!(
                    setup.decode(bank, &result.unwrap().unwrap()),
                    expected,
                    "old tip receiver {old_tip_receiver}, old block builder {old_block_builder}, \
                     create {create}"
                );
            }
        }
    }

    #[test]
    fn crank_rejects_invalid_config() {
        let setup = Setup::new();
        let bank = setup.bank(1, Pubkey::new_unique());
        let block_builder = Pubkey::new_unique();
        assert_eq!(
            setup.crank(&bank, &block_builder),
            Err(TipCrankError::InvalidConfig)
        );

        let valid = config(Pubkey::new_unique(), Pubkey::new_unique(), 5);
        let mut wrong_discriminator = valid.clone();
        wrong_discriminator[0] ^= 1;
        let program = setup.tip_payment_program;
        for (data, owner) in [
            (valid.clone(), Pubkey::new_unique()),
            (valid[..valid.len() - 10].to_vec(), program),
            (wrong_discriminator, program),
            (config(Pubkey::default(), Pubkey::new_unique(), 5), program),
            (config(Pubkey::new_unique(), Pubkey::default(), 5), program),
        ] {
            bank.store_account(&setup.tip_payment_config(), &account(&data, &owner));
            assert_eq!(
                setup.crank(&bank, &block_builder),
                Err(TipCrankError::InvalidConfig)
            );
        }
    }

    #[test]
    fn crank_rejects_colliding_accounts() {
        let setup = Setup::new();
        let config = config(Pubkey::new_unique(), Pubkey::new_unique(), 5);
        let vote_account = Pubkey::new_unique();
        let bank = setup.bank(1, vote_account);
        setup.store_config(&bank, &config);
        // The crank without the system program
        setup.create_tip_receiver(&bank);
        for block_builder in [
            Pubkey::default(),
            setup.tip_payment_config(),
            vote_account,
            setup.tip_payment_program,
        ] {
            let result = setup.crank(&bank, &block_builder);
            assert_eq!(result, Err(TipCrankError::InvalidBlockBuilder));
        }
        for (slot, vote_account) in [(2, setup.keypair.pubkey()), (3, setup.tip_payment_program)] {
            let bank = setup.bank(slot, vote_account);
            setup.store_config(&bank, &config);
            let result = setup.crank(&bank, &Pubkey::new_unique());
            assert_eq!(result, Err(TipCrankError::DuplicateAccount));
        }
    }
}
