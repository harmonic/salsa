// Ported from jito-solana (core/src/tip_manager.rs)
// Adapted: Bank replaced with TipAccountData, RuntimeTransaction replaced with serialized Vec<u8>.
#![allow(dead_code)]

use {
    crate::{
        tip_distribution::{
            InitializeTipDistributionAccountInstruction,
            InitializeTipDistributionConfigInstruction, JitoTipDistributionConfig,
            TipDistributionAccount, TipDistributionError,
        },
        tip_payment::{
            ChangeBlockBuilderInstruction, ChangeTipReceiverInstruction,
            InitializeTipPaymentInstruction, JitoTipPaymentConfig, TipPaymentError,
        },
    },
    log::{debug, info},
    smallvec::SmallVec,
    solana_clock::Epoch,
    solana_hash::Hash,
    solana_instruction::{AccountMeta, Instruction},
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_sdk_ids::system_program,
    solana_signer::Signer,
    solana_transaction::Transaction,
    std::collections::HashSet,
    thiserror::Error,
};

// Jito-solana ports — keep close to upstream; silence workspace lints here
#[allow(clippy::arithmetic_side_effects)]
mod tip_distribution;
#[allow(clippy::arithmetic_side_effects)]
mod tip_payment;

#[derive(Debug, Clone, PartialEq, Error)]
pub enum TipManagerError {
    #[error("Account missing")]
    AccountMissing,
    #[error("Tip payment error: {0}")]
    TipPaymentError(#[from] TipPaymentError),
    #[error("Tip distribution error: {0}")]
    TipDistributionError(#[from] TipDistributionError),
}

pub type Result<T> = std::result::Result<T, TipManagerError>;

/// Block builder fee configuration: which account receives the block-builder
/// cut of tips and at what commission
pub struct BlockBuilderFeeInfo {
    /// Pubkey of the account that receives the block-builder cut of tips
    pub block_builder: Pubkey,
    /// Block-builder commission in basis points (0-10000), out of the tip total
    pub block_builder_commission: u64,
}

/// Replaces Bank for account lookups in the external scheduler.
/// Account data is fetched via IPC READ_ACCOUNT requests.
#[derive(Default)]
pub struct TipAccountData {
    pub tip_payment_config_data: Option<Vec<u8>>,
    pub tip_payment_config_owner: Option<Pubkey>,
    pub tip_distribution_config_data: Option<Vec<u8>>,
    pub tip_distribution_config_owner: Option<Pubkey>,
    pub tip_distribution_pda_data: Option<Vec<u8>>,
    pub tip_distribution_pda_owner: Option<Pubkey>,
    pub epoch: Epoch,
}

#[derive(Debug, Clone)]
struct TipPaymentProgramInfo {
    program_id: Pubkey,

    config_pda_bump: (Pubkey, u8),
    tip_pda_0: (Pubkey, u8),
    tip_pda_1: (Pubkey, u8),
    tip_pda_2: (Pubkey, u8),
    tip_pda_3: (Pubkey, u8),
    tip_pda_4: (Pubkey, u8),
    tip_pda_5: (Pubkey, u8),
    tip_pda_6: (Pubkey, u8),
    tip_pda_7: (Pubkey, u8),
}

/// Contains metadata regarding the tip-distribution account.
/// The PDAs contained in this struct are presumed to be owned by the program.
#[derive(Debug, Clone)]
struct TipDistributionProgramInfo {
    /// The tip-distribution program_id.
    program_id: Pubkey,

    /// Singleton [Config] PDA and bump tuple.
    config_pda_and_bump: (Pubkey, u8),
}

/// This config is used on each invocation to the `initialize_tip_distribution_account` instruction.
#[derive(Debug, Clone)]
pub struct TipDistributionAccountConfig {
    /// The account with authority to upload merkle-roots to this validator's [TipDistributionAccount].
    pub merkle_root_upload_authority: Pubkey,

    /// This validator's vote account.
    pub vote_account: Pubkey,

    /// This validator's commission rate BPS for tips in the [TipDistributionAccount].
    pub commission_bps: u16,
}

impl Default for TipDistributionAccountConfig {
    fn default() -> Self {
        Self {
            merkle_root_upload_authority: Pubkey::new_unique(),
            vote_account: Pubkey::new_unique(),
            commission_bps: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TipManager {
    tip_payment_program_info: TipPaymentProgramInfo,
    tip_distribution_program_info: TipDistributionProgramInfo,
    tip_distribution_account_config: TipDistributionAccountConfig,
    tip_accounts: HashSet<Pubkey>,
}

#[derive(Clone)]
pub struct TipManagerConfig {
    pub tip_payment_program_id: Pubkey,
    pub tip_distribution_program_id: Pubkey,
    pub tip_distribution_account_config: TipDistributionAccountConfig,
}

impl Default for TipManagerConfig {
    fn default() -> Self {
        TipManagerConfig {
            tip_payment_program_id: Pubkey::new_unique(),
            tip_distribution_program_id: Pubkey::new_unique(),
            tip_distribution_account_config: TipDistributionAccountConfig::default(),
        }
    }
}

impl TipManager {
    pub fn new(config: TipManagerConfig) -> TipManager {
        let TipManagerConfig {
            tip_payment_program_id,
            tip_distribution_program_id,
            tip_distribution_account_config,
        } = config;

        // https://github.com/jito-foundation/jito-programs/blob/8f55af0a9b31ac2192415b59ce2c47329ee255a2/mev-programs/programs/tip-payment/src/lib.rs#L33C42-L33C56
        let tip_payment_config_pda_bump =
            JitoTipPaymentConfig::find_program_address(&tip_payment_program_id);
        let tip_payment_account_pdas =
            JitoTipPaymentConfig::find_tip_payment_account_pdas(&tip_payment_program_id);

        let tip_distribution_config_pubkey_bump =
            JitoTipDistributionConfig::find_program_address(&tip_distribution_program_id);

        let tip_accounts = HashSet::from_iter(tip_payment_account_pdas.iter().map(|pda| pda.0));

        TipManager {
            tip_payment_program_info: TipPaymentProgramInfo {
                program_id: tip_payment_program_id,
                config_pda_bump: tip_payment_config_pda_bump,
                tip_pda_0: tip_payment_account_pdas[0],
                tip_pda_1: tip_payment_account_pdas[1],
                tip_pda_2: tip_payment_account_pdas[2],
                tip_pda_3: tip_payment_account_pdas[3],
                tip_pda_4: tip_payment_account_pdas[4],
                tip_pda_5: tip_payment_account_pdas[5],
                tip_pda_6: tip_payment_account_pdas[6],
                tip_pda_7: tip_payment_account_pdas[7],
            },
            tip_distribution_program_info: TipDistributionProgramInfo {
                program_id: tip_distribution_program_id,
                config_pda_and_bump: tip_distribution_config_pubkey_bump,
            },
            tip_distribution_account_config,
            tip_accounts,
        }
    }

    pub fn tip_payment_program_id(&self) -> Pubkey {
        self.tip_payment_program_info.program_id
    }

    pub fn tip_distribution_program_id(&self) -> Pubkey {
        self.tip_distribution_program_info.program_id
    }

    /// Returns the [Config] account owned by the tip-payment program.
    pub fn tip_payment_config_pubkey(&self) -> Pubkey {
        self.tip_payment_program_info.config_pda_bump.0
    }

    /// Returns the [Config] account owned by the tip-distribution program.
    pub fn tip_distribution_config_pubkey(&self) -> Pubkey {
        self.tip_distribution_program_info.config_pda_and_bump.0
    }

    pub fn get_tip_accounts(&self) -> &HashSet<Pubkey> {
        &self.tip_accounts
    }

    fn get_tip_payment_config_account(
        &self,
        account_state: &TipAccountData,
    ) -> Result<JitoTipPaymentConfig> {
        let data = account_state
            .tip_payment_config_data
            .as_ref()
            .ok_or(TipManagerError::AccountMissing)?;
        let owner = account_state
            .tip_payment_config_owner
            .as_ref()
            .ok_or(TipManagerError::AccountMissing)?;

        JitoTipPaymentConfig::from_account_data(
            data,
            owner,
            &self.tip_payment_program_info.program_id,
        )
        .map_err(TipManagerError::TipPaymentError)
    }

    /// Only called once during contract creation.
    pub fn initialize_tip_payment_program_tx(
        &self,
        blockhash: Hash,
        keypair: &Keypair,
    ) -> Result<Vec<u8>> {
        let init_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: InitializeTipPaymentInstruction::to_instruction_data(
                self.tip_payment_program_info.config_pda_bump.1,
                self.tip_payment_program_info.tip_pda_0.1,
                self.tip_payment_program_info.tip_pda_1.1,
                self.tip_payment_program_info.tip_pda_2.1,
                self.tip_payment_program_info.tip_pda_3.1,
                self.tip_payment_program_info.tip_pda_4.1,
                self.tip_payment_program_info.tip_pda_5.1,
                self.tip_payment_program_info.tip_pda_6.1,
                self.tip_payment_program_info.tip_pda_7.1,
            )?,
            accounts: vec![
                AccountMeta::new(self.tip_payment_program_info.config_pda_bump.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_0.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_1.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_2.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_3.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_4.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_5.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_6.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_7.0, false),
                AccountMeta::new_readonly(system_program::id(), false),
                AccountMeta::new(keypair.pubkey(), true),
            ],
        };
        let tx = Transaction::new_signed_with_payer(
            &[init_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            blockhash,
        );
        Ok(bincode::serialize(&tx).unwrap())
    }

    /// Returns this validator's [TipDistributionAccount] PDA derived from the provided epoch.
    pub fn get_my_tip_distribution_pda(&self, epoch: Epoch) -> Pubkey {
        TipDistributionAccount::find_program_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            epoch,
        )
        .0
    }

    /// Returns whether or not the tip-payment program should be initialized.
    pub fn should_initialize_tip_payment_program(&self, account_state: &TipAccountData) -> bool {
        match (
            &account_state.tip_payment_config_data,
            &account_state.tip_payment_config_owner,
        ) {
            (None, _) => true,
            (Some(_), Some(owner)) => owner != &self.tip_payment_program_info.program_id,
            _ => true,
        }
    }

    /// Returns whether or not the tip-distribution program's [Config] PDA should be initialized.
    pub fn should_initialize_tip_distribution_config(
        &self,
        account_state: &TipAccountData,
    ) -> bool {
        match (
            &account_state.tip_distribution_config_data,
            &account_state.tip_distribution_config_owner,
        ) {
            (None, _) => true,
            (Some(_), Some(owner)) => owner != &self.tip_distribution_program_info.program_id,
            _ => true,
        }
    }

    /// Returns whether or not the current [TipDistributionAccount] PDA should be initialized for this epoch.
    pub fn should_init_tip_distribution_account(&self, account_state: &TipAccountData) -> bool {
        match (
            &account_state.tip_distribution_pda_data,
            &account_state.tip_distribution_pda_owner,
        ) {
            (None, _) => true,
            // Since anyone can derive the PDA and send it lamports we must also check the owner is the program.
            (Some(_), Some(owner)) => owner != &self.tip_distribution_program_info.program_id,
            _ => true,
        }
    }

    /// Creates an [Initialize] transaction object.
    pub fn initialize_tip_distribution_config_tx(
        &self,
        blockhash: Hash,
        kp: &Keypair,
    ) -> Result<Vec<u8>> {
        let ix = Instruction {
            program_id: self.tip_distribution_program_info.program_id,
            data: InitializeTipDistributionConfigInstruction::to_instruction_data(
                kp.pubkey(),
                kp.pubkey(),
                10,
                10_000,
                self.tip_distribution_program_info.config_pda_and_bump.1,
            )?,
            accounts: vec![
                AccountMeta::new(
                    self.tip_distribution_program_info.config_pda_and_bump.0,
                    false,
                ),
                AccountMeta::new_readonly(system_program::id(), false),
                AccountMeta::new(kp.pubkey(), true),
            ],
        };

        let tx = Transaction::new_signed_with_payer(&[ix], Some(&kp.pubkey()), &[kp], blockhash);
        Ok(bincode::serialize(&tx).unwrap())
    }

    /// Creates an [InitializeTipDistributionAccount] transaction object using the provided Epoch.
    pub fn initialize_tip_distribution_account_tx(
        &self,
        epoch: Epoch,
        blockhash: Hash,
        kp: &Keypair,
    ) -> Result<Vec<u8>> {
        let (tip_distribution_account, bump) = TipDistributionAccount::find_program_address(
            &self.tip_distribution_program_info.program_id,
            &self.tip_distribution_account_config.vote_account,
            epoch,
        );

        let ix = Instruction {
            program_id: self.tip_distribution_program_info.program_id,
            data: InitializeTipDistributionAccountInstruction::to_instruction_data(
                self.tip_distribution_account_config
                    .merkle_root_upload_authority,
                self.tip_distribution_account_config.commission_bps,
                bump,
            )?,
            accounts: vec![
                AccountMeta::new_readonly(
                    self.tip_distribution_program_info.config_pda_and_bump.0,
                    false,
                ),
                AccountMeta::new(tip_distribution_account, false),
                AccountMeta::new_readonly(self.tip_distribution_account_config.vote_account, false),
                AccountMeta::new(kp.pubkey(), true),
                AccountMeta::new_readonly(system_program::id(), false),
            ],
        };

        let tx = Transaction::new_signed_with_payer(&[ix], Some(&kp.pubkey()), &[kp], blockhash);
        Ok(bincode::serialize(&tx).unwrap())
    }

    /// Builds a transaction that changes the current tip receiver to new_tip_receiver.
    /// The on-chain program will transfer tips sitting in the tip accounts to the tip receiver
    /// before changing ownership.
    pub fn change_tip_receiver_and_block_builder_tx(
        &self,
        new_tip_receiver: &Pubkey,
        blockhash: Hash,
        keypair: &Keypair,
        block_builder: &Pubkey,
        block_builder_commission: u64,
        tip_payment_config: &JitoTipPaymentConfig,
    ) -> Result<Vec<u8>> {
        self.build_change_tip_receiver_and_block_builder_tx(
            &tip_payment_config.tip_receiver(),
            new_tip_receiver,
            blockhash,
            keypair,
            &tip_payment_config.block_builder(),
            block_builder,
            block_builder_commission,
        )
    }

    pub fn build_change_tip_receiver_and_block_builder_tx(
        &self,
        old_tip_receiver: &Pubkey,
        new_tip_receiver: &Pubkey,
        blockhash: Hash,
        keypair: &Keypair,
        old_block_builder: &Pubkey,
        block_builder: &Pubkey,
        block_builder_commission: u64,
    ) -> Result<Vec<u8>> {
        let change_tip_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: ChangeTipReceiverInstruction::to_instruction_data(),
            accounts: vec![
                AccountMeta::new(self.tip_payment_program_info.config_pda_bump.0, false),
                AccountMeta::new(*old_tip_receiver, false),
                AccountMeta::new(*new_tip_receiver, false),
                AccountMeta::new(*old_block_builder, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_0.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_1.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_2.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_3.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_4.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_5.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_6.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_7.0, false),
                AccountMeta::new(keypair.pubkey(), true),
            ],
        };

        let change_block_builder_ix = Instruction {
            program_id: self.tip_payment_program_info.program_id,
            data: ChangeBlockBuilderInstruction::to_instruction_data(block_builder_commission)?,
            accounts: vec![
                AccountMeta::new(self.tip_payment_program_info.config_pda_bump.0, false),
                AccountMeta::new(*new_tip_receiver, false), // tip receiver will have just changed in previous ix
                AccountMeta::new(*old_block_builder, false),
                AccountMeta::new(*block_builder, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_0.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_1.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_2.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_3.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_4.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_5.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_6.0, false),
                AccountMeta::new(self.tip_payment_program_info.tip_pda_7.0, false),
                AccountMeta::new(keypair.pubkey(), true),
            ],
        };
        let tx = Transaction::new_signed_with_payer(
            &[change_tip_ix, change_block_builder_ix],
            Some(&keypair.pubkey()),
            &[keypair],
            blockhash,
        );
        Ok(bincode::serialize(&tx).unwrap())
    }

    /// Return a bundle that is capable of calling the initialize instructions on the two tip payment programs
    /// This is mainly helpful for local development and shouldn't run on testnet and mainnet, assuming the
    /// correct TipManager configuration is set.
    pub fn get_initialize_tip_programs_bundle(
        &self,
        account_state: &TipAccountData,
        blockhash: Hash,
        keypair: &Keypair,
    ) -> Result<SmallVec<[Vec<u8>; 2]>> {
        let mut transactions = SmallVec::with_capacity(2);
        if self.should_initialize_tip_payment_program(account_state) {
            info!("should_initialize_tip_payment_program=true");
            transactions.push(self.initialize_tip_payment_program_tx(blockhash, keypair)?);
        }

        if self.should_initialize_tip_distribution_config(account_state) {
            info!("should_initialize_tip_distribution_config=true");
            transactions.push(self.initialize_tip_distribution_config_tx(blockhash, keypair)?);
        }

        Ok(transactions)
    }

    pub fn get_tip_programs_crank_bundle(
        &self,
        account_state: &TipAccountData,
        keypair: &Keypair,
        block_builder_fee_info: &BlockBuilderFeeInfo,
        blockhash: Hash,
    ) -> Result<SmallVec<[Vec<u8>; 2]>> {
        let mut transactions = SmallVec::with_capacity(2);
        if self.should_init_tip_distribution_account(account_state) {
            info!("should_init_tip_distribution_account=true");
            transactions.push(self.initialize_tip_distribution_account_tx(
                account_state.epoch,
                blockhash,
                keypair,
            )?);
        }

        let tip_payment_config = self.get_tip_payment_config_account(account_state)?;
        let my_tip_receiver = self.get_my_tip_distribution_pda(account_state.epoch);

        if tip_payment_config.tip_receiver() != my_tip_receiver
            || tip_payment_config.block_builder() != block_builder_fee_info.block_builder
            || tip_payment_config.block_builder_commission_pct()
                != block_builder_fee_info.block_builder_commission
        {
            debug!("change_tip_receiver=true");
            transactions.push(self.change_tip_receiver_and_block_builder_tx(
                &my_tip_receiver,
                blockhash,
                keypair,
                &block_builder_fee_info.block_builder,
                block_builder_fee_info.block_builder_commission,
                &tip_payment_config,
            )?);
        }

        Ok(transactions)
    }
}
