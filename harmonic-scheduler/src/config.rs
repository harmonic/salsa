//! CLI configuration for the harmonic-scheduler binary

use crate::tip_manager;
use clap::builder::RangedU64ValueParser;
use clap::{Parser, ValueEnum};
use solana_pubkey::Pubkey;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "harmonic-scheduler",
    version,
    about = "External scheduler for the Agave validator"
)]
pub struct Config {
    /// Path to validator identity keypair file
    #[arg(short = 'i', long, value_name = "KEYPAIR")]
    pub identity: String,

    /// Redirect logging to the specified file; SIGUSR1 re-opens the file
    #[arg(short = 'o', long, value_name = "FILE")]
    pub log: Option<PathBuf>,

    #[command(flatten)]
    pub block_engine: BlockEngineConfig,

    #[command(flatten)]
    pub tpu: TpuConfig,

    #[command(flatten)]
    pub validator: ValidatorConfig,
}

/// Block engine connection
#[derive(Debug, Parser)]
pub struct BlockEngineConfig {
    /// Block engine gRPC endpoint
    #[arg(long)]
    pub block_engine_url: String,

    /// Scheduling strategy the block builder should use
    #[arg(long, value_enum, default_value_t = Strategy::Fba)]
    pub strategy: Strategy,
}

/// Scheduling strategy selected by the validator
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Strategy {
    /// First-in, first-out
    Fifo,
    /// Frequent batch auction
    Fba,
    /// Maximal realized extractable value
    Mrev,
}

/// Remote TPU (relayer) connection
#[derive(Debug, Parser)]
pub struct TpuConfig {
    /// Remote TPU gRPC endpoint
    #[arg(long, visible_alias = "relayer-url", value_name = "URL")]
    pub remote_tpu_url: String,
}

/// Validator IPC connection and scheduling parameters
#[derive(Debug, Parser)]
pub struct ValidatorConfig {
    /// Path to the validator's scheduler IPC socket
    #[arg(long)]
    pub validator_socket: PathBuf,

    /// Number of Agave worker threads to request (minimum 1)
    #[arg(long, default_value_t = 8, value_parser = RangedU64ValueParser::<usize>::new().range(1..))]
    pub num_workers: usize,

    #[command(flatten)]
    pub tip: TipConfig,
}

/// Tip program configuration
#[derive(Debug, Parser)]
pub struct TipConfig {
    /// The public key of the tip-payment program
    #[arg(long = "tip-payment-program-pubkey", value_parser = parse_pubkey)]
    pub tip_payment_program: Pubkey,

    /// The public key of the tip-distribution program
    #[arg(long = "tip-distribution-program-pubkey", value_parser = parse_pubkey)]
    pub tip_distribution_program: Pubkey,

    /// Validator vote account pubkey
    #[arg(long, value_parser = parse_pubkey)]
    pub vote_account: Pubkey,

    /// The public key of the authorized merkle-root uploader
    #[arg(long = "merkle-root-upload-authority", value_parser = parse_pubkey)]
    pub merkle_root_upload_authority: Pubkey,

    /// The commission validator takes from tips expressed in basis points (0-10000)
    #[arg(long = "commission-bps", default_value_t = 0, value_parser = RangedU64ValueParser::<u16>::new().range(..=10000))]
    pub commission_bps: u16,
}

impl From<TipConfig> for tip_manager::TipManagerConfig {
    fn from(c: TipConfig) -> Self {
        Self {
            tip_payment_program_id: c.tip_payment_program,
            tip_distribution_program_id: c.tip_distribution_program,
            tip_distribution_account_config: tip_manager::TipDistributionAccountConfig {
                merkle_root_upload_authority: c.merkle_root_upload_authority,
                vote_account: c.vote_account,
                commission_bps: c.commission_bps,
            },
        }
    }
}

fn parse_pubkey(s: &str) -> Result<Pubkey, String> {
    s.parse::<Pubkey>()
        .map_err(|e| format!("invalid pubkey '{s}': {e}"))
}
