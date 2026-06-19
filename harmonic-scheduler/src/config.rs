//! CLI configuration for the harmonic-scheduler binary

use agave_scheduling_utils::handshake::MAX_WORKERS;
use clap::Parser;
use clap::builder::RangedU64ValueParser;
use solana_pubkey::Pubkey;
use std::net::SocketAddr;
use std::path::PathBuf;
use validator_protos::block_engine::SchedulingStrategy;

#[derive(Debug, Parser)]
#[command(
    name = "harmonic-scheduler",
    version,
    about = "External scheduler for the Agave validator"
)]
pub struct Config {
    /// Use DIR as ledger location
    #[arg(short = 'l', long, value_name = "DIR", default_value = "ledger")]
    pub ledger: PathBuf,

    /// Redirect logging to the specified file; SIGUSR1 re-opens the file
    #[arg(short = 'o', long, value_name = "FILE")]
    pub log: Option<PathBuf>,

    #[command(flatten)]
    pub block_engine: BlockEngineConfig,

    #[command(flatten)]
    pub tpu: RemoteTpuConfig,

    #[command(flatten)]
    pub validator: ValidatorConfig,

    #[command(flatten)]
    pub tip: TipConfig,

    #[command(flatten)]
    pub backrun: BackrunConfig,
}

/// Co-located backrun stream. Disabled unless `--backrun-listen-addr` is set.
///
/// When enabled, the scheduler serves a `BackrunService` gRPC endpoint. While
/// this validator is the leader it streams its own incoming (non-vote)
/// transactions to the connected strategy server, and includes any backrun
/// bundles the strategy sends back in the block it is building.
#[derive(Debug, Parser)]
pub struct BackrunConfig {
    /// Address to serve the backrun gRPC stream on (e.g. 0.0.0.0:50051).
    /// Leave unset to disable the feature entirely.
    #[arg(long, value_name = "ADDR")]
    pub backrun_listen_addr: Option<SocketAddr>,

    /// If set, require this value in the `x-token` metadata of backrun clients.
    #[arg(long, value_name = "TOKEN")]
    pub backrun_x_token: Option<String>,
}

/// Block engine connection
#[derive(Debug, Parser)]
pub struct BlockEngineConfig {
    /// Block engine gRPC endpoint
    #[arg(long)]
    pub block_engine_url: String,

    /// Scheduling strategy the block builder should use
    #[arg(long, value_parser = parse_strategy, default_value = "fba")]
    pub strategy: SchedulingStrategy,
}

fn parse_strategy(s: &str) -> Result<SchedulingStrategy, String> {
    match s.to_ascii_lowercase().as_str() {
        "fifo" => Ok(SchedulingStrategy::Fifo),
        "fba" => Ok(SchedulingStrategy::Fba),
        "mrev" => Ok(SchedulingStrategy::Mrev),
        _ => Err(format!("expected one of: fifo, fba, mrev, got '{s}'")),
    }
}

/// Remote TPU (relayer) connection
#[derive(Debug, Parser)]
pub struct RemoteTpuConfig {
    /// Remote TPU gRPC endpoint
    #[arg(long, visible_alias = "relayer-url", value_name = "URL")]
    pub remote_tpu_url: String,
}

/// Validator IPC connection and scheduling parameters
#[derive(Debug, Parser)]
pub struct ValidatorConfig {
    /// Number of Agave worker threads to request (1..=MAX_WORKERS)
    #[arg(long, default_value_t = 8, value_parser = RangedU64ValueParser::<usize>::new().range(1..=MAX_WORKERS as u64))]
    pub num_workers: usize,
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

    /// The public key of the authorized merkle-root uploader
    #[arg(long = "merkle-root-upload-authority", value_parser = parse_pubkey)]
    pub merkle_root_upload_authority: Pubkey,

    /// The commission validator takes from tips expressed in basis points (0-10000)
    #[arg(long = "commission-bps", default_value_t = 0, value_parser = RangedU64ValueParser::<u16>::new().range(..=10000))]
    pub commission_bps: u16,
}

fn parse_pubkey(s: &str) -> Result<Pubkey, String> {
    s.parse::<Pubkey>()
        .map_err(|e| format!("invalid pubkey '{s}': {e}"))
}
