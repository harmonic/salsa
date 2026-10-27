//! Command-line arguments

use {
    agave_scheduler_handshake::MAX_WORKERS,
    clap::{Parser, builder::RangedU64ValueParser},
    std::path::PathBuf,
    tonic::transport::Uri,
    validator_protos::block_engine::SchedulingStrategy,
};

#[derive(Debug, Parser)]
#[command(
    version = concat!(env!("CARGO_PKG_VERSION"), "+", env!("AGAVE_GIT_COMMIT_HASH")),
    about
)]
pub struct Config {
    /// Use DIR as ledger location
    #[arg(short = 'l', long, value_name = "DIR", default_value = "ledger")]
    pub ledger: PathBuf,

    /// Redirect logging to the specified file; SIGUSR1 re-opens the file
    #[arg(short = 'o', long, value_name = "FILE")]
    pub log: Option<PathBuf>,

    /// Block engine gRPC endpoint
    #[arg(long, value_name = "URL", value_parser = parse_url)]
    pub block_engine_url: Uri,

    /// Block builder scheduling strategy: fifo, fba, or mrev
    #[arg(long, value_parser = parse_strategy, default_value = "fba")]
    pub strategy: SchedulingStrategy,

    /// Remote TPU gRPC endpoint
    #[arg(long, visible_alias = "relayer-url", value_name = "URL", value_parser = parse_url)]
    pub remote_tpu_url: Uri,

    /// Validator banking threads to request
    #[arg(
        long,
        default_value_t = 8,
        value_parser = RangedU64ValueParser::<usize>::new().range(1..=MAX_WORKERS as u64)
    )]
    pub num_workers: usize,
}

fn parse_url(url: &str) -> Result<Uri, String> {
    let uri = url.parse::<Uri>().map_err(|err| err.to_string())?;
    match uri.scheme_str() {
        Some("http" | "https") if uri.host().is_some() => Ok(uri),
        _ => Err("expected an http:// or https:// URL".to_string()),
    }
}

fn parse_strategy(strategy: &str) -> Result<SchedulingStrategy, String> {
    match strategy.to_ascii_lowercase().as_str() {
        "fifo" => Ok(SchedulingStrategy::Fifo),
        "fba" => Ok(SchedulingStrategy::Fba),
        "mrev" => Ok(SchedulingStrategy::Mrev),
        _ => Err("expected fifo, fba, or mrev".to_string()),
    }
}
