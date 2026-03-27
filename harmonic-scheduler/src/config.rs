use clap::Parser;

#[derive(Parser)]
#[command(
    name = "harmonic-scheduler",
    version,
    about = "Harmonic external scheduler for Agave validator"
)]
pub struct Config {
    /// Path to the validator's scheduler IPC socket
    #[arg(long)]
    pub validator_socket: String,

    /// Auction house (block engine) gRPC endpoint
    #[arg(long)]
    pub auction_url: String,

    /// TPU proxy (relayer) gRPC endpoint
    #[arg(long)]
    pub tpu_proxy_url: String,

    /// Path to validator identity keypair file
    #[arg(long)]
    pub identity: String,

    /// Number of Agave worker threads to request
    #[arg(long, default_value_t = 8)]
    pub num_workers: usize,

    /// Tip payment program ID (required for crank transaction generation)
    #[arg(long)]
    pub tip_payment_program: Option<String>,

    /// Tip distribution program ID (required for crank transaction generation)
    #[arg(long)]
    pub tip_distribution_program: Option<String>,
}
