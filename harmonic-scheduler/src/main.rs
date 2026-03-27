mod auction_client;
mod auth;
mod config;
mod crank;
mod fallback;
mod ingest;
mod ipc;
mod scheduler;
mod scheduling;
mod tpu_proxy_client;

use {
    arc_swap::ArcSwap,
    auction_client::{AuctionClient, BlockBuilderFeeInfo},
    clap::Parser,
    config::Config,
    crank::{CrankConfig, CrankGenerator},
    scheduler::Scheduler,
    solana_keypair::read_keypair_file,
    solana_pubkey::Pubkey,
    std::{str::FromStr, sync::Arc},
    tpu_proxy_client::TpuProxyClient,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let config = Config::parse();
    let identity = Arc::new(read_keypair_file(&config.identity)?);

    let (packet_tx, packet_rx) = crossbeam_channel::bounded(65536);
    let (block_tx, block_rx) = crossbeam_channel::bounded(16);
    let (control_tx, control_rx) = crossbeam_channel::bounded(4);
    let (leader_tx, leader_rx) = tokio::sync::mpsc::unbounded_channel();

    let fee_info = Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
        block_builder: Pubkey::default(),
        block_builder_commission: 0,
    }));

    // Create CrankGenerator if tip programs are configured.
    let crank = match (
        &config.tip_payment_program,
        &config.tip_distribution_program,
    ) {
        (Some(payment), Some(distribution)) => {
            let tip_payment_program =
                Pubkey::from_str(payment).expect("invalid tip_payment_program pubkey");
            let tip_distribution_program =
                Pubkey::from_str(distribution).expect("invalid tip_distribution_program pubkey");
            Some(CrankGenerator::new(
                CrankConfig {
                    tip_payment_program,
                    tip_distribution_program,
                },
                fee_info.clone(),
            ))
        }
        _ => None,
    };

    let auction = tokio::spawn(
        AuctionClient::new(
            config.auction_url.clone(),
            identity.clone(),
            block_tx,
            leader_rx,
            fee_info.clone(),
        )
        .run(),
    );

    let relayer = tokio::spawn(
        TpuProxyClient::new(
            config.tpu_proxy_url.clone(),
            identity.clone(),
            packet_tx,
            control_tx,
        )
        .run(),
    );

    let validator_socket = config.validator_socket.clone();
    let num_workers = config.num_workers;
    let scheduler = tokio::task::spawn_blocking(move || {
        Scheduler::new(
            &validator_socket,
            num_workers,
            packet_rx,
            block_rx,
            control_rx,
            leader_tx,
            crank,
        )
        .run()
    });

    tokio::select! {
        r = auction => log::error!("auction exited: {r:?}"),
        r = relayer => log::error!("relayer exited: {r:?}"),
        r = scheduler => log::error!("scheduler exited: {r:?}"),
    }
    std::process::exit(1);
}
