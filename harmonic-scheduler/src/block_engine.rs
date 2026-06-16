//! Block engine gRPC client

use crate::auth::{
    self, AuthInterceptor, AuthSession, GRPC_CONNECTION_BACKOFF, MAX_GRPC_MESSAGE_SIZE,
};
use crate::config::BlockEngineConfig;
use crate::state::block_engine_active;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use bytes::Bytes;
use log::{error, info, trace, warn};
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tip_manager::BlockBuilderFeeInfo;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, sleep};
use tonic::Streaming;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use validator_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use validator_protos::block_engine::{
    BlockBuilderFeeInfoRequest, SetStrategyRequest, SubmitLeaderWindowInfoRequest,
    SubscribeBundlesRequest, SubscribeBundlesResponse, SubscribePacketsRequest,
    SubscribePacketsResponse,
};

/// How often to refresh the block-builder fee info from the block engine
pub const FEE_INFO_REFRESH_INTERVAL: Duration = Duration::from_mins(10);

/// Authenticated block-engine gRPC client with the scheduler's bearer-token interceptor
type Client = BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>;

/// Scheduler -> block engine leader-window announcement
#[derive(Clone, Copy)]
pub struct LeaderNotification {
    /// The slot we are leader for
    pub slot: u64,
    /// Wall-clock time at which the leader slot began
    pub start_time: SystemTime,
}

impl std::fmt::Display for LeaderNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "slot={} start_time={}",
            self.slot,
            humantime::format_rfc3339(self.start_time)
        )
    }
}

/// Block engine client loop
pub async fn run(
    config: BlockEngineConfig,
    mut identity_rx: watch::Receiver<Arc<Keypair>>,
    mut block_tx: rtrb::Producer<(u64, Vec<Bytes>)>,
    leader_rx: watch::Receiver<Option<LeaderNotification>>,
    block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
) {
    if identity_rx.changed().await.is_err() {
        return;
    }
    loop {
        let identity = identity_rx.borrow_and_update().clone();

        // Connect to block engine and subscribe to streams
        let (session, mut bundle_stream, mut packet_stream, mut block_stream) =
            match connect(&config, identity).await {
                Ok(result) => result,
                Err(e) => {
                    warn!("connect failed: {e:#}");
                    sleep(GRPC_CONNECTION_BACKOFF).await;
                    continue;
                }
            };

        // Reconnect if any of these branches complete
        let _guard = block_engine_active();
        tokio::select! {
            biased;
            Err(e) = submit_leader_notifications(session.client.clone(), leader_rx.clone()) => {
                error!("submit leader notification failed: {e:#}")
            }
            res = forward_blocks(&mut block_stream, &mut block_tx) => match res {
                Ok(()) => {} // clean shutdown
                Err(e) => error!("block stream error: {e:#}"),
            },
            res = identity_rx.changed() => match res {
                Ok(()) => {}
                Err(_) => return,
            },
            Err(e) = refresh_fee_info(session.client.clone(), block_builder_fee_info.clone()) => {
                warn!("fee refresh failed: {e:#}")
            }
            res = drain_stream(&mut bundle_stream) => match res {
                Ok(()) => {} // clean shutdown
                Err(e) => error!("bundle stream error: {e:#}"),
            },
            res = drain_stream(&mut packet_stream) => match res {
                Ok(()) => {} // clean shutdown
                Err(e) => error!("packet stream error: {e:#}"),
            },
        }
    }
}

/// Read and discard a server-streaming response until close or error
async fn drain_stream<T>(stream: &mut Streaming<T>) -> Result<(), tonic::Status> {
    while let Some(_) = stream.message().await? {}
    Ok(())
}

/// Connect to the block engine and subscribe to data streams
async fn connect(
    config: &BlockEngineConfig,
    identity: Arc<Keypair>,
) -> Result<(
    AuthSession<Client>,
    Streaming<SubscribeBundlesResponse>,
    Streaming<SubscribePacketsResponse>,
    Streaming<SubscribeBundlesResponse>,
)> {
    info!("connecting to {}", config.block_engine_url);
    let mut session = auth::connect(&config.block_engine_url, identity, |svc| {
        BlockEngineValidatorClient::new(svc).max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    })
    .await?;
    info!("setting strategy: {:?}", config.strategy);
    // TODO: error-check this once all block engines implement SetStrategy
    let _ = session
        .client
        .set_strategy(SetStrategyRequest {
            strategy: config.strategy as i32,
        })
        .await;
    info!("subscribing to bundles stream");
    let bundles_stream = session
        .client
        .subscribe_bundles2(SubscribeBundlesRequest {})
        .await?
        .into_inner();
    info!("subscribing to packets stream");
    let packets_stream = session
        .client
        .subscribe_packets(SubscribePacketsRequest {})
        .await?
        .into_inner();
    info!("subscribing to block stream");
    let block_stream = session
        .client
        .subscribe_blocks(SubscribeBundlesRequest {})
        .await?
        .into_inner();
    Ok((session, bundles_stream, packets_stream, block_stream))
}

/// Forward block subscription messages into `block_tx`
async fn forward_blocks(
    stream: &mut Streaming<SubscribeBundlesResponse>,
    block_tx: &mut rtrb::Producer<(u64, Vec<Bytes>)>,
) -> Result<(), tonic::Status> {
    let mut dropped: usize = 0;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            msg = stream.message() => match msg? {
                Some(response) => {
                    for (uuid, bundle) in response
                        .bundles
                        .into_iter()
                        .filter_map(|b| b.bundle.map(|bundle| (b.uuid, bundle)))
                    {
                        let slot = uuid.parse::<u64>().expect("block uuid should be a valid slot");
                        let txs: Vec<Bytes> = bundle.packets.into_iter().map(|p| p.data).collect();
                        let n = txs.len();
                        trace!("received {n} transactions: slot={slot}");
                        if block_tx.push((slot, txs)).is_err() {
                            dropped = dropped.saturating_add(n);
                        }
                    }
                }
                None => return Ok(()),
            },
            _ = tick.tick() => {
                if dropped != 0 {
                    warn!("dropping blocks: dropped={dropped}");
                    dropped = 0;
                }
            }
        }
    }
}

/// Submit leader window notifications
async fn submit_leader_notifications(
    mut client: Client,
    mut leader_rx: watch::Receiver<Option<LeaderNotification>>,
) -> Result<(), tonic::Status> {
    while leader_rx.changed().await.is_ok() {
        let notification = *leader_rx.borrow_and_update();
        if let Some(notification) = notification {
            info!("submitting leader notification: {notification}");
            let timer = rdtsc::Instant::now();
            client
                .submit_leader_window_info(SubmitLeaderWindowInfoRequest {
                    start_timestamp: Some(prost_types::Timestamp::from(notification.start_time)),
                    slot: notification.slot,
                })
                .await?;
            info!("submitted leader notification: rtt={}us", timer.elapsed_us());
        }
    }
    Ok(())
}

/// Periodically refresh the block-builder fee info
async fn refresh_fee_info(
    mut client: Client,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
) -> Result<()> {
    let mut tick = tokio::time::interval(FEE_INFO_REFRESH_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        info!("refreshing block builder fee info");
        let info = client
            .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
            .await
            .context("get_block_builder_fee_info")?
            .into_inner();
        let block_builder = Pubkey::from_str(&info.pubkey)
            .with_context(|| format!("invalid block builder pubkey '{}'", info.pubkey))?;
        let block_builder_commission = info.commission;
        info!(
            "refreshed block builder fee info: pubkey={block_builder}, \
             commission={block_builder_commission}",
        );
        fee_info.store(Arc::new(BlockBuilderFeeInfo {
            block_builder,
            block_builder_commission,
        }));
    }
}
