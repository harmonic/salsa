//! Block engine subscription: leader-window notifications, block ingest, fee info

use crate::auth;
use crate::config::BlockEngineConfig;
use crate::error::ErrorExt;
use arc_swap::ArcSwap;
use harmonic_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use harmonic_protos::block_engine::{
    BlockBuilderFeeInfoRequest, SubmitLeaderWindowInfoRequest, SubscribeBundlesRequest,
    SubscribeBundlesResponse,
};
use log::{debug, info, log_enabled, trace, warn};
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, sleep};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

/// How often to refresh the block-builder fee info from the block engine
const FEE_INFO_REFRESH_INTERVAL: Duration = Duration::from_mins(10);
/// Backoff between block-engine connection attempts
const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// Authenticated block-engine gRPC client with the scheduler's bearer-token interceptor
type Client = BlockEngineValidatorClient<InterceptedService<Channel, auth::AuthInterceptor>>;

/// Scheduler -> block engine leader-window announcement
#[derive(Clone, Copy)]
pub struct LeaderNotification {
    /// The slot we are leader for
    pub slot: u64,
    /// Wall-clock time at which the leader slot began
    pub start_time: SystemTime,
}

impl Default for LeaderNotification {
    fn default() -> Self {
        Self {
            slot: 0,
            start_time: SystemTime::UNIX_EPOCH,
        }
    }
}

/// Block builder fee configuration from the block engine
pub struct BlockBuilderFeeInfo {
    /// Pubkey of the account that receives the block-builder cut of tips
    pub block_builder: Pubkey,
    /// Block-builder commission in basis points (0-10000), out of the tip total
    pub block_builder_commission: u64,
}

/// Block engine client loop. Reconnects automatically on failure
pub async fn run(
    config: BlockEngineConfig,
    identity: Arc<Keypair>,
    mut block_tx: rtrb::Producer<Vec<u8>>,
    mut leader_rx: watch::Receiver<LeaderNotification>,
    block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
) {
    loop {
        info!("connecting to block engine at {}", config.block_engine_url);
        let (channel, auth) = auth::connect(&config.block_engine_url, identity.clone()).await;
        let mut client = BlockEngineValidatorClient::with_interceptor(channel, auth.interceptor());

        debug!("subscribing to block engine stream");
        let block_stream = match client.subscribe_blocks(SubscribeBundlesRequest {}).await {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                warn!("failed to subscribe to block stream: {}", e.chain());
                sleep(CONNECTION_BACKOFF).await;
                continue;
            }
        };
        info!("subscribed to block engine stream");

        if let Err(e) = stream(
            &mut client,
            block_stream,
            &mut block_tx,
            &mut leader_rx,
            &block_builder_fee_info,
        )
        .await
        {
            if log_enabled!(log::Level::Debug) {
                warn!("block engine stream ended: {}", e.chain());
            } else {
                warn!("block engine stream ended");
            }
        } else {
            info!("block engine stream closed");
        }
        sleep(CONNECTION_BACKOFF).await;
    }
}

/// Main block-engine event loop
async fn stream(
    client: &mut Client,
    mut block_stream: tonic::Streaming<SubscribeBundlesResponse>,
    block_tx: &mut rtrb::Producer<Vec<u8>>,
    leader_rx: &mut watch::Receiver<LeaderNotification>,
    block_builder_fee_info: &ArcSwap<BlockBuilderFeeInfo>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut fee_tick = tokio::time::interval(FEE_INFO_REFRESH_INTERVAL);
    fee_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut current_leader_slot = leader_rx.borrow_and_update().slot;

    loop {
        tokio::select! {
            msg = block_stream.message() => {
                let Some(resp) = msg? else {
                    return Ok(()); // stream closed, reconnect
                };
                handle_block_message(resp, block_tx, current_leader_slot);
            }
            Ok(()) = leader_rx.changed() => {
                let notification = *leader_rx.borrow_and_update();
                current_leader_slot = notification.slot;
                submit_leader_window(client, notification).await;
            }
            _ = fee_tick.tick() => {
                refresh_fee_info(client, block_builder_fee_info).await;
            }
        }
    }
}

/// Submit leader-window info to the block engine
async fn submit_leader_window(client: &mut Client, notification: LeaderNotification) {
    let LeaderNotification { slot, start_time } = notification;
    let req = SubmitLeaderWindowInfoRequest {
        start_timestamp: Some(prost_types::Timestamp::from(start_time)),
        slot,
    };
    debug!("submitting leader window info for slot {slot}");
    match client.submit_leader_window_info(req).await {
        Ok(_) => info!("submitted leader window info for slot {slot}"),
        Err(e) => warn!(
            "failed to submit leader window info for slot {slot}: {}",
            e.chain()
        ),
    }
}

/// Parse a subscribe_blocks stream message and forward its transactions to `block_tx`.
/// All bundles in a response share a slot; responses for other slots are dropped
fn handle_block_message(
    resp: SubscribeBundlesResponse,
    block_tx: &mut rtrb::Producer<Vec<u8>>,
    current_slot: u64,
) {
    let Some(first) = resp.bundles.first() else {
        return;
    };
    // Block engine does not mix blocks for multiple slots in one message
    let slot = match first.uuid.parse::<u64>() {
        Ok(s) => s,
        Err(e) => {
            warn!("failed to parse slot for block: {e}");
            return;
        }
    };
    if slot != current_slot {
        warn!("dropping block for slot {slot} (current slot {current_slot})");
        return;
    }

    let mut count: usize = 0;
    for mut p in resp
        .bundles
        .into_iter()
        .filter_map(|b| b.bundle)
        .flat_map(|b| b.packets)
    {
        let size = p.meta.as_ref().map_or(p.data.len(), |m| m.size as usize);
        p.data.truncate(size);
        if p.data.is_empty() {
            continue;
        }
        if block_tx.push(p.data).is_err() {
            warn!("block queue full, dropping txs for slot {slot}");
            break;
        }
        count = count.saturating_add(1);
    }
    trace!("forwarded {count} block transactions for slot {slot}");
}

/// Pull the current block-builder fee info and stash it in `fee_info`
async fn refresh_fee_info(client: &mut Client, fee_info: &ArcSwap<BlockBuilderFeeInfo>) {
    let info = match client
        .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            warn!("failed to refresh block builder fee info: {}", e.chain());
            return;
        }
    };
    let block_builder = match Pubkey::from_str(&info.pubkey) {
        Ok(pk) => pk,
        Err(e) => {
            warn!("invalid block builder pubkey '{}': {e}", info.pubkey);
            return;
        }
    };
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
