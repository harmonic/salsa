//! Block engine gRPC client

use crate::auth::{
    self, AuthInterceptor, AuthSession, GRPC_CONNECTION_BACKOFF, MAX_GRPC_MESSAGE_SIZE,
};
use crate::config::BlockEngineConfig;
use crate::ipc::shmem::is_valid_tx_len;
use crate::state::block_engine_active;
use anyhow::{Context, Result};
use arc_swap::ArcSwap;
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
use validator_protos::block::{Block, Transaction};
use validator_protos::block_engine::block_engine_validator_client::BlockEngineValidatorClient;
use validator_protos::block_engine::{
    BlockBuilderFeeInfoRequest, SetStrategyRequest, SubmitLeaderWindowInfoRequest,
    SubscribeBlocksRequest, SubscribeBundlesRequest, SubscribeBundlesResponse,
    SubscribePacketsRequest, SubscribePacketsResponse,
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
    /// Wall-clock time the slot was announced, used by the block engine to measure latency
    pub start_time: SystemTime,
    /// Wall-clock time at which the leader slot ends
    pub end_time: SystemTime,
}

impl std::fmt::Display for LeaderNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "slot={} start_time={} end_time={}",
            self.slot,
            humantime::format_rfc3339(self.start_time),
            humantime::format_rfc3339(self.end_time)
        )
    }
}

/// Block engine client loop
pub async fn run(
    config: BlockEngineConfig,
    mut identity_rx: watch::Receiver<Arc<Keypair>>,
    mut block_tx: rtrb::Producer<Block>,
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
    while stream.message().await?.is_some() {}
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
    Streaming<Block>,
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
        .subscribe_blocks2(SubscribeBlocksRequest {
            version: crate::version::VERSION.to_string(),
            commit_hash: crate::version::COMMIT.to_string(),
        })
        .await?
        .into_inner();
    Ok((session, bundles_stream, packets_stream, block_stream))
}

/// Forward block subscription messages into `block_tx`
async fn forward_blocks(
    stream: &mut Streaming<Block>,
    block_tx: &mut rtrb::Producer<Block>,
) -> Result<(), tonic::Status> {
    let mut dropped: usize = 0;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            msg = stream.message() => match msg? {
                Some(block) => {
                    let n = block.transactions.len();
                    trace!("received {n} transactions: slot={}", block.slot);
                    if block_tx.push(block).is_err() {
                        dropped = dropped.saturating_add(n);
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

/// Split a block at bundle boundaries into `(revert_protected, transactions)`; `bundle_id == 0`
/// marks a standalone transaction. Bundles are atomic, so one unallocatable member drops the bundle.
pub fn split_bundles(block: &Block) -> impl Iterator<Item = (bool, &[Transaction])> {
    block
        .transactions
        .chunk_by(|a, b| a.bundle_id != 0 && a.bundle_id == b.bundle_id)
        .filter(
            |group| match group.iter().find(|tx| !is_valid_tx_len(&tx.transaction)) {
                Some(tx) => {
                    warn!(
                        "dropping bundle with invalid transaction length: slot={} len={}",
                        block.slot,
                        tx.transaction.len()
                    );
                    false
                }
                None => true,
            },
        )
        .map(|group| (group[0].bundle_id != 0, group))
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
                    end_timestamp: Some(prost_types::Timestamp::from(notification.end_time)),
                })
                .await?;
            info!(
                "submitted leader notification: rtt={}us",
                timer.elapsed_us()
            );
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn tx(marker: u8, bundle_id: u64) -> Transaction {
        Transaction {
            transaction: Bytes::from(vec![marker]),
            bundle_id,
        }
    }

    #[test]
    fn split_bundles_marks_length_one_bundles_revert_protected() {
        let block = Block {
            slot: 7,
            transactions: vec![
                tx(1, 0),
                tx(2, 5),
                tx(3, 0),
                tx(4, 0),
                tx(5, 9),
                tx(6, 9),
                tx(7, 5),
            ],
        };
        let shape: Vec<(bool, Vec<u8>)> = split_bundles(&block)
            .map(|(protected, txs)| (protected, txs.iter().map(|tx| tx.transaction[0]).collect()))
            .collect();
        assert_eq!(
            shape,
            vec![
                (false, vec![1]),
                (true, vec![2]),
                (false, vec![3]),
                (false, vec![4]),
                (true, vec![5, 6]),
                (true, vec![7]),
            ]
        );
    }

    #[test]
    fn split_bundles_drops_whole_bundle_with_invalid_member() {
        let empty = Transaction {
            transaction: Bytes::new(),
            bundle_id: 3,
        };
        let block = Block {
            slot: 7,
            transactions: vec![tx(1, 3), empty, tx(2, 0)],
        };
        let shape: Vec<u8> = split_bundles(&block)
            .flat_map(|(_, txs)| txs.iter().map(|tx| tx.transaction[0]))
            .collect();
        assert_eq!(shape, vec![2]);
    }
}
