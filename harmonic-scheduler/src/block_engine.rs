//! Harmonic Block Engine client

use {
    crate::{
        admin_rpc::AdminRpc,
        grpc::{AuthInterceptor, Connection, MAX_GRPC_MESSAGE_SIZE, connect_channel},
        scheduler::{
            self, BlockEngineChannels, LeaderNotification,
            channel::{BundleMeta, PushError, REVERT_PROTECT},
        },
    },
    agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    anyhow::{Context, Result},
    arc_swap::ArcSwap,
    clap::ValueEnum,
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    std::{
        str::FromStr,
        sync::Arc,
        time::{Duration, Instant, SystemTime},
    },
    tokio::{
        sync::watch,
        task::{JoinSet, coop::consume_budget},
        time::{MissedTickBehavior, interval, sleep, timeout},
    },
    tonic::{
        Streaming,
        codegen::InterceptedService,
        transport::{Channel, Uri},
    },
    tracing::{debug, error, info, trace, warn},
    validator_protos::{
        block::Block,
        block_engine::{
            BlockBuilderFeeInfoRequest, SchedulingStrategy, SetStrategyRequest,
            SubmitLeaderWindowInfoRequest, SubscribeBlocksRequest, SubscribeBundlesRequest,
            SubscribeBundlesResponse, SubscribePacketsRequest, SubscribePacketsResponse,
            block_engine_validator_client::BlockEngineValidatorClient,
        },
    },
};

/// Backoff between block engine connection attempts
const CONNECT_BACKOFF: Duration = Duration::from_secs(1);
/// How often to refresh the block builder's fee info
const FEE_INFO_REFRESH_INTERVAL: Duration = Duration::from_mins(10);
/// How often to report stream drops
const DROP_REPORT_INTERVAL: Duration = Duration::from_secs(1);
/// How long a stream waits for room in its full channel
const FULL_CHANNEL_RETRY: Duration = Duration::from_millis(1);

/// Authenticated block-engine gRPC client
type Client = BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>;

/// Block builder scheduling strategy
#[derive(Clone, Copy, Debug, ValueEnum, strum_macros::Display)]
#[strum(serialize_all = "kebab-case")]
pub enum Strategy {
    Fifo,
    Fba,
    Mrev,
}

impl Strategy {
    /// Three-letter code publishing the strategy in the tip crank's memo
    fn memo(self) -> &'static str {
        match self {
            Self::Fifo => "FIF",
            Self::Fba => "FBA",
            Self::Mrev => "MRV",
        }
    }
}

impl From<Strategy> for SchedulingStrategy {
    fn from(strategy: Strategy) -> Self {
        match strategy {
            Strategy::Fifo => Self::Fifo,
            Strategy::Fba => Self::Fba,
            Strategy::Mrev => Self::Mrev,
        }
    }
}

/// Which account receives the block builder's cut of tips, and how much
#[derive(Clone, Copy)]
struct BlockBuilderFeeInfo {
    /// Pubkey of the account that receives the block builder's cut of tips
    block_builder: Pubkey,
    /// Block builder commission, as a percentage (0-100) of the tips
    commission: u64,
}

/// Connected, authenticated, and subscribed block engine session
pub struct BlockEngine {
    connection: Connection,
    client: Client,
    fee_info: BlockBuilderFeeInfo,
    /// Publishes the scheduling strategy in each tip crank
    memo: &'static str,
    bundle_stream: Streaming<SubscribeBundlesResponse>,
    packet_stream: Streaming<SubscribePacketsResponse>,
    block_stream: Streaming<Block>,
    /// Keeps the block stream's own connection open
    block_channel: Channel,
}

impl BlockEngine {
    /// Connect to the block engine, retrying until it succeeds
    pub async fn connect(url: &Uri, strategy: Strategy, admin: &AdminRpc) -> Self {
        info!(%url, "connecting to block engine");
        let mut failed = false;
        loop {
            match Self::try_connect(url, strategy, admin).await {
                Ok(block_engine) => {
                    info!(
                        %strategy,
                        block_builder = %block_engine.fee_info.block_builder,
                        commission = block_engine.fee_info.commission,
                        "connected to block engine"
                    );
                    return block_engine;
                }
                Err(e) if !failed => {
                    warn!(error = %format_args!("{e:#}"), "block engine connect failed, retrying");
                }
                Err(e) => debug!(error = %format_args!("{e:#}"), "block engine connect failed"),
            }
            failed = true;
            sleep(CONNECT_BACKOFF).await;
        }
    }

    /// Spawn the unit
    pub fn spawn(
        self,
        tasks: &mut JoinSet<Result<()>>,
        admin: Arc<AdminRpc>,
        channels: BlockEngineChannels,
    ) {
        let Self {
            mut connection,
            client,
            fee_info,
            memo,
            bundle_stream,
            packet_stream,
            block_stream,
            block_channel,
        } = self;
        let BlockEngineChannels {
            blocks,
            bundles,
            packets,
            tip_cranks,
            leader_rx,
        } = channels;
        let fee_info = Arc::new(ArcSwap::from_pointee(fee_info));

        tasks.spawn(submit_leader_notifications(
            client.clone(),
            leader_rx.clone(),
        ));
        tasks.spawn(sign_tip_cranks(
            Arc::clone(&admin),
            Arc::clone(&fee_info),
            memo,
            leader_rx,
            tip_cranks,
        ));
        // spawn each loop as its own task, so bulk stream decoding never delays the others
        tasks.spawn(forward_blocks(block_stream, block_channel, blocks));
        tasks.spawn(forward_bundles(bundle_stream, bundles));
        tasks.spawn(forward_packets(packet_stream, packets));
        tasks.spawn(refresh_fee_info(client, fee_info));
        tasks.spawn(async move {
            connection
                .refresh_tokens(&admin)
                .await
                .context("block engine auth")
        });
    }

    /// Connect, authenticate, set the strategy, fetch fee info, and subscribe to every stream
    async fn try_connect(url: &Uri, strategy: Strategy, admin: &AdminRpc) -> Result<Self> {
        let connection = Connection::connect(url, admin).await?;
        let mut client = new_client(connection.channel.clone(), &connection.interceptor);
        client
            .set_strategy(SetStrategyRequest {
                strategy: SchedulingStrategy::from(strategy) as i32,
            })
            .await
            .context("set_strategy")?;

        let fee_info = get_fee_info(&mut client).await?;

        let bundle_stream = client
            .subscribe_bundles2(SubscribeBundlesRequest {})
            .await
            .context("subscribe bundles")?
            .into_inner();
        let packet_stream = client
            .subscribe_packets(SubscribePacketsRequest {})
            .await
            .context("subscribe packets")?
            .into_inner();
        // Its own connection, so block data never queues behind bundle and packet traffic
        let block_channel = connect_channel(&connection.endpoint)
            .await
            .context("block connection")?;
        let block_stream = new_client(block_channel.clone(), &connection.interceptor)
            .subscribe_blocks2(SubscribeBlocksRequest {
                version: env!("CARGO_PKG_VERSION").to_string(),
                commit_hash: env!("AGAVE_GIT_COMMIT_HASH").to_string(),
            })
            .await
            .context("subscribe blocks")?
            .into_inner();

        Ok(Self {
            connection,
            client,
            fee_info,
            memo: strategy.memo(),
            bundle_stream,
            packet_stream,
            block_stream,
            block_channel,
        })
    }
}

fn new_client(channel: Channel, interceptor: &AuthInterceptor) -> Client {
    BlockEngineValidatorClient::with_interceptor(channel, interceptor.clone())
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}

/// Fetch the block builder pubkey and commission
async fn get_fee_info(client: &mut Client) -> Result<BlockBuilderFeeInfo> {
    let info = client
        .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
        .await
        .context("get_block_builder_fee_info")?
        .into_inner();
    let block_builder = Pubkey::from_str(&info.pubkey)
        .with_context(|| format!("invalid block builder pubkey '{}'", info.pubkey))?;
    Ok(BlockBuilderFeeInfo {
        block_builder,
        commission: info.commission,
    })
}

/// Periodically refresh the block builder's fee info
async fn refresh_fee_info(
    mut client: Client,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
) -> Result<()> {
    let mut refresh = interval(FEE_INFO_REFRESH_INTERVAL);
    refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Fetched at connect
    refresh.tick().await;
    loop {
        refresh.tick().await;
        match get_fee_info(&mut client).await {
            Ok(info) => {
                info!(
                    block_builder = %info.block_builder,
                    commission = info.commission,
                    "refreshed block builder fee info"
                );
                fee_info.store(Arc::new(info));
            }
            Err(e) => warn!(error = %format_args!("{e:#}"), "fee info refresh failed"),
        }
    }
}

/// Announce each leader slot to the block engine, giving up on it when the slot ends
async fn submit_leader_notifications(
    mut client: Client,
    mut leader_rx: watch::Receiver<Option<LeaderNotification>>,
) -> Result<()> {
    while leader_rx.changed().await.is_ok() {
        let Some(notification) = *leader_rx.borrow_and_update() else {
            continue;
        };
        let LeaderNotification {
            slot,
            start_time,
            end_time,
        } = notification;
        let request = SubmitLeaderWindowInfoRequest {
            start_timestamp: Some(prost_types::Timestamp::from(start_time)),
            slot,
            end_timestamp: Some(prost_types::Timestamp::from(end_time)),
        };
        // Useless to the block engine once the slot has ended
        let deadline = end_time
            .duration_since(SystemTime::now())
            .unwrap_or_default();
        let sent = Instant::now();
        match timeout(deadline, client.submit_leader_window_info(request)).await {
            Ok(Ok(_)) => info!(
                slot,
                rtt_us = sent.elapsed().as_micros(),
                start_time = %humantime::format_rfc3339(start_time),
                end_time = %humantime::format_rfc3339(end_time),
                "submitted leader notification"
            ),
            Ok(Err(status)) => warn!(
                slot,
                code = ?status.code(),
                error = status.message(),
                "leader notification failed"
            ),
            Err(_) => warn!(slot, "leader notification unanswered when the slot ended"),
        }
    }
    // Clean shutdown: the leader thread exited, so the session is ending
    Ok(())
}

/// Have the validator sign each announced leader slot's tip crank
async fn sign_tip_cranks(
    admin: Arc<AdminRpc>,
    fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    memo: &'static str,
    mut leader_rx: watch::Receiver<Option<LeaderNotification>>,
    mut tip_cranks: scheduler::channel::Channel<TransactionPtrBatch<(), BundleMeta>>,
) -> Result<()> {
    while leader_rx.changed().await.is_ok() {
        let Some(LeaderNotification { slot, .. }) = *leader_rx.borrow_and_update() else {
            continue;
        };
        let BlockBuilderFeeInfo {
            block_builder,
            commission,
        } = **fee_info.load();
        // Not timed out here: the leader bounds its own wait
        let crank = admin
            .sign_tip_crank(slot, block_builder, commission, memo)
            .await
            .unwrap_or_else(|e| {
                error!(slot, error = %format_args!("{e:#}"), "tip crank not signed");
                None
            });
        // Queued empty when there is none, so the leader stops waiting for it
        let meta = BundleMeta {
            slot,
            flags: REVERT_PROTECT,
            id: 0,
        };
        if let Err(e) = tip_cranks.push(crank.as_deref(), meta) {
            error!(slot, error = ?e, "tip crank not queued");
        }
    }
    // Clean shutdown: the leader thread exited, so the session is ending
    Ok(())
}

/// Copy each block bundle into shared memory and queue it
async fn forward_blocks(
    mut stream: Streaming<Block>,
    _channel: Channel, // keeps the stream's connection open
    mut blocks: scheduler::channel::Channel<TransactionPtrBatch<(), BundleMeta>>,
) -> Result<()> {
    // The slot of a corrupt block, whose rest is dropped
    let mut corrupt = None;
    // The slot of the latest block, ended early if the stream stops
    let mut latest = None;
    loop {
        let block = match stream.message().await {
            Ok(Some(block)) => block,
            end => {
                if let Some(slot) = latest.filter(|&slot| corrupt != Some(slot)) {
                    end_block(&mut blocks, slot).await;
                }
                // Ok(None) is a clean shutdown: the block engine closed the stream
                return end.map(|_| ()).context("block stream");
            }
        };
        latest = Some(block.slot);
        trace!(
            slot = block.slot,
            txs = block.transactions.len(),
            "received block"
        );
        if corrupt == Some(block.slot) {
            continue;
        }
        // A bundle is consecutive transactions sharing a nonzero `bundle_id`;
        // with 0, a transaction stands alone
        for bundle in block
            .transactions
            .chunk_by(|a, b| a.bundle_id != 0 && a.bundle_id == b.bundle_id)
        {
            let meta = BundleMeta {
                slot: block.slot,
                flags: if bundle.first().is_some_and(|tx| tx.bundle_id != 0) {
                    REVERT_PROTECT
                } else {
                    0
                },
                id: 0,
            };
            let pushed = loop {
                match blocks.push(bundle.iter().map(|tx| &*tx.transaction), meta) {
                    // Never drop block transactions for room, backpressure instead
                    Err(PushError::ChannelFull | PushError::OutOfMemory) => {
                        sleep(FULL_CHANNEL_RETRY).await;
                    }
                    pushed => break pushed,
                }
            };
            if pushed.is_err() {
                error!(
                    slot = block.slot,
                    txs = bundle.len(),
                    "invalid bundle in block, dropping the rest of the block"
                );
                corrupt = Some(block.slot);
                end_block(&mut blocks, block.slot).await;
                break;
            }
        }
        // Yield during a backlog, so this task can't hold a worker indefinitely
        consume_budget().await;
    }
}

/// Queue an empty batch for `slot`: the block stage falls back for the rest of its block
async fn end_block(
    blocks: &mut scheduler::channel::Channel<TransactionPtrBatch<(), BundleMeta>>,
    slot: Slot,
) {
    let meta = BundleMeta {
        slot,
        flags: 0,
        id: 0,
    };
    // Never dropped for room, like the block's bundles
    while let Err(PushError::ChannelFull | PushError::OutOfMemory) =
        blocks.push(std::iter::empty::<&[u8]>(), meta)
    {
        sleep(FULL_CHANNEL_RETRY).await;
    }
}

/// Copy each bundle into shared memory and queue it
async fn forward_bundles(
    mut stream: Streaming<SubscribeBundlesResponse>,
    mut bundles: scheduler::channel::Channel<TransactionPtrBatch<(), BundleMeta>>,
) -> Result<()> {
    let mut dropped = scheduler::channel::Metrics::default();
    let mut report = interval(DROP_REPORT_INTERVAL);
    report.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            msg = stream.message() => match msg.context("bundle stream")? {
                Some(response) => {
                    trace!(bundles = response.bundles.len(), "received bundles");
                    for bundle in response.bundles.into_iter().filter_map(|b| b.bundle) {
                        // Bundles are for no particular slot, and revert protected
                        let meta = BundleMeta {
                            slot: 0,
                            flags: REVERT_PROTECT,
                            id: 0,
                        };
                        let pushed = loop {
                            match bundles.push(bundle.packets.iter().map(|packet| &*packet.data), meta) {
                                // Backpressure the block engine rather than drop
                                Err(PushError::ChannelFull | PushError::OutOfMemory) => {
                                    sleep(FULL_CHANNEL_RETRY).await;
                                }
                                pushed => break pushed,
                            }
                        };
                        if let Err(e) = pushed {
                            dropped.add(e, bundle.packets.len());
                        }
                    }
                }
                // Clean shutdown: the block engine closed the stream
                None => return Ok(()),
            },
            _ = report.tick() => {
                // A quiet channel still gives back what the leader freed
                bundles.allocator.clean_remote_frees();
                dropped.report("bundle");
            }
        }
        // Yield during a backlog, so this task can't hold a worker indefinitely
        consume_budget().await;
    }
}

/// Copy each packet into shared memory and queue it
async fn forward_packets(
    mut stream: Streaming<SubscribePacketsResponse>,
    mut packets: scheduler::channel::Channel<TransactionPtr>,
) -> Result<()> {
    let mut dropped = scheduler::channel::Metrics::default();
    let mut report = interval(DROP_REPORT_INTERVAL);
    report.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            msg = stream.message() => match msg.context("block engine packet stream")? {
                Some(response) => {
                    trace!(
                        packets = response.batch.as_ref().map_or(0, |batch| batch.packets.len()),
                        "received packet batch"
                    );
                    let batch = response.batch.into_iter().flat_map(|batch| batch.packets);
                    for packet in batch {
                        let discard = packet
                            .meta
                            .as_ref()
                            .and_then(|meta| meta.flags.as_ref())
                            .is_some_and(|flags| flags.discard);
                        if !discard && let Err(e) = packets.push(&packet.data) {
                            dropped.add(e, 1);
                        }
                    }
                }
                // Clean shutdown: the block engine closed the stream
                None => return Ok(()),
            },
            _ = report.tick() => {
                // A quiet channel still gives back what the leader freed
                packets.allocator.clean_remote_frees();
                dropped.report("packet");
            }
        }
        // Yield during a backlog, so this task can't hold a worker indefinitely
        consume_budget().await;
    }
}
