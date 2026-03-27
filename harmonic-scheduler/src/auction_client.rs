use {
    crate::auth::{self, AuthInterceptor, AuthSession},
    arc_swap::ArcSwap,
    harmonic_protos::auction::{
        self, BlockBuilderFeeInfoRequest, SubmitLeaderWindowInfoRequest,
        block_engine_validator_client::BlockEngineValidatorClient,
    },
    log::{info, warn},
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    std::{
        str::FromStr,
        sync::Arc,
        time::{Duration, SystemTime},
    },
    tokio::{sync::mpsc, time::sleep},
    tonic::{Status, transport::Channel},
};

/// Backoff duration between reconnection attempts.
const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// How often to re-fetch block builder fee info.
const FEE_INFO_REFRESH_INTERVAL: Duration = Duration::from_secs(600); // 10 minutes

/// Authenticated block engine client type alias.
type Client =
    BlockEngineValidatorClient<tonic::codegen::InterceptedService<Channel, AuthInterceptor>>;

/// A block received from the auction house, ready for scheduling.
pub struct AuctionBlock {
    /// Raw serialized transactions in execution order.
    pub transactions: Vec<Vec<u8>>,
    /// Target slot for this block.
    pub slot: u64,
}

/// Block builder fee configuration from the auction house.
pub struct BlockBuilderFeeInfo {
    pub block_builder: Pubkey,
    pub block_builder_commission: u64,
}

/// Maintains a connection to the auction house (block engine).
///
/// Subscribes to the block stream, forwards leader slot notifications,
/// and periodically refreshes block builder fee info.
/// Auth token refresh is handled automatically by the AuthSession.
/// Automatically reconnects with backoff on failure.
pub struct AuctionClient {
    /// Auction house gRPC endpoint URL.
    url: String,
    /// Validator identity keypair for auth.
    identity: Arc<Keypair>,
    /// Channel to send received blocks to the scheduler (crossbeam for OS thread).
    block_tx: crossbeam_channel::Sender<AuctionBlock>,
    /// Channel to receive leader slot notifications from the scheduler.
    leader_rx: mpsc::UnboundedReceiver<(u64, SystemTime)>,
    /// Block builder fee config, shared with the crank generator.
    block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
}

impl AuctionClient {
    pub fn new(
        url: String,
        identity: Arc<Keypair>,
        block_tx: crossbeam_channel::Sender<AuctionBlock>,
        leader_rx: mpsc::UnboundedReceiver<(u64, SystemTime)>,
        block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    ) -> Self {
        Self {
            url,
            identity,
            block_tx,
            leader_rx,
            block_builder_fee_info,
        }
    }

    /// Run the auction client, reconnecting with backoff on failure.
    pub async fn run(mut self) {
        let mut fee_tick = tokio::time::interval(FEE_INFO_REFRESH_INTERVAL);

        loop {
            let (channel, auth) = self.connect().await;

            let mut client =
                BlockEngineValidatorClient::with_interceptor(channel, auth.interceptor());

            let mut block_stream = match client
                .subscribe_blocks(auction::SubscribeBundlesRequest {})
                .await
            {
                Ok(resp) => {
                    info!("subscribed to auction block stream");
                    resp.into_inner()
                }
                Err(e) => {
                    warn!("failed to subscribe to blocks: {e}");
                    continue;
                }
            };

            loop {
                tokio::select! {
                    msg = block_stream.message() => {
                        match msg {
                            Ok(Some(resp)) => self.handle_block(resp),
                            Ok(None) => break, // stream ended (server restart)
                            Err(e) => {
                                warn!("failed to receive auction block: {e}");
                                break;
                            }
                        }
                    }
                    Some((slot, timestamp)) = self.leader_rx.recv() => {
                        if let Err(e) = self.submit_leader_window(&mut client, slot, timestamp).await {
                            warn!("failed to submit leader window info for slot {slot}: {e}");
                            break;
                        }
                    }
                    _ = fee_tick.tick() => {
                        if let Err(e) = self.refresh_fee_info(&mut client).await {
                            warn!("failed to refresh block builder fee info: {e}");
                            break;
                        }
                    }
                }
            }
            // Connection lost — loop back to reconnect immediately.
        }
    }

    /// Retry connection with backoff until successful.
    async fn connect(&self) -> (Channel, AuthSession) {
        loop {
            match auth::authenticate(&self.url, self.identity.clone()).await {
                Ok(result) => {
                    info!("connected to auction house: {}", self.url);
                    return result;
                }
                Err(e) => {
                    warn!("failed to connect to auction house: {e}");
                    sleep(CONNECTION_BACKOFF).await;
                }
            }
        }
    }

    /// Extract blocks from a SubscribeBundlesResponse and send to the scheduler.
    fn handle_block(&self, resp: auction::SubscribeBundlesResponse) {
        for bundle in resp.bundles {
            let slot = match bundle.uuid.parse::<u64>() {
                Ok(slot) => slot,
                Err(e) => {
                    warn!("failed to parse block uuid as slot: {e}");
                    continue;
                }
            };

            if let Some(block) = bundle.bundle {
                let transactions: Vec<Vec<u8>> = block
                    .packets
                    .into_iter()
                    .map(|mut p| {
                        let size = p
                            .meta
                            .as_ref()
                            .map(|m| m.size as usize)
                            .unwrap_or(p.data.len());
                        p.data.truncate(size);
                        p.data
                    })
                    .collect();

                if !transactions.is_empty() {
                    let _ = self.block_tx.try_send(AuctionBlock { transactions, slot });
                }
            }
        }
    }

    /// Notify the auction house of an upcoming leader slot.
    async fn submit_leader_window(
        &self,
        client: &mut Client,
        slot: u64,
        timestamp: SystemTime,
    ) -> Result<(), Status> {
        client
            .submit_leader_window_info(SubmitLeaderWindowInfoRequest {
                start_timestamp: Some(prost_types::Timestamp::from(timestamp)),
                slot,
            })
            .await?;
        info!("submitted leader window info for slot {slot}");
        Ok(())
    }

    /// Fetch block builder fee info from the auction house and publish it.
    async fn refresh_fee_info(&self, client: &mut Client) -> Result<(), Status> {
        let info = client
            .get_block_builder_fee_info(BlockBuilderFeeInfoRequest {})
            .await?
            .into_inner();
        let block_builder = Pubkey::from_str(&info.pubkey).unwrap_or_default();
        info!(
            "refreshed block builder fee info: pubkey={block_builder}, commission={}",
            info.commission
        );
        self.block_builder_fee_info
            .store(Arc::new(BlockBuilderFeeInfo {
                block_builder,
                block_builder_commission: info.commission,
            }));
        Ok(())
    }
}
