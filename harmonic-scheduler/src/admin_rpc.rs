//! Admin RPC client

use crate::config::TipConfig;
use crate::state::{admin_rpc_active, tpu_override_exit};
use anyhow::{Result, anyhow};
use arc_swap::ArcSwap;
use jsonrpc_core::Params;
use jsonrpc_core_client::transports::ipc;
use jsonrpc_core_client::{RawClient, RpcError};
use log::{error, info, warn};
use serde_json::json;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{oneshot, watch};
use tokio::time::{MissedTickBehavior, interval, sleep};

/// Backoff between admin RPC connection attempts
const CONNECT_BACKOFF: Duration = Duration::from_secs(5);

/// How often to poll the admin RPC for identity / vote-account changes
const IDENTITY_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How often to recompute the gossip TPU target from lifecycle / override state
const TPU_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Validator's TPU + tpu_forwards QUIC advertisement
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct TpuConfig {
    pub tpu_quic: Option<SocketAddr>,
    pub tpu_forwards_quic: Option<SocketAddr>,
}

/// Admin RPC client loop
pub async fn run(
    admin_rpc: PathBuf,
    tip_config: TipConfig,
    identity_tx: watch::Sender<Arc<Keypair>>,
    tip_manager_tx: watch::Sender<Arc<TipManager>>,
    remote_tpu: Arc<ArcSwap<Option<TpuConfig>>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut last_vote_account: Option<Pubkey> = None;
    while matches!(shutdown.try_recv(), Err(TryRecvError::Empty)) {
        let (client, identity, vote_account, original_tpu) = match connect(&admin_rpc).await {
            Ok(result) => result,
            Err(e) => {
                warn!("admin rpc connect failed: {e:#}");
                sleep(CONNECT_BACKOFF).await;
                continue;
            }
        };
        if identity != identity_tx.borrow() {
            info!("identity={}", identity.pubkey());
            identity_tx
                .send(Arc::new(identity))
                .expect("identity_rx should be open");
        }
        if last_vote_account != Some(vote_account) {
            info!("vote_account={vote_account}");
            tip_manager_tx
                .send(tip_manager(&tip_config, vote_account))
                .expect("tip_manager_rx should be open");
            last_vote_account = Some(vote_account);
        }

        let _guard = admin_rpc_active();
        tokio::select! {
            _ = watch_identity(
                &client,
                &tip_config,
                &mut last_vote_account,
                &identity_tx,
                &tip_manager_tx,
            ) => {}
            Err(e) = watch_tpu(&client, &original_tpu, &remote_tpu) => {
                warn!("set tpu failed: {e:#}");
            }
            _ = &mut shutdown => {
                if let Err(e) = set_tpu(&client, &original_tpu).await {
                    warn!("restore original tpu failed: {e:#}");
                }
                return;
            }
        }
        // At this point admin RPC is down, no need to restore OG TPU
    }
}

/// Connect to the admin RPC, fetch identity, vote account, and tpu config
async fn connect(admin_rpc: &Path) -> Result<(RawClient, Keypair, Pubkey, TpuConfig)> {
    info!("connecting to admin rpc");
    let client = ipc::connect(admin_rpc)
        .await
        .map_err(|e| anyhow!("ipc connect: {e}"))?;
    let (identity, vote_account) = fetch_identity(&client)
        .await
        .map_err(|e| anyhow!("fetch identity: {e}"))?;
    let tpu_config = fetch_tpu(&client).await?;
    Ok((client, identity, vote_account, tpu_config))
}

/// Build a `TipManager` for the current vote account from static tip config
fn tip_manager(tip_config: &TipConfig, vote_account: Pubkey) -> Arc<TipManager> {
    Arc::new(TipManager::new(TipManagerConfig {
        tip_payment_program_id: tip_config.tip_payment_program,
        tip_distribution_program_id: tip_config.tip_distribution_program,
        tip_distribution_account_config: TipDistributionAccountConfig {
            merkle_root_upload_authority: tip_config.merkle_root_upload_authority,
            vote_account,
            commission_bps: tip_config.commission_bps,
        },
    }))
}

/// Fetch identity and vote account from the admin RPC
async fn fetch_identity(client: &RawClient) -> Result<(Keypair, Pubkey), RpcError> {
    let identity: Vec<u8> =
        serde_json::from_value(client.call_method("getIdentity", Params::None).await?)
            .expect("getIdentity response should be valid");
    let identity = Keypair::try_from(identity.as_slice()).expect("keypair bytes should be valid");

    let vote_account: Pubkey =
        serde_json::from_value(client.call_method("getVoteAccount", Params::None).await?)
            .expect("getVoteAccount response should be valid");
    Ok((identity, vote_account))
}

/// Fetch the validator's currently-advertised QUIC TPU and TPU-forwards
async fn fetch_tpu(client: &RawClient) -> Result<TpuConfig> {
    let tpu_quic: Option<SocketAddr> = serde_json::from_value(
        client
            .call_method("getPublicTpuAddress", Params::None)
            .await
            .map_err(|e| anyhow!("getPublicTpuAddress: {e}"))?,
    )?;
    let tpu_forwards_quic: Option<SocketAddr> = serde_json::from_value(
        client
            .call_method("getPublicTpuForwardsAddress", Params::None)
            .await
            .map_err(|e| anyhow!("getPublicTpuForwardsAddress: {e}"))?,
    )?;
    Ok(TpuConfig {
        tpu_quic,
        tpu_forwards_quic,
    })
}

/// Publish gossip TPU + tpu_forwards QUIC entries
async fn set_tpu(client: &RawClient, tpu_config: &TpuConfig) -> Result<()> {
    if let Some(socket) = tpu_config.tpu_quic {
        info!("setting tpu_quic: {socket}");
        client
            .call_method("setPublicTpuAddress", Params::Array(vec![json!(socket)]))
            .await
            .map_err(|e| anyhow!("setPublicTpuAddress: {e}"))?;
    }
    if let Some(socket) = tpu_config.tpu_forwards_quic {
        info!("setting tpu_forwards_quic: {socket}");
        client
            .call_method(
                "setPublicTpuForwardsAddress",
                Params::Array(vec![json!(socket)]),
            )
            .await
            .map_err(|e| anyhow!("setPublicTpuForwardsAddress: {e}"))?;
    }
    Ok(())
}

/// Poll the admin RPC for identity / vote-account changes, returning on RPC error
async fn watch_identity(
    client: &RawClient,
    tip_config: &TipConfig,
    last_vote_account: &mut Option<Pubkey>,
    identity_tx: &watch::Sender<Arc<Keypair>>,
    tip_manager_tx: &watch::Sender<Arc<TipManager>>,
) {
    let mut tick = interval(IDENTITY_POLL_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        tick.tick().await;
        let (identity, vote_account) = match fetch_identity(client).await {
            Ok(result) => result,
            Err(e) => {
                error!("failed to fetch identity: {e:#}");
                return;
            }
        };

        if identity != identity_tx.borrow() {
            info!("identity={}", identity.pubkey());
            identity_tx
                .send(Arc::new(identity))
                .expect("identity_rx should be open");
        }

        if *last_vote_account != Some(vote_account) {
            info!("vote_account={vote_account}",);
            tip_manager_tx
                .send(tip_manager(tip_config, vote_account))
                .expect("tip_manager_rx should be open");
            *last_vote_account = Some(vote_account);
        }
    }
}

/// Poll lifecycle + TPU override state and apply changes to the gossip TPU
async fn watch_tpu(
    client: &RawClient,
    original_tpu: &TpuConfig,
    remote_tpu: &ArcSwap<Option<TpuConfig>>,
) -> Result<()> {
    let mut tick = interval(TPU_POLL_INTERVAL);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        // Wait for remote_tpu, block_engine, and validator IPC to all come up
        while tpu_override_exit() {
            tick.tick().await;
        }
        let target = (**remote_tpu.load()).expect("remote_tpu should be set when active bit is on");
        set_tpu(client, &target).await?;
        // Wait for any of them to break
        while !tpu_override_exit() {
            tick.tick().await;
        }
        set_tpu(client, original_tpu).await?;
    }
}
