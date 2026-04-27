//! Remote-TPU subscription: fetches the TPU override address, streams packets, forwards to the nonvote queue

use crate::auth;
use crate::config::TpuConfig;
use crate::error::ErrorExt;
use harmonic_protos::relayer::relayer_client::RelayerClient;
use harmonic_protos::relayer::{
    GetTpuConfigsRequest, SubscribePacketsRequest, SubscribePacketsResponse,
    subscribe_packets_response,
};
use log::{debug, info, log_enabled, trace, warn};
use rdtsc::Instant;
use solana_keypair::Keypair;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, sleep};
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

/// Maximum time between heartbeats before we consider the relayer disconnected
const HEARTBEAT_TIMEOUT_MS: u64 = 400;
/// Backoff between relayer connection attempts
const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// Authenticated relayer gRPC client with the scheduler's bearer-token interceptor
type Client = RelayerClient<InterceptedService<Channel, auth::AuthInterceptor>>;

/// Remote TPU client loop. Reconnects automatically on failure
pub async fn run(
    config: TpuConfig,
    identity: Arc<Keypair>,
    mut tx: rtrb::Producer<Vec<u8>>,
    remote_tpu: watch::Sender<Option<SocketAddrV4>>,
) {
    loop {
        info!("connecting to remote TPU at {}", config.remote_tpu_url);
        let (channel, auth) = auth::connect(&config.remote_tpu_url, identity.clone()).await;
        let mut client = RelayerClient::with_interceptor(channel, auth.interceptor());

        let addr = match fetch_tpu_address(&mut client).await {
            Ok(addr) => addr,
            Err(e) => {
                warn!("failed to fetch remote TPU address: {}", e.chain());
                sleep(CONNECTION_BACKOFF).await;
                continue;
            }
        };

        debug!("subscribing to remote TPU packet stream");
        let packet_stream = match client.subscribe_packets(SubscribePacketsRequest {}).await {
            Ok(resp) => resp.into_inner(),
            Err(e) => {
                warn!("failed to subscribe to remote TPU: {}", e.chain());
                sleep(CONNECTION_BACKOFF).await;
                continue;
            }
        };
        info!("subscribed to remote TPU packet stream");

        remote_tpu.send_replace(Some(addr));
        if let Err(e) = stream(packet_stream, &mut tx).await {
            if log_enabled!(log::Level::Debug) {
                warn!("remote TPU stream ended: {}", e.chain());
            } else {
                warn!("remote TPU stream ended");
            }
        } else {
            info!("remote TPU stream closed");
        }
        remote_tpu.send_replace(None);
        sleep(CONNECTION_BACKOFF).await;
    }
}

/// Main remote TPU event loop
async fn stream(
    mut packet_stream: tonic::Streaming<SubscribePacketsResponse>,
    tx: &mut rtrb::Producer<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut heartbeat_tick = tokio::time::interval(Duration::from_millis(HEARTBEAT_TIMEOUT_MS));
    heartbeat_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_heartbeat = Instant::now();

    loop {
        tokio::select! {
            msg = packet_stream.message() => {
                let Some(resp) = msg? else {
                    return Ok(()); // stream closed, reconnect
                };
                handle_message(resp, &mut last_heartbeat, tx);
            }
            _ = heartbeat_tick.tick() => {
                if last_heartbeat.elapsed_ms() > HEARTBEAT_TIMEOUT_MS {
                    return Err("heartbeat timeout".into());
                }
            }
        }
    }
}

/// Forward each packet's raw bytes to `tx`, or update the heartbeat clock
fn handle_message(
    resp: SubscribePacketsResponse,
    last_heartbeat: &mut Instant,
    tx: &mut rtrb::Producer<Vec<u8>>,
) {
    match resp.msg {
        Some(subscribe_packets_response::Msg::Batch(batch)) => {
            trace!("received batch of {} packets", batch.packets.len());
            for mut p in batch.packets {
                let size = p.meta.as_ref().map_or(p.data.len(), |m| m.size as usize);
                p.data.truncate(size);
                if p.data.is_empty() {
                    warn!("discarding empty packet");
                    continue;
                }
                if tx.push(p.data).is_err() {
                    warn!("nonvote queue full, dropping remote TPU packets");
                    break;
                }
            }
        }
        Some(subscribe_packets_response::Msg::Heartbeat(_)) => {
            trace!("received heartbeat");
            *last_heartbeat = Instant::now();
        }
        None => {}
    }
}

/// Fetch the remote TPU address via GetTpuConfigs RPC
async fn fetch_tpu_address(
    client: &mut Client,
) -> Result<SocketAddrV4, Box<dyn std::error::Error + Send + Sync>> {
    debug!("fetching remote TPU address");
    let resp = client
        .get_tpu_configs(GetTpuConfigsRequest {})
        .await?
        .into_inner();
    let tpu_socket = resp
        .tpu
        .ok_or("missing tpu socket in GetTpuConfigsResponse")?;
    let addr = SocketAddrV4::new(
        tpu_socket.ip.parse()?,
        u16::try_from(tpu_socket.port).map_err(|_| "invalid tpu port")?,
    );
    info!("fetched remote TPU address: {addr}");
    Ok(addr)
}
