//! Remote-TPU gRPC client

use crate::admin_rpc::TpuConfig;
use crate::auth::{
    self, AuthInterceptor, AuthSession, GRPC_CONNECTION_BACKOFF, MAX_GRPC_MESSAGE_SIZE,
};
use crate::config::RemoteTpuConfig;
use crate::state::remote_tpu_active;
use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use bytes::Bytes;
use log::{error, info, trace, warn};
use solana_keypair::Keypair;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use subscribe_packets_response::Msg;
use tokio::sync::watch;
use tokio::time::{MissedTickBehavior, sleep, timeout};
use tonic::Streaming;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;
use validator_protos::relayer::relayer_client::RelayerClient;
use validator_protos::relayer::{
    GetTpuConfigsRequest, SubscribePacketsRequest, SubscribePacketsResponse,
    subscribe_packets_response,
};

/// Timeout between remote TPU messages before assuming disconnect
/// The remote TPU sends a heartbeat every 300ms
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(400);

/// Authenticated relayer gRPC client with the scheduler's bearer-token interceptor
type Client = RelayerClient<InterceptedService<Channel, AuthInterceptor>>;

/// Remote TPU client loop. Returns when `identity_rx` closes (admin_rpc shutdown)
pub async fn run(
    config: RemoteTpuConfig,
    mut identity_rx: watch::Receiver<Arc<Keypair>>,
    mut packet_tx: rtrb::Producer<Bytes>,
    remote_tpu: Arc<ArcSwap<Option<TpuConfig>>>,
) {
    if identity_rx.changed().await.is_err() {
        return;
    }
    loop {
        let identity = identity_rx.borrow_and_update().clone();

        // Connect to relayer, publish its TPU address, subscribe to packets
        let (_session, tpu_config, mut packet_stream) = match connect(&config, identity).await {
            Ok(result) => result,
            Err(e) => {
                warn!("connect failed: {e:#}");
                sleep(GRPC_CONNECTION_BACKOFF).await;
                continue;
            }
        };

        remote_tpu.store(Arc::new(Some(tpu_config)));

        let _guard = remote_tpu_active();
        tokio::select! {
            biased;
            res = identity_rx.changed() => match res {
                Ok(()) => {}
                Err(_) => return,
            },
            res = forward_packets(&mut packet_stream, &mut packet_tx) => match res {
                Ok(()) => {} // clean shutdown
                Err(e) => error!("packet stream error: {e:#}"),
            },
        }
    }
}

/// Connect to the relayer, get its TPU address, and subscribe to the packet stream
async fn connect(
    config: &RemoteTpuConfig,
    identity: Arc<Keypair>,
) -> Result<(
    AuthSession<Client>,
    TpuConfig,
    Streaming<SubscribePacketsResponse>,
)> {
    info!("connecting to {}", config.remote_tpu_url);
    let mut session = auth::connect(&config.remote_tpu_url, identity, |svc| {
        RelayerClient::new(svc).max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    })
    .await?;

    info!("fetching remote tpu");
    let response = session
        .client
        .get_tpu_configs(GetTpuConfigsRequest {})
        .await?
        .into_inner();
    // Relayers report the UDP TPU port; QUIC lives at port+6 on the same host
    let tpu_quic = response.tpu.map(|socket| {
        let ip = socket.ip.parse().expect("tpu ip should parse");
        let port: u16 = socket.port.try_into().expect("tpu port should fit u16");
        SocketAddr::new(ip, port.saturating_add(6))
    });
    let tpu_forwards_quic = response.tpu_forward.map(|socket| {
        let ip = socket.ip.parse().expect("tpu_forward ip should parse");
        let port: u16 = socket
            .port
            .try_into()
            .expect("tpu_forward port should fit u16");
        SocketAddr::new(ip, port.saturating_add(6))
    });
    info!("tpu_quic={tpu_quic:?}, tpu_forwards_quic={tpu_forwards_quic:?}");
    let tpu_config = TpuConfig {
        tpu_quic,
        tpu_forwards_quic,
    };

    info!("subscribing to packet stream");
    let packet_stream = session
        .client
        .subscribe_packets(SubscribePacketsRequest {})
        .await?
        .into_inner();

    Ok((session, tpu_config, packet_stream))
}

/// Forward packets from the relayer into `packet_tx`
async fn forward_packets(
    stream: &mut Streaming<SubscribePacketsResponse>,
    packet_tx: &mut rtrb::Producer<Bytes>,
) -> Result<()> {
    let mut dropped: usize = 0;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = timeout(HEARTBEAT_TIMEOUT, stream.message()) => match msg {
                Ok(Ok(Some(response))) => match response.msg {
                    Some(Msg::Batch(batch)) => {
                        trace!("received batch of {} packets", batch.packets.len());
                        let Ok(chunk) = packet_tx.write_chunk_uninit(batch.packets.len()) else {
                            dropped = dropped.saturating_add(batch.packets.len());
                            continue;
                        };
                        chunk.fill_from_iter(batch.packets.into_iter().map(|packet| packet.data));
                    }
                    Some(Msg::Heartbeat(_)) => trace!("received heartbeat"),
                    None => trace!("received empty message"),
                },
                Ok(Ok(None)) => return Ok(()),
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => bail!("heartbeat timeout"),
            },
            _ = tick.tick() => {
                if dropped != 0 {
                    warn!("dropping packets: dropped={dropped}");
                    dropped = 0;
                }
            }
        }
    }
}
