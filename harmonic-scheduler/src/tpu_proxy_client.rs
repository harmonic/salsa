use {
    crate::{
        auth::{self, AuthSession},
        scheduler::ControlEvent,
    },
    harmonic_protos::tpu_proxy::{self, relayer_client::RelayerClient},
    log::{info, warn},
    solana_keypair::Keypair,
    std::{
        net::{IpAddr, SocketAddr},
        str::FromStr,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time::sleep,
    tonic::transport::Channel,
};

/// Backoff duration between reconnection attempts.
const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// Maximum time without a heartbeat before considering the connection dead.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(1000);

/// A packet received from the TPU proxy.
pub struct RelayerPacket {
    /// Raw transaction bytes.
    pub data: Vec<u8>,
    /// Whether this is a simple vote transaction.
    pub is_vote: bool,
}

/// Proxy TPU information obtained from the relayer's GetTpuConfigs RPC.
#[derive(Clone, Debug)]
pub struct ProxyTpuInfo {
    /// IPv6-mapped address bytes (16 bytes). All zeros if unavailable.
    addr: [u8; 16],
    /// Port number. 0 if unavailable.
    port: u16,
}

impl ProxyTpuInfo {
    /// Returns the proxy TPU address in IPv6-mapped format suitable for
    /// control messages, or None if no proxy address is available.
    pub fn proxy_tpu(&self) -> Option<([u8; 16], u16)> {
        if self.port == 0 {
            None
        } else {
            Some((self.addr, self.port))
        }
    }
}

/// Convert a SocketAddr to IPv6-mapped bytes.
fn socket_addr_to_ipv6_mapped(addr: &SocketAddr) -> ([u8; 16], u16) {
    let ipv6 = match addr.ip() {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    };
    (ipv6.octets(), addr.port())
}

/// TPU proxy client with internal reconnect loop.
///
/// Connects to the relayer, fetches proxy TPU config, streams packets,
/// and signals the scheduler via `control_tx` on connect/disconnect.
/// Packets are forwarded via `packet_tx` (crossbeam for the OS scheduler thread).
pub struct TpuProxyClient {
    /// Relayer gRPC endpoint URL.
    url: String,
    /// Validator identity keypair for auth.
    identity: Arc<Keypair>,
    /// Channel to send received packets to the scheduler (crossbeam for OS thread).
    packet_tx: crossbeam_channel::Sender<Vec<RelayerPacket>>,
    /// Channel to send control events (connect/disconnect) to the scheduler.
    control_tx: crossbeam_channel::Sender<ControlEvent>,
}

impl TpuProxyClient {
    pub fn new(
        url: String,
        identity: Arc<Keypair>,
        packet_tx: crossbeam_channel::Sender<Vec<RelayerPacket>>,
        control_tx: crossbeam_channel::Sender<ControlEvent>,
    ) -> Self {
        Self {
            url,
            identity,
            packet_tx,
            control_tx,
        }
    }

    /// Run the TPU proxy client with automatic reconnection.
    ///
    /// This is an infinite async loop: connect, GetTpuConfigs, send
    /// Connected event, stream packets, on disconnect send Disconnected
    /// event, backoff, and loop.
    pub async fn run(self) {
        loop {
            // Connect with retry.
            let (channel, auth, proxy_info) = self.connect_with_retry().await;

            // Signal scheduler that relayer is connected.
            let _ = self
                .control_tx
                .send(ControlEvent::RelayerConnected(proxy_info));

            // Stream packets until disconnect.
            self.stream_packets(&channel, &auth).await;

            // Signal scheduler that relayer disconnected.
            let _ = self.control_tx.send(ControlEvent::RelayerDisconnected);

            // Backoff before reconnecting.
            sleep(CONNECTION_BACKOFF).await;
        }
    }

    /// Connect to the relayer and fetch TPU configs, retrying with backoff.
    async fn connect_with_retry(&self) -> (Channel, AuthSession, ProxyTpuInfo) {
        loop {
            match auth::authenticate(&self.url, self.identity.clone()).await {
                Ok((channel, auth)) => {
                    info!("connected to TPU proxy: {}", self.url);

                    let proxy_info = match fetch_tpu_configs(&channel, &auth).await {
                        Ok(info) => {
                            info!("obtained proxy TPU info: {:?}", info);
                            info
                        }
                        Err(e) => {
                            warn!("failed to get TPU configs, using no proxy: {e}");
                            ProxyTpuInfo {
                                addr: [0; 16],
                                port: 0,
                            }
                        }
                    };

                    return (channel, auth, proxy_info);
                }
                Err(e) => {
                    warn!("failed to connect to TPU proxy: {e}");
                    sleep(CONNECTION_BACKOFF).await;
                }
            }
        }
    }

    /// Subscribe to the packet stream and forward packets until disconnect.
    async fn stream_packets(&self, channel: &Channel, auth: &AuthSession) {
        let mut client = RelayerClient::with_interceptor(channel.clone(), auth.interceptor());

        let mut packet_stream = match client
            .subscribe_packets(tpu_proxy::SubscribePacketsRequest {})
            .await
        {
            Ok(resp) => {
                info!("subscribed to TPU proxy stream");
                resp.into_inner()
            }
            Err(e) => {
                warn!("failed to subscribe to TPU proxy: {e}");
                return;
            }
        };

        let mut heartbeat_tick = tokio::time::interval(HEARTBEAT_TIMEOUT);
        let mut last_heartbeat = Instant::now();
        loop {
            tokio::select! {
                msg = packet_stream.message() => {
                    match msg {
                        Ok(Some(resp)) => self.handle_message(resp, &mut last_heartbeat),
                        Ok(None) => {
                            info!("TPU proxy stream ended");
                            return;
                        }
                        Err(e) => {
                            warn!("failed to receive TPU proxy packet: {e}");
                            return;
                        }
                    }
                }
                _ = heartbeat_tick.tick() => {
                    if last_heartbeat.elapsed() > HEARTBEAT_TIMEOUT {
                        warn!("failed to receive heartbeat from TPU proxy");
                        return;
                    }
                }
            }
        }
    }

    /// Handle a single message from the packet stream.
    fn handle_message(
        &self,
        resp: tpu_proxy::SubscribePacketsResponse,
        last_heartbeat: &mut Instant,
    ) {
        match resp.msg {
            Some(tpu_proxy::subscribe_packets_response::Msg::Batch(batch)) => {
                let packets: Vec<RelayerPacket> = batch
                    .packets
                    .into_iter()
                    .filter_map(|mut p| {
                        let size = p
                            .meta
                            .as_ref()
                            .map(|m| m.size as usize)
                            .unwrap_or(p.data.len());
                        if size == 0 {
                            return None;
                        }
                        let is_vote = p
                            .meta
                            .as_ref()
                            .and_then(|m| m.flags.as_ref())
                            .is_some_and(|f| f.simple_vote_tx);
                        p.data.truncate(size);
                        Some(RelayerPacket {
                            data: p.data,
                            is_vote,
                        })
                    })
                    .collect();
                if !packets.is_empty() {
                    let _ = self.packet_tx.try_send(packets);
                }
            }
            Some(tpu_proxy::subscribe_packets_response::Msg::Heartbeat(_)) => {
                *last_heartbeat = Instant::now();
            }
            None => {}
        }
    }
}

/// Fetch TPU configs from the relayer to determine its proxy TPU address.
async fn fetch_tpu_configs(
    channel: &Channel,
    auth: &AuthSession,
) -> Result<ProxyTpuInfo, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = RelayerClient::with_interceptor(channel.clone(), auth.interceptor());

    let resp = client
        .get_tpu_configs(tpu_proxy::GetTpuConfigsRequest {})
        .await?
        .into_inner();

    let tpu_socket = resp
        .tpu
        .ok_or("missing tpu socket in GetTpuConfigsResponse")?;

    let ip = IpAddr::from_str(&tpu_socket.ip)?;
    let port = tpu_socket.port as u16;
    let socket_addr = SocketAddr::new(ip, port);
    let (addr, port) = socket_addr_to_ipv6_mapped(&socket_addr);

    Ok(ProxyTpuInfo { addr, port })
}
