//! Remote TPU client

use {
    crate::{
        admin_rpc::AdminRpc,
        grpc::{Connection, MAX_GRPC_MESSAGE_SIZE},
        scheduler,
    },
    agave_scheduling_utils::transaction_ptr::TransactionPtr,
    anyhow::{Context, Result, bail},
    std::{
        net::{IpAddr, SocketAddr},
        sync::Arc,
        time::Duration,
    },
    subscribe_packets_response::Msg,
    tokio::{
        task::{JoinSet, coop::consume_budget},
        time::{MissedTickBehavior, interval, sleep},
    },
    tonic::{
        Streaming,
        transport::{Channel, Uri},
    },
    tracing::{debug, info, trace, warn},
    validator_protos::{
        relayer::{
            GetTpuConfigsRequest, SubscribePacketsRequest, SubscribePacketsResponse,
            relayer_client::RelayerClient, subscribe_packets_response,
        },
        shared::Socket,
    },
};

/// Backoff between remote TPU connection attempts
const CONNECT_BACKOFF: Duration = Duration::from_secs(1);
/// Silence after which the remote TPU is assumed gone; it sends a heartbeat every 300ms
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(1);
/// How often to report stream drops
const DROP_REPORT_INTERVAL: Duration = Duration::from_secs(1);

/// Connected and subscribed remote TPU session; it checks auth only at subscribe
pub struct RemoteTpu {
    stream: Streaming<SubscribePacketsResponse>,
    /// Keeps the stream's connection open
    channel: Channel,
    /// Remote TPU QUIC address advertised in gossip as the validator's TPU
    tpu: SocketAddr,
    /// Remote TPU QUIC address advertised in gossip as the validator's TPU forwards
    tpu_forwards: SocketAddr,
}

impl RemoteTpu {
    /// Connect to the remote TPU, retrying until it succeeds
    pub async fn connect(url: &Uri, admin: &AdminRpc) -> Self {
        info!(%url, "connecting to remote tpu");
        let mut failed = false;
        loop {
            match Self::try_connect(url, admin).await {
                Ok(remote_tpu) => {
                    info!(
                        tpu = %remote_tpu.tpu,
                        tpu_forwards = %remote_tpu.tpu_forwards,
                        "connected to remote tpu"
                    );
                    return remote_tpu;
                }
                Err(e) if !failed => {
                    warn!(error = %format_args!("{e:#}"), "remote tpu connect failed, retrying");
                }
                Err(e) => debug!(error = %format_args!("{e:#}"), "remote tpu connect failed"),
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
        packets: scheduler::channel::Channel<TransactionPtr>,
    ) {
        tasks.spawn(async move {
            admin
                .set_tpu_override(self.tpu, self.tpu_forwards)
                .await
                .context("tpu override")?;
            forward_packets(self.stream, self.channel, packets)
                .await
                .context("remote tpu")
        });
    }

    /// Connect, authenticate, get the remote TPU's addresses, and subscribe
    async fn try_connect(url: &Uri, admin: &AdminRpc) -> Result<Self> {
        let connection = Connection::connect(url, admin).await?;
        let mut client =
            RelayerClient::with_interceptor(connection.channel.clone(), connection.interceptor)
                .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE);

        let response = client
            .get_tpu_configs(GetTpuConfigsRequest {})
            .await
            .context("get tpu configs")?
            .into_inner();
        let tpu = quic_addr(response.tpu, "tpu")?;
        let tpu_forwards = quic_addr(response.tpu_forward, "tpu_forward")?;

        let stream = client
            .subscribe_packets(SubscribePacketsRequest {})
            .await
            .context("subscribe packets")?
            .into_inner();
        Ok(Self {
            stream,
            channel: connection.channel,
            tpu,
            tpu_forwards,
        })
    }
}

/// Convert a UDP socket the remote TPU reports into its QUIC address (port+6)
fn quic_addr(socket: Option<Socket>, what: &str) -> Result<SocketAddr> {
    let socket = socket.with_context(|| format!("missing {what}"))?;
    let ip: IpAddr = socket
        .ip
        .parse()
        .with_context(|| format!("invalid {what} ip '{}'", socket.ip))?;
    let port = u16::try_from(socket.port)
        .ok()
        .and_then(|port| port.checked_add(6))
        .with_context(|| format!("invalid {what} port {}", socket.port))?;
    Ok(SocketAddr::new(ip, port))
}

/// Copy each packet from the remote TPU into shared memory and queue it
async fn forward_packets(
    mut stream: Streaming<SubscribePacketsResponse>,
    _channel: Channel, // keeps the stream's connection open
    mut packets: scheduler::channel::Channel<TransactionPtr>,
) -> Result<()> {
    let mut dropped = scheduler::channel::Metrics::default();
    let mut report = interval(DROP_REPORT_INTERVAL);
    report.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Ticks after HEARTBEAT_TIMEOUT without a message; each message restarts it
    let mut timeout = interval(HEARTBEAT_TIMEOUT);
    timeout.reset();
    loop {
        tokio::select! {
            biased;
            msg = stream.message() => {
                timeout.reset();
                let Some(response) = msg.context("remote TPU stream")? else {
                    // Clean shutdown: the remote TPU closed the stream
                    return Ok(());
                };
                match response.msg {
                    Some(Msg::Batch(batch)) => {
                        trace!(packets = batch.packets.len(), "received packet batch");
                        for packet in &batch.packets {
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
                    Some(Msg::Heartbeat(_)) => trace!("received heartbeat"),
                    None => trace!("received empty message"),
                }
            }
            _ = timeout.tick() => bail!("no message in {HEARTBEAT_TIMEOUT:?}"),
            _ = report.tick() => {
                // A quiet channel still gives back what the leader freed
                packets.allocator.clean_remote_frees();
                dropped.report("remote TPU");
            }
        }
        // Yield during a backlog, so this task can't hold a worker indefinitely
        consume_budget().await;
    }
}
