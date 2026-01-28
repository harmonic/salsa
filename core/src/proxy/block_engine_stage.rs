//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - Acts as a system that sends high profit bundles and transactions to a validator.
//! - Sends transactions and bundles to the validator.
//!
//! Architecture for high-throughput (70k+ pps):
//! - Receive task does ZERO processing - just forwards raw protos to channel
//! - Single dedicated std::thread does all CPU-bound conversion work (lives for entire process)
//! - This ensures epoll/receive path never blocks

use {
    crate::{
        banking_trace::BankingPacketSender,
        packet_bundle::PacketBundle,
        proto_packet_to_packet,
        proxy::{
            auth::{generate_auth_tokens, maybe_refresh_auth_tokens, AuthInterceptor},
            ProxyError,
        },
    },
    ahash::HashMapExt,
    arc_swap::ArcSwap,
    crossbeam_channel::{bounded, Receiver, Sender, TryRecvError},
    itertools::Itertools,
    jito_protos::proto::{
        auth::{auth_service_client::AuthServiceClient, Token},
        block_engine::{
            self, block_engine_validator_client::BlockEngineValidatorClient,
            BlockBuilderFeeInfoRequest, BlockEngineEndpoint, GetBlockEngineEndpointRequest,
            SubmitLeaderWindowInfoRequest,
        },
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_perf::packet::PacketBatch,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        collections::hash_map::Entry,
        net::{SocketAddr, ToSocketAddrs},
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant, SystemTime},
    },
    thiserror::Error,
    tokio::{
        task,
        time::{interval, sleep, timeout},
    },
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint, Uri},
        Streaming,
    },
};

const CONNECTION_TIMEOUT_S: u64 = 10;
const CONNECTION_BACKOFF_S: u64 = 5;
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// Channel capacity for raw proto messages. Sized for burst handling.
/// At 70k pps with ~100 packets/msg = 700 msgs/sec, 10k buffer = ~14 sec headroom
const RAW_PROTO_CHANNEL_CAPACITY: usize = 10_000;

/// Message types for the converter thread
enum RawProtoMessage {
    Bundle(block_engine::SubscribeBundlesResponse),
    Packet(block_engine::SubscribePacketsResponse),
}

#[derive(Default)]
struct BlockEngineStageStats {
    num_bundles: AtomicU64,
    num_bundle_packets: AtomicU64,
    num_packets: AtomicU64,
    num_empty_packets: AtomicU64,
}

impl BlockEngineStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!(
            "block_engine_stage-stats",
            ("num_bundles", self.num_bundles.swap(0, Ordering::Relaxed), i64),
            ("num_bundle_packets", self.num_bundle_packets.swap(0, Ordering::Relaxed), i64),
            ("num_packets", self.num_packets.swap(0, Ordering::Relaxed), i64),
            ("num_empty_packets", self.num_empty_packets.swap(0, Ordering::Relaxed), i64)
        );
    }
}

#[derive(Clone, Default)]
pub struct BlockBuilderFeeInfo {
    pub block_builder: Pubkey,
    pub block_builder_commission: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockEngineConfig {
    /// Block Engine URL
    pub block_engine_url: String,

    /// Disables Block Engine auto-configuration. This stops the validator client from using the most performant Block Engine region. Values provided to `--block-engine-url` will be used as-is.
    pub disable_block_engine_autoconfig: bool,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

pub struct BlockEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}

#[allow(dead_code)]
#[derive(Error, Debug)]
enum PingError<'a> {
    #[error("Failed to send ping: {0}")]
    CommandFailure(#[from] std::io::Error),

    #[error("Ping command exited with non-zero status: {1:?} for host: {0}")]
    NonZeroExit(&'a str, Option<i32>),

    #[error("No valid RTT found in ping output")]
    NoRttFound,

    #[error("Failed to parse RTT: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
}

impl BlockEngineStage {
    const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_S);
    const CONNECTION_BACKOFF: Duration = Duration::from_secs(CONNECTION_BACKOFF_S);

    pub fn new(
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        bundle_tx: Sender<Vec<PacketBundle>>,
        cluster_info: Arc<ClusterInfo>,
        packet_tx: Sender<PacketBatch>,
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: Arc<AtomicBool>,
        leader_window_receiver: tokio::sync::mpsc::Receiver<(SystemTime, u64)>,
    ) -> Self {
        let block_builder_fee_info = block_builder_fee_info.clone();

        // Create channel for raw proto messages - lives for entire process
        let (raw_tx, raw_rx) = bounded::<RawProtoMessage>(RAW_PROTO_CHANNEL_CAPACITY);

        // Stats shared between receiver and converter
        let stats = Arc::new(BlockEngineStageStats::default());

        // Spawn converter thread ONCE - lives for entire process
        let converter_exit = exit.clone();
        let converter_stats = stats.clone();
        let converter_bundle_tx = bundle_tx.clone();
        let converter_packet_tx = packet_tx.clone();
        let converter_banking_packet_sender = banking_packet_sender.clone();
        // Note: trust_packets is read from config per-connection, but converter needs it.
        // We'll read it once here; if it changes, reconnect will pick it up.
        let trust_packets = block_engine_config.lock().unwrap().trust_packets;

        let converter_thread = Builder::new()
            .name("blk-eng-convert".to_string())
            .spawn(move || {
                Self::converter_loop(
                    raw_rx,
                    converter_bundle_tx,
                    converter_packet_tx,
                    converter_banking_packet_sender,
                    trust_packets,
                    converter_stats,
                    converter_exit,
                );
            })
            .unwrap();

        // Spawn main receive thread
        let main_thread = Builder::new()
            .name("block-engine-stage".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .thread_name("blk-eng-rt")
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(Self::start(
                    block_engine_config,
                    cluster_info,
                    exit,
                    block_builder_fee_info,
                    shredstream_receiver_address,
                    bam_enabled,
                    leader_window_receiver,
                    raw_tx,
                    stats,
                ));
            })
            .unwrap();

        Self {
            t_hdls: vec![main_thread, converter_thread],
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }

    /// Converter loop - runs for entire process lifetime on dedicated thread
    fn converter_loop(
        raw_rx: Receiver<RawProtoMessage>,
        bundle_tx: Sender<Vec<PacketBundle>>,
        packet_tx: Sender<PacketBatch>,
        banking_packet_sender: BankingPacketSender,
        trust_packets: bool,
        stats: Arc<BlockEngineStageStats>,
        exit: Arc<AtomicBool>,
    ) {
        // Pre-allocate reusable buffers - never reallocated unless capacity exceeded
        let mut packet_buf = Vec::with_capacity(Self::GIANT_BUNDLE_PACKET_CAPACITY);

        while !exit.load(Ordering::Relaxed) {
            let msg = match raw_rx.recv_timeout(Duration::from_millis(100)) {
                Ok(msg) => msg,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            };

            Self::process_raw_message(
                msg,
                &bundle_tx,
                &packet_tx,
                &banking_packet_sender,
                trust_packets,
                &stats,
                &mut packet_buf,
            );

            // Drain any additional ready messages
            loop {
                match raw_rx.try_recv() {
                    Ok(msg) => {
                        Self::process_raw_message(
                            msg,
                            &bundle_tx,
                            &packet_tx,
                            &banking_packet_sender,
                            trust_packets,
                            &stats,
                            &mut packet_buf,
                        );
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => return,
                }
            }
        }
    }

    /// Pre-allocated capacity for giant bundles (~3MiB worth of packets)
    /// At ~1.2KB per packet, 3MiB ≈ 2500 packets
    const GIANT_BUNDLE_PACKET_CAPACITY: usize = 2500;

    /// Process a single raw proto message - either bundle or packet
    fn process_raw_message(
        msg: RawProtoMessage,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        trust_packets: bool,
        stats: &BlockEngineStageStats,
        packet_buf: &mut Vec<solana_perf::packet::BytesPacket>,
    ) {
        match msg {
            RawProtoMessage::Bundle(bundles_response) => {
                let bundle_count = bundles_response.bundles.len();
                let mut bundles = Vec::with_capacity(bundle_count);

                for bundle in bundles_response.bundles {
                    let Ok(slot) = u64::from_str(&bundle.uuid) else {
                        warn!("invalid bundle slot: {}", bundle.uuid);
                        continue;
                    };

                    let Some(bundle_data) = bundle.bundle else {
                        continue;
                    };

                    let packets = bundle_data.packets;
                    let packet_count = packets.len();

                    for packet in packets {
                        packet_buf.push(proto_packet_to_packet(packet));
                    }

                    stats.num_bundle_packets.fetch_add(packet_count as u64, Ordering::Relaxed);

                    // drain() keeps packet_buf's capacity, collect() allocates for downstream
                    bundles.push(PacketBundle {
                        batch: PacketBatch::from(packet_buf.drain(..).collect::<Vec<_>>()),
                        slot,
                    });
                }

                stats.num_bundles.fetch_add(bundles.len() as u64, Ordering::Relaxed);

                let queue_depth = bundle_tx.len();
                if queue_depth > 100 {
                    datapoint_warn!(
                        "block_engine_stage-bundle_backpressure",
                        ("queue_depth", queue_depth, i64),
                    );
                }

                if let Err(e) = bundle_tx.send(bundles) {
                    warn!("Failed to send bundles (receiver dropped?): {:?}", e);
                }
            }
            RawProtoMessage::Packet(resp) => {
                let Some(proto_batch) = resp.batch else {
                    stats.num_empty_packets.fetch_add(1, Ordering::Relaxed);
                    return;
                };

                let packets = proto_batch.packets;
                if packets.is_empty() {
                    stats.num_empty_packets.fetch_add(1, Ordering::Relaxed);
                    return;
                }

                let packet_count = packets.len();

                for packet in packets {
                    packet_buf.push(proto_packet_to_packet(packet));
                }

                stats.num_packets.fetch_add(packet_count as u64, Ordering::Relaxed);

                let packet_batch = PacketBatch::from(packet_buf.drain(..).collect::<Vec<_>>());

                if trust_packets {
                    if let Err(e) = banking_packet_sender.send(Arc::new(vec![packet_batch])) {
                        warn!("Failed to send trusted packets: {:?}", e);
                    }
                } else if let Err(e) = packet_tx.send(packet_batch) {
                    warn!("Failed to send packets: {:?}", e);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: Arc<AtomicBool>,
        mut leader_window_receiver: tokio::sync::mpsc::Receiver<(SystemTime, u64)>,
        raw_tx: Sender<RawProtoMessage>,
        stats: Arc<BlockEngineStageStats>,
    ) {
        let mut error_count: u64 = 0;

        while !exit.load(Ordering::Relaxed) {
            let local_block_engine_config =
                task::block_in_place(|| block_engine_config.lock().unwrap().clone());
            if !Self::is_valid_block_engine_config(&local_block_engine_config) {
                shredstream_receiver_address.store(Arc::new(None));
                sleep(Self::CONNECTION_BACKOFF).await;
                continue;
            }

            if let Err(e) = Self::connect_auth_and_stream(
                &block_engine_config,
                &cluster_info,
                &exit,
                &block_builder_fee_info,
                &shredstream_receiver_address,
                &local_block_engine_config,
                &bam_enabled,
                &mut leader_window_receiver,
                &raw_tx,
                &stats,
            )
            .await
            {
                match e {
                    ProxyError::AuthenticationPermissionDenied => {
                        warn!("block engine permission denied. not on leader schedule. ignore if hot-spare.")
                    }
                    e => {
                        error_count += 1;
                        datapoint_warn!(
                            "block_engine_stage-proxy_error",
                            ("count", error_count, i64),
                            ("error", e.to_string(), String),
                        );
                    }
                }
                sleep(Self::CONNECTION_BACKOFF).await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_auth_and_stream(
        block_engine_config: &Arc<Mutex<BlockEngineConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: &Arc<ArcSwap<Option<SocketAddr>>>,
        local_config: &BlockEngineConfig,
        bam_enabled: &Arc<AtomicBool>,
        leader_window_receiver: &mut tokio::sync::mpsc::Receiver<(SystemTime, u64)>,
        raw_tx: &Sender<RawProtoMessage>,
        stats: &Arc<BlockEngineStageStats>,
    ) -> crate::proxy::Result<()> {
        if bam_enabled.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(10)).await;
            return Ok(());
        }

        let endpoint = Self::get_endpoint(&local_config.block_engine_url)?;

        // if !local_config.disable_block_engine_autoconfig {
        //     datapoint_info!(
        //         "block_engine_stage-connect",
        //         "type" => "autoconfig",
        //         ("count", 1, i64),
        //     );
        //     return Self::connect_auth_and_stream_autoconfig(
        //         endpoint,
        //         local_config,
        //         block_engine_config,
        //         cluster_info,
        //         exit,
        //         block_builder_fee_info,
        //         shredstream_receiver_address,
        //         bam_enabled,
        //         leader_window_receiver,
        //         raw_tx,
        //         stats,
        //     )
        //     .await;
        // }

        // if let Some((_best_url, (best_socket, _best_latency_us))) =
        //     Self::get_ranked_endpoints(&endpoint)
        //         .await?
        //         .into_iter()
        //         .min_by_key(|(_url, (_socket, latency_us))| *latency_us)
        // {
        //     shredstream_receiver_address.store(Arc::new(Some(best_socket)));
        // }

        let _ = shredstream_receiver_address; // suppress unused warning when autoconfig disabled

        datapoint_info!(
            "block_engine_stage-connect",
            "type" => "direct",
            ("count", 1, i64),
        );

        Self::connect_auth_and_stream_direct(
            &endpoint,
            local_config,
            block_engine_config,
            cluster_info,
            exit,
            block_builder_fee_info,
            bam_enabled,
            leader_window_receiver,
            raw_tx,
            stats,
        )
        .await
        .inspect(|_| {
            datapoint_info!(
                "block_engine_stage-connect",
                "type" => "closed_connection",
                ("url", endpoint.uri().to_string(), String),
                ("count", 1, i64),
            )
        })
    }

    fn get_endpoint(block_engine_url: &str) -> Result<Endpoint, ProxyError> {
        let mut backend_endpoint = Endpoint::from_shared(block_engine_url.to_owned())
            .map_err(|_| {
                ProxyError::BlockEngineEndpointError(format!(
                    "invalid block engine url value: {block_engine_url}",
                ))
            })?
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .keep_alive_while_idle(true)
            .keep_alive_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .initial_connection_window_size(16 * 1024 * 1024)
            .initial_stream_window_size(4 * 1024 * 1024);
        if block_engine_url.starts_with("https") {
            backend_endpoint = backend_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|_| {
                    ProxyError::BlockEngineEndpointError(format!(
                        "failed to set tls_config for block engine: {block_engine_url}",
                    ))
                })?;
        }
        Ok(backend_endpoint)
    }

    /// Runs a single `ping -c 1 <ip>` command and returns the RTT in microseconds, or an error.
    #[allow(dead_code)]
    async fn ping(host: &str) -> Result<u64, PingError> {
        let output = tokio::process::Command::new("ping")
            .arg("-c")
            .arg("1")
            .arg("-w")
            .arg("2")
            .arg(host)
            .output()
            .await?;

        if !output.status.success() {
            warn!(
                "Ping error to host: {host}. Stdout: {}, Stderr:{}, return code: {:?}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
                output.status.code()
            );
            return Err(PingError::NonZeroExit(host, output.status.code()));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let Some(rtt_str) = line
                .find("time=")
                .map(|index| &line[index + "time=".len()..])
                .and_then(|rtt_str| rtt_str.find(" ms").map(|index| &rtt_str[..index]))
            else {
                continue;
            };

            let rtt = rtt_str.parse::<f64>()?;
            return Ok((rtt * 1000.0).round() as u64);
        }

        Err(PingError::NoRttFound)
    }

    /// Ping all candidate endpoints concurrently, aggregate best RTT per endpoint
    #[allow(dead_code)]
    async fn ping_and_rank_endpoints(
        endpoints: &[BlockEngineEndpoint],
    ) -> ahash::HashMap<String, (SocketAddr, u64)> {
        const PING_COUNT: usize = 3;

        let endpoints_to_ping = endpoints
            .iter()
            .flat_map(|endpoint| std::iter::repeat_n(endpoint, PING_COUNT))
            .filter_map(|endpoint| {
                let uri = endpoint
                    .block_engine_url
                    .parse::<Uri>()
                    .inspect_err(|e| {
                        warn!(
                            "Failed to parse URI: {}, Error: {e}",
                            endpoint.block_engine_url
                        )
                    })
                    .ok()?;
                let _ = uri.host()?;
                Some((endpoint, uri))
            })
            .collect_vec();

        let ping_res = futures::future::join_all(
            endpoints_to_ping
                .iter()
                .map(|(_endpoint, uri)| Self::ping(uri.host().unwrap())),
        )
        .await;

        let mut agg_endpoints: ahash::HashMap<String, (SocketAddr, u64)> =
            ahash::HashMap::with_capacity(endpoints.len());
        let mut best_endpoint = (None, u64::MAX);

        ping_res
            .iter()
            .zip(endpoints_to_ping.iter())
            .for_each(|(maybe_ping_res, (endpoint, _uri))| {
                let Ok(latency_us) = maybe_ping_res else {
                    return;
                };
                if *latency_us <= best_endpoint.1 {
                    best_endpoint = (Some(endpoint), *latency_us);
                };
                datapoint_info!(
                    "block_engine_stage-autoconfig_ping",
                    ("endpoint", endpoint.block_engine_url, String),
                    ("latency_us", *latency_us, i64),
                );
                match agg_endpoints.entry(endpoint.block_engine_url.clone()) {
                    Entry::Occupied(mut ent) => {
                        let (_shredstream_socket, best_ping_us) = ent.get_mut();
                        if latency_us <= best_ping_us {
                            *best_ping_us = *latency_us;
                        }
                    }
                    Entry::Vacant(entry) => {
                        let Some(shredstream_socket) = endpoint
                            .shredstream_receiver_address
                            .to_socket_addrs()
                            .inspect_err(|e| {
                                warn!(
                                    "Failed to resolve shredstream address {}, error: {e}",
                                    endpoint.shredstream_receiver_address
                                )
                            })
                            .ok()
                            .and_then(|mut shredstream_sockets| shredstream_sockets.next())
                        else {
                            return;
                        };
                        entry.insert((shredstream_socket, *latency_us));
                    }
                }
            });

        agg_endpoints
    }

    /// Discover candidate endpoints either ranked via ping or using global fallback.
    #[allow(dead_code)]
    async fn get_ranked_endpoints(
        backend_endpoint: &Endpoint,
    ) -> crate::proxy::Result<ahash::HashMap<String, (SocketAddr, u64)>> {
        let mut endpoint_discovery = BlockEngineValidatorClient::connect(backend_endpoint.clone())
            .await
            .map_err(ProxyError::BlockEngineConnectionError)?;
        let endpoints = endpoint_discovery
            .get_block_engine_endpoints(GetBlockEngineEndpointRequest {})
            .await
            .map_err(ProxyError::BlockEngineRequestError)?
            .into_inner();

        datapoint_info!(
            "block_engine_stage-autoconfig",
            ("regioned_count", endpoints.regioned_endpoints.len(), i64),
            ("count", 1, i64),
        );

        let endpoint_latencies = Self::ping_and_rank_endpoints(&endpoints.regioned_endpoints).await;

        if endpoint_latencies.is_empty() {
            let Some(global) = endpoints.global_endpoint else {
                return Err(ProxyError::BlockEngineEndpointError(
                    "Block engine configuration failed: no reachable endpoints found".to_owned(),
                ));
            };

            let Some(ss) = global
                .shredstream_receiver_address
                .to_socket_addrs()
                .inspect_err(|e| {
                    datapoint_warn!(
                        "block_engine_stage-autoconfig_error",
                        "type" => "shredstream_resolve",
                        ("address", global.shredstream_receiver_address, String),
                        ("count", 1, i64),
                        ("err", e.to_string(), String),
                    );
                })
                .ok()
                .and_then(|mut shredstream_sockets| shredstream_sockets.next())
            else {
                return Err(ProxyError::BlockEngineEndpointError(
                    "Failed to resolve global shredstream receiver address".to_owned(),
                ));
            };

            return Ok(ahash::HashMap::from_iter([(
                global.block_engine_url,
                (ss, u64::MAX),
            )]));
        }

        Ok(endpoint_latencies)
    }

    /// Connect using autoconfig - discovers endpoints, ranks by latency, connects to best
    #[allow(dead_code)]
    #[allow(clippy::too_many_arguments)]
    async fn connect_auth_and_stream_autoconfig(
        endpoint: Endpoint,
        local_block_engine_config: &BlockEngineConfig,
        global_block_engine_config: &Arc<Mutex<BlockEngineConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: &Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: &Arc<AtomicBool>,
        leader_window_receiver: &mut tokio::sync::mpsc::Receiver<(SystemTime, u64)>,
        raw_tx: &Sender<RawProtoMessage>,
        stats: &Arc<BlockEngineStageStats>,
    ) -> crate::proxy::Result<()> {
        let candidates = Self::get_ranked_endpoints(&endpoint).await?;

        let mut attempted = false;
        let mut backend_endpoint = endpoint.clone();
        let endpoint_count = candidates.len();

        for (block_engine_url, (shredstream_socket, latency_us)) in candidates
            .into_iter()
            .sorted_unstable_by_key(|(_endpoint, (_shredstream_socket, latency_us))| *latency_us)
        {
            if block_engine_url != local_block_engine_config.block_engine_url {
                info!(
                    "Selected best Block Engine url: {block_engine_url}, Shredstream socket: {shredstream_socket}, ping: ({:?})",
                    Duration::from_micros(latency_us)
                );
                backend_endpoint = Self::get_endpoint(block_engine_url.as_str())?;
            }
            // shredstream_receiver_address.store(Arc::new(Some(shredstream_socket)));
            shredstream_receiver_address.store(Arc::new(None));
            attempted = true;

            let connect_start = Instant::now();
            match Self::connect_auth_and_stream_direct(
                &backend_endpoint,
                local_block_engine_config,
                global_block_engine_config,
                cluster_info,
                exit,
                block_builder_fee_info,
                bam_enabled,
                leader_window_receiver,
                raw_tx,
                stats,
            )
            .await
            {
                Ok(()) => {
                    datapoint_info!(
                        "block_engine_stage-connect",
                        "type" => "closed_connection",
                        ("url", backend_endpoint.uri().to_string(), String),
                        ("count", 1, i64),
                    );
                    return Ok(());
                }
                Err(e) => {
                    match &e {
                        ProxyError::AuthenticationPermissionDenied => warn!(
                            "block engine permission denied. not on leader schedule. ignore if hot-spare."
                        ),
                        other => {
                            datapoint_warn!(
                                "block_engine_stage-autoconfig_error",
                                ("url", block_engine_url, String),
                                ("count", 1, i64),
                                ("error", other.to_string(), String),
                            );
                        }
                    }

                    if connect_start.elapsed() > Self::CONNECTION_TIMEOUT * 3 {
                        return Err(e);
                    }
                }
            }
        }

        if !attempted {
            return Err(ProxyError::BlockEngineEndpointError(
                "autoconfig failed: no endpoints available after ping ranking".to_string(),
            ));
        }

        Err(ProxyError::BlockEngineEndpointError(format!(
            "autoconfig failed: all {endpoint_count} candidate endpoints failed to connect",
        )))
    }

    /// Direct connection to a specific endpoint (used by both direct and autoconfig paths)
    #[allow(clippy::too_many_arguments)]
    async fn connect_auth_and_stream_direct(
        endpoint: &Endpoint,
        local_config: &BlockEngineConfig,
        global_config: &Arc<Mutex<BlockEngineConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        bam_enabled: &Arc<AtomicBool>,
        leader_window_receiver: &mut tokio::sync::mpsc::Receiver<(SystemTime, u64)>,
        raw_tx: &Sender<RawProtoMessage>,
        stats: &Arc<BlockEngineStageStats>,
    ) -> crate::proxy::Result<()> {
        let keypair = cluster_info.keypair().clone();

        debug!("connecting to auth: {}", endpoint.uri());
        let auth_channel = timeout(Self::CONNECTION_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| ProxyError::AuthenticationConnectionTimeout)?
            .map_err(|e| ProxyError::AuthenticationConnectionError(e.to_string()))?;

        let mut auth_client = AuthServiceClient::new(auth_channel);

        debug!("generating authentication token");
        let (access_token, refresh_token) = timeout(
            Self::CONNECTION_TIMEOUT,
            generate_auth_tokens(&mut auth_client, &keypair),
        )
        .await
        .map_err(|_| ProxyError::AuthenticationTimeout)??;

        let backend_url = endpoint.uri().to_string();
        datapoint_info!(
            "block_engine_stage-tokens_generated",
            ("url", backend_url, String),
            ("count", 1, i64),
        );

        debug!("connecting to block engine: {}", endpoint.uri());
        let block_engine_channel = timeout(Self::CONNECTION_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| ProxyError::BlockEngineConnectionTimeout)?
            .map_err(ProxyError::BlockEngineConnectionError)?;

        let access_token = Arc::new(Mutex::new(access_token));
        let mut client = BlockEngineValidatorClient::with_interceptor(
            block_engine_channel,
            AuthInterceptor::new(access_token.clone()),
        )
        .max_encoding_message_size(MAX_MESSAGE_SIZE)
        .max_decoding_message_size(MAX_MESSAGE_SIZE);

        datapoint_info!(
            "block_engine_stage-connected",
            ("url", backend_url, String),
            ("count", 1, i64),
        );

        let packet_stream = timeout(
            Self::CONNECTION_TIMEOUT,
            client.subscribe_packets(block_engine::SubscribePacketsRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("block_engine_subscribe_packets".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        let bundle_stream = timeout(
            Self::CONNECTION_TIMEOUT,
            client.subscribe_bundles(block_engine::SubscribeBundlesRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("subscribe_bundles".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        debug!("Successfully subscribed to block engine bundles and packets");

        let block_builder_info = timeout(
            Self::CONNECTION_TIMEOUT,
            client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        {
            let block_builder_fee_info = block_builder_fee_info.clone();
            task::spawn_blocking(move || {
                let mut bb_fee = block_builder_fee_info.lock().unwrap();
                bb_fee.block_builder_commission = block_builder_info.commission;
                if let Ok(pk) = Pubkey::from_str(&block_builder_info.pubkey) {
                    bb_fee.block_builder = pk
                }
            })
            .await
            .unwrap();
        }

        Self::consume_streams(
            client,
            packet_stream,
            bundle_stream,
            local_config,
            global_config,
            exit,
            block_builder_fee_info,
            auth_client,
            access_token,
            refresh_token,
            keypair,
            cluster_info,
            &backend_url,
            bam_enabled,
            leader_window_receiver,
            raw_tx,
            stats,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_streams(
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        mut packet_stream: Streaming<block_engine::SubscribePacketsResponse>,
        mut bundle_stream: Streaming<block_engine::SubscribeBundlesResponse>,
        local_config: &BlockEngineConfig,
        global_config: &Arc<Mutex<BlockEngineConfig>>,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        mut auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        mut refresh_token: Token,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        block_engine_url: &str,
        bam_enabled: &Arc<AtomicBool>,
        leader_window_receiver: &mut tokio::sync::mpsc::Receiver<(SystemTime, u64)>,
        raw_tx: &Sender<RawProtoMessage>,
        stats: &Arc<BlockEngineStageStats>,
    ) -> crate::proxy::Result<()> {
        const METRICS_TICK: Duration = Duration::from_secs(1);
        const MAINTENANCE_TICK: Duration = Duration::from_secs(10 * 60);
        let refresh_within_s: u64 = METRICS_TICK.as_secs().saturating_mul(3).saturating_div(2);

        let mut num_full_refreshes: u64 = 1;
        let mut num_refresh_access_token: u64 = 0;
        let mut metrics_and_auth_tick = interval(METRICS_TICK);
        let mut maintenance_tick = interval(MAINTENANCE_TICK);

        info!("connected to packet and bundle stream");

        while !exit.load(Ordering::Relaxed) {
            if bam_enabled.load(Ordering::Relaxed) {
                info!("bam enabled, exiting block engine stage");
                return Ok(());
            }

            tokio::select! {
                // CRITICAL: These branches do ZERO processing - just forward to channel
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    if raw_tx.try_send(RawProtoMessage::Packet(resp)).is_err() {
                        datapoint_warn!(
                            "block_engine_stage-channel_full",
                            ("type", "packet", String),
                            ("count", 1, i64),
                        );
                    }
                }
                maybe_bundles = bundle_stream.message() => {
                    let resp = maybe_bundles?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    if raw_tx.try_send(RawProtoMessage::Bundle(resp)).is_err() {
                        datapoint_warn!(
                            "block_engine_stage-channel_full",
                            ("type", "bundle", String),
                            ("count", 1, i64),
                        );
                    }
                }
                maybe_leader_window_notification = leader_window_receiver.recv() => {
                    Self::handle_leader_window_notification(maybe_leader_window_notification, &mut client).await?;
                }
                _ = metrics_and_auth_tick.tick() => {
                    stats.report();

                    if cluster_info.id() != keypair.pubkey() {
                        return Err(ProxyError::AuthenticationConnectionError("validator identity changed".to_string()));
                    }

                    if !task::block_in_place(|| global_config.lock().unwrap().eq(local_config)) {
                        return Err(ProxyError::BlockEngineConfigChanged);
                    }

                    let (maybe_new_access, maybe_new_refresh) = maybe_refresh_auth_tokens(
                        &mut auth_client,
                        &access_token,
                        &refresh_token,
                        cluster_info,
                        &Self::CONNECTION_TIMEOUT,
                        refresh_within_s,
                    ).await?;

                    if let Some(new_token) = maybe_new_access {
                        num_refresh_access_token += 1;
                        datapoint_info!(
                            "block_engine_stage-refresh_access_token",
                            ("url", &block_engine_url, String),
                            ("count", num_refresh_access_token, i64),
                        );
                        task::block_in_place(|| {
                            *access_token.lock().unwrap() = new_token;
                        });
                    }
                    if let Some(new_token) = maybe_new_refresh {
                        num_full_refreshes += 1;
                        datapoint_info!(
                            "block_engine_stage-tokens_generated",
                            ("url", &block_engine_url, String),
                            ("count", num_full_refreshes, i64),
                        );
                        refresh_token = new_token;
                    }
                }
                _ = maintenance_tick.tick() => {
                    let block_builder_info = timeout(
                        Self::CONNECTION_TIMEOUT,
                        client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest{})
                    )
                    .await
                    .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
                    .map_err(|e| ProxyError::MethodError(e.to_string()))?
                    .into_inner();

                    let block_builder_fee_info = block_builder_fee_info.clone();
                    task::spawn_blocking(move || {
                        let mut bb_fee = block_builder_fee_info.lock().unwrap();
                        bb_fee.block_builder_commission = block_builder_info.commission;
                        if let Ok(pk) = Pubkey::from_str(&block_builder_info.pubkey) {
                            bb_fee.block_builder = pk
                        }
                    })
                    .await
                    .unwrap();
                }
            }
        }

        Ok(())
    }

    async fn handle_leader_window_notification(
        notification: Option<(SystemTime, u64)>,
        client: &mut BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
    ) -> crate::proxy::Result<()> {
        if let Some((time, slot)) = notification {
            trace!("Handling leader window notification ({:?}, {})", time, slot);

            if let Err(e) = client
                .submit_leader_window_info(SubmitLeaderWindowInfoRequest {
                    start_timestamp: Some(prost_types::Timestamp::from(time)),
                    slot,
                })
                .await
            {
                warn!("failed to notify leader window to block engine: {}", e);
            }
        }

        Ok(())
    }

    pub fn is_valid_block_engine_config(config: &BlockEngineConfig) -> bool {
        if config.block_engine_url.is_empty() {
            warn!("can't connect to block_engine. missing block_engine_url.");
            return false;
        }
        if let Err(e) = Endpoint::from_str(&config.block_engine_url) {
            error!(
                "can't connect to block engine. error creating block engine endpoint - {}",
                e
            );
            return false;
        }
        true
    }
}
