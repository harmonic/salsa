//! Co-located backrun stream.
//!
//! While this validator is the leader, the scheduler streams its own incoming
//! (non-vote) transactions to a connected strategy server and accepts backrun
//! bundles back to include in the block. This module hosts the `BackrunService`
//! gRPC server and the channels bridging the async runtime to the synchronous
//! scheduler thread:
//!
//! * `tx_out` (broadcast): scheduler/TPU thread -> searcher (`SubscribeBackruns`)
//! * `bundle_in` (mpsc): searcher (`SendBundle`) -> scheduler thread
//!
//! Leader gating lives in the scheduler/TPU threads via a shared `is_leader`
//! flag; this server is a thin transport.

pub mod proto {
    tonic::include_proto!("backrun");
}

use bytes::Bytes;
use log::{info, warn};
use proto::backrun_service_server::{BackrunService, BackrunServiceServer};
use proto::subscribe_backruns_response::UpdateOneof;
use proto::{
    SendBundleRequest, SendBundleResponse, SubscribeBackrunsRequest, SubscribeBackrunsResponse,
    SubscribeUpdatePing, TransactionMessage,
};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

/// Broadcast capacity for the outbound transaction stream.
pub const TX_OUT_CAPACITY: usize = 8192;
/// Keepalive ping interval on the backrun subscription.
const PING_INTERVAL: Duration = Duration::from_secs(5);

/// gRPC service bridging searcher <-> scheduler thread.
#[derive(Clone)]
pub struct BackrunBridge {
    tx_out: broadcast::Sender<Bytes>,
    bundle_in: mpsc::UnboundedSender<Vec<Bytes>>,
    x_token: Option<Arc<String>>,
}

impl BackrunBridge {
    pub fn new(
        tx_out: broadcast::Sender<Bytes>,
        bundle_in: mpsc::UnboundedSender<Vec<Bytes>>,
        x_token: Option<String>,
    ) -> Self {
        Self {
            tx_out,
            bundle_in,
            x_token: x_token.map(Arc::new),
        }
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let Some(expected) = &self.x_token else {
            return Ok(());
        };
        let provided = request
            .metadata()
            .get("x-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if provided != expected.as_str() {
            return Err(Status::unauthenticated("invalid x-token"));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl BackrunService for BackrunBridge {
    async fn send_bundle(
        &self,
        request: Request<SendBundleRequest>,
    ) -> Result<Response<SendBundleResponse>, Status> {
        self.authorize(&request)?;
        let req = request.into_inner();
        let txs: Vec<Bytes> = req
            .transactions
            .into_iter()
            .map(|t| Bytes::from(t.content))
            .filter(|b| !b.is_empty())
            .collect();
        if txs.is_empty() {
            return Err(Status::invalid_argument("bundle has no transactions"));
        }
        // Non-blocking; the scheduler thread drains on its next leader tick.
        if self.bundle_in.send(txs).is_err() {
            return Err(Status::unavailable("scheduler not running"));
        }
        Ok(Response::new(SendBundleResponse { uuid: next_uuid() }))
    }

    type SubscribeBackrunsStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeBackrunsResponse, Status>> + Send>>;

    async fn subscribe_backruns(
        &self,
        request: Request<Streaming<SubscribeBackrunsRequest>>,
    ) -> Result<Response<Self::SubscribeBackrunsStream>, Status> {
        self.authorize(&request)?;
        info!("backrun subscriber connected: {:?}", request.remote_addr());

        // Drain client keepalives.
        let mut inbound = request.into_inner();
        tokio::spawn(async move { while let Ok(Some(_)) = inbound.message().await {} });

        let mut rx = self.tx_out.subscribe();
        let (tx, out_rx) = mpsc::channel::<Result<SubscribeBackrunsResponse, Status>>(1024);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PING_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    received = rx.recv() => match received {
                        Ok(data) => {
                            if tx.send(Ok(tx_update(data))).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!("backrun stream lagged, skipped {skipped} txs");
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    },
                    _ = tick.tick() => {
                        if tx.send(Ok(ping_update())).await.is_err() {
                            break;
                        }
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }
}

/// Serve the backrun gRPC endpoint until shutdown.
pub async fn serve(addr: SocketAddr, bridge: BackrunBridge) {
    info!("backrun stream listening on {addr}");
    if let Err(e) = Server::builder()
        .add_service(BackrunServiceServer::new(bridge))
        .serve(addr)
        .await
    {
        warn!("backrun server exited: {e}");
    }
}

fn next_uuid() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{now:016x}{seq:08x}")
}

fn now_ts() -> prost_types::Timestamp {
    prost_types::Timestamp::from(SystemTime::now())
}

fn tx_update(data: Bytes) -> SubscribeBackrunsResponse {
    SubscribeBackrunsResponse {
        ts: Some(now_ts()),
        update_oneof: Some(UpdateOneof::Transaction(TransactionMessage {
            content: data.to_vec(),
        })),
    }
}

fn ping_update() -> SubscribeBackrunsResponse {
    SubscribeBackrunsResponse {
        ts: Some(now_ts()),
        update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
    }
}
