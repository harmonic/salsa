//! gRPC authentication via Ed25519 challenge-response

use anyhow::{Result, anyhow};
use arc_swap::ArcSwap;
use bytes::Bytes;
use log::{debug, trace, warn};
use solana_keypair::Keypair;
use solana_signer::Signer;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use validator_protos::auth::auth_service_client::AuthServiceClient;
use validator_protos::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
};

/// Deadline for establishing the TCP + TLS + HTTP/2 connection to the endpoint
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-RPC deadline once a connection is established
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// TCP keepalive idle time
const TCP_KEEPALIVE: Duration = Duration::from_mins(1);
/// HTTP/2 connection-level flow-control window
const HTTP2_CONNECTION_WINDOW: u32 = 64 * 1024 * 1024;
/// HTTP/2 per-stream flow-control window
const HTTP2_STREAM_WINDOW: u32 = 16 * 1024 * 1024;
/// Max gRPC message size on inbound responses; matches the auction server's encoding cap
pub const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
/// How often the background refresh task wakes to check token expiry
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_mins(5);
/// Refresh any token whose expiry is within this window
const REFRESH_WITHIN: Duration = Duration::from_hours(1);
/// Backoff between gRPC connection / authentication attempts
pub const GRPC_CONNECTION_BACKOFF: Duration = Duration::from_secs(5);

/// Authenticated gRPC client with a background token-refresh task
pub struct AuthSession<C> {
    pub client: C,
    pub refresh: JoinHandle<()>,
}

impl<C> Drop for AuthSession<C> {
    fn drop(&mut self) {
        debug!("aborting token refresh loop");
        self.refresh.abort();
    }
}

/// Attaches the current bearer token to every gRPC request
#[derive(Clone)]
pub struct AuthInterceptor(Arc<ArcSwap<AsciiMetadataValue>>);

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", (**self.0.load()).clone());
        Ok(req)
    }
}

/// gRPC connect with authentication
pub async fn connect<C>(
    url: &str,
    identity: Arc<Keypair>,
    build: fn(InterceptedService<Channel, AuthInterceptor>) -> C,
) -> Result<AuthSession<C>> {
    debug!("connecting to {url}");
    let endpoint = make_endpoint(url)?;
    let channel = endpoint.connect().await?;
    trace!("connected to {url}");
    let mut auth_client = AuthServiceClient::new(channel.clone());
    let tokens = generate_auth_tokens(&mut auth_client, &identity).await?;
    let bearer = Arc::new(ArcSwap::from_pointee(bearer_header(&tokens.access)?));
    let refresh = tokio::spawn(refresh_loop(auth_client, identity, bearer.clone(), tokens));
    let client = build(InterceptedService::new(channel, AuthInterceptor(bearer)));
    debug!("authenticated to {url}");
    Ok(AuthSession { client, refresh })
}

/// The access + refresh token pair returned by the auth service
struct AuthTokens {
    access: Token,
    refresh: Token,
}

/// Challenge-sign-exchange flow that yields a fresh access + refresh token pair
async fn generate_auth_tokens(
    client: &mut AuthServiceClient<Channel>,
    identity: &Keypair,
) -> Result<AuthTokens> {
    trace!("generating auth challenge");
    let challenge = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Validator as i32,
            pubkey: Bytes::copy_from_slice(identity.pubkey().as_ref()),
        })
        .await?
        .into_inner()
        .challenge;

    trace!("signing auth challenge");
    let message = format!("{}-{}", identity.pubkey(), challenge);
    let signature = identity.sign_message(message.as_bytes());

    trace!("generating auth tokens");
    let resp = client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: message,
            client_pubkey: Bytes::copy_from_slice(identity.pubkey().as_ref()),
            signed_challenge: Bytes::copy_from_slice(signature.as_ref()),
        })
        .await?
        .into_inner();

    trace!("validating auth tokens");
    let access = resp
        .access_token
        .filter(|t| t.expires_at_utc.is_some())
        .ok_or_else(|| anyhow!("missing or non-expiring access token"))?;
    let refresh = resp
        .refresh_token
        .filter(|t| t.expires_at_utc.is_some())
        .ok_or_else(|| anyhow!("missing or non-expiring refresh token"))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    debug!(
        "generated tokens: access_expires={}s, refresh_expires={}s",
        token_expiry(&access).saturating_sub(now),
        token_expiry(&refresh).saturating_sub(now),
    );

    Ok(AuthTokens { access, refresh })
}

/// Rotates `bearer` in the background as access/refresh tokens approach expiry
async fn refresh_loop(
    mut client: AuthServiceClient<Channel>,
    identity: Arc<Keypair>,
    bearer: Arc<ArcSwap<AsciiMetadataValue>>,
    mut tokens: AuthTokens,
) {
    debug!("token refresh loop started");
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let refresh_expires = token_expiry(&tokens.refresh).saturating_sub(now);
        let access_expires = token_expiry(&tokens.access).saturating_sub(now);
        trace!("token check: refresh_expires={refresh_expires}s, access_expires={access_expires}s");

        if refresh_expires <= REFRESH_WITHIN.as_secs() {
            if let Err(e) = reauth(&mut client, &identity, &bearer, &mut tokens).await {
                warn!("reauthentication failed: {e:#}");
            }
        } else if access_expires <= REFRESH_WITHIN.as_secs() {
            if let Err(e) = refresh(&mut client, &bearer, &mut tokens).await {
                warn!("token refresh failed: {e:#}");
            }
        }

        sleep(REFRESH_CHECK_INTERVAL).await;
    }
}

/// Full re-authentication: generate a fresh access + refresh pair and rotate `bearer`
async fn reauth(
    client: &mut AuthServiceClient<Channel>,
    identity: &Keypair,
    bearer: &ArcSwap<AsciiMetadataValue>,
    tokens: &mut AuthTokens,
) -> Result<()> {
    trace!("reauthenticating");
    *tokens = generate_auth_tokens(client, identity).await?;
    bearer.store(Arc::new(bearer_header(&tokens.access)?));
    Ok(())
}

/// Refresh only the access token using the existing refresh token
async fn refresh(
    client: &mut AuthServiceClient<Channel>,
    bearer: &ArcSwap<AsciiMetadataValue>,
    tokens: &mut AuthTokens,
) -> Result<()> {
    trace!("refreshing access token");
    let access = client
        .refresh_access_token(RefreshAccessTokenRequest {
            refresh_token: tokens.refresh.value.clone(),
        })
        .await?
        .into_inner()
        .access_token;

    trace!("validating refreshed access token");
    let access = access
        .filter(|t| t.expires_at_utc.is_some())
        .ok_or_else(|| anyhow!("refresh returned missing or non-expiring token"))?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    debug!(
        "refreshed access token: access_expires={}s",
        token_expiry(&access).saturating_sub(now)
    );

    tokens.access = access;
    bearer.store(Arc::new(bearer_header(&tokens.access)?));
    Ok(())
}

/// Unix timestamp (seconds) at which `token` expires, or `0` if unknown
fn token_expiry(token: &Token) -> u64 {
    token
        .expires_at_utc
        .as_ref()
        .and_then(|ts| u64::try_from(ts.seconds).ok())
        .unwrap_or(0)
}

/// Format an access token as the `Bearer <value>` HTTP authorization header
fn bearer_header(access: &Token) -> Result<AsciiMetadataValue> {
    Ok(format!("Bearer {}", access.value).parse()?)
}

/// Build a tonic [`Endpoint`] from `url`, applying timeouts, keepalive, and TLS
fn make_endpoint(url: &str) -> Result<Endpoint> {
    let mut endpoint = url
        .parse::<Endpoint>()?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .initial_connection_window_size(HTTP2_CONNECTION_WINDOW)
        .initial_stream_window_size(HTTP2_STREAM_WINDOW)
        .http2_adaptive_window(true);
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint =
            endpoint.tls_config(tonic::transport::ClientTlsConfig::new().with_enabled_roots())?;
        debug!("using TLS endpoint for {url}");
    }
    Ok(endpoint)
}
