//! gRPC authentication via Ed25519 challenge-response

use crate::error::ErrorExt;
use arc_swap::ArcSwap;
use harmonic_protos::auth::auth_service_client::AuthServiceClient;
use harmonic_protos::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role, Token,
};
use log::{debug, info, log_enabled, trace, warn};
use solana_keypair::Keypair;
use solana_signer::Signer;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::AbortHandle;
use tokio::time::sleep;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

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
/// Backoff between authentication attempts after a failure
const CONNECTION_BACKOFF: Duration = Duration::from_secs(5);
/// How often the background refresh task wakes to check token expiry
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_mins(5);
/// Refresh any token whose expiry is within this window
const REFRESH_WITHIN: Duration = Duration::from_hours(1);

/// Keep-alive handle for an authenticated gRPC connection
pub struct AuthSession {
    /// Rotated by the refresh task; shared lock-free with `AuthInterceptor` clones
    bearer: Arc<ArcSwap<AsciiMetadataValue>>,
    refresh_handle: AbortHandle,
}

impl AuthSession {
    /// Returns an [`AuthInterceptor`] sharing this session's bearer token
    pub fn interceptor(&self) -> AuthInterceptor {
        AuthInterceptor(self.bearer.clone())
    }
}

impl Drop for AuthSession {
    /// Cancels the background token-refresh task
    fn drop(&mut self) {
        debug!("auth session dropped, aborting token refresh loop");
        self.refresh_handle.abort();
    }
}

/// Attaches the current bearer token to every gRPC request
pub struct AuthInterceptor(Arc<ArcSwap<AsciiMetadataValue>>);

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", (**self.0.load()).clone());
        Ok(req)
    }
}

/// Connect with authentication, retrying all failures with a fixed backoff
pub async fn connect(url: &str, identity: Arc<Keypair>) -> (Channel, AuthSession) {
    debug!("connecting to {url}");
    loop {
        match authenticate(url, &identity).await {
            Ok(result) => {
                info!("connected to {url}");
                return result;
            }
            Err(e) => {
                warn!("failed to connect to {url}: {}", e.chain());
                sleep(CONNECTION_BACKOFF).await;
            }
        }
    }
}

/// Perform one full authentication round-trip and build an [`AuthSession`]
async fn authenticate(
    url: &str,
    identity: &Arc<Keypair>,
) -> Result<(Channel, AuthSession), Box<dyn std::error::Error + Send + Sync>> {
    debug!("authenticating with {url}");
    let endpoint = make_endpoint(url)?;
    let channel = endpoint.connect().await?;

    let mut auth_client = AuthServiceClient::new(channel.clone());
    let tokens = generate_auth_tokens(&mut auth_client, identity).await?;

    let header: AsciiMetadataValue = format!("Bearer {}", tokens.access.value).parse()?;
    let bearer = Arc::new(ArcSwap::from_pointee(header));

    let refresh_handle = tokio::spawn(refresh_loop(
        auth_client,
        bearer.clone(),
        tokens,
        identity.clone(),
    ))
    .abort_handle();

    Ok((
        channel,
        AuthSession {
            bearer,
            refresh_handle,
        },
    ))
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
) -> Result<AuthTokens, Box<dyn std::error::Error + Send + Sync>> {
    debug!("requesting auth challenge");
    let challenge = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Validator as i32,
            pubkey: identity.pubkey().as_ref().to_vec(),
        })
        .await?
        .into_inner()
        .challenge;

    let message = format!("{}-{}", identity.pubkey(), challenge);
    let signature = identity.sign_message(message.as_bytes());

    debug!("signed challenge, requesting tokens");
    let resp = client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: message,
            client_pubkey: identity.pubkey().as_ref().to_vec(),
            signed_challenge: signature.as_ref().to_vec(),
        })
        .await?
        .into_inner();

    let access = resp
        .access_token
        .filter(|t| t.expires_at_utc.is_some())
        .ok_or("missing or non-expiring access token")?;
    let refresh = resp
        .refresh_token
        .filter(|t| t.expires_at_utc.is_some())
        .ok_or("missing or non-expiring refresh token")?;

    if log_enabled!(log::Level::Debug) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        debug!(
            "generated tokens: access_expires_in={}s, refresh_expires_in={}s",
            token_expiry(&access).saturating_sub(now),
            token_expiry(&refresh).saturating_sub(now),
        );
    }

    Ok(AuthTokens { access, refresh })
}

/// Rotates `bearer` in the background as access/refresh tokens approach expiry
async fn refresh_loop(
    mut client: AuthServiceClient<Channel>,
    bearer: Arc<ArcSwap<AsciiMetadataValue>>,
    mut tokens: AuthTokens,
    identity: Arc<Keypair>,
) {
    debug!("token refresh loop started");
    loop {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let refresh_expires_in = token_expiry(&tokens.refresh).saturating_sub(now);
        let access_expires_in = token_expiry(&tokens.access).saturating_sub(now);
        trace!(
            "token check: refresh expires in {refresh_expires_in}s, access expires in \
             {access_expires_in}s",
        );

        if refresh_expires_in <= REFRESH_WITHIN.as_secs() {
            if let Err(e) = reauth(&mut client, &bearer, &mut tokens, &identity).await {
                warn!("auth re-authentication failed: {}", e.chain());
            }
        } else if access_expires_in <= REFRESH_WITHIN.as_secs()
            && let Err(e) = refresh(&mut client, &bearer, &mut tokens).await
        {
            warn!("auth token refresh failed: {}", e.chain());
        }

        sleep(REFRESH_CHECK_INTERVAL).await;
    }
}

/// Full re-authentication: generate a fresh access + refresh pair and rotate `bearer`
async fn reauth(
    client: &mut AuthServiceClient<Channel>,
    bearer: &ArcSwap<AsciiMetadataValue>,
    tokens: &mut AuthTokens,
    identity: &Keypair,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let new_tokens = generate_auth_tokens(client, identity).await?;
    let header: AsciiMetadataValue = format!("Bearer {}", new_tokens.access.value).parse()?;
    bearer.store(Arc::new(header));
    *tokens = new_tokens;
    info!("re-authenticated with new token");
    Ok(())
}

/// Refresh only the access token using the existing refresh token
async fn refresh(
    client: &mut AuthServiceClient<Channel>,
    bearer: &ArcSwap<AsciiMetadataValue>,
    tokens: &mut AuthTokens,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let access = client
        .refresh_access_token(RefreshAccessTokenRequest {
            refresh_token: tokens.refresh.value.clone(),
        })
        .await?
        .into_inner()
        .access_token
        .filter(|t| t.expires_at_utc.is_some())
        .ok_or("refresh returned missing or non-expiring token")?;
    let header: AsciiMetadataValue = format!("Bearer {}", access.value).parse()?;
    bearer.store(Arc::new(header));
    tokens.access = access;
    info!("refreshed access token");
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

/// Build a tonic [`Endpoint`] from `url`, applying timeouts, keepalive, and TLS
fn make_endpoint(url: &str) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    let mut endpoint = url
        .parse::<Endpoint>()?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .initial_connection_window_size(HTTP2_CONNECTION_WINDOW)
        .initial_stream_window_size(HTTP2_STREAM_WINDOW);
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(tonic::transport::ClientTlsConfig::new())?;
        debug!("endpoint using TLS");
    }
    Ok(endpoint)
}
