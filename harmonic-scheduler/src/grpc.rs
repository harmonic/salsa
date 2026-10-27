//! Authenticated gRPC client connections

use {
    crate::admin_rpc::AdminRpc,
    anyhow::{Context, Result, anyhow},
    arc_swap::ArcSwap,
    bytes::Bytes,
    std::{
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::time::{sleep, timeout},
    tonic::{
        Code, Request, Status,
        metadata::AsciiMetadataValue,
        transport::{Channel, ClientTlsConfig, Endpoint, Uri},
    },
    tracing::{debug, warn},
    validator_protos::auth::{
        GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role,
        Token, auth_service_client::AuthServiceClient,
    },
};

/// Deadline for the TCP, TLS, and HTTP/2 handshakes of a new connection
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-RPC deadline once a connection is established
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
/// HTTP/2 PING interval when no frames have been received
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// Deadline for an HTTP/2 PING ack before the connection is closed
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(10);
/// HTTP/2 connection-level flow-control window
const HTTP2_CONNECTION_WINDOW: u32 = 64 * 1024 * 1024;
/// HTTP/2 per-stream flow-control window
const HTTP2_STREAM_WINDOW: u32 = 16 * 1024 * 1024;
/// Max size of a gRPC message we accept
pub const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
/// Delay before retrying a failed token refresh
const REFRESH_RETRY_INTERVAL: Duration = Duration::from_mins(1);
/// New challenges requested after the auth service aborts the exchange
const MAX_ABORTED_EXCHANGES: usize = 10;

/// Attaches the current bearer token to every gRPC request
#[derive(Clone)]
pub struct AuthInterceptor(Arc<ArcSwap<AsciiMetadataValue>>);

impl AuthInterceptor {
    fn new(access: &AuthToken) -> Result<Self> {
        Ok(Self(Arc::new(ArcSwap::from_pointee(bearer_header(
            access,
        )?))))
    }

    fn set(&self, access: &AuthToken) -> Result<()> {
        self.0.store(Arc::new(bearer_header(access)?));
        Ok(())
    }
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        req.metadata_mut()
            .insert("authorization", (**self.0.load()).clone());
        Ok(req)
    }
}

/// A token and the local times at which to refresh it and at which it expires
struct AuthToken {
    value: String,
    /// Unix seconds; half of the lifetime observed when the token was received
    refresh_at: u64,
    /// Unix seconds
    expires_at: u64,
}

impl AuthToken {
    fn new(label: &str, token: Option<Token>) -> Result<Self> {
        let token = token.ok_or_else(|| anyhow!("missing {label} token"))?;
        let expires_at = token
            .expires_at_utc
            .as_ref()
            .and_then(|ts| u64::try_from(ts.seconds).ok())
            .ok_or_else(|| anyhow!("non-expiring {label} token"))?;
        let now = now();
        let lifetime = expires_at.saturating_sub(now);
        debug!(token = label, expires_in_s = lifetime, "received token");
        Ok(Self {
            value: token.value,
            refresh_at: now.saturating_add(lifetime / 2),
            expires_at,
        })
    }
}

/// The access + refresh token pair returned by the auth service
struct Tokens {
    access: AuthToken,
    refresh: AuthToken,
}

/// An authenticated connection to one gRPC endpoint
pub struct Connection {
    pub channel: Channel,
    pub interceptor: AuthInterceptor,
    pub endpoint: Endpoint,
    tokens: Tokens,
}

impl Connection {
    /// Connect to `url` and run the challenge-response handshake
    pub async fn connect(url: &Uri, admin: &AdminRpc) -> Result<Self> {
        let endpoint = endpoint(url)?;
        let channel = connect_channel(&endpoint).await?;
        let tokens =
            generate_auth_tokens(&mut AuthServiceClient::new(channel.clone()), admin).await?;
        let interceptor = AuthInterceptor::new(&tokens.access)?;
        Ok(Self {
            channel,
            interceptor,
            endpoint,
            tokens,
        })
    }

    /// Renew tokens at half their lifetime; Err once the access token expires unrenewed
    pub async fn refresh_tokens(&mut self, admin: &AdminRpc) -> Result<()> {
        let mut client = AuthServiceClient::new(self.channel.clone());
        loop {
            let refresh_at = self
                .tokens
                .access
                .refresh_at
                .min(self.tokens.refresh.refresh_at);
            sleep(Duration::from_secs(refresh_at.saturating_sub(now()))).await;
            if let Err(e) = self.renew(&mut client, admin).await {
                if now() >= self.tokens.access.expires_at {
                    return Err(e.context("access token expired"));
                }
                warn!(error = %format_args!("{e:#}"), "token renewal failed, retrying");
                sleep(REFRESH_RETRY_INTERVAL).await;
            }
        }
    }

    /// Renew the access token, or sign in again if the refresh token is old or refreshing fails
    async fn renew(
        &mut self,
        client: &mut AuthServiceClient<Channel>,
        admin: &AdminRpc,
    ) -> Result<()> {
        if now() < self.tokens.refresh.refresh_at {
            debug!("refreshing access token");
            match refresh_access_token(client, &self.tokens.refresh).await {
                Ok(access) => {
                    self.tokens.access = access;
                    return self.interceptor.set(&self.tokens.access);
                }
                Err(e) => warn!(
                    error = %format_args!("{e:#}"),
                    "access token refresh failed, signing in again"
                ),
            }
        }
        debug!("signing in again");
        self.tokens = generate_auth_tokens(client, admin).await?;
        self.interceptor.set(&self.tokens.access)
    }
}

/// Get a fresh access and refresh token pair through the challenge-sign exchange
async fn generate_auth_tokens(
    client: &mut AuthServiceClient<Channel>,
    admin: &AdminRpc,
) -> Result<Tokens> {
    let identity = admin.identity();
    let pubkey = Bytes::copy_from_slice(identity.as_ref());
    let mut aborted = 0;
    let response = loop {
        let challenge = client
            .generate_auth_challenge(GenerateAuthChallengeRequest {
                role: Role::Validator as i32,
                pubkey: pubkey.clone(),
            })
            .await
            .context("auth challenge")?
            .into_inner()
            .challenge;

        let signature = admin.sign_challenge(&challenge).await?;
        match client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge: format!("{identity}-{challenge}"),
                client_pubkey: pubkey.clone(),
                signed_challenge: Bytes::copy_from_slice(signature.as_ref()),
            })
            .await
        {
            Ok(response) => break response.into_inner(),
            Err(status) if status.code() == Code::Aborted && aborted < MAX_ABORTED_EXCHANGES => {
                aborted = aborted.saturating_add(1);
                debug!(
                    reason = status.message(),
                    "auth exchange aborted, requesting a new challenge"
                );
            }
            Err(status) => return Err(status).context("auth tokens"),
        }
    };

    Ok(Tokens {
        access: AuthToken::new("access", response.access_token)?,
        refresh: AuthToken::new("refresh", response.refresh_token)?,
    })
}

/// Refresh only the access token using the existing refresh token
async fn refresh_access_token(
    client: &mut AuthServiceClient<Channel>,
    refresh: &AuthToken,
) -> Result<AuthToken> {
    let access = client
        .refresh_access_token(RefreshAccessTokenRequest {
            refresh_token: refresh.value.clone(),
        })
        .await
        .context("refresh access token")?
        .into_inner()
        .access_token;
    AuthToken::new("access", access)
}

/// Current Unix time in seconds
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Format an access token as the `Bearer <value>` HTTP authorization header
fn bearer_header(access: &AuthToken) -> Result<AsciiMetadataValue> {
    Ok(format!("Bearer {}", access.value).parse()?)
}

/// Open a connection to `endpoint` within `CONNECT_TIMEOUT`
pub async fn connect_channel(endpoint: &Endpoint) -> Result<Channel> {
    // tonic's own connect timeout covers only TCP, not the TLS and HTTP/2 handshakes
    timeout(CONNECT_TIMEOUT, endpoint.connect())
        .await
        .context("connect timed out")?
        .context("connect")
}

/// Build a tonic [`Endpoint`] from `url`, applying timeouts, keepalive, and TLS
fn endpoint(url: &Uri) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from(url.clone())
        .timeout(RPC_TIMEOUT)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(HTTP2_KEEPALIVE_INTERVAL)
        .keep_alive_timeout(HTTP2_KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        // Fixed windows: adaptive flow control resets both to 64KB and only grows them via BDP pings
        .http2_adaptive_window(false)
        .initial_connection_window_size(HTTP2_CONNECTION_WINDOW)
        .initial_stream_window_size(HTTP2_STREAM_WINDOW);
    if endpoint.uri().scheme_str() == Some("https") {
        endpoint = endpoint.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    }
    Ok(endpoint)
}
