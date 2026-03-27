use {
    arc_swap::ArcSwap,
    harmonic_protos::auth::{
        GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest, Role,
        Token, auth_service_client::AuthServiceClient,
    },
    log::warn,
    solana_keypair::Keypair,
    solana_signer::Signer,
    std::{sync::Arc, time::Duration},
    tonic::{
        transport::{Channel, Endpoint},
        {Request, Status},
    },
};

/// How often to check if tokens need refreshing.
const REFRESH_CHECK_INTERVAL: Duration = Duration::from_secs(300); // 5 minutes

/// How close to expiry a token must be before we refresh it.
const REFRESH_WITHIN_S: u64 = 3600; // 1 hour

/// Timeout for establishing a TCP connection to the gRPC endpoint.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Timeout for individual gRPC RPCs (does not set grpc-timeout header).
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP keepalive interval for detecting dead connections.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// HTTP/2 connection-level flow control window.
const HTTP2_CONNECTION_WINDOW: u32 = 16 * 1024 * 1024; // 16 MB

/// HTTP/2 per-stream flow control window.
const HTTP2_STREAM_WINDOW: u32 = 4 * 1024 * 1024; // 4 MB

/// Opaque auth state for a single gRPC connection.
///
/// Holds a shared bearer token string that is read by the interceptor
/// and updated by a background refresh task. The refresh task is automatically
/// cancelled when this session is dropped.
pub struct AuthSession {
    bearer_token: Arc<ArcSwap<String>>,
    refresh_handle: tokio::task::AbortHandle,
}

/// Cancels the background token refresh task when the session is dropped.
impl Drop for AuthSession {
    fn drop(&mut self) {
        self.refresh_handle.abort();
    }
}

impl AuthSession {
    /// Returns an interceptor that attaches the current bearer token to requests.
    pub fn interceptor(&self) -> AuthInterceptor {
        AuthInterceptor(self.bearer_token.clone())
    }
}

/// Interceptor that attaches the current bearer token to every gRPC request.
#[derive(Clone)]
pub(crate) struct AuthInterceptor(Arc<ArcSwap<String>>);

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        // Load the pre-formatted "Bearer <token>" string from the shared ArcSwap
        // and set it as the authorization metadata header.
        req.metadata_mut().insert(
            "authorization",
            self.0
                .load()
                .parse()
                .map_err(|_| Status::internal("invalid auth token"))?,
        );
        Ok(req)
    }
}

/// Authenticate against the auth service at the given URL.
///
/// Creates a gRPC channel, performs Ed25519 challenge-response authentication,
/// and spawns a background task to refresh tokens before they expire.
///
/// Returns the channel (for creating service clients with `with_interceptor()`)
/// and an AuthSession that must be kept alive for the duration of the connection.
pub async fn authenticate(
    url: &str,
    identity: Arc<Keypair>,
) -> Result<(Channel, AuthSession), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = make_endpoint(url)?;
    let channel = endpoint.connect().await?;

    let mut auth_client = AuthServiceClient::new(channel.clone());
    let tokens = generate_auth_tokens(&mut auth_client, &identity).await?;

    let bearer_token = Arc::new(ArcSwap::from_pointee(format!(
        "Bearer {}",
        tokens.access.value
    )));

    let handle = tokio::spawn(refresh_loop(
        auth_client,
        bearer_token.clone(),
        tokens,
        identity,
    ));

    Ok((
        channel,
        AuthSession {
            bearer_token,
            refresh_handle: handle.abort_handle(),
        },
    ))
}

/// Access and refresh token pair.
struct AuthTokens {
    access: Token,
    refresh: Token,
}

/// Perform Ed25519 challenge-response authentication.
async fn generate_auth_tokens(
    client: &mut AuthServiceClient<Channel>,
    identity: &Keypair,
) -> Result<AuthTokens, Box<dyn std::error::Error + Send + Sync>> {
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

    let resp = client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge: message,
            client_pubkey: identity.pubkey().as_ref().to_vec(),
            signed_challenge: signature.as_ref().to_vec(),
        })
        .await?
        .into_inner();

    Ok(AuthTokens {
        access: resp.access_token.ok_or("missing access token")?,
        refresh: resp.refresh_token.ok_or("missing refresh token")?,
    })
}

/// Background token refresh loop. Runs until aborted via AbortHandle.
async fn refresh_loop(
    mut client: AuthServiceClient<Channel>,
    bearer_token: Arc<ArcSwap<String>>,
    mut tokens: AuthTokens,
    identity: Arc<Keypair>,
) {
    loop {
        tokio::time::sleep(REFRESH_CHECK_INTERVAL).await;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if token_expiry(&tokens.refresh).saturating_sub(now) <= REFRESH_WITHIN_S {
            // Refresh token expiring — full re-auth needed.
            match generate_auth_tokens(&mut client, &identity).await {
                Ok(new_tokens) => {
                    bearer_token.store(Arc::new(format!("Bearer {}", new_tokens.access.value)));
                    tokens = new_tokens;
                }
                Err(e) => warn!("auth re-authentication failed: {e}"),
            }
        } else if token_expiry(&tokens.access).saturating_sub(now) <= REFRESH_WITHIN_S {
            // Access token expiring — refresh it.
            match client
                .refresh_access_token(RefreshAccessTokenRequest {
                    refresh_token: tokens.refresh.value.clone(),
                })
                .await
            {
                Ok(resp) => {
                    if let Some(access) = resp.into_inner().access_token {
                        bearer_token.store(Arc::new(format!("Bearer {}", access.value)));
                        tokens.access = access;
                    }
                }
                Err(e) => warn!("auth token refresh failed: {e}"),
            }
        }
    }
}

/// Extract the expiry timestamp from a token as seconds since epoch.
fn token_expiry(token: &Token) -> u64 {
    token
        .expires_at_utc
        .as_ref()
        .map(|ts| ts.seconds as u64)
        .unwrap_or(0)
}

/// Build a tonic Endpoint with standard transport settings.
fn make_endpoint(url: &str) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    let mut endpoint = Endpoint::from_shared(url.to_owned())?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .initial_connection_window_size(HTTP2_CONNECTION_WINDOW)
        .initial_stream_window_size(HTTP2_STREAM_WINDOW);
    if url.starts_with("https") {
        endpoint = endpoint.tls_config(tonic::transport::ClientTlsConfig::new())?;
    }
    Ok(endpoint)
}
