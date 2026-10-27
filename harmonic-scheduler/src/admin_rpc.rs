//! Validator admin RPC client

use {
    anyhow::{Context, Result, anyhow, bail},
    base64::{Engine, prelude::BASE64_STANDARD},
    jsonrpc_core::Params,
    jsonrpc_core_client::{RawClient, transports::ipc},
    serde::{Deserialize, de::DeserializeOwned},
    serde_json::{Value, json},
    solana_clock::Slot,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{net::SocketAddr, path::Path, str::FromStr, time::Duration},
    tokio::time::{sleep, timeout},
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
};

/// Timeout for each admin RPC call
const CALL_TIMEOUT: Duration = Duration::from_secs(1);
/// Backoff between admin RPC connection attempts
const CONNECT_BACKOFF: Duration = Duration::from_secs(1);
/// How often to poll the validator's identity
const IDENTITY_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Admin RPC connection client
pub struct AdminRpc {
    /// Raw connection to the admin RPC socket
    client: RawClient,
    /// Cancelled once a call times out: the validator stopped answering, so the session ends
    broken: CancellationToken,
    /// Cached identity pubkey
    identity: Pubkey,
    /// Cached tip payment program pubkey
    tip_program: Pubkey,
}

/// `signAuthChallenge` reply
#[derive(Deserialize)]
struct AuthSignature {
    pubkey: String,
    signature: String,
}

impl AdminRpc {
    /// Connect to the admin RPC, retrying until it succeeds
    pub async fn connect(path: &Path) -> Self {
        info!(path = %path.display(), "connecting to admin rpc");
        let mut failed = false;
        loop {
            match Self::try_connect(path).await {
                Ok(rpc) => {
                    info!(identity = %rpc.identity, tip_program = %rpc.tip_program, "connected to admin rpc");
                    return rpc;
                }
                Err(e) if !failed => {
                    warn!(error = %format_args!("{e:#}"), "admin rpc connect failed, retrying");
                }
                Err(e) => debug!(error = %format_args!("{e:#}"), "admin rpc connect failed"),
            }
            failed = true;
            sleep(CONNECT_BACKOFF).await;
        }
    }

    async fn try_connect(path: &Path) -> Result<Self> {
        let client = ipc::connect(path)
            .await
            .map_err(|e| anyhow!("ipc connect: {e}"))?;
        let broken = CancellationToken::new();
        let identity = get_identity(&client, &broken).await?;
        let program: String = call(&client, &broken, "getTipPaymentProgram", Params::None).await?;
        let tip_program = Pubkey::from_str(&program)
            .with_context(|| format!("getTipPaymentProgram: invalid pubkey '{program}'"))?;
        Ok(Self {
            client,
            broken,
            identity,
            tip_program,
        })
    }

    /// Poll until the identity changes (Ok) or the RPC fails (Err)
    pub async fn watch(&self) -> Result<()> {
        loop {
            tokio::select! {
                () = sleep(IDENTITY_POLL_INTERVAL) => {}
                () = self.broken.cancelled() => bail!("a call timed out"),
            }
            let identity = get_identity(&self.client, &self.broken).await?;
            if identity != self.identity {
                info!(old = %self.identity, new = %identity, "identity changed");
                return Ok(());
            }
        }
    }

    /// The validator's identity, read at connect
    pub fn identity(&self) -> Pubkey {
        self.identity
    }

    /// The tip-payment program the validator's tip crank calls
    pub fn tip_program(&self) -> Pubkey {
        self.tip_program
    }

    /// The tip crank for `slot`, signed by the validator; None if none is needed
    pub async fn sign_tip_crank(
        &self,
        slot: Slot,
        block_builder: Pubkey,
        commission: u64,
        memo: &str,
    ) -> Result<Option<Vec<u8>>> {
        let params = Params::Array(vec![
            json!(slot),
            json!(block_builder.to_string()),
            json!(commission),
            json!(memo),
        ]);
        let crank: Option<String> =
            call(&self.client, &self.broken, "signTipCrank", params).await?;
        let Some(crank) = crank else {
            return Ok(None);
        };
        let crank = BASE64_STANDARD
            .decode(crank)
            .context("signTipCrank: invalid base64")?;
        Ok(Some(crank))
    }

    /// Advertise `tpu` and `tpu_forwards` in gossip until the session ends
    pub async fn set_tpu_override(&self, tpu: SocketAddr, tpu_forwards: SocketAddr) -> Result<()> {
        let params = Params::Array(vec![json!(tpu), json!(tpu_forwards)]);
        call::<Value>(&self.client, &self.broken, "setTpuOverride", params).await?;
        info!(%tpu, %tpu_forwards, "tpu override set");
        Ok(())
    }

    /// Advertise the validator's own TPU again; on failure the validator does so at session end
    pub async fn clear_tpu_override(&self) {
        match call::<Value>(&self.client, &self.broken, "clearTpuOverride", Params::None).await {
            Ok(_) => info!("tpu override cleared"),
            Err(e) => warn!(error = %format_args!("{e:#}"), "clear tpu override failed"),
        }
    }

    /// The identity's signature over `"<identity>-<challenge>"`; fails if another identity signed
    pub async fn sign_challenge(&self, challenge: &str) -> Result<Signature> {
        let reply: AuthSignature = call(
            &self.client,
            &self.broken,
            "signAuthChallenge",
            Params::Array(vec![json!(challenge)]),
        )
        .await?;
        if reply.pubkey != self.identity.to_string() {
            bail!(
                "signAuthChallenge: signed as {}, not {}",
                reply.pubkey,
                self.identity
            );
        }
        Signature::from_str(&reply.signature).context("signAuthChallenge: invalid signature")
    }
}

/// Call `method` with `params` and deserialize the result; a timeout cancels `broken`
async fn call<T: DeserializeOwned>(
    client: &RawClient,
    broken: &CancellationToken,
    method: &str,
    params: Params,
) -> Result<T> {
    // Spawned, so the reply finds its receiver even if this caller is dropped
    // or times out; a reply without one ends the client's transport
    let request = tokio::spawn(client.call_method(method, params));
    let Ok(reply) = timeout(CALL_TIMEOUT, request).await else {
        warn!(method, "admin rpc call timed out");
        broken.cancel();
        bail!("{method}: timed out");
    };
    let value = reply
        .with_context(|| format!("{method}: request task"))?
        .map_err(|e| anyhow!("{method}: {e}"))?;
    serde_json::from_value(value).with_context(|| format!("{method}: invalid response"))
}

async fn get_identity(client: &RawClient, broken: &CancellationToken) -> Result<Pubkey> {
    let contact_info: Value = call(client, broken, "contactInfo", Params::None).await?;
    let id = contact_info
        .get("id")
        .and_then(Value::as_str)
        .context("contactInfo: missing id")?;
    Pubkey::from_str(id).with_context(|| format!("contactInfo: invalid id '{id}'"))
}
