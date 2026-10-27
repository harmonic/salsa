//! Session lifecycle: connect every unit, run until one stops, then tear all down

use {
    crate::{
        admin_rpc::AdminRpc, block_engine::BlockEngine, config::Config, remote_tpu::RemoteTpu,
        scheduler::Scheduler,
    },
    anyhow::{Context, Result},
    std::{sync::Arc, time::Duration},
    tokio::{task::JoinSet, time::sleep},
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
};

/// Backoff between sessions
const SESSION_BACKOFF: Duration = Duration::from_secs(1);
/// The validator's admin RPC socket, in the ledger directory
const ADMIN_RPC_SOCKET: &str = "admin.rpc";
/// The validator's scheduler IPC socket, in the ledger directory
const SCHEDULER_IPC_SOCKET: &str = "scheduler_bindings.ipc";

/// Run sessions until `token` is cancelled
pub async fn run(config: &Config, token: &CancellationToken) -> Result<()> {
    while !token.is_cancelled() {
        match run_session(config, token).await? {
            Ok(()) => info!("session ended"),
            Err(e) => warn!(error = %format_args!("{e:#}"), "session ended"),
        }
        token.run_until_cancelled(sleep(SESSION_BACKOFF)).await;
    }
    Ok(())
}

/// Run one session; Err if its scheduler threads don't stop, else why it ended
async fn run_session(config: &Config, token: &CancellationToken) -> Result<Result<()>> {
    let Some(admin) = token
        .run_until_cancelled(AdminRpc::connect(&config.ledger.join(ADMIN_RPC_SOCKET)))
        .await
    else {
        return Ok(Ok(()));
    };
    let admin = Arc::new(admin);

    let connect = async {
        let (block_engine, remote_tpu) = tokio::join!(
            BlockEngine::connect(&config.block_engine_url, config.strategy, &admin),
            RemoteTpu::connect(&config.remote_tpu_url, &admin),
        );
        // The validator hands block production over at the handshake
        let scheduler = Scheduler::connect(
            &config.ledger.join(SCHEDULER_IPC_SOCKET),
            config.num_workers,
        )
        .await;
        (block_engine, remote_tpu, scheduler)
    };
    // An identity change, admin RPC loss, or shutdown while connecting ends the session
    let (block_engine, remote_tpu, scheduler) = tokio::select! {
        units = connect => units,
        result = admin.watch() => return Ok(result.context("admin rpc")),
        () = token.cancelled() => return Ok(Ok(())),
    };

    let session = token.child_token();
    let (threads, channels) = match scheduler.spawn(&session, admin.tip_program()) {
        Ok(spawned) => spawned,
        Err(e) => return Ok(Err(e)),
    };

    let mut units = JoinSet::new();
    block_engine.spawn(&mut units, Arc::clone(&admin), channels.block_engine);
    remote_tpu.spawn(&mut units, Arc::clone(&admin), channels.remote_tpu);
    let watched = Arc::clone(&admin);
    units.spawn(async move { watched.watch().await.context("admin rpc") });

    // The first unit task to stop ends the session
    let result = tokio::select! {
        Some(joined) = units.join_next() => joined.unwrap_or_else(|e| Err(e.into())),
        // The scheduler threads cancel `session` when they exit
        () = session.cancelled() => Ok(()),
    };
    units.shutdown().await;

    // Clients go back to the validator's own TPU, which this scheduler still serves
    admin.clear_tpu_override().await;
    // The leader thread finishes its leader window
    session.cancel();
    // A new session must not start beside scheduler threads that still run
    let stopped = threads.join().await?;
    // If the threads stopped first, how they stopped is why the session ended
    Ok(result.and(stopped))
}
