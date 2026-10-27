//! Harmonic Agave external scheduler

mod admin_rpc;
mod block_engine;
mod config;
mod grpc;
mod logging;
mod remote_tpu;
mod scheduler;
mod session;

use {
    anyhow::{Context, Result},
    clap::Parser,
    config::Config,
    std::{io, process::ExitCode},
    tokio::signal::unix::{SignalKind, signal},
    tokio_util::sync::CancellationToken,
    tracing::{error, info},
};

/// Async runtime threads, one per stream decoder
const NUM_ASYNC_WORKER_THREADS: usize = 4;

fn main() -> ExitCode {
    let config = Config::parse();
    let Some(_log_guard) = logging::init(config.log.as_deref(), config.log_format) else {
        return ExitCode::FAILURE;
    };
    // Destructured so a new setting doesn't compile until it's logged here
    let Config {
        ledger,
        log,
        log_format,
        block_engine_url,
        strategy,
        remote_tpu_url,
        num_workers,
    } = &config;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        commit = env!("AGAVE_GIT_COMMIT_HASH"),
        ledger = %ledger.display(),
        log = log.as_deref().map(|path| tracing::field::display(path.display())),
        %log_format,
        %block_engine_url,
        %strategy,
        %remote_tpu_url,
        num_workers,
        "starting"
    );
    match run(&config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %format_args!("{e:#}"), "fatal error, exiting");
            ExitCode::FAILURE
        }
    }
}

/// Run until a shutdown signal (Ok) or a fatal error (Err)
fn run(config: &Config) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(NUM_ASYNC_WORKER_THREADS)
        .thread_name("hmonic-async")
        .enable_all()
        .build()
        .context("start the async runtime")?;
    let result = runtime.block_on(async {
        let token = CancellationToken::new();
        spawn_shutdown_handler(token.clone()).context("install shutdown signal handlers")?;
        logging::spawn_reopen_handler(config.log.as_deref()).context("install SIGUSR1 handler")?;
        session::run(config, &token).await
    });
    // Don't wait on blocking tasks that outlived teardown's deadlines
    runtime.shutdown_background();
    result
}

/// Cancel `token` on the first SIGINT, SIGTERM, or SIGHUP
fn spawn_shutdown_handler(token: CancellationToken) -> io::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sighup = signal(SignalKind::hangup())?;
    tokio::spawn(async move {
        let signal = tokio::select! {
            _ = sigint.recv() => "SIGINT",
            _ = sigterm.recv() => "SIGTERM",
            _ = sighup.recv() => "SIGHUP",
        };
        info!(signal, "shutting down");
        token.cancel();
    });
    Ok(())
}
