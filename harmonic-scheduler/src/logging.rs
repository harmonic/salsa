//! Logging setup

use {
    anyhow::{Context, Result},
    std::{
        fs::OpenOptions,
        io::{self, IsTerminal},
        path::Path,
    },
    tokio::signal::unix::{SignalKind, signal},
    tracing::{error, info},
    tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard},
    tracing_subscriber::{EnvFilter, filter::LevelFilter},
};

/// Level logged when `RUST_LOG` sets none
const DEFAULT_LEVEL: LevelFilter = LevelFilter::INFO;

/// Log to stderr, redirected to `logfile` if set; None if it can't be opened
pub fn init(logfile: Option<&Path>) -> Option<WorkerGuard> {
    // Hot threads don't wait on the log file; a full queue blocks instead of dropping lines
    let (writer, guard) = NonBlockingBuilder::default()
        .lossy(false)
        .thread_name("hmonic-log")
        .finish(io::stderr());
    let filter = EnvFilter::builder()
        .with_default_directive(DEFAULT_LEVEL.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_ansi(logfile.is_none() && io::stderr().is_terminal())
        .init();
    if let Some(logfile) = logfile
        && let Err(e) = redirect_stderr(logfile)
    {
        error!(error = %format_args!("{e:#}"), "failed to open the log file");
        return None;
    }
    Some(guard)
}

/// Reopen `logfile`, if set, on each SIGUSR1, so logrotate can swap it out
pub fn spawn_reopen_handler(logfile: Option<&Path>) -> io::Result<()> {
    let Some(logfile) = logfile else {
        return Ok(());
    };
    let mut sigusr1 = signal(SignalKind::user_defined1())?;
    let logfile = logfile.to_path_buf();
    // Detached: it holds nothing to tear down, and ends with the runtime
    tokio::spawn(async move {
        while sigusr1.recv().await.is_some() {
            info!(path = %logfile.display(), "received SIGUSR1, reopening log file");
            if let Err(e) = redirect_stderr(&logfile) {
                error!(error = %format_args!("{e:#}"), "failed to reopen the log file");
            }
        }
    });
    Ok(())
}

/// Point stderr at `logfile` so panic messages land in it too
fn redirect_stderr(logfile: &Path) -> Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(logfile)
        .with_context(|| format!("open {}", logfile.display()))?;
    nix::unistd::dup2_stderr(file).context("redirect stderr")?;
    Ok(())
}
