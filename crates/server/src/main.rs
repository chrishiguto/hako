//! `hakod` — the always-on hako engine host.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use server::{Daemon, DaemonConfig, EngineRuntime, FileSecrets};

const DEFAULT_ADDR: &str = "127.0.0.1:7878";
const DEFAULT_RUNS_ROOT: &str = ".hako/runs";
const DEFAULT_SECRETS_ROOT: &str = ".hako/secrets";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let token = std::env::var("HAKO_TOKEN")
        .map_err(|_| "HAKO_TOKEN must contain the daemon bearer token")?;
    let address: SocketAddr = std::env::var("HAKO_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_owned())
        .parse()?;
    let runs_root = std::env::var_os("HAKO_RUNS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_RUNS_ROOT));
    let secrets_root = std::env::var_os("HAKO_SECRETS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_SECRETS_ROOT));

    // Before the listener: a daemon whose secret store is readable by
    // the box's other users must not come up at all.
    let secrets = Arc::new(FileSecrets::open(secrets_root)?);
    let daemon = Daemon::load(
        DaemonConfig::new(token, runs_root),
        Arc::new(EngineRuntime::production(secrets)),
    )
    .await?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!("hakod {} listening on {address}", env!("CARGO_PKG_VERSION"));
    axum::serve(listener, daemon.router())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }
}
