//! `hakod` — the always-on hako engine host.

use std::sync::Arc;

use server::{Daemon, EngineRuntime, FileSecrets, HostConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = HostConfig::from_env()?;
    // Before the listener: a daemon whose secret store is readable by
    // the box's other users must not come up at all.
    let secrets = Arc::new(FileSecrets::open(config.secrets_root)?);
    let daemon = Daemon::load(
        config.daemon,
        Arc::new(EngineRuntime::production(config.sandbox, secrets)),
    )
    .await?;
    let address = config.address;
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
