//! The `hakod` host: authenticated HTTP routes, the in-memory run
//! registry, and the wiring that drives engine kernels detached from
//! request lifetimes.

mod http;
mod registry;
mod runtime;

use std::path::PathBuf;
use std::sync::Arc;

use axum::Router;

pub use http::{SERVED_ROUTES, ServedRoute};
pub use runtime::EngineRuntime;

use http::AppState;
use registry::RunRegistry;

/// The daemon settings needed by the first API slice.
#[derive(Clone)]
pub struct DaemonConfig {
    token: String,
    runs_root: PathBuf,
}

impl DaemonConfig {
    pub fn new(token: impl Into<String>, runs_root: impl Into<PathBuf>) -> Self {
        Self {
            token: token.into(),
            runs_root: runs_root.into(),
        }
    }
}

/// A loaded daemon. Constructing it reconstructs the registry from
/// the run store before any request can be served.
#[derive(Clone)]
pub struct Daemon {
    state: Arc<AppState>,
}

impl Daemon {
    pub async fn load(
        config: DaemonConfig,
        runtime: Arc<EngineRuntime>,
    ) -> Result<Self, ServerError> {
        if config.token.is_empty() {
            return Err(ServerError::EmptyToken);
        }
        runtime.preflight().await?;
        let registry = RunRegistry::load(config.runs_root).await?;
        Ok(Self {
            state: Arc::new(AppState {
                token: config.token,
                registry,
                runtime,
            }),
        })
    }

    /// Builds a cheap cloneable router over this daemon's shared state.
    pub fn router(&self) -> Router {
        http::router(self.state.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("the daemon bearer token cannot be empty")]
    EmptyToken,
    #[error("run registry I/O on {path}: {source}")]
    RegistryIo {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Store(#[from] engine::StoreError),
    #[error(transparent)]
    Sandbox(#[from] engine::SandboxError),
}

impl ServerError {
    fn registry_io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::RegistryIo {
            path: path.into(),
            source,
        }
    }
}
