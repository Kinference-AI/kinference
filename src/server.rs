//! The `kinference` server. `run` loads the config, builds the fleet and the
//! router, starts the control plane and the admin listener, serves gRPC
//! until a signal, and shuts down.

use std::error::Error as StdError;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use crate::admin::{self, AdminHandles};
use crate::config::Config;
use crate::{metrics, version};
use kinference_controller::{self as controller, ControllerHandles};
use kinference_fleet::Fleet;
use kinference_policy::provider_health::ProviderHealthStore;
use kinference_policy::store::PolicyStore;
use kinference_router::grpc;
use kinference_router::service::{KinferenceService, PolicyHandles};

#[derive(Parser, Debug)]
#[command(
    name = "kinference",
    version = version::VERSION,
    about = "KV-cache aware routing scorer"
)]
struct Args {
    /// Path to the JSON config file.
    #[arg(long, short = 'c')]
    config: PathBuf,
}

/// Parse the command line and run the server until it is told to stop.
pub async fn run() -> Result<()> {
    init_logging()?;
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    let listen_addr = cfg.listen;
    tracing::info!(
        version = version::VERSION,
        listen = %listen_addr,
        worker_sets = cfg.worker_sets.len(),
        profiles = cfg.profiles.len(),
        "starting kinference"
    );

    let cancel = CancellationToken::new();
    // Pre-bind so "ready" is logged only once the socket is listening.
    // `TcpIncoming::new` binds synchronously and fails with an io::Error.
    let incoming = tonic::transport::server::TcpIncoming::new(listen_addr, true, None)
        .map_err(|e| anyhow::anyhow!("failed to bind gRPC listener on {listen_addr}: {e}"))?;

    // One Prometheus registry; each group registers its families, and
    // the admin listener renders it on `/metrics`.
    let registry = metrics::registry();
    // `None` when policy is off, which `/readyz` treats as nothing to wait
    // for. Provider health exists either way: the feedback RPCs record it
    // and the status writer publishes it.
    let policy_store = cfg.policy.enabled.then(PolicyStore::new);
    let (worker_sets_ready_tx, worker_sets_ready) =
        tokio::sync::watch::channel(!cfg.kubernetes.enabled);
    let provider_health = Arc::new(ProviderHealthStore::new());

    let kubernetes =
        controller::discovery::factory(cfg.kubernetes.enabled, !cfg.worker_sets.is_empty())
            .context("building Kubernetes discovery")?;
    let fleet = Fleet::new(cfg.fleet()?, kubernetes, &registry, cancel.clone());
    let handles = fleet.handles.clone();

    let service = KinferenceService::start(
        cfg.router(),
        handles.clone(),
        PolicyHandles {
            store: policy_store.clone(),
            provider_health: provider_health.clone(),
        },
        &registry,
        cancel.clone(),
    )
    .context("starting the router")?;

    // Config-declared sets are materialized now, CR-declared ones with the
    // watch. Their keys can't collide: CR keys are namespaced, config keys
    // never are.
    let manager = fleet.start().context("starting the fleet")?;

    let controller = controller::start(
        cfg.controller(),
        ControllerHandles {
            manager,
            pools: handles.pools.clone(),
            scrape: handles.scrape.clone(),
            store: policy_store.clone(),
            provider_health,
            worker_sets_ready: worker_sets_ready_tx,
        },
        &registry,
        cancel.clone(),
    )
    .await
    .context("starting the control plane")?;

    if let Some(addr) = cfg.admin_listen {
        admin::spawn(
            addr,
            AdminHandles {
                pools: handles.pools.clone(),
                policy: policy_store,
                worker_sets_ready,
                registry: registry.clone(),
                scrape: handles.scrape,
            },
            cancel.clone(),
        );
    }

    tracing::info!(
        listen = %listen_addr,
        worker_sets = handles.pools.snapshot().len(),
        "ready"
    );
    let served = Server::builder()
        .add_service(grpc::server(service))
        .serve_with_incoming_shutdown(incoming, wait_for_shutdown_signal())
        .await;

    cancel.cancel();
    controller.shutdown().await;
    served?;
    tracing::info!("shutdown complete");
    Ok(())
}

/// The log subscriber, from `RUST_LOG` or `info`. `KINFERENCE_LOG_FORMAT`
/// selects `text`, the default, or `json`, one object per line with the
/// fields at the top level. ANSI colors only on a TTY, and not with NO_COLOR
/// (https://no-color.org). Both variables are read here, before the config
/// file, because every other env var is read by `Config::load`.
fn init_logging() -> Result<()> {
    let filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    let format = std::env::var("KINFERENCE_LOG_FORMAT");
    match format.as_deref() {
        Ok("json") => tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(filter())
            .init(),
        Ok("" | "text") | Err(std::env::VarError::NotPresent) => {
            let use_ansi = std::io::IsTerminal::is_terminal(&std::io::stdout())
                && std::env::var_os("NO_COLOR").is_none();
            tracing_subscriber::fmt()
                .with_env_filter(filter())
                .with_ansi(use_ansi)
                .init()
        }
        Ok(other) => anyhow::bail!("KINFERENCE_LOG_FORMAT must be text or json, not {other:?}"),
        Err(e) => return Err(e.clone()).context("reading KINFERENCE_LOG_FORMAT"),
    }
    Ok(())
}

/// Resolve on SIGINT or SIGTERM.
///
/// SIGTERM is what Kubernetes and `docker stop` send; without handling it
/// there is no graceful shutdown (no draining, no Lease vacate).
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                // Degrade to SIGINT-only rather than refusing to start.
                tracing::warn!(
                    error = &e as &dyn StdError,
                    "failed to install the SIGTERM handler; only SIGINT shuts down"
                );
                let _ = tokio::signal::ctrl_c().await;
                tracing::info!(signal = "SIGINT", "shutting down");
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!(signal = "SIGINT", "shutting down"),
            _ = term.recv() => tracing::info!(signal = "SIGTERM", "shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!(signal = "SIGINT", "shutting down");
    }
}
