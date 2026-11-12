//! The admin HTTP listener.
//!
//! Serves `GET /readyz`, used by the K8s readinessProbe; `GET /metrics`; and
//! the read-only policy API under `/api` (`GET /api/policy/context`,
//! `POST /api/policy/simulate`, see [`policy_api`]). Only started when
//! `admin_listen` is configured, so set it in production to get the readiness
//! probe.

use std::error::Error as StdError;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    extract::{FromRef, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use tokio_util::sync::CancellationToken;

use crate::metrics;
use kinference_fleet::gauges::FleetGauges;
use kinference_fleet::pool::PoolRegistry;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_policy::store::PolicyStore;

mod policy_api;

/// State shared by every route via `FromRef`.
#[derive(Clone)]
struct AppState {
    worker_sets_ready: tokio::sync::watch::Receiver<bool>,
    /// The live pool set, so `/readyz` reports on pools added after startup.
    pools: Arc<PoolRegistry>,
    /// CRD policy, when enabled. `/readyz` gates on it having listed; see
    /// [`readyz`].
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
    /// Prometheus registry for `/metrics`.
    registry: prometheus::Registry,
    /// The fleet gauges, registered in `registry`; `/metrics` refreshes their
    /// tree dump before rendering.
    gauges: FleetGauges,
    /// The policy API's state, built once so every request shares one
    /// simulation semaphore; see [`policy_api::PolicyApiState`].
    policy_api: policy_api::PolicyApiState,
}

impl FromRef<AppState> for Arc<PoolRegistry> {
    fn from_ref(state: &AppState) -> Self {
        state.pools.clone()
    }
}

/// The fleet state and initialization gates for `/readyz`.
#[derive(Clone)]
struct ReadyzState {
    worker_sets_ready: tokio::sync::watch::Receiver<bool>,
    pools: Arc<PoolRegistry>,
    policy: Option<Arc<kinference_policy::store::PolicyStore>>,
}

impl FromRef<AppState> for ReadyzState {
    fn from_ref(state: &AppState) -> Self {
        Self {
            worker_sets_ready: state.worker_sets_ready.clone(),
            pools: state.pools.clone(),
            policy: state.policy.clone(),
        }
    }
}

/// What `/metrics` needs: the registry to render and the fleet gauges whose
/// tree dump is refreshed first.
#[derive(Clone)]
struct MetricsState {
    registry: prometheus::Registry,
    gauges: FleetGauges,
}

impl FromRef<AppState> for MetricsState {
    fn from_ref(state: &AppState) -> Self {
        Self {
            registry: state.registry.clone(),
            gauges: state.gauges.clone(),
        }
    }
}

/// Every route on the admin listener.
///
/// Everything answers in JSON, failures included: an unknown path is a JSON
/// 404 naming the real endpoints, and a wrong method a JSON 405.
fn router(state: AppState) -> Router {
    let get_only = || async { policy_api::method_not_allowed("GET, HEAD") };
    Router::new()
        .route(
            "/api/policy/context",
            get(policy_api::context).fallback(get_only),
        )
        .route(
            "/api/policy/simulate",
            post(policy_api::simulate)
                .fallback(|| async { policy_api::method_not_allowed("POST") }),
        )
        .route("/api", any(policy_api::not_found))
        .route("/api/*rest", any(policy_api::not_found))
        .route("/metrics", get(metrics))
        .route("/readyz", get(readyz))
        // Whatever the routes above cannot match, including a trailing-slash
        // `/api/policy/context/`: matchit doesn't backtrack from a static
        // prefix into `/api/*rest`.
        .fallback(fallback)
        // Applies to every route, but only `simulate` reads a body.
        .layer(axum::extract::DefaultBodyLimit::max(
            policy_api::MAX_BODY_BYTES,
        ))
        .with_state(state)
}

/// The router-wide fallback: a JSON 404 naming the real endpoints, for any
/// path the routes above do not match.
async fn fallback(uri: axum::http::Uri) -> Response {
    policy_api::not_found(uri).await.into_response()
}

/// What the admin routes read, built by the server.
pub struct AdminHandles {
    pub worker_sets_ready: tokio::sync::watch::Receiver<bool>,
    pub pools: Arc<PoolRegistry>,
    /// CRD policy, when enabled; `/readyz` gates on it having listed.
    pub policy: Option<Arc<PolicyStore>>,
    /// The Prometheus registry `/metrics` renders.
    pub registry: prometheus::Registry,
    pub scrape: Arc<MetricsRegistry>,
}

/// Serve the admin listener in the background until `cancel`.
pub fn spawn(addr: SocketAddr, handles: AdminHandles, cancel: CancellationToken) {
    tokio::spawn(async move {
        if let Err(err) = serve(addr, handles, cancel).await {
            tracing::error!(error = &err as &dyn StdError, "admin listener failed");
        }
    });
}

pub async fn serve(
    addr: SocketAddr,
    handles: AdminHandles,
    cancel: CancellationToken,
) -> std::io::Result<()> {
    let AdminHandles {
        worker_sets_ready,
        pools,
        policy,
        registry,
        scrape,
    } = handles;
    let state = AppState {
        worker_sets_ready,
        policy_api: policy_api::PolicyApiState::new(policy.clone(), pools.clone(), scrape.clone()),
        gauges: FleetGauges::new(pools.clone(), scrape, &registry),
        pools,
        policy,
        registry,
    };
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(listen = %addr, "admin listener ready");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await?;
    Ok(())
}

/// Prometheus scrape target, in the text-exposition format.
async fn metrics(State(st): State<MetricsState>) -> Response {
    st.gauges.refresh_index().await;
    (
        [(header::CONTENT_TYPE, metrics::CONTENT_TYPE)],
        metrics::render(&st.registry),
    )
        .into_response()
}

/// K8s readinessProbe target: can this process answer a `Pick`?
///
/// Not "is there anywhere to route to": a `Pick` for a model nothing serves
/// has a correct answer (`NO_WORKERS`), and a provider-only deployment has no
/// workers at all.
///
/// Returns 503 until the enabled worker-set and policy watches initialize.
async fn readyz(State(st): State<ReadyzState>) -> Response {
    if !*st.worker_sets_ready.borrow() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "the InferenceWorkerSet watch has not completed its initial list\n",
        )
            .into_response();
    }
    if let Some(store) = st.policy.as_ref()
        && !store.snapshot().listed()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "policy is enabled but the CRD watch has not listed yet; a Pick naming a pool \
             would silently bypass its policy\n",
        )
            .into_response();
    }

    // Ready either way; the body reports the fleet state for operators.
    let snapshot = st.pools.snapshot();
    let pools_n = snapshot.len();
    let ready: usize = snapshot.iter().filter(|p| p.registry.any_ready()).count();
    (
        StatusCode::OK,
        format!("ready\nworker sets: {pools_n}, with ready endpoints: {ready}\n"),
    )
        .into_response()
}

#[cfg(test)]
mod tests;
