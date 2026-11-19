//! CRD-driven routing policy (`kinference.buoyant.io/v1alpha1`).
//!
//! When a `Pick` names an `InferencePool`, kinference reads that pool, the
//! `InferencePolicy` it points at, and the destinations it lists, and routes
//! accordingly. A `Pick` with no pool is unaffected.
//!
//! A policy does not override `PredictorConfig` or `PickerConfig`, which come
//! from the worker set's profile in kinference's config. It governs the layer above the
//! picker: which candidates exist, which are dropped, and how survivors are
//! ranked.
//!
//! There are two kinds of destination:
//!
//! - An `InferenceWorkerSet` is a pool. It carries its own `serviceRef`, ports
//!   and `profile`, and resolves by `(namespace, name)` against
//!   the live pool set.
//! - An `InferenceProvider` lives outside the cluster. It resolves to a
//!   `GatewayBackend` reference, a wire format and a declared model table, and
//!   the caller of `Pick` dials it. kinference never reads that backend, so
//!   what it knows about the provider's health comes from the caller (see
//!   [`provider_health`]).
//!
//! This crate holds the resource types and everything that can be computed
//! without a cluster. `kinfctl` builds on it, and the router imports it; the
//! controller holds the watchers and status writers.
//!
//! - [`crd`]: the resource types. `charts/kinference-crds/templates/*.yaml` is
//!   the source of truth.
//! - `predicate`: the predicate grammar, compiled to a typed AST once per
//!   policy revision.
//! - `compile`: `InferencePolicy` to `CompiledPolicy`, collecting warnings.
//! - `engine`: candidate generation, exclusion, selection.
//! - `explain`: a rendered trace of what `engine` decided and why.
//! - `resolve`: the CRDs to the resolved snapshot, as a pure function.
//! - `services`: the router's view of `Service`s, for port numbers.
//! - `store`: the holder of the resolved snapshot the controller publishes.
//! - `rejection`: why a worker set produced no pool.
//! - `provider_health`: what callers report about off-cluster destinations.
//! - `lint` and `warning`: name checks against the pools a policy serves, and
//!   the warnings they and `compile` produce.
//! - `check`: validation of manifests before they are applied, behind
//!   `kinfctl check`.

pub mod check;
pub mod compile;
pub mod crd;
pub mod engine;
pub mod explain;
pub mod lint;
pub mod predicate;
pub mod provider_health;
pub mod rejection;
pub mod resolve;
pub mod services;
pub mod store;
pub mod warning;

use std::sync::atomic::{AtomicU64, Ordering};

/// The pool a request named, lifted off the wire so callers stay
/// proto-agnostic.
#[derive(Debug, Clone)]
pub struct PoolRef {
    pub namespace: String,
    pub name: String,
    /// The revision the gateway held when it routed. Opaque; compare for
    /// equality only.
    pub resource_version: String,
}

impl PoolRef {
    pub fn label(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// The pseudo-model meaning "kinference chooses the model". It is resolved to
/// a concrete model by `choose_model_for_auto` before pool and endpoint
/// selection; no worker serves this name.
pub const AUTO_MODEL: &str = "auto";

/// Opaque, process-unique handle to a pool. The readable name is `pool::Pool::name`.
///
/// Never reused, since pools come and go: a recycled id would let stale
/// references silently target another pool. The field is private so ids can
/// only be minted by [`PoolId::next`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PoolId(u64);

impl PoolId {
    /// Mint a fresh id. Monotonic and never recycled, so a stale id resolves to
    /// `None` rather than to somebody else's pool.
    pub fn next() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        PoolId(NEXT.fetch_add(1, Ordering::Relaxed))
    }

    /// A placeholder for simulated candidates that belong to no pool. `next()`
    /// starts at 1, so this is never minted and resolves to nothing.
    pub const UNSCORED: PoolId = PoolId(0);

    /// Construct a specific id. Test-only; production ids come from
    /// [`PoolId::next`].
    #[cfg(any(test, feature = "test-util"))]
    pub fn from_raw(n: u64) -> Self {
        PoolId(n)
    }
}

impl std::fmt::Display for PoolId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "pool#{}", self.0)
    }
}

/// A worker set's stable identity, as opposed to the runtime-assigned `PoolId`.
/// This is what reconciles match on.
///
/// `namespace` is `None` for config-declared worker sets. It is part of the key so
/// same-named sets in different namespaces don't merge into one pool.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolKey {
    pub namespace: Option<String>,
    pub name: String,
}

impl PoolKey {
    pub fn local(name: impl Into<String>) -> Self {
        Self {
            namespace: None,
            name: name.into(),
        }
    }

    pub fn namespaced(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: Some(namespace.into()),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for PoolKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.namespace {
            Some(ns) => write!(f, "{ns}/{}", self.name),
            None => write!(f, "{}", self.name),
        }
    }
}

/// Request attributes the CALLER resolved, lifted off the wire.
///
/// kinference does not parse HTTP, so identity arrives already extracted by
/// the caller.
///
/// `tenant` has no presence bit on the wire, so `""` means undetermined. It is
/// matched as a value: only a rule naming the empty string matches it, so a
/// rule set with `unmatched: Reject` refuses unattributed traffic.
///
/// There is no `path`: the route already matched on it to reach the pool.
#[derive(Debug, Clone, Default)]
pub struct RequestIdentity {
    pub tenant: String,
}
