//! Resolving the CRDs into the snapshot the pick path reads.
//!
//! [`resolve`] is a free function over the contents of the CRD stores: the
//! router's `store::PolicyWatcher` calls it on every rebuild, and `kinfctl
//! check` calls it over manifests read from disk, so the two agree.
//!
//! An `InferenceWorkerSet` becomes a pool, so a destination naming one is a
//! `(namespace, name)` lookup against the live pool set (see [`PoolIndex`]).
//! A set that was rejected reports why (`UnknownProfile`,
//! `InvalidKvBlockSize`); any other miss is `NoMatchingPool`, normally the
//! brief window before the reconcile runs.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use kube::{Resource, ResourceExt};

use super::PoolId;
use super::compile::CompiledPolicy;
use super::crd::{
    ApiFormat, BackendType, HTTPRoute, InferencePolicy, InferencePool, InferenceProvider,
    InferenceWorkerSet, IntOrString, ModelAlias, ParentRef, ProviderModel, ReferenceGrant,
};
use super::engine::ObjectKey;
use super::services::ServiceIndex;

/// `(namespace, name)`.
pub type ObjKey = (String, String);

/// Why a destination didn't resolve. Mirrors
/// `status.unresolvedDestinations[].reason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnresolvedReason {
    /// The named `InferenceWorkerSet` doesn't exist.
    NotFound,
    /// It exists, but kinference has no pool for it. Usually transient: the set
    /// was just created and the reconcile hasn't run yet.
    NoMatchingPool,
    /// It names a routing profile the router does not define, which is fixed in
    /// the router's config rather than the CR.
    UnknownProfile,
    /// Its `kvBlockSize` is missing or not a usable block size.
    InvalidKvBlockSize,
    /// `kind` was absent, or something other than `InferenceWorkerSet` or
    /// `InferenceProvider`.
    UnsupportedKind,
    /// Two destinations of different kinds have the same name, which
    /// `backends.names` and `order` couldn't tell apart.
    AmbiguousName,
    /// A cross-namespace reference with no `ReferenceGrant` permitting it in the
    /// target namespace.
    ///
    /// For a provider's `backendRef` this stops one namespace using another's
    /// vendor credential, so it fails closed.
    CrossNamespaceNotGranted,
    /// The object is missing a field kinference needs to route to it.
    Incomplete,
    /// An `InferenceProvider` declared a wire format nothing translates to.
    ///
    /// The schema also forbids these, but an object that predates it or
    /// bypassed validation must still deserialize, so `ApiFormat` keeps every
    /// variant and they are refused here.
    UnsupportedApiFormat,
}

impl UnresolvedReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            UnresolvedReason::NotFound => "NotFound",
            UnresolvedReason::NoMatchingPool => "NoMatchingPool",
            UnresolvedReason::UnknownProfile => "UnknownProfile",
            UnresolvedReason::InvalidKvBlockSize => "InvalidKvBlockSize",
            UnresolvedReason::UnsupportedKind => "UnsupportedKind",
            UnresolvedReason::AmbiguousName => "AmbiguousName",
            UnresolvedReason::CrossNamespaceNotGranted => "CrossNamespaceNotGranted",
            UnresolvedReason::Incomplete => "Incomplete",
            UnresolvedReason::UnsupportedApiFormat => "UnsupportedApiFormat",
        }
    }
}

/// Why a pool's `policyRef` did not resolve.
///
/// Distinct because the fixes differ: a missing `InferencePolicy`, or a
/// `policyRef` naming the wrong kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DanglingPolicy {
    /// No `InferencePolicy` by that name in the pool's own namespace. The
    /// lookup is same-namespace only.
    NotFound { name: String },
    /// `group`/`kind` name something that is not a `kinference.buoyant.io`
    /// `InferencePolicy`.
    WrongKind {
        name: String,
        group: String,
        kind: String,
    },
}

impl DanglingPolicy {
    pub fn name(&self) -> &str {
        match self {
            Self::NotFound { name } | Self::WrongKind { name, .. } => name,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Unresolved {
    pub kind: String,
    pub name: String,
    /// The namespace that the reference points into, which may differ from
    /// the pool's.
    pub namespace: String,
    /// UID of the offending object, when it exists. Used as a status cache key;
    /// not published.
    pub uid: Option<String>,
    pub reason: UnresolvedReason,
}

/// An `InferenceWorkerSet` that resolved to a kinference pool.
#[derive(Debug, Clone)]
pub struct ResolvedWorkerSet {
    /// The worker set's own name; `backend.name` in policy predicates.
    pub name: String,
    pub namespace: String,
    /// Object labels, for `backends.selector.matchLabels`.
    pub labels: BTreeMap<String, String>,
    /// The pool this worker set became.
    pub pool_id: PoolId,
    pub service_name: String,
    /// `serviceRef.port` as written. Left unresolved since Services are
    /// outside the publish gate; [`super::services::ServiceIndex::port_number`]
    /// resolves it in this set's namespace.
    pub service_port: Option<IntOrString>,
    /// Whether `spec.kvEvents` was declared. Can't be inferred at runtime,
    /// since a missing publisher looks like an idle one.
    pub kv_events_declared: bool,
    /// `spec.models` aliases. Empty means every discovered model is routable
    /// under the exact name `/v1/models` reports.
    pub aliases: Vec<ModelAlias>,
}

impl ResolvedWorkerSet {
    /// Upstream model names this set could serve for a canonical (client-facing)
    /// name.
    ///
    /// Aliases are additive: the canonical name itself stays routable. Two rows
    /// sharing an alias yield two upstreams, i.e. two candidates. Whether an
    /// upstream is actually served is checked later against the worker
    /// registry.
    pub fn upstreams_for(&self, canonical: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .aliases
            .iter()
            .filter(|m| {
                m.name.as_deref() == Some(canonical) || m.aliases.iter().any(|a| a == canonical)
            })
            .filter_map(|m| m.name.clone())
            .collect();
        if !out.iter().any(|u| u == canonical) {
            out.push(canonical.to_string());
        }
        out
    }

    /// Every client-facing name declared for `upstream`, sorted.
    ///
    /// Unlike [`Self::canonical_for`], returns all of them, or none.
    pub fn aliases_for(&self, upstream: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .aliases
            .iter()
            .filter(|m| m.name.as_deref() == Some(upstream))
            .flat_map(|m| m.aliases.iter().cloned())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// The first alias declared for an upstream name, or the upstream name
    /// itself.
    pub fn canonical_for(&self, upstream: &str) -> String {
        self.aliases
            .iter()
            .find(|m| m.name.as_deref() == Some(upstream))
            .and_then(|m| m.aliases.first().cloned())
            .unwrap_or_else(|| upstream.to_string())
    }
}

/// An `InferenceProvider` that resolved far enough to route to.
///
/// That only means the provider is well-formed and permitted to name its
/// backend. The `GatewayBackend` is never read, so whether it works arrives as
/// caller feedback.
#[derive(Debug, Clone)]
pub struct ResolvedProvider {
    /// The provider's own name; `backend.name` in policy predicates.
    pub name: String,
    pub namespace: String,
    /// The object's UID, part of the status writer's cache key.
    pub uid: Option<String>,
    /// Object labels, for `backends.selector.matchLabels`.
    pub labels: BTreeMap<String, String>,
    /// The `GatewayBackend`, with its namespace resolved so the caller never
    /// has to default one.
    pub backend_ref: ObjectKey,
    /// `spec.path`: the path the caller dispatches on instead of the one the
    /// client sent. `None` leaves the request line alone. Not validated beyond
    /// emptiness.
    pub path: Option<String>,
    /// Declared model table, since there is no `/v1/models` to poll.
    pub models: Vec<ProviderModel>,
}

impl ResolvedProvider {
    /// `(name, context_window)` for a requested name, one per matching row.
    pub fn upstreams_for(&self, canonical: &str) -> Vec<(String, Option<u32>)> {
        self.models
            .iter()
            .filter(|m| {
                // The vendor's own name matches without an alias.
                m.name.as_deref() == Some(canonical) || m.aliases.iter().any(|a| a == canonical)
            })
            .filter_map(|m| m.name.clone().map(|n| (n, m.context_window)))
            .collect()
    }

    /// The client-facing aliases that this provider declares for `upstream`, sorted.
    pub fn aliases_for(&self, upstream: &str) -> Vec<String> {
        let mut out: Vec<String> = self
            .models
            .iter()
            .filter(|m| m.name.as_deref() == Some(upstream))
            .flat_map(|m| m.aliases.iter().cloned())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Every client-facing name this provider serves, sorted.
    pub fn served_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .models
            .iter()
            .filter_map(|m| m.name.clone())
            .chain(self.models.iter().flat_map(|m| m.aliases.iter().cloned()))
            .collect();
        out.sort();
        out.dedup();
        out
    }
}

/// A destination a pool lists, of either class.
///
/// An enum rather than a trait, since most routing signals exist for only one
/// class and callers must handle which one they have.
#[derive(Debug, Clone)]
pub enum ResolvedDestination {
    Local(ResolvedWorkerSet),
    Provider(ResolvedProvider),
}

impl ResolvedDestination {
    /// The name a policy's `backends.names` or `order` would use.
    pub fn name(&self) -> &str {
        match self {
            Self::Local(w) => &w.name,
            Self::Provider(p) => &p.name,
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Self::Local(w) => &w.namespace,
            Self::Provider(p) => &p.namespace,
        }
    }

    pub fn labels(&self) -> &BTreeMap<String, String> {
        match self {
            Self::Local(w) => &w.labels,
            Self::Provider(p) => &p.labels,
        }
    }

    pub fn backend_type(&self) -> BackendType {
        match self {
            Self::Local(_) => BackendType::Local,
            Self::Provider(_) => BackendType::Provider,
        }
    }

    pub fn as_local(&self) -> Option<&ResolvedWorkerSet> {
        match self {
            Self::Local(w) => Some(w),
            Self::Provider(_) => None,
        }
    }

    pub fn as_provider(&self) -> Option<&ResolvedProvider> {
        match self {
            Self::Provider(p) => Some(p),
            Self::Local(_) => None,
        }
    }

    /// The CR kind, for `status.unresolvedDestinations[].kind` and logs.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Local(_) => "InferenceWorkerSet",
            Self::Provider(_) => "InferenceProvider",
        }
    }
}

/// One `InferencePolicy`, compiled, whether or not anything routes through it.
///
/// Held separately from the pools so unreferenced policies get status too.
#[derive(Debug, Clone)]
pub struct ResolvedPolicy {
    pub namespace: String,
    pub name: String,
    /// The object's UID, part of the status writer's cache key.
    pub uid: Option<String>,
    /// For `observedGeneration`.
    pub generation: Option<i64>,
    pub policy: Arc<CompiledPolicy>,
    /// The spec exactly as applied, since `CompiledPolicy` is lossy. Shared
    /// with the watcher's store.
    pub spec: Arc<crate::crd::InferencePolicySpec>,
    /// Pools whose `policyRef` names this policy, sorted. Empty means nothing
    /// routes through it.
    pub referenced_by: Vec<String>,
    /// Names in the policy that match nothing in the pools referencing it (see
    /// [`super::lint`]). Empty when nothing references the policy.
    pub reference_warnings: Vec<crate::warning::PolicyWarning>,
}

impl ResolvedPolicy {
    /// Compile and name warnings, in that order.
    pub fn all_warnings(&self) -> impl Iterator<Item = &crate::warning::PolicyWarning> {
        self.policy.warnings.iter().chain(&self.reference_warnings)
    }
}

/// One `InferenceWorkerSet` in scope, whether or not a pool names it.
///
/// Published for every set the store lists, so unreferenced sets get status
/// too.
#[derive(Debug, Clone)]
pub struct ResolvedWorkerSetEntry {
    pub namespace: String,
    pub name: String,
    /// The object's UID, part of the status writer's cache key.
    pub uid: Option<String>,
    /// For `observedGeneration`.
    pub generation: Option<i64>,
    /// `Ok` once the set bound to a live pool; `Err` is why it did not.
    pub bound: Result<ResolvedWorkerSet, UnresolvedReason>,
    /// Pools that route to this set, sorted. Empty doesn't mean unreachable;
    /// see `InferenceWorkerSetStatus`.
    pub referenced_by: Vec<String>,
}

/// One `InferencePool`, fully resolved.
#[derive(Debug, Clone)]
pub struct ResolvedPool {
    pub namespace: String,
    pub name: String,
    /// The object's UID, part of the status writer's cache key.
    pub uid: Option<String>,
    /// The revision this snapshot was built from, for the freshness check
    /// against `PickRequest.inference_pool.resource_version`.
    pub resource_version: String,
    pub generation: Option<i64>,
    /// Compiled policy, or the implicit serve-as-asked policy when `policyRef`
    /// is absent.
    pub policy: Arc<CompiledPolicy>,
    /// `policyRef` did not resolve, and why. The pool still serves without a
    /// policy (fail-open), with `ResolvedRefs=False`.
    pub dangling_policy: Option<DanglingPolicy>,
    /// Destinations kinference can route to, of either class. A policy cannot
    /// reach anything outside this list.
    pub destinations: Vec<ResolvedDestination>,
    pub unresolved: Vec<Unresolved>,
    /// Gateways that route to this pool, from HTTPRoutes naming it.
    pub parents: Vec<ParentRef>,
}

impl ResolvedPool {
    /// Look up a destination by the name a policy would use. Unambiguous,
    /// since collisions are rejected as [`UnresolvedReason::AmbiguousName`].
    pub fn backend(&self, name: &str) -> Option<&ResolvedDestination> {
        self.destinations.iter().find(|d| d.name() == name)
    }

    /// Local destinations only, for the callers that genuinely need a pool.
    pub fn local_destinations(&self) -> impl Iterator<Item = &ResolvedWorkerSet> {
        self.destinations.iter().filter_map(|d| d.as_local())
    }
}

/// Immutable snapshot of all resolved pools.
#[derive(Debug, Default)]
pub struct PolicyIndex {
    pools: HashMap<ObjKey, Arc<ResolvedPool>>,
    /// Every `InferencePolicy` in scope, compiled, referenced or not.
    policies: Vec<Arc<ResolvedPolicy>>,
    /// Every `InferenceWorkerSet` in scope, bound or not, referenced or not.
    worker_sets: Vec<Arc<ResolvedWorkerSetEntry>>,
    /// Every `Service` in scope. Not an input to [`resolve`]; the watcher
    /// attaches it afterwards, so pools and Service ports are read from one
    /// snapshot.
    services: ServiceIndex,
    /// False only on the initial `Default` snapshot, before every reflector has
    /// completed its first list. Distinguishes "no such pool" from "not listed
    /// yet".
    listed: bool,
}

impl PolicyIndex {
    pub fn lookup(&self, namespace: &str, name: &str) -> Option<&Arc<ResolvedPool>> {
        self.pools.get(&(namespace.to_string(), name.to_string()))
    }

    /// A listed snapshot with nothing in it, as in a cluster with no
    /// `InferencePool`s.
    pub fn listed_empty() -> Self {
        Self {
            listed: true,
            ..Default::default()
        }
    }

    /// Whether this snapshot reflects a completed list of every watched kind.
    /// A `lookup` miss is only meaningful when this is true.
    pub fn listed(&self) -> bool {
        self.listed
    }

    pub fn pools(&self) -> impl Iterator<Item = &Arc<ResolvedPool>> {
        self.pools.values()
    }

    /// Every policy in scope, including ones no pool references.
    pub fn policies(&self) -> impl Iterator<Item = &Arc<ResolvedPolicy>> {
        self.policies.iter()
    }

    /// Every worker set in scope, including ones no pool references and ones
    /// that bound to no pool.
    pub fn worker_sets(&self) -> impl Iterator<Item = &Arc<ResolvedWorkerSetEntry>> {
        self.worker_sets.iter()
    }

    /// The `Service` view. Check [`ServiceIndex::listed`] before reading a miss
    /// as an absence, since Services are outside the publish gate.
    pub fn services(&self) -> &ServiceIndex {
        &self.services
    }

    /// Attach the `Service` view. Called by the watcher once per rebuild.
    pub fn with_services(mut self, services: ServiceIndex) -> Self {
        self.services = services;
        self
    }

    pub fn len(&self) -> usize {
        self.pools.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pools.is_empty()
    }
}

/// Which pool each worker set became, keyed by namespaced identity. Built from
/// the current pool set on every resolve.
pub type PoolIndex = BTreeMap<super::PoolKey, PoolId>;

/// Why each worker set that produced no pool was refused, as recorded by the
/// router's `worker_sets::desired_pools`.
pub type RejectionIndex = BTreeMap<super::PoolKey, UnresolvedReason>;

/// Project the resolver's rejections into the reasons status reports.
pub fn rejection_index(rejections: &[super::rejection::Rejection]) -> RejectionIndex {
    use super::rejection::RejectReason;
    rejections
        .iter()
        .map(|r| {
            let reason = match r.reason {
                RejectReason::MissingProfile | RejectReason::UnknownProfile { .. } => {
                    UnresolvedReason::UnknownProfile
                }
                RejectReason::MissingKvBlockSize | RejectReason::InvalidKvBlockSize { .. } => {
                    UnresolvedReason::InvalidKvBlockSize
                }
                // From the pool's side, a set with no Service has no pool.
                RejectReason::MissingServiceRef => UnresolvedReason::NoMatchingPool,
                // The object doesn't say enough to route to, as with a provider
                // missing its `backendRef`.
                RejectReason::ModelMissingName | RejectReason::KvEventsMissingPort => {
                    UnresolvedReason::Incomplete
                }
            };
            (r.key.clone(), reason)
        })
        .collect()
}

/// Rebuild the whole index from the current contents of the CRD stores.
///
/// Takes no Services; the result carries an unlisted, empty `Service` view
/// until [`PolicyIndex::with_services`] attaches one.
#[allow(clippy::too_many_arguments)]
pub fn resolve(
    pools: &[Arc<InferencePool>],
    policies: &[Arc<InferencePolicy>],
    worker_sets: &[Arc<InferenceWorkerSet>],
    providers: &[Arc<InferenceProvider>],
    routes: &[Arc<HTTPRoute>],
    grants: &[Arc<ReferenceGrant>],
    pools_by_key: &PoolIndex,
    rejections: &RejectionIndex,
) -> PolicyIndex {
    // The caller only resolves once every store has listed.
    let mut index = PolicyIndex {
        listed: true,
        ..Default::default()
    };

    // Compile every policy up front, so shared policies compile once and
    // unreferenced ones still report status and warnings.
    let mut compiled: HashMap<ObjKey, Arc<CompiledPolicy>> = HashMap::new();
    let mut referenced_by: HashMap<ObjKey, Vec<String>> = HashMap::new();
    // Which pools route to which worker set, from the resolved destinations so
    // selectors and the implicit "every set in this namespace" count too.
    let mut ws_referenced_by: HashMap<ObjKey, Vec<String>> = HashMap::new();
    for p in policies {
        let Some(pns) = p.namespace() else { continue };
        let key = (pns, p.name_any());
        compiled.insert(
            key,
            Arc::new(CompiledPolicy::compile(&p.name_any(), &p.spec)),
        );
    }

    for pool in pools {
        let Some(ns) = pool.namespace() else { continue };
        let name = pool.name_any();

        // --- policy -----------------------------------------------------
        //
        // Same-namespace only.
        let (policy, dangling_policy) = match pool.spec.policy_ref.as_ref() {
            None => (
                // No policy: serve every request as asked.
                Arc::new(CompiledPolicy::serve_as_asked("<implicit>")),
                None,
            ),
            Some(pref) => {
                // Omitted `group`/`kind` default to the right values.
                let group = pref.group.as_deref().unwrap_or(POLICY_GROUP);
                let kind = pref.kind.as_deref().unwrap_or(POLICY_KIND);
                if group != POLICY_GROUP || kind != POLICY_KIND {
                    (
                        Arc::new(CompiledPolicy::serve_as_asked("<wrong-kind>")),
                        Some(DanglingPolicy::WrongKind {
                            name: pref.name.clone(),
                            group: group.to_string(),
                            kind: kind.to_string(),
                        }),
                    )
                } else {
                    match compiled.get(&(ns.clone(), pref.name.clone())) {
                        Some(p) => {
                            referenced_by
                                .entry((ns.clone(), pref.name.clone()))
                                .or_default()
                                .push(name.clone());
                            (p.clone(), None)
                        }
                        None => (
                            // Fail open, and report it in status.
                            Arc::new(CompiledPolicy::serve_as_asked("<missing>")),
                            Some(DanglingPolicy::NotFound {
                                name: pref.name.clone(),
                            }),
                        ),
                    }
                }
            }
        };

        // --- destinations -----------------------------------------------
        let mut destinations: Vec<ResolvedDestination> = Vec::new();
        let mut unresolved = Vec::new();
        let sets_in = |target_ns: &str| -> Vec<&Arc<InferenceWorkerSet>> {
            worker_sets
                .iter()
                .filter(|w| w.namespace().as_deref() == Some(target_ns))
                .collect()
        };
        let provs_in = |target_ns: &str| -> Vec<&Arc<InferenceProvider>> {
            providers
                .iter()
                .filter(|p| p.namespace().as_deref() == Some(target_ns))
                .collect()
        };

        if pool.spec.destinations.is_empty() {
            // Every `InferenceWorkerSet` in this namespace. Providers must always
            // be named.
            for ws in sets_in(&ns) {
                match bind(ws, pools_by_key, rejections) {
                    Ok(rws) => destinations.push(ResolvedDestination::Local(rws)),
                    Err(reason) => unresolved.push(Unresolved {
                        kind: "InferenceWorkerSet".into(),
                        name: ws.name_any(),
                        namespace: ns.clone(),
                        uid: ws.uid(),
                        reason,
                    }),
                }
            }
        } else {
            for dest in &pool.spec.destinations {
                // `kind` is required and has no default.
                let kind = dest.kind.as_deref().unwrap_or("");
                if kind != "InferenceWorkerSet" && kind != "InferenceProvider" {
                    unresolved.push(Unresolved {
                        kind: if kind.is_empty() {
                            "<unset>".into()
                        } else {
                            kind.to_string()
                        },
                        name: dest.name.clone().unwrap_or_default(),
                        namespace: dest.namespace.clone().unwrap_or_else(|| ns.clone()),
                        uid: None,
                        reason: UnresolvedReason::UnsupportedKind,
                    });
                    continue;
                }
                let target_ns = dest.namespace.as_deref().unwrap_or(ns.as_str());
                let label = dest.name.clone().unwrap_or_else(|| "<selector>".into());

                // Cross-namespace needs a grant in the target namespace, and fails
                // closed, including when the watch can't see that namespace.
                if target_ns != ns.as_str()
                    && !grant_permits(
                        grants,
                        target_ns,
                        "kinference.buoyant.io",
                        kind,
                        dest.name.as_deref(),
                        &ns,
                        "InferencePool",
                    )
                {
                    unresolved.push(Unresolved {
                        kind: kind.to_string(),
                        name: label,
                        namespace: target_ns.to_string(),
                        uid: None,
                        reason: UnresolvedReason::CrossNamespaceNotGranted,
                    });
                    continue;
                }

                let matches_selector = |labels: &BTreeMap<String, String>| {
                    dest.selector.as_ref().is_some_and(|sel| {
                        sel.match_labels
                            .iter()
                            .all(|(k, v)| labels.get(k) == Some(v))
                    })
                };

                // Exactly one of name or selector is set (admission-enforced).
                let mut any_matched = false;
                if kind == "InferenceWorkerSet" {
                    for ws in sets_in(target_ns) {
                        let hit = match &dest.name {
                            Some(n) => &ws.name_any() == n,
                            None => matches_selector(&ws.labels().clone().into_iter().collect()),
                        };
                        if !hit {
                            continue;
                        }
                        any_matched = true;
                        push_destination(
                            &mut destinations,
                            &mut unresolved,
                            bind(ws, pools_by_key, rejections).map(ResolvedDestination::Local),
                            "InferenceWorkerSet",
                            &ws.name_any(),
                            target_ns,
                            ws.uid(),
                        );
                    }
                } else {
                    for prov in provs_in(target_ns) {
                        let hit = match &dest.name {
                            Some(n) => &prov.name_any() == n,
                            None => matches_selector(&prov.labels().clone().into_iter().collect()),
                        };
                        if !hit {
                            continue;
                        }
                        any_matched = true;
                        push_destination(
                            &mut destinations,
                            &mut unresolved,
                            resolve_provider(prov, grants).map(ResolvedDestination::Provider),
                            "InferenceProvider",
                            &prov.name_any(),
                            target_ns,
                            prov.uid(),
                        );
                    }
                }
                if !any_matched {
                    unresolved.push(Unresolved {
                        kind: kind.to_string(),
                        name: label,
                        namespace: target_ns.to_string(),
                        uid: None,
                        reason: UnresolvedReason::NotFound,
                    });
                }
            }
        }

        // --- parents (from HTTPRoutes naming this pool) -----------------
        let mut parents: Vec<ParentRef> = Vec::new();
        for route in routes {
            let Some(route_ns) = route.namespace() else {
                continue;
            };
            let targets_us = route.spec.rules.iter().any(|r| {
                r.backend_refs.iter().any(|b| {
                    b.is_inference_pool()
                        && b.name == name
                        // backendRef namespace defaults to the route's own.
                        && b.namespace.as_deref().unwrap_or(route_ns.as_str()) == ns.as_str()
                })
            });
            if !targets_us {
                continue;
            }
            for p in &route.spec.parent_refs {
                let already = parents.iter().any(|q| {
                    q.name == p.name
                        && q.namespace.as_deref().unwrap_or(route_ns.as_str())
                            == p.namespace.as_deref().unwrap_or(route_ns.as_str())
                });
                if !already {
                    let mut p = p.clone();
                    // Default the parent's namespace to the route's, so status
                    // identifies the Gateway unambiguously.
                    if p.namespace.is_none() {
                        p.namespace = Some(route_ns.clone());
                    }
                    parents.push(p);
                }
            }
        }

        for rws in destinations.iter().filter_map(|d| d.as_local()) {
            ws_referenced_by
                .entry((rws.namespace.clone(), rws.name.clone()))
                .or_default()
                .push(name.clone());
        }

        let resolved = ResolvedPool {
            namespace: ns.clone(),
            name: name.clone(),
            uid: pool.uid(),
            resource_version: pool.resource_version().unwrap_or_default(),
            generation: pool.meta().generation,
            policy,
            dangling_policy,
            destinations,
            unresolved,
            parents,
        };
        index.pools.insert((ns, name), Arc::new(resolved));
    }

    // Publish every policy, referenced or not, sorted so the snapshot is stable
    // for the status writer's last-written comparison.
    let resolved_policies: Vec<Arc<ResolvedPolicy>> = policies
        .iter()
        .filter_map(|p| {
            let namespace = p.namespace()?;
            let name = p.name_any();
            let key = (namespace.clone(), name.clone());
            let mut referenced_by = referenced_by.remove(&key).unwrap_or_default();
            referenced_by.sort();
            referenced_by.dedup();
            // Names are checked against the pools that use this policy.
            let attached: Vec<&ResolvedPool> = referenced_by
                .iter()
                .filter_map(|pool| index.pools.get(&(namespace.clone(), pool.clone())))
                .map(Arc::as_ref)
                .collect();
            let reference_warnings = crate::lint::reference_warnings(&p.spec, &attached).warnings;
            Some(Arc::new(ResolvedPolicy {
                namespace,
                name,
                uid: p.uid(),
                generation: p.meta().generation,
                policy: compiled.get(&key)?.clone(),
                spec: Arc::new(p.spec.clone()),
                referenced_by,
                reference_warnings,
            }))
        })
        .collect();
    index.policies = resolved_policies;
    index
        .policies
        .sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));

    // Publish every worker set, bound or not, referenced or not, sorted like
    // the policies.
    index.worker_sets = worker_sets
        .iter()
        .filter_map(|w| {
            let namespace = w.namespace()?;
            let name = w.name_any();
            let key = (namespace.clone(), name.clone());
            let mut referenced_by = ws_referenced_by.remove(&key).unwrap_or_default();
            referenced_by.sort();
            referenced_by.dedup();
            Some(Arc::new(ResolvedWorkerSetEntry {
                namespace,
                name,
                uid: w.uid(),
                generation: w.meta().generation,
                bound: bind(w, pools_by_key, rejections),
                referenced_by,
            }))
        })
        .collect();
    index
        .worker_sets
        .sort_by(|a, b| (&a.namespace, &a.name).cmp(&(&b.namespace, &b.name)));
    index
}

/// Find the pool a worker set became.
///
/// A miss reports the reason recorded when the set was rejected, since this
/// module can't see the router's profiles. With none recorded, it is
/// `NoMatchingPool`.
fn bind(
    ws: &Arc<InferenceWorkerSet>,
    pools_by_key: &PoolIndex,
    rejections: &RejectionIndex,
) -> Result<ResolvedWorkerSet, UnresolvedReason> {
    let namespace = ws.namespace().unwrap_or_default();
    let key = super::PoolKey::namespaced(&namespace, ws.name_any());
    let pool_id = *pools_by_key.get(&key).ok_or_else(|| {
        rejections
            .get(&key)
            .copied()
            .unwrap_or(UnresolvedReason::NoMatchingPool)
    })?;
    Ok(ResolvedWorkerSet {
        name: ws.name_any(),
        namespace,
        labels: ws.labels().clone().into_iter().collect(),
        pool_id,
        service_name: ws.spec.service_ref.name.clone(),
        service_port: ws.spec.service_ref.port.clone(),
        kv_events_declared: ws.spec.kv_events.is_some(),
        aliases: ws.spec.models.clone(),
    })
}

/// Add a resolved destination, or record why it did not resolve.
///
/// Also handles duplicates: the same object reached twice is dropped, and
/// different kinds under one name are rejected.
fn push_destination(
    destinations: &mut Vec<ResolvedDestination>,
    unresolved: &mut Vec<Unresolved>,
    resolved: Result<ResolvedDestination, UnresolvedReason>,
    kind: &str,
    name: &str,
    namespace: &str,
    uid: Option<String>,
) {
    let reason = match resolved {
        Ok(d) => {
            match destinations.iter().find(|e| e.name() == d.name()) {
                // Same object reached twice, e.g. by name and by a selector.
                Some(e) if e.kind() == d.kind() => return,
                // Different kinds under one name.
                Some(_) => UnresolvedReason::AmbiguousName,
                None => {
                    destinations.push(d);
                    return;
                }
            }
        }
        Err(reason) => reason,
    };
    unresolved.push(Unresolved {
        kind: kind.to_string(),
        name: name.to_string(),
        namespace: namespace.to_string(),
        uid,
        reason,
    });
}

/// Resolve an `InferenceProvider` to the shallow form kinference routes on.
///
/// Enforces the fields the schema requires: a provider missing its
/// `backendRef`, its `api` or a usable model row is `Incomplete`.
///
/// Whether the `GatewayBackend` exists or the credential works is not checked;
/// that arrives from the caller as `Abort(BACKEND_*)`.
fn resolve_provider(
    prov: &Arc<InferenceProvider>,
    grants: &[Arc<ReferenceGrant>],
) -> Result<ResolvedProvider, UnresolvedReason> {
    let namespace = prov.namespace().unwrap_or_default();
    let backend = prov
        .spec
        .backend_ref
        .as_ref()
        .ok_or(UnresolvedReason::Incomplete)?;
    let backend_name = backend.name.clone().ok_or(UnresolvedReason::Incomplete)?;
    let api = prov.spec.api.ok_or(UnresolvedReason::Incomplete)?;
    // Nothing translates other formats yet.
    if api != ApiFormat::OpenAI {
        return Err(UnresolvedReason::UnsupportedApiFormat);
    }

    // The group/kind the ref points at, defaulted as the CRD documents.
    let group = backend.group.as_deref().unwrap_or(GATEWAY_BACKEND_GROUP);
    let kind = backend.kind.as_deref().unwrap_or(GATEWAY_BACKEND_KIND);

    // Resolved here so the pick response is always fully qualified.
    let backend_ns = backend
        .namespace
        .clone()
        .unwrap_or_else(|| namespace.clone());

    // The credential boundary. The grant lives in the target namespace, so this
    // needs no access to the backend itself.
    if backend_ns != namespace
        && !grant_permits(
            grants,
            &backend_ns,
            group,
            kind,
            Some(backend_name.as_str()),
            &namespace,
            "InferenceProvider",
        )
    {
        return Err(UnresolvedReason::CrossNamespaceNotGranted);
    }

    // At least one row naming the vendor's model.
    let models: Vec<ProviderModel> = prov
        .spec
        .models
        .iter()
        .filter(|m| m.name.is_some())
        .cloned()
        .collect();
    if models.is_empty() {
        return Err(UnresolvedReason::Incomplete);
    }

    Ok(ResolvedProvider {
        name: prov.name_any(),
        namespace,
        uid: prov.uid(),
        labels: prov.labels().clone().into_iter().collect(),
        backend_ref: ObjectKey::new(backend_ns, backend_name),
        // An empty path means no rewrite. Anything else passes through; the
        // schema's `^/` is the only check.
        path: prov
            .spec
            .path
            .as_deref()
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string),
        models,
    })
}

pub const POLICY_GROUP: &str = "kinference.buoyant.io";
pub const POLICY_KIND: &str = "InferencePolicy";
pub const GATEWAY_BACKEND_GROUP: &str = "gateway.linkerd.io";
pub const GATEWAY_BACKEND_KIND: &str = "GatewayBackend";

/// Whether a `ReferenceGrant` in `target_ns` permits `from_kind` objects in
/// `from_ns` to reference `(group, kind, name)` there.
///
/// `from_kind` is the kind that holds the reference, as the Gateway API defines
/// `from`: `InferencePool` for a pool's destination, `InferenceProvider` for a
/// provider's `backendRef`. A grant for one does not cover the other.
///
/// Fails closed: a grant kinference can't see counts as absent.
fn grant_permits(
    grants: &[Arc<ReferenceGrant>],
    target_ns: &str,
    group: &str,
    kind: &str,
    name: Option<&str>,
    from_ns: &str,
    from_kind: &str,
) -> bool {
    grants
        .iter()
        .filter(|g| g.namespace().as_deref() == Some(target_ns))
        .any(|g| {
            let from_ok = g.spec.from.iter().any(|f| {
                f.namespace.as_deref() == Some(from_ns)
                    && f.group.as_deref().unwrap_or("kinference.buoyant.io")
                        == "kinference.buoyant.io"
                    && f.kind.as_deref() == Some(from_kind)
            });
            // Absent `to[].name` means every object of that kind, per the
            // Gateway API.
            let to_ok = g.spec.to.iter().any(|t| {
                t.group.as_deref().unwrap_or("") == group
                    && t.kind.as_deref() == Some(kind)
                    && match (&t.name, name) {
                        (None, _) => true,
                        (Some(n), Some(want)) => n == want,
                        (Some(_), None) => false,
                    }
            });
            from_ok && to_ok
        })
}

#[cfg(test)]
mod tests;
