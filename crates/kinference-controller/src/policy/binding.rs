//! The controller side: publishing a `BackendBinding` per `InferencePool`.
//!
//! A gateway has no schema for `InferencePool`, so it can't resolve one as a
//! backendRef. A `BackendBinding` (`gateway.linkerd.io/v1alpha1`) claims a
//! foreign `(group, kind, name)` and states what it resolves to in native
//! kinds. kinference already resolves every pool down to Services and
//! `GatewayBackend`s, so it writes that translation.
//!
//! One binding per pool, named after it and in its namespace: the gateway
//! matches a claim against route refs in the binding's namespace.
//!
//! Each destination becomes:
//!
//! - an `InferenceWorkerSet`: its `serviceRef` as a core `Service`, on the port
//!   that ref resolves to.
//! - an `InferenceProvider`: its `backendRef` as a `GatewayBackend`, with no
//!   port, since the port is in the backend's `baseURL` and the schema rejects
//!   a ref that also gives one.
//!
//! The Service port number is read off the Service itself
//! ([`kinference_policy::services::ServiceIndex`]), since `serviceRef.port` may be a name
//! or absent. A ref that doesn't resolve to a number is left out of the binding
//! with a warning: a portless Service ref resolves to no endpoints, and
//! guessing could send traffic to the metrics port. Likewise nothing is
//! published until the Service watch has listed.
//!
//! The binding also lists an `Extension`
//! (`policy.backend_binding_extension`), which the gateway prepends to the
//! filter chain of every rule routing to the pool; it asks kinference which
//! endpoint each request goes to. It may live in another namespace
//! (`policy.backend_binding_extension_namespace`), which the gateway honors
//! only if the `Extension` admits the pool's namespace.
//!
//! Destinations come from the resolved snapshot rather than
//! `spec.destinations`, so a binding lists exactly what kinference would pick
//! among. A pool whose destinations all fail keeps an empty binding, so the
//! gateway reports `Accepted=False` on it.
//!
//! A binding this writer did not create is never touched. Writes are compared
//! against the live object listed this tick, so out-of-band edits are
//! corrected.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kinference_policy::crd::{
    BackendBinding, BackendBindingSpec, BindingBackendRef, BindingClaim, BindingObjectReference,
    ExtensionReference, InferencePool,
};
use kinference_policy::resolve::{
    GATEWAY_BACKEND_GROUP, GATEWAY_BACKEND_KIND, POLICY_GROUP, ResolvedDestination, ResolvedPool,
};
use kinference_policy::services::{ServiceIndex, UnresolvedPort};
use kube::api::ObjectMeta;
use kube::{Resource, ResourceExt};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::Error;
use crate::api::ApiServer;
use crate::sync;
use kinference_policy::store::PolicyStore;

/// Server-side-apply field manager. A field this writer stops setting is
/// removed on the next apply.
const FIELD_MANAGER: &str = "kinference";

/// Marks a binding as this writer's; others are never updated or pruned.
const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const MANAGED_BY_VALUE: &str = "kinference";

/// The pool a binding was derived from. Duplicates `spec.claim.name` so
/// bindings can be selected by label.
const POOL_LABEL: &str = "kinference.buoyant.io/inference-pool";

/// The kind bindings written here claim, in group [`POLICY_GROUP`].
const POOL_KIND: &str = "InferencePool";

/// A pool's desired binding, plus the destinations left out of it.
pub struct Desired {
    pub binding: BackendBinding,
    /// `(destination name, why)` for every worker set left out because its
    /// Service port did not resolve.
    pub omitted: Vec<(String, UnresolvedPort)>,
}

/// The `BackendBinding` a pool should have, metadata included.
///
/// `extension` is the picker `Extension`, `None` when the operator cleared it.
/// `services` supplies the Service port numbers; if it hasn't listed, every
/// worker set is omitted as [`UnresolvedPort::NotListed`].
pub fn desired_binding(
    pool: &ResolvedPool,
    extension: Option<&ExtensionReference>,
    services: &ServiceIndex,
) -> Desired {
    let mut backend_refs = Vec::with_capacity(pool.destinations.len());
    let mut omitted = Vec::new();

    for dest in &pool.destinations {
        match dest {
            // Unset group and kind mean a core Service. It is looked up in the
            // worker set's namespace, not the pool's, since a cross-namespace
            // destination names a Service beside itself.
            ResolvedDestination::Local(ws) => {
                let port = match services.port_number(
                    &ws.namespace,
                    &ws.service_name,
                    ws.service_port.as_ref(),
                ) {
                    Ok(port) => port,
                    Err(why) => {
                        omitted.push((ws.name.clone(), why));
                        continue;
                    }
                };
                backend_refs.push(BindingBackendRef {
                    name: ws.name.clone(),
                    target: BindingObjectReference {
                        group: None,
                        kind: None,
                        name: ws.service_name.clone(),
                        namespace: qualify(&pool.namespace, &ws.namespace),
                        port: Some(port),
                    },
                });
            }
            // kinference never reads the `GatewayBackend`, which holds the
            // vendor credential; the gateway resolves the reference.
            ResolvedDestination::Provider(prov) => backend_refs.push(BindingBackendRef {
                name: prov.name.clone(),
                target: BindingObjectReference {
                    group: Some(GATEWAY_BACKEND_GROUP.to_string()),
                    kind: Some(GATEWAY_BACKEND_KIND.to_string()),
                    name: prov.backend_ref.name.clone(),
                    namespace: qualify(&pool.namespace, &prov.backend_ref.namespace),
                    port: None,
                },
            }),
        }
    }

    let binding = BackendBinding {
        metadata: ObjectMeta {
            name: Some(binding_name(pool)),
            namespace: Some(pool.namespace.clone()),
            labels: Some(BTreeMap::from([
                (MANAGED_BY_LABEL.to_string(), MANAGED_BY_VALUE.to_string()),
                (POOL_LABEL.to_string(), pool.name.clone()),
            ])),
            // Garbage collection deletes the binding with its pool. The
            // reconcile's pruning covers pools whose UID is unknown.
            owner_references: pool.uid.as_ref().map(|uid| {
                vec![OwnerReference {
                    api_version: InferencePool::api_version(&()).to_string(),
                    kind: InferencePool::kind(&()).to_string(),
                    name: pool.name.clone(),
                    uid: uid.clone(),
                    controller: Some(true),
                    // Would need `pools/finalizers` RBAC, and a binding that
                    // outlives its pool for one GC cycle is harmless.
                    block_owner_deletion: None,
                }]
            }),
            ..Default::default()
        },
        spec: BackendBindingSpec {
            claim: BindingClaim {
                group: POLICY_GROUP.to_string(),
                kind: POOL_KIND.to_string(),
                name: pool.name.clone(),
            },
            backend_refs,
            // Empty when the operator cleared the name; since this writer owns
            // the field, that removes the extension from existing bindings.
            extensions: extension
                .map(|e| {
                    vec![ExtensionReference {
                        name: e.name.clone(),
                        namespace: e
                            .namespace
                            .as_deref()
                            .and_then(|ns| qualify(&pool.namespace, ns)),
                    }]
                })
                .unwrap_or_default(),
        },
        status: None,
    };

    Desired { binding, omitted }
}

/// The binding's name: the pool's own. It may collide with an operator's own
/// binding, which is why every write checks ownership first.
pub fn binding_name(pool: &ResolvedPool) -> String {
    pool.name.clone()
}

/// A target's namespace, omitted when it is the binding's own, so equivalent
/// bindings compare equal.
fn qualify(binding_ns: &str, target_ns: &str) -> Option<String> {
    (binding_ns != target_ns).then(|| target_ns.to_string())
}

/// Whether this writer created the binding. Only these are updated or pruned.
fn is_ours(binding: &BackendBinding) -> bool {
    binding.labels().get(MANAGED_BY_LABEL).map(String::as_str) == Some(MANAGED_BY_VALUE)
}

/// Whether the live object already says what it should.
///
/// Compares only what this writer sets: the spec, the two labels and the owner
/// reference.
fn in_sync(current: &BackendBinding, desired: &BackendBinding) -> bool {
    let spec_matches =
        serde_json::to_value(&current.spec).ok() == serde_json::to_value(&desired.spec).ok();
    let labels_match = desired
        .labels()
        .iter()
        .all(|(k, v)| current.labels().get(k) == Some(v));
    let owners_match = match desired.metadata.owner_references.as_ref() {
        // By UID: a binding owned by a since-recreated pool would be GC'd.
        Some(want) => want.iter().all(|w| {
            current
                .metadata
                .owner_references
                .iter()
                .flatten()
                .any(|c| c.uid == w.uid)
        }),
        None => true,
    };
    spec_matches && labels_match && owners_match
}

/// Creates, updates and prunes the `BackendBinding`s, on a tick.
pub struct BindingWriter<A: ApiServer> {
    api: A,
    /// Must match `PolicyWatcher`'s scope, or pruning would judge bindings
    /// against a snapshot that never contained their pools. `None` is every
    /// namespace.
    namespace: Option<String>,
    store: Arc<PolicyStore>,
    /// See `PolicyConfig::binding_extension`.
    extension: Option<ExtensionReference>,
    /// Whether this replica may write.
    leader: watch::Receiver<bool>,
    /// Warnings logged at the last reconcile, so a standing one is logged once.
    /// Replaced each reconcile, so a problem that clears and recurs is logged
    /// again.
    warned: Mutex<HashSet<String>>,
}

impl<A: ApiServer> BindingWriter<A> {
    pub fn new(
        api: A,
        namespace: Option<String>,
        store: Arc<PolicyStore>,
        extension: Option<ExtensionReference>,
        leader: watch::Receiver<bool>,
    ) -> Self {
        Self {
            api,
            namespace,
            store,
            extension,
            leader,
            warned: Mutex::new(HashSet::new()),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
        // Whether the current run of failed reconciles has been logged.
        let mut failing = false;
        // Cleared once the elector is gone. A closed channel's `changed()`
        // returns at once on every poll, so polling it after that would spin.
        let mut leader_open = true;
        loop {
            tokio::select! {
                // The elector closes the leader channel only after cancel
                // fires, so checking cancel first keeps a clean shutdown
                // from reading as a lost elector.
                biased;
                _ = cancel.cancelled() => return,
                r = self.leader.changed(), if leader_open => {
                    match r {
                        Ok(()) => {
                            if *self.leader.borrow_and_update() {
                                tracing::debug!(
                                    "promoted to leader; writing bindings from the next tick"
                                );
                            } else {
                                tracing::debug!(
                                    "demoted from leader; stopping writes"
                                );
                            }
                        }
                        // Elector gone. Nothing renews the lease now, so a
                        // rival can claim it: stop writing.
                        Err(_) => {
                            leader_open = false;
                            tracing::error!("leader channel closed; stopping writes");
                        }
                    }
                }
                _ = interval.tick() => {
                    if !self.leading() {
                        continue;
                    }
                    match self.reconcile_once().await {
                        Ok(()) => failing = false,
                        // Never fatal; already-programmed routes keep working.
                        Err(e) => {
                            if !std::mem::replace(&mut failing, true) {
                                tracing::warn!(error = &e as &dyn StdError, "backend binding reconcile failed");
                            }
                        }
                    }
                }
            }
        }
    }

    /// Whether this replica may write. A closed leader channel counts as
    /// lost leadership.
    fn leading(&self) -> bool {
        self.leader.has_changed().is_ok() && *self.leader.borrow()
    }

    async fn reconcile_once(&self) -> Result<(), Error> {
        let snapshot = self.store.snapshot();
        // Before the first complete list every pool looks deleted, and pruning
        // on that view would delete every binding in the cluster.
        if !snapshot.listed() {
            return Ok(());
        }

        // The Service watch is outside the publish gate, so the snapshot can be
        // listed while the Service index is not. Hold rather than drop every
        // worker set from the bindings. A missing `services` RBAC grant also
        // ends up here, hence the warning.
        if !snapshot.services().listed() {
            self.warn_new(HashSet::from([
                "waiting for the Service watch to list; if this persists, check the \
                 services get/list/watch grant"
                    .to_string(),
            ]));
            return Ok(());
        }

        let mut port_warnings: HashSet<String> = HashSet::new();
        let desired: HashMap<(String, String), BackendBinding> = snapshot
            .pools()
            .map(|pool| {
                let Desired { binding, omitted } =
                    desired_binding(pool, self.extension.as_ref(), snapshot.services());
                for (dest, why) in omitted {
                    port_warnings.insert(format!(
                        "{}/{}: left out destination {dest}, whose Service port did not \
                         resolve ({}: {why})",
                        pool.namespace,
                        pool.name,
                        why.as_str(),
                    ));
                }
                ((pool.namespace.clone(), binding_name(pool)), binding)
            })
            .collect();

        let existing = match self
            .api
            .list::<BackendBinding>(self.namespace.as_deref())
            .await
        {
            Ok(list) => list,
            Err(kube::Error::Api(e)) if e.code == 404 => {
                // The CRD is not installed. Expected without this gateway, but
                // worth a warning.
                self.warn_new(HashSet::from([
                    "BackendBinding CRD not installed; install the gateway.linkerd.io \
                     CRDs or set policy.backend_bindings = false"
                        .to_string(),
                ]));
                return Ok(());
            }
            Err(source) => {
                return Err(Error::Api {
                    op: "list BackendBinding",
                    source,
                });
            }
        };

        let mut by_key: HashMap<(String, String), &BackendBinding> = HashMap::new();
        for b in &existing {
            let Some(ns) = b.namespace() else { continue };
            by_key.insert((ns, b.name_any()), b);
        }

        let mut warnings: HashSet<String> = port_warnings;
        let mut applied = 0usize;

        for (key, want) in &desired {
            if let Some(current) = by_key.get(key) {
                if !is_ours(current) {
                    // Someone else's object under the name we want; never
                    // adopt it.
                    warnings.insert(format!(
                        "{}/{} exists and is not managed by kinference; leaving it alone",
                        key.0, key.1
                    ));
                    continue;
                }
                if let Some(reason) = rejected_reason(current) {
                    warnings.insert(format!(
                        "the gateway rejected {}/{}: {reason}",
                        key.0, key.1
                    ));
                }
                if in_sync(current, want) {
                    continue;
                }
            }
            // A reconcile spans many writes, and leadership can end midway.
            if !self.leading() {
                return Ok(());
            }
            // Server-side apply covers both create and update. `apply` forces,
            // taking back fields another manager grabbed; otherwise the apply
            // would conflict on every tick and never converge.
            match self.api.apply(&key.0, &key.1, FIELD_MANAGER, want).await {
                Ok(_) => {
                    applied += 1;
                    tracing::debug!(namespace = %key.0, name = %key.1, "backend binding applied");
                }
                // A standing warning rather than an error, so one namespace's
                // failure doesn't block the rest and logs once.
                Err(e) => {
                    warnings.insert(format!("failed to apply {}/{}: {e}", key.0, key.1));
                }
            }
        }

        // Prune what owner-reference GC doesn't cover: pools that left the
        // watch scope, or whose binding was created before their UID was known.
        for binding in &existing {
            let Some(ns) = binding.namespace() else {
                continue;
            };
            let name = binding.name_any();
            if !is_ours(binding) || desired.contains_key(&(ns.clone(), name.clone())) {
                continue;
            }
            if binding.meta().deletion_timestamp.is_some() {
                continue;
            }
            if !self.leading() {
                return Ok(());
            }
            match self.api.delete::<BackendBinding>(&ns, &name).await {
                Ok(_) => tracing::info!(
                    namespace = %ns,
                    name = %name,
                    "deleted the backend binding of a removed InferencePool"
                ),
                Err(e) => {
                    warnings.insert(format!("failed to delete {ns}/{name}: {e}"));
                }
            }
        }

        if applied > 0 {
            tracing::debug!(applied, "backend bindings reconciled");
        }
        self.warn_new(warnings);
        Ok(())
    }

    /// Log the conditions that were not standing at the last reconcile, and
    /// remember the current set as the baseline for the next one.
    fn warn_new(&self, warnings: HashSet<String>) {
        let mut warned = sync::lock(&self.warned);
        for w in &warnings {
            if !warned.contains(w) {
                tracing::warn!(warning = %w, "backend binding warning");
            }
        }
        *warned = warnings;
    }
}

/// The gateway's reason for refusing a binding, if it did.
fn rejected_reason(binding: &BackendBinding) -> Option<String> {
    let conditions = &binding.status.as_ref()?.conditions;
    let accepted = conditions.iter().find(|c| c.r#type == "Accepted")?;
    (accepted.status == "False").then(|| format!("{}: {}", accepted.reason, accepted.message))
}

#[cfg(test)]
mod tests;
