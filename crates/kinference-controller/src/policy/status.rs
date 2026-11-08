//! Publishing `status` on the kinference.buoyant.io resources.
//!
//! A conforming gateway does not program a route until the referenced pool
//! reports `Accepted=True`, so nothing routes until this runs.
//!
//! - We watch the objects we patch, so every write wakes our own watch. Status
//!   is patched only when it differs from what we last wrote; that is what
//!   keeps the loop from writing forever.
//! - `lastTransitionTime` is reused while `(type, status, reason)` is unchanged,
//!   so a rewrite doesn't look like a transition.
//!
//! Status computation is pure over `(snapshot, PoolFacts)`; only the patching
//! is async.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};

use kinference_policy::PoolId;
use kinference_policy::crd::{
    Condition, ContextWindow, EndpointCounts, InferencePolicy, InferencePolicyStatus,
    InferencePool, InferencePoolStatus, InferenceProvider, InferenceProviderStatus,
    InferenceWorkerSet, InferenceWorkerSetStatus, KvEventsStatus, ParentStatus, PoolModelStatus,
    UnresolvedDestination, WorkerSetModelStatus,
};
use kinference_policy::provider_health::{FailureKind, ProviderHealth};
use kinference_policy::resolve::{
    DanglingPolicy, ResolvedPolicy, ResolvedPool, ResolvedWorkerSet, UnresolvedReason,
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::api::{ApiServer, Object};
use crate::sync;
use kinference_fleet::pool::Pool;
use kinference_fleet::scrape::MetricsRegistry;
use kinference_policy::store::PolicyStore;

/// The controller identity written into `status.parents[].controllerName`.
pub const CONTROLLER_NAME: &str = "kinference.buoyant.io/router";

/// What one worker set's pool actually has right now, from the live registries.
#[derive(Debug, Clone, Default)]
pub struct PoolFacts {
    /// Endpoints that passed the readiness gate (models resolved and initial KV
    /// sync done), as `/readyz` defines it.
    pub ready_endpoints: i64,
    /// Endpoints kinference has decoded a KV cache event from. Independent of
    /// readiness, since a ZMQ `connect()` succeeds even when nothing publishes.
    pub event_endpoints: i64,
    /// Discovered models, keyed by the upstream name the engine reports.
    pub models: Vec<ModelFact>,
}

#[derive(Debug, Clone)]
pub struct ModelFact {
    pub upstream: String,
    pub ready_endpoints: i64,
    /// Context window range across ready endpoints, which may disagree
    /// mid-rollout.
    pub ctx_min: Option<i64>,
    pub ctx_max: Option<i64>,
}

/// Gather live facts for every pool.
///
/// Takes a slice rather than the registry so that all facts in one status
/// write come from the same pool snapshot.
pub fn gather_facts(pools: &[Arc<Pool>], metrics: &MetricsRegistry) -> HashMap<PoolId, PoolFacts> {
    let mut out = HashMap::new();
    for pool in pools {
        let mut ready_endpoints = 0i64;
        let mut event_endpoints = 0i64;
        // upstream model -> (count, min, max)
        let mut per_model: BTreeMap<String, (i64, Option<i64>, Option<i64>)> = BTreeMap::new();
        for (num, entry) in pool.registry.iter_snapshot() {
            // Counted before the readiness gate, so endpoints awaiting their
            // first `/v1/models` scrape don't look like dead publishers.
            if entry.kv_events_observed {
                event_endpoints += 1;
            }
            if !entry.is_ready() {
                continue;
            }
            ready_endpoints += 1;
            let info = metrics.models(num);
            for m in &entry.current_models {
                let window = info
                    .models
                    .iter()
                    .find(|e| &e.name == m)
                    .and_then(|e| e.max_model_len)
                    .map(|v| v as i64);
                let slot = per_model.entry(m.clone()).or_insert((0, None, None));
                slot.0 += 1;
                if let Some(w) = window {
                    slot.1 = Some(slot.1.map_or(w, |cur: i64| cur.min(w)));
                    slot.2 = Some(slot.2.map_or(w, |cur: i64| cur.max(w)));
                }
            }
        }
        out.insert(
            pool.id,
            PoolFacts {
                ready_endpoints,
                event_endpoints,
                models: per_model
                    .into_iter()
                    .map(|(upstream, (n, lo, hi))| ModelFact {
                        upstream,
                        ready_endpoints: n,
                        ctx_min: lo,
                        ctx_max: hi,
                    })
                    .collect(),
            },
        );
    }
    out
}

fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Build a condition, reusing `previous`'s timestamp when nothing transitioned.
fn condition(
    r#type: &str,
    ok: bool,
    reason: &str,
    message: &str,
    generation: Option<i64>,
    previous: Option<&Condition>,
) -> Condition {
    let status = if ok { "True" } else { "False" };
    let last_transition_time = previous
        .filter(|p| p.r#type == r#type && p.status == status && p.reason == reason)
        .map(|p| p.last_transition_time.clone())
        .unwrap_or_else(now_rfc3339);
    Condition {
        r#type: r#type.to_string(),
        status: status.to_string(),
        observed_generation: generation,
        last_transition_time,
        reason: reason.to_string(),
        message: message.to_string(),
    }
}

/// Join the STATE tokens for a printer column.
///
/// One string because a `[*]` printer column renders only the first array
/// element. `primary` is always present, so a blank column means no status was
/// written.
fn state(primary: &str, qualifiers: &[(bool, &str)]) -> Option<String> {
    let mut out = String::from(primary);
    for (present, tok) in qualifiers {
        if *present {
            out.push(',');
            out.push_str(tok);
        }
    }
    Some(out)
}

fn find<'a>(conds: Option<&'a [Condition]>, ty: &str) -> Option<&'a Condition> {
    conds?.iter().find(|c| c.r#type == ty)
}

/// Desired `InferencePool` status.
pub fn desired_pool_status(
    resolved: &ResolvedPool,
    facts: &HashMap<PoolId, PoolFacts>,
    previous: Option<&InferencePoolStatus>,
) -> InferencePoolStatus {
    // What this pool can reach, across destinations: (destinations serving the
    // model, min context window, max context window). The windows stay `None`
    // when no destination declares one.
    type ModelAgg = (Vec<String>, Option<i64>, Option<i64>);
    let mut by_model: BTreeMap<String, ModelAgg> = BTreeMap::new();
    for dest in &resolved.destinations {
        // A provider reports what it declares, since there is nothing to
        // scrape. This is where a model-name mismatch between a provider and a
        // worker set becomes visible.
        if let Some(prov) = dest.as_provider() {
            for m in &prov.models {
                let (Some(name), Some(window)) = (m.name.clone(), m.context_window) else {
                    let Some(name) = m.name.clone() else { continue };
                    let slot = by_model
                        .entry(name)
                        .or_insert_with(|| (Vec::new(), None, None));
                    if !slot.0.contains(&prov.name) {
                        slot.0.push(prov.name.clone());
                    }
                    continue;
                };
                let slot = by_model
                    .entry(name)
                    .or_insert_with(|| (Vec::new(), None, None));
                if !slot.0.contains(&prov.name) {
                    slot.0.push(prov.name.clone());
                }
                let w = window as i64;
                slot.1 = Some(slot.1.map_or(w, |c: i64| c.min(w)));
                slot.2 = Some(slot.2.map_or(w, |c: i64| c.max(w)));
            }
            continue;
        }
        let Some(dest) = dest.as_local() else {
            continue;
        };
        let Some(f) = facts.get(&dest.pool_id) else {
            continue;
        };
        for m in &f.models {
            // A pool reports what a client can ask for, so it keys on
            // client-facing names: one entry per alias, or the upstream name
            // when there are none.
            let names = {
                let a = dest.aliases_for(&m.upstream);
                if a.is_empty() {
                    vec![m.upstream.clone()]
                } else {
                    a
                }
            };
            for name in names {
                let slot = by_model
                    .entry(name)
                    .or_insert_with(|| (Vec::new(), None, None));
                if !slot.0.contains(&dest.name) {
                    slot.0.push(dest.name.clone());
                }
                if let Some(lo) = m.ctx_min {
                    slot.1 = Some(slot.1.map_or(lo, |c: i64| c.min(lo)));
                }
                if let Some(hi) = m.ctx_max {
                    slot.2 = Some(slot.2.map_or(hi, |c: i64| c.max(hi)));
                }
            }
        }
    }

    let models: Vec<PoolModelStatus> = by_model
        .into_iter()
        .map(|(name, (served_by, lo, hi))| PoolModelStatus {
            name,
            served_by,
            context_window: (lo.is_some() || hi.is_some())
                .then_some(ContextWindow { min: lo, max: hi }),
        })
        .collect();

    // Either a destination didn't resolve, or the policyRef dangles.
    let refs_ok = resolved.unresolved.is_empty() && resolved.dangling_policy.is_none();
    let refs_reason = if !resolved.unresolved.is_empty() {
        "UnresolvedDestinations"
    } else {
        match &resolved.dangling_policy {
            Some(DanglingPolicy::NotFound { .. }) => "PolicyNotFound",
            // The policy may exist; the ref is what needs fixing.
            Some(DanglingPolicy::WrongKind { .. }) => "PolicyRefWrongKind",
            None => "ResolvedRefs",
        }
    };
    let refs_msg = match (&resolved.dangling_policy, resolved.unresolved.len()) {
        (Some(DanglingPolicy::NotFound { name }), _) => {
            format!("policyRef `{name}` does not exist; serving without policy")
        }
        (Some(DanglingPolicy::WrongKind { name, group, kind }), _) => format!(
            "policyRef `{name}` names {group}/{kind}, not {}/{}; serving without policy",
            kinference_policy::resolve::POLICY_GROUP,
            kinference_policy::resolve::POLICY_KIND
        ),
        (None, 0) => "all destinations and the policyRef resolved".to_string(),
        (None, n) => format!("{n} destination(s) did not resolve"),
    };

    // Written both top-level and per parent.
    let accepted_ok = !resolved.destinations.is_empty();
    let accepted_reason = if accepted_ok {
        "Accepted"
    } else {
        "NoDestinations"
    };
    let accepted_msg = format!(
        "{} destination(s) resolved to a routable pool",
        resolved.destinations.len()
    );

    let parents: Vec<ParentStatus> = resolved
        .parents
        .iter()
        .map(|p| {
            let prev = previous.and_then(|s| {
                s.parents
                    .iter()
                    .find(|q| q.parent_ref.name == p.name)
                    .map(|q| q.conditions.as_slice())
            });
            ParentStatus {
                parent_ref: p.clone(),
                controller_name: Some(CONTROLLER_NAME.to_string()),
                conditions: vec![
                    // The handshake the gateway waits on. Unresolved
                    // destinations don't retract it, since a pool with one good
                    // destination still routes.
                    condition(
                        "Accepted",
                        accepted_ok,
                        accepted_reason,
                        &accepted_msg,
                        resolved.generation,
                        find(prev, "Accepted"),
                    ),
                    condition(
                        "ResolvedRefs",
                        refs_ok,
                        refs_reason,
                        &refs_msg,
                        resolved.generation,
                        find(prev, "ResolvedRefs"),
                    ),
                ],
            }
        })
        .collect();

    let own = previous.map(|s| s.conditions.as_slice());
    InferencePoolStatus {
        // Qualifiers, since the pool still routes: other destinations stay
        // usable, and a bad policyRef fails open.
        state: state(
            accepted_reason,
            &[
                (!resolved.unresolved.is_empty(), "UnresolvedDestinations"),
                (
                    matches!(
                        resolved.dangling_policy,
                        Some(DanglingPolicy::NotFound { .. })
                    ),
                    "PolicyNotFound",
                ),
                (
                    matches!(
                        resolved.dangling_policy,
                        Some(DanglingPolicy::WrongKind { .. })
                    ),
                    "PolicyRefWrongKind",
                ),
            ],
        ),
        destination_count: Some(resolved.destinations.len() as i64),
        conditions: vec![
            condition(
                "Accepted",
                accepted_ok,
                accepted_reason,
                &accepted_msg,
                resolved.generation,
                find(own, "Accepted"),
            ),
            condition(
                "ResolvedRefs",
                refs_ok,
                refs_reason,
                &refs_msg,
                resolved.generation,
                find(own, "ResolvedRefs"),
            ),
        ],
        models,
        unresolved_destinations: resolved
            .unresolved
            .iter()
            .map(|u| UnresolvedDestination {
                kind: Some(u.kind.clone()),
                name: Some(u.name.clone()),
                reason: Some(u.reason.as_str().to_string()),
            })
            .collect(),
        parents,
    }
}

/// Status for a worker set the router refused to materialize.
///
/// Everything derived from a pool is cleared rather than omitted, so stale
/// counts from before the rejection don't linger.
pub fn rejected_worker_set_status(
    reason: &kinference_policy::rejection::RejectReason,
    referenced_by: &[String],
    previous: Option<&InferenceWorkerSetStatus>,
) -> InferenceWorkerSetStatus {
    let prev = previous.map(|s| s.conditions.as_slice());
    InferenceWorkerSetStatus {
        // No `NotReady` qualifier: a refused set has no endpoints by
        // construction.
        state: state(reason.as_condition_reason(), &[]),
        models: Vec::new(),
        // Kept: it comes from the pools, and shows which pool lost a
        // destination.
        referenced_by: referenced_by.to_vec(),
        endpoints: Some(EndpointCounts { ready: Some(0) }),
        kv_events: None,
        conditions: vec![
            condition(
                "Accepted",
                false,
                reason.as_condition_reason(),
                &reason.to_string(),
                None,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                false,
                "NotAccepted",
                "the worker set was not accepted, so it has no endpoints",
                None,
                find(prev, "Ready"),
            ),
        ],
    }
}

/// Desired `InferenceProvider` status.
///
/// Health comes entirely from the caller's reports: kinference never dials the
/// vendor, reads the `GatewayBackend`, or holds a credential.
///
/// - `ResolvedRefs=False`: the configuration is wrong (a `backendRef` that
///   doesn't resolve, or rejected credentials).
/// - `Ready=False`: the vendor is failing (5xx, or no response).
pub fn desired_provider_status(
    health: &ProviderHealth,
    previous: Option<&InferenceProviderStatus>,
) -> InferenceProviderStatus {
    let prev = previous.map(|s| s.conditions.as_slice());
    let config_error = health
        .last_failure
        .as_ref()
        .filter(|(kind, _)| kind.is_configuration_error());
    let upstream_error = health
        .last_failure
        .as_ref()
        .filter(|(kind, _)| !kind.is_configuration_error());

    let refs = match config_error {
        Some((kind, detail)) => condition(
            "ResolvedRefs",
            false,
            kind.as_str(),
            &format!(
                "the caller could not dispatch through this provider's backendRef: {detail}. \
                 kinference does not read GatewayBackends, so this is reported rather than \
                 observed"
            ),
            None,
            find(prev, "ResolvedRefs"),
        ),
        // Only means the ref is well-formed and permitted, not that the backend
        // exists; kinference can't see that.
        None => condition(
            "ResolvedRefs",
            true,
            "Resolved",
            "backendRef is well-formed and permitted; whether it resolves is the \
             caller's to report",
            None,
            find(prev, "ResolvedRefs"),
        ),
    };

    let ready = if health.suppressed {
        let detail = health
            .last_failure
            .as_ref()
            .map(|(_, d)| d.clone())
            .unwrap_or_else(|| "repeated dispatch failures".to_string());
        condition(
            "Ready",
            false,
            "DispatchFailing",
            &format!("producing no candidates after repeated dispatch failures: {detail}"),
            None,
            find(prev, "Ready"),
        )
    } else if let Some((_, detail)) = upstream_error {
        // Not suppressed, so still serving, but surface the error.
        condition(
            "Ready",
            true,
            "Serving",
            &format!("serving, with a recent upstream error: {detail}"),
            None,
            find(prev, "Ready"),
        )
    } else {
        condition(
            "Ready",
            true,
            "Serving",
            "producing candidates",
            None,
            find(prev, "Ready"),
        )
    };

    // Written on the healthy path too, so a fixed provider flips to True
    // instead of losing the condition.
    let accepted = condition(
        "Accepted",
        true,
        "Accepted",
        "resolved into a pool destination",
        None,
        find(prev, "Accepted"),
    );

    InferenceProviderStatus {
        // `Suppressed`: well-formed but producing no candidates. The backend
        // tokens come from the caller's reports.
        state: state(
            "Accepted",
            &[
                (
                    matches!(config_error, Some((FailureKind::Unresolved, _))),
                    "BackendUnresolved",
                ),
                (
                    matches!(config_error, Some((FailureKind::Unauthorized, _))),
                    "BackendUnauthorized",
                ),
                (health.suppressed, "Suppressed"),
            ],
        ),
        // `Accepted` first, as in every other kind.
        conditions: vec![accepted, refs, ready],
    }
}

/// Desired `InferenceProvider` status for one kinference refused.
///
/// `Accepted=False` rather than `ResolvedRefs=False`: the spec itself is what
/// needs fixing.
pub fn rejected_provider_status(
    reason: kinference_policy::resolve::UnresolvedReason,
    previous: Option<&InferenceProviderStatus>,
) -> InferenceProviderStatus {
    use kinference_policy::resolve::UnresolvedReason as R;
    let prev = previous.map(|s| s.conditions.as_slice());
    let msg = match reason {
        R::UnsupportedApiFormat => {
            "spec.api names a wire format nothing translates to. Only OpenAI is \
             supported; point baseURL at the vendor's OpenAI-compatible endpoint, or \
             front it with a proxy that exposes one"
        }
        R::Incomplete => {
            "spec is missing something required: backendRef.name, api, or at least \
             one models[] row naming what the vendor calls a model"
        }
        R::CrossNamespaceNotGranted => {
            "backendRef crosses namespaces, and no ReferenceGrant in the target \
             namespace lets InferenceProviders from this namespace reference it"
        }
        R::AmbiguousName => {
            "another destination of a different kind in this pool has the same name, \
             so a policy naming it would be ambiguous"
        }
        _ => "the pool refused this destination; see the pool's unresolvedDestinations",
    };
    InferenceProviderStatus {
        state: state(reason.as_str(), &[]),
        conditions: vec![
            condition(
                "Accepted",
                false,
                reason.as_str(),
                msg,
                None,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                false,
                "NotAccepted",
                "refused, so it produces no candidates",
                None,
                find(prev, "Ready"),
            ),
        ],
    }
}

/// Desired `InferenceWorkerSet` status.
pub fn desired_worker_set_status(
    dest: &ResolvedWorkerSet,
    facts: &HashMap<PoolId, PoolFacts>,
    referenced_by: &[String],
    generation: Option<i64>,
    previous: Option<&InferenceWorkerSetStatus>,
) -> InferenceWorkerSetStatus {
    let f = facts.get(&dest.pool_id).cloned().unwrap_or_default();
    let models: Vec<WorkerSetModelStatus> = f
        .models
        .iter()
        .map(|m| WorkerSetModelStatus {
            // Keyed on what the engines reported, with aliases alongside.
            upstream_name: m.upstream.clone(),
            aliases: dest.aliases_for(&m.upstream),
            ready_endpoints: Some(m.ready_endpoints),
            context_window: (m.ctx_min.is_some() || m.ctx_max.is_some()).then_some(ContextWindow {
                min: m.ctx_min,
                max: m.ctx_max,
            }),
        })
        .collect();
    let prev = previous.map(|s| s.conditions.as_slice());
    // An unresolvable serviceRef never gets here; it is reported on the pool
    // as `unresolvedDestinations`.
    let ready = f.ready_endpoints > 0;
    // Not folded into `Accepted` as for an unreferenced policy: a worker set no
    // pool names still serves every `Pick` that names no pool.
    let referenced = !referenced_by.is_empty();
    let referenced_msg = if referenced {
        format!("destination of pool(s) {}", referenced_by.join(", "))
    } else {
        format!(
            "no InferencePool lists this set in spec.destinations, so no pool-scoped policy \
             applies to it. It still serves a Pick that names no pool. Attach it with: \
             kubectl -n {} patch infpool <pool> --type=json -p \
             '[{{\"op\":\"add\",\"path\":\"/spec/destinations/-\",\"value\":\
{{\"kind\":\"InferenceWorkerSet\",\"name\":\"{}\"}}}}]'",
            dest.namespace, dest.name
        )
    };
    InferenceWorkerSetStatus {
        // `Unreferenced` is a qualifier: the set is accepted and serving, just
        // without a pool-scoped policy over it.
        state: state(
            "Accepted",
            &[(!ready, "NotReady"), (!referenced, "Unreferenced")],
        ),
        models,
        referenced_by: referenced_by.to_vec(),
        endpoints: Some(EndpointCounts {
            ready: Some(f.ready_endpoints),
        }),
        // Always reported, including `configured: false`, since a missing event
        // stream is otherwise invisible.
        kv_events: Some(KvEventsStatus {
            configured: Some(dest.kv_events_declared),
            subscribed_endpoints: Some(f.event_endpoints),
        }),
        conditions: vec![
            condition(
                "Accepted",
                true,
                "Accepted",
                &format!("materialized as a pool; Service `{}`", dest.service_name),
                generation,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                ready,
                if ready {
                    "EndpointsReady"
                } else {
                    "NoReadyEndpoints"
                },
                &format!("{} ready endpoint(s)", f.ready_endpoints),
                generation,
                find(prev, "Ready"),
            ),
            condition(
                "Referenced",
                referenced,
                if referenced {
                    "InPool"
                } else {
                    "NoPoolNamesIt"
                },
                &referenced_msg,
                generation,
                find(prev, "Referenced"),
            ),
        ],
    }
}

/// Desired status for a set kinference has no pool for, and that no rejection
/// explains.
///
/// Normally the brief window between the object appearing and the next
/// reconcile; if it persists, `serviceRef` names a Service that does not exist.
/// Needed because an unreferenced set appears in no pool's
/// `unresolvedDestinations`.
pub fn unbound_worker_set_status(
    reason: UnresolvedReason,
    referenced_by: &[String],
    generation: Option<i64>,
    previous: Option<&InferenceWorkerSetStatus>,
) -> InferenceWorkerSetStatus {
    let prev = previous.map(|s| s.conditions.as_slice());
    InferenceWorkerSetStatus {
        state: state(reason.as_str(), &[]),
        models: Vec::new(),
        referenced_by: referenced_by.to_vec(),
        endpoints: Some(EndpointCounts { ready: Some(0) }),
        kv_events: None,
        conditions: vec![
            condition(
                "Accepted",
                false,
                reason.as_str(),
                "kinference has no pool for this worker set, so nothing routes to it",
                generation,
                find(prev, "Accepted"),
            ),
            condition(
                "Ready",
                false,
                "NotAccepted",
                "the worker set was not accepted, so it has no endpoints",
                generation,
                find(prev, "Ready"),
            ),
        ],
    }
}

/// Desired `InferencePolicy` status.
///
/// Written for every policy in scope, not only those a pool references, so an
/// unattached policy still reports its warnings. `Accepted` says whether any
/// pool routes through it.
pub fn desired_policy_status(
    resolved: &ResolvedPolicy,
    previous: Option<&InferencePolicyStatus>,
) -> InferencePolicyStatus {
    let policy = &resolved.policy;
    let generation = resolved.generation;
    let prev = previous.map(|s| s.conditions.as_slice());
    // Compile and name warnings, as one list. Warnings never make a policy
    // unusable.
    let warnings: Vec<String> = resolved.all_warnings().map(ToString::to_string).collect();
    let compiled = if warnings.is_empty() {
        format!("{} rule(s) compiled", policy.rules.len())
    } else {
        format!(
            "{} rule(s) compiled with {} warning(s)",
            policy.rules.len(),
            warnings.len()
        )
    };
    // `Accepted` means in force on some pool, not just compiled.
    let accepted = !resolved.referenced_by.is_empty();
    let (reason, msg) = if accepted {
        (
            "Accepted",
            format!(
                "{compiled}; in force on pool(s) {}",
                resolved.referenced_by.join(", ")
            ),
        )
    } else {
        (
            "NotReferenced",
            format!(
                "{compiled}, but no InferencePool names it in spec.policyRef, so it is not in force. \
                 Attach it with: kubectl -n {} patch infpool <pool> --type=merge \
                 -p '{{\"spec\":{{\"policyRef\":{{\"name\":\"{}\"}}}}}}'",
                resolved.namespace, resolved.name
            ),
        )
    };
    // Joined into one string, as in `state()`.
    let mut state = vec![if accepted { "InForce" } else { "NotReferenced" }];
    if !warnings.is_empty() {
        state.push("HasWarnings");
    }

    InferencePolicyStatus {
        state: Some(state.join(",")),
        referenced_by: resolved.referenced_by.clone(),
        warnings: fit_status_warnings(warnings),
        conditions: vec![condition(
            "Accepted",
            accepted,
            reason,
            &msg,
            generation,
            find(prev, "Accepted"),
        )],
    }
}

/// `status.warnings` limits from the InferencePolicy CRD schema
/// (`charts/kinference-crds/templates/kinference.buoyant.io_inferencepolicies.yaml`).
const STATUS_WARNINGS_MAX_ITEMS: usize = 32;
const STATUS_WARNING_MAX_CHARS: usize = 2048;

/// Fit warnings inside what the CRD schema admits.
///
/// Exceeding either limit makes the API server reject the whole status patch.
/// Keeps the first 31, then a note saying how many were cut. Entries are
/// truncated by character, as the schema counts characters.
fn fit_status_warnings(all: Vec<String>) -> Vec<String> {
    let clip = |w: String| {
        if w.chars().count() <= STATUS_WARNING_MAX_CHARS {
            w
        } else {
            let mut t: String = w.chars().take(STATUS_WARNING_MAX_CHARS - 1).collect();
            t.push('…');
            t
        }
    };
    if all.len() <= STATUS_WARNINGS_MAX_ITEMS {
        return all.into_iter().map(clip).collect();
    }
    let kept = STATUS_WARNINGS_MAX_ITEMS - 1;
    let cut = all.len() - kept;
    let mut out: Vec<String> = all.into_iter().take(kept).map(clip).collect();
    out.push(format!(
        "…and {cut} more warning(s), not shown because status holds at most \
         {STATUS_WARNINGS_MAX_ITEMS}. The full list is in GET /api/policy/context on the admin \
         port, or run `kinfctl check` on the manifest"
    ));
    out
}

type CacheKey = (&'static str, String, String, Option<String>);

/// The status writer's last-written cache key: kind, namespace, name, UID.
///
/// Includes the UID so a recreated object, which starts with empty status,
/// doesn't hit its predecessor's entry and get skipped.
fn cache_key(kind: &'static str, ns: &str, name: &str, uid: Option<&str>) -> CacheKey {
    (
        kind,
        ns.to_string(),
        name.to_string(),
        uid.map(str::to_string),
    )
}

/// The live state the status writer reads, built once in `server`.
pub struct StatusHandles {
    pub store: Arc<PolicyStore>,
    pub pools: Arc<kinference_fleet::pool::PoolRegistry>,
    /// Worker sets that produced no pool, which appear in no `PolicyIndex`.
    pub rejections: Arc<super::worker_sets::RejectionStore>,
    pub metrics: Arc<MetricsRegistry>,
    /// Caller-reported provider health, the only source for a provider's
    /// status.
    pub provider_health: Arc<kinference_policy::provider_health::ProviderHealthStore>,
    /// Whether this replica may write. Always `true` when leader election is
    /// disabled.
    pub leader: watch::Receiver<bool>,
    pub prom: Arc<crate::metrics::ControllerMetrics>,
}

/// Patches status onto the CRDs, on a tick, only when something changed.
pub struct StatusWriter<A: ApiServer> {
    api: A,
    store: Arc<PolicyStore>,
    pools: Arc<kinference_fleet::pool::PoolRegistry>,
    rejections: Arc<super::worker_sets::RejectionStore>,
    metrics: Arc<MetricsRegistry>,
    provider_health: Arc<kinference_policy::provider_health::ProviderHealthStore>,
    /// Last status we wrote, per [`cache_key`]. Both the loop-breaker and the
    /// source of stable `lastTransitionTime`s.
    last: Mutex<HashMap<CacheKey, serde_json::Value>>,
    leader: watch::Receiver<bool>,
    prom: Arc<crate::metrics::ControllerMetrics>,
    /// Policy warnings already logged, as `ns/name: warning`, so each is logged
    /// once when it first appears.
    warned: Mutex<HashSet<String>>,
    /// Objects whose last status patch failed, so a failing patch logs once
    /// until it succeeds.
    failing: Mutex<HashSet<CacheKey>>,
}

impl<A: ApiServer> StatusWriter<A> {
    pub fn new(api: A, handles: StatusHandles) -> Self {
        let StatusHandles {
            store,
            pools,
            rejections,
            metrics,
            provider_health,
            leader,
            prom,
        } = handles;
        // Seed the gauge so a follower reports 0 before its first transition.
        prom.set_leader(*leader.borrow());
        Self {
            api,
            store,
            pools,
            rejections,
            metrics,
            provider_health,
            leader,
            prom,
            last: Mutex::new(HashMap::new()),
            warned: Mutex::new(HashSet::new()),
            failing: Mutex::new(HashSet::new()),
        }
    }

    pub async fn run(mut self, cancel: CancellationToken) {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(10));
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
                // Awaited rather than polled, so the cache is reset on every
                // promotion.
                r = self.leader.changed(), if leader_open => {
                    match r {
                        Ok(()) => {
                            let now_leader = *self.leader.borrow_and_update();
                            self.prom.set_leader(now_leader);
                            if now_leader {
                                self.on_promotion();
                            } else {
                                self.on_demotion();
                            }
                        }
                        // Elector gone. Nothing renews the lease now, so a
                        // rival can claim it: stop writing.
                        Err(_) => {
                            leader_open = false;
                            tracing::error!("leader channel closed; stopping writes");
                            self.prom.record_leader_election_error();
                            if *self.leader.borrow() {
                                self.prom.set_leader(false);
                                self.on_demotion();
                            }
                        }
                    }
                }
                _ = interval.tick() => {
                    if !self.leading() {
                        continue;
                    }
                    self.reconcile_once().await;
                }
            }
        }
    }

    /// Whether this replica may write. A closed leader channel counts as
    /// lost leadership.
    fn leading(&self) -> bool {
        self.leader.has_changed().is_ok() && *self.leader.borrow()
    }

    /// Drop the last-written cache on becoming leader.
    ///
    /// The cache records what this replica wrote, which may predate whatever
    /// the previous leader wrote since; trusting it would skip needed patches.
    /// Clearing it re-asserts everything, and the next write stamps fresh
    /// `lastTransitionTime`s.
    fn on_promotion(&self) {
        let dropped = {
            let mut last = sync::lock(&self.last);
            let n = last.len();
            last.clear();
            n
        };
        tracing::debug!(
            dropped_cache_entries = dropped,
            "promoted to leader; rewriting all status"
        );
    }

    /// Withdraw the policy warning counts on losing the lease.
    ///
    /// Only `reconcile_once` sets `kinference_policy_warnings`, and a follower
    /// never runs it, so a demoted replica would otherwise keep exporting stale
    /// counts.
    fn on_demotion(&self) {
        self.prom.set_policy_warnings(&[]);
        tracing::debug!("demoted from leader; stopping writes");
    }

    async fn reconcile_once(&self) {
        let snapshot = self.store.snapshot();
        // Not `snapshot.is_empty()`, which only counts pools: worker sets and
        // policies need status even with no pools.
        if !snapshot.listed() {
            return;
        }
        // One snapshot for the whole reconcile, so every CR describes the same
        // fleet.
        let facts = gather_facts(self.pools.snapshot().as_slice(), &self.metrics);

        for resolved in snapshot.pools() {
            // --- pool -----------------------------------------------------
            let prev = self.previous::<InferencePoolStatus>(
                "InferencePool",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
            );
            let desired = desired_pool_status(resolved, &facts, prev.as_ref());
            self.patch::<InferencePool>(
                "InferencePool",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
                &desired,
            )
            .await;

            // --- providers -------------------------------------------------
            for prov in resolved.destinations.iter().filter_map(|d| d.as_provider()) {
                let key = kinference_policy::engine::ObjectKey::new(&prov.namespace, &prov.name);
                let prev = self.previous::<InferenceProviderStatus>(
                    "InferenceProvider",
                    &prov.namespace,
                    &prov.name,
                    prov.uid.as_deref(),
                );
                let desired =
                    desired_provider_status(&self.provider_health.health(&key), prev.as_ref());
                self.patch::<InferenceProvider>(
                    "InferenceProvider",
                    &prov.namespace,
                    &prov.name,
                    prov.uid.as_deref(),
                    &desired,
                )
                .await;
            }

            // --- rejected providers ----------------------------------------
            //
            // Never destinations, so the loop above misses them. Names starting
            // with `<` are placeholders (`<unset>`, `<selector>`), not objects.
            for u in resolved
                .unresolved
                .iter()
                .filter(|u| u.kind == "InferenceProvider" && !u.name.starts_with('<'))
            {
                let prev = self.previous::<InferenceProviderStatus>(
                    "InferenceProvider",
                    &resolved.namespace,
                    &u.name,
                    u.uid.as_deref(),
                );
                let desired = rejected_provider_status(u.reason, prev.as_ref());
                self.patch::<InferenceProvider>(
                    "InferenceProvider",
                    &resolved.namespace,
                    &u.name,
                    u.uid.as_deref(),
                    &desired,
                )
                .await;
            }
        }

        // --- worker sets --------------------------------------------------
        //
        // Over the sets themselves rather than pool destinations, so sets no
        // pool names still get status. Rejected: the object is wrong. Unbound:
        // kinference has no pool for it. Bound: what it serves, and whether a
        // pool routes to it.
        let rejected = self.rejections.snapshot();
        for entry in snapshot.worker_sets() {
            let prev = self.previous::<InferenceWorkerSetStatus>(
                "InferenceWorkerSet",
                &entry.namespace,
                &entry.name,
                entry.uid.as_deref(),
            );
            // A rejection wins over the bind failure it causes, since it names
            // the actual problem.
            let rejection = rejected.iter().find(|r| {
                r.key.name == entry.name
                    && r.key.namespace.as_deref().unwrap_or_default() == entry.namespace
            });
            let desired = match (rejection, &entry.bound) {
                (Some(r), _) => {
                    rejected_worker_set_status(&r.reason, &entry.referenced_by, prev.as_ref())
                }
                (None, Ok(dest)) => desired_worker_set_status(
                    dest,
                    &facts,
                    &entry.referenced_by,
                    entry.generation,
                    prev.as_ref(),
                ),
                (None, Err(reason)) => unbound_worker_set_status(
                    *reason,
                    &entry.referenced_by,
                    entry.generation,
                    prev.as_ref(),
                ),
            };
            self.patch::<InferenceWorkerSet>(
                "InferenceWorkerSet",
                &entry.namespace,
                &entry.name,
                entry.uid.as_deref(),
                &desired,
            )
            .await;
        }

        // --- policies -----------------------------------------------------
        //
        // Over the policies themselves, so unreferenced ones get status too.
        self.report_policy_warnings(&snapshot);
        for resolved in snapshot.policies() {
            let prev = self.previous::<InferencePolicyStatus>(
                "InferencePolicy",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
            );
            let desired = desired_policy_status(resolved, prev.as_ref());
            self.patch::<InferencePolicy>(
                "InferencePolicy",
                &resolved.namespace,
                &resolved.name,
                resolved.uid.as_deref(),
                &desired,
            )
            .await;
        }
    }

    /// Log warnings that are new since the last pass, and publish per-policy
    /// counts to `kinference_policy_warnings`.
    ///
    /// Each warning is logged once, when it first appears.
    fn report_policy_warnings(&self, snapshot: &kinference_policy::resolve::PolicyIndex) {
        let mut current: HashSet<String> = HashSet::new();
        let mut rows: Vec<(String, &'static str, i64)> = Vec::new();
        for resolved in snapshot.policies() {
            let key = format!("{}/{}", resolved.namespace, resolved.name);
            let mut by_code: BTreeMap<&'static str, i64> = BTreeMap::new();
            for w in resolved.all_warnings() {
                *by_code.entry(w.code.as_str()).or_default() += 1;
                current.insert(format!("{key}: {w}"));
            }
            rows.extend(by_code.into_iter().map(|(code, n)| (key.clone(), code, n)));
        }
        self.prom.set_policy_warnings(&rows);
        let mut warned = sync::lock(&self.warned);
        for line in &current {
            if !warned.contains(line) {
                tracing::warn!(warning = %line, "policy warning");
            }
        }
        *warned = current;
    }

    fn previous<T: serde::de::DeserializeOwned>(
        &self,
        kind: &'static str,
        ns: &str,
        name: &str,
        uid: Option<&str>,
    ) -> Option<T> {
        let last = sync::lock(&self.last);
        let v = last.get(&cache_key(kind, ns, name, uid))?;
        serde_json::from_value(v.clone()).ok()
    }

    /// Patch `status` if it differs from what we last wrote. See the module
    /// docs for why this comparison matters.
    async fn patch<K: Object>(
        &self,
        kind: &'static str,
        ns: &str,
        name: &str,
        uid: Option<&str>,
        desired: &impl serde::Serialize,
    ) {
        let Ok(value) = serde_json::to_value(desired) else {
            return;
        };
        let key = cache_key(kind, ns, name, uid);
        {
            let last = sync::lock(&self.last);
            if last.get(&key) == Some(&value) {
                return;
            }
        }
        match self.api.patch_status::<K>(ns, name, &value).await {
            Ok(_) => {
                tracing::debug!(kind, ns, name, "status patched");
                sync::lock(&self.failing).remove(&key);
                sync::lock(&self.last).insert(key, value);
            }
            Err(e) => {
                // Don't cache on failure, so the next tick retries.
                if sync::lock(&self.failing).insert(key) {
                    tracing::warn!(
                        kind,
                        ns,
                        name,
                        error = &e as &dyn StdError,
                        "status patch failed"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
