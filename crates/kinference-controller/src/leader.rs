//! Leader election over a `coordination.k8s.io/v1` Lease.
//!
//! Exactly one replica writes CRD status and `BackendBinding`s; every
//! replica still routes. The claiming
//! rules: the holder renews inside a grace window before expiry, a
//! rival waits out the current claim's expiry before trying,
//! takeovers bump `leaseTransitions`, and a
//! clean shutdown vacates the lease so the peer takes over
//! immediately. Writes are optimistic-concurrency replaces (the
//! `resourceVersion` of the last-seen lease rides along), so two
//! replicas racing for the same claim cannot both win: the loser
//! gets a conflict, re-reads, and backs off.

use std::error::Error as StdError;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::Timestamp;
use kube::api::ObjectMeta;
use serde::Deserialize;
use tokio::sync::watch;

use crate::api::ApiServer;

/// `leader_election`: elect a single CRD status writer across replicas.
///
/// Only status writing is gated; every replica still routes. Two writers that
/// disagree would overwrite each other's status forever.
#[derive(Debug, Clone, Deserialize)]
pub struct LeaderElectionConfig {
    /// Enable before scaling past one replica. Fails closed: a replica that
    /// can't access the Lease writes nothing. Needs `get`/`create`/`update` on
    /// `coordination.k8s.io` `leases`.
    #[serde(default)]
    pub enabled: bool,
    /// Lease name. Must differ between kinference Deployments sharing a
    /// namespace.
    #[serde(default = "default_lease_name")]
    pub lease_name: String,
    /// Namespace holding the Lease; `None` uses the pod's own namespace.
    #[serde(default)]
    pub namespace: Option<String>,
    /// How long a claim stays valid without renewal; also the worst-case
    /// failover delay when a leader dies without vacating.
    #[serde(default = "default_lease_duration_secs")]
    pub lease_duration_secs: u64,
    /// Renew once within this window of expiry. Must be less than
    /// `lease_duration_secs`.
    #[serde(default = "default_renew_grace_secs")]
    pub renew_grace_secs: u64,
    /// Delay before retrying after an apiserver error or a lost race.
    #[serde(default = "default_lease_retry_secs")]
    pub retry_secs: u64,
    /// This replica's `holderIdentity`, from `POD_NAME` at startup; see
    /// [`identity`]. Not a file key.
    #[serde(skip)]
    pub identity: String,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            lease_name: default_lease_name(),
            namespace: None,
            lease_duration_secs: default_lease_duration_secs(),
            renew_grace_secs: default_renew_grace_secs(),
            retry_secs: default_lease_retry_secs(),
            identity: String::new(),
        }
    }
}

fn default_lease_name() -> String {
    "kinference".to_string()
}
// Matches client-go's leader-election defaults: a 15s claim renewed every ~5s.
fn default_lease_duration_secs() -> u64 {
    15
}
fn default_renew_grace_secs() -> u64 {
    10
}
fn default_lease_retry_secs() -> u64 {
    2
}

/// Timings and identity for one elector, from `leader_election`.
///
/// `renew_grace < lease_duration` is an invariant, enforced by
/// `Config::validate_leader_election`: at or above it the holder is always
/// inside its own renewal window, so the loop never sleeps and rewrites the
/// Lease as fast as the apiserver will take it.
#[derive(Debug, Clone)]
pub struct LeaseParams {
    pub name: String,
    /// How long a claim is valid without renewal.
    pub lease_duration: Duration,
    /// Renew once inside this window before expiry (so a healthy holder
    /// renews roughly every `lease_duration - renew_grace`).
    pub renew_grace: Duration,
    /// Delay before re-deciding after an apiserver error or conflict.
    pub retry: Duration,
}

impl From<&LeaderElectionConfig> for LeaseParams {
    fn from(c: &LeaderElectionConfig) -> Self {
        Self {
            name: c.lease_name.clone(),
            lease_duration: Duration::from_secs(c.lease_duration_secs),
            renew_grace: Duration::from_secs(c.renew_grace_secs),
            retry: Duration::from_secs(c.retry_secs),
        }
    }
}

/// What the elector should do next, decided from the last-known
/// lease state. Pure so the claiming rules are unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    /// Write a claim now (free, expired, malformed, or ours and due
    /// for renewal).
    Claim,
    /// Sleep until this instant, then re-decide (someone else's
    /// claim is current, or ours doesn't need renewing yet).
    WaitUntil(Timestamp),
}

/// Run the elector until `shutdown` resolves, publishing leadership
/// transitions on `tx`. Apiserver errors are retried forever; if our
/// own claim lapses meanwhile the channel demotes to follower.
pub async fn run<A: ApiServer>(
    api: A,
    namespace: &str,
    identity: String,
    params: LeaseParams,
    tx: watch::Sender<bool>,
    shutdown: impl Future<Output = ()>,
) {
    let mut shutdown = std::pin::pin!(shutdown);
    let mut known: Option<Lease> = None;
    // Whether the current run of lease API failures has been logged; reset on
    // success, so a failure after a recovery warns again.
    let mut failing = false;

    loop {
        if known.is_none() {
            known = match fetch_or_create(&api, namespace, &params.name).await {
                Ok(lease) => {
                    failing = false;
                    Some(lease)
                }
                Err(e) => {
                    if !std::mem::replace(&mut failing, true) {
                        tracing::warn!(
                            error = &e as &dyn StdError,
                            "failed to read or create the lease; retrying"
                        );
                    }
                    None
                }
            };
        }

        let now = Timestamp::now();
        let wait = match next_action(now, spec_of(known.as_ref()), &identity, params.renew_grace) {
            Action::Claim => match known.take() {
                // Fetch failed above: retry it.
                None => params.retry,
                Some(prev) => match claim(&api, namespace, &identity, prev, now, &params).await {
                    Ok(lease) => {
                        failing = false;
                        known = Some(lease);
                        // Successful claims re-decide immediately and
                        // land in WaitUntil; rejections re-read first.
                        Duration::ZERO
                    }
                    Err(e) => {
                        if !is_conflict(&e) && !std::mem::replace(&mut failing, true) {
                            tracing::warn!(
                                error = &e as &dyn StdError,
                                "lease claim failed; retrying"
                            );
                        }
                        params.retry
                    }
                },
            },
            Action::WaitUntil(t) => {
                // Wake just past the boundary so the re-decide lands
                // on the other side of it.
                millis_until(now, t) + Duration::from_millis(100)
            }
        };

        publish(&tx, spec_of(known.as_ref()), &identity, Timestamp::now());

        if !wait.is_zero() {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                () = tokio::time::sleep(wait) => {}
            }
            // A rival's claim may have been renewed (or vacated)
            // while we slept; never claim from a stale view.
            if !is_current_holder(spec_of(known.as_ref()), &identity, Timestamp::now()) {
                known = None;
            }
        }
    }

    // Held at shutdown: vacate so the peer claims immediately
    // instead of waiting out our expiry.
    if is_current_holder(spec_of(known.as_ref()), &identity, Timestamp::now())
        && let Some(lease) = known
        && let Err(e) = vacate(&api, namespace, lease, &params.name).await
    {
        tracing::warn!(
            error = &e as &dyn StdError,
            "failed to vacate lease on shutdown"
        );
    }
    let _ = tx.send(false);
}

/// Get the lease, creating it empty on first contact. The caller retries an
/// error.
///
/// Matches on `Status::code` rather than kube's `is_not_found()` /
/// `is_conflict()`, which compare the `reason` string and so miss a bare
/// 404/409 with no reason.
async fn fetch_or_create<A: ApiServer>(
    api: &A,
    namespace: &str,
    name: &str,
) -> Result<Lease, kube::Error> {
    match api.get::<Lease>(namespace, name).await {
        Err(kube::Error::Api(ae)) if ae.code == 404 => {
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(name.to_string()),
                    namespace: Some(namespace.to_string()),
                    ..Default::default()
                },
                spec: None,
            };
            match api.create(namespace, &lease).await {
                // Lost the create race to the peer: read theirs.
                Err(kube::Error::Api(ae)) if ae.code == 409 => {
                    api.get::<Lease>(namespace, name).await
                }
                created => created,
            }
        }
        got => got,
    }
}

/// Decide from the last-known lease state.
fn next_action(
    now: Timestamp,
    spec: Option<&LeaseSpec>,
    identity: &str,
    renew_grace: Duration,
) -> Action {
    let Some(spec) = spec else {
        return Action::Claim;
    };
    let Some(holder) = spec.holder_identity.as_deref().filter(|h| !h.is_empty()) else {
        return Action::Claim;
    };
    // A claim without renew time or duration can't be honored.
    let Some(expiry) = expiry_of(spec) else {
        return Action::Claim;
    };
    if now >= expiry {
        return Action::Claim;
    }
    if holder != identity {
        return Action::WaitUntil(expiry);
    }
    let renew_at = expiry - renew_grace;
    if now >= renew_at {
        Action::Claim
    } else {
        Action::WaitUntil(renew_at)
    }
}

/// Write a claim over `prev` (optimistic concurrency via `prev`'s
/// `resourceVersion`). Taking over from another/expired holder bumps
/// `leaseTransitions` and stamps a fresh `acquireTime`; a renewal
/// carries both forward.
async fn claim<A: ApiServer>(
    api: &A,
    namespace: &str,
    identity: &str,
    mut prev: Lease,
    now: Timestamp,
    params: &LeaseParams,
) -> Result<Lease, kube::Error> {
    let prev_spec = prev.spec.take().unwrap_or_default();
    let renewal = prev_spec.holder_identity.as_deref() == Some(identity);
    let transitions = prev_spec.lease_transitions.unwrap_or(0);
    prev.spec = Some(LeaseSpec {
        holder_identity: Some(identity.to_string()),
        lease_duration_seconds: Some(
            params
                .lease_duration
                .as_secs()
                .try_into()
                .unwrap_or(i32::MAX),
        ),
        acquire_time: if renewal {
            prev_spec.acquire_time.or(Some(MicroTime(now)))
        } else {
            Some(MicroTime(now))
        },
        renew_time: Some(MicroTime(now)),
        lease_transitions: Some(if renewal {
            transitions
        } else {
            transitions + 1
        }),
        ..Default::default()
    });
    api.replace(namespace, &params.name, &prev).await
}

/// Clear the holder fields (keeping the transition counter) so the
/// peer's next decision is `Claim`.
async fn vacate<A: ApiServer>(
    api: &A,
    namespace: &str,
    mut lease: Lease,
    name: &str,
) -> Result<(), kube::Error> {
    let transitions = lease.spec.take().and_then(|s| s.lease_transitions);
    lease.spec = Some(LeaseSpec {
        lease_transitions: transitions,
        ..Default::default()
    });
    api.replace(namespace, name, &lease).await?;
    Ok(())
}

fn publish(tx: &watch::Sender<bool>, spec: Option<&LeaseSpec>, identity: &str, now: Timestamp) {
    let leader = is_current_holder(spec, identity, now);
    tx.send_if_modified(|v| {
        if *v == leader {
            false
        } else {
            tracing::info!(leader, "leadership changed");
            *v = leader;
            true
        }
    });
}

fn is_current_holder(spec: Option<&LeaseSpec>, identity: &str, now: Timestamp) -> bool {
    spec.is_some_and(|s| {
        s.holder_identity.as_deref() == Some(identity)
            && expiry_of(s).is_some_and(|expiry| now < expiry)
    })
}

fn expiry_of(spec: &LeaseSpec) -> Option<Timestamp> {
    let MicroTime(renewed) = spec.renew_time.as_ref()?;
    let secs = spec.lease_duration_seconds?;
    Timestamp::from_millisecond(renewed.as_millisecond() + i64::from(secs) * 1000).ok()
}

fn spec_of(lease: Option<&Lease>) -> Option<&LeaseSpec> {
    lease.and_then(|l| l.spec.as_ref())
}

fn millis_until(now: Timestamp, target: Timestamp) -> Duration {
    u64::try_from(target.as_millisecond() - now.as_millisecond())
        .map_or(Duration::ZERO, Duration::from_millis)
}

fn is_conflict(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(ae) if ae.code == 409)
}

/// The pod's own namespace, from the service-account mount. Used when
/// `leader_election.namespace` is unset so a normal in-cluster deployment
/// needs no configuration at all.
pub fn pod_namespace() -> Option<String> {
    std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// This replica's `holderIdentity` on the Lease.
///
/// `pod_name` (`POD_NAME` from the downward API, as shown by `kubectl get
/// pods`); else the hostname; else a random name.
pub fn identity(pod_name: Option<String>) -> String {
    if let Some(name) = pod_name
        && !name.trim().is_empty()
    {
        return name;
    }
    if let Ok(host) = hostname()
        && !host.trim().is_empty()
    {
        return host;
    }
    let mut b = [0u8; 8];
    let _ = getrandom::fill(&mut b);
    format!("kinference-{}", u64::from_le_bytes(b))
}

fn hostname() -> std::io::Result<String> {
    std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string())
}

#[cfg(test)]
mod tests;
