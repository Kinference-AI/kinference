//! Leader election over a `coordination.k8s.io/v1` Lease.
//!
//! Exactly one replica writes CRD status and `BackendBinding`s; every
//! replica still routes. The holder renews inside a grace window before
//! expiry. A rival waits out the current claim before trying. Takeovers
//! bump `leaseTransitions`, and a clean shutdown vacates the lease so that
//! the peer takes over at once. Each replica times a claim on its own
//! monotonic clock, from when it first saw that claim, and never from the
//! holder's `renewTime`. Clock skew between replicas therefore can't cut a
//! claim short. Writes are optimistic-concurrency replaces that send the
//! `resourceVersion` of the last-seen lease. Two replicas that race for
//! the same claim therefore can't both win: the loser gets a conflict,
//! re-reads, and backs off.

use std::error::Error as StdError;
use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use k8s_openapi::jiff::Timestamp;
use kube::api::ObjectMeta;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::Instant;

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

/// The last Lease that this replica read or wrote, timed on its own clock.
#[derive(Debug, Clone)]
struct Seen {
    lease: Lease,
    /// When this replica first read the current spec, or sent the claim
    /// that wrote it.
    since: Instant,
    /// This process wrote the current spec. A claim under our identity that
    /// an earlier process left is re-taken, not trusted.
    ours: bool,
}

impl Seen {
    /// Fold a freshly read `lease` into `prev`. An unchanged spec keeps
    /// `prev`'s `since` and `ours`.
    fn observe(prev: Option<Seen>, lease: Lease, now: Instant) -> Seen {
        match prev {
            Some(p) if p.lease.spec == lease.spec => Seen {
                lease,
                since: p.since,
                ours: p.ours,
            },
            _ => Seen {
                lease,
                since: now,
                ours: false,
            },
        }
    }

    fn holder(&self) -> Option<&str> {
        let spec = self.lease.spec.as_ref()?;
        spec.holder_identity.as_deref().filter(|h| !h.is_empty())
    }

    /// When the current claim lapses on this replica's clock. `None` for a
    /// claim without a valid duration.
    fn expiry(&self) -> Option<Instant> {
        let secs = self.lease.spec.as_ref()?.lease_duration_seconds?;
        self.since
            .checked_add(Duration::from_secs(u64::try_from(secs).ok()?))
    }

    fn leads(&self, now: Instant) -> bool {
        self.ours && self.expiry().is_some_and(|expiry| now < expiry)
    }
}

/// What the elector should do next, decided from the last-seen
/// lease. Pure so the claiming rules are unit-testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    /// Write a claim now: the lease is free, expired, malformed, or ours
    /// and due for renewal.
    Claim,
    /// Sleep until this instant, then re-decide. Someone else's claim is
    /// current, or ours doesn't need renewing yet.
    WaitUntil(Instant),
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
    // Kept across re-reads, so that an unchanged claim keeps its `since`.
    let mut seen: Option<Seen> = None;
    // Whether `seen` must be re-read before the next decision.
    let mut stale = true;
    // Whether the current run of lease API failures has been logged; reset on
    // success, so a failure after a recovery warns again.
    let mut failing = false;

    loop {
        if stale {
            match fetch_or_create(&api, namespace, &params.name).await {
                Ok(lease) => {
                    failing = false;
                    stale = false;
                    seen = Some(Seen::observe(seen.take(), lease, Instant::now()));
                }
                Err(e) => {
                    if !std::mem::replace(&mut failing, true) {
                        tracing::warn!(
                            error = &e as &dyn StdError,
                            "failed to read or create the lease; retrying"
                        );
                    }
                }
            }
        }

        let now = Instant::now();
        let wait = match seen.as_mut().filter(|_| !stale) {
            // The read above failed: retry it.
            None => params.retry,
            Some(s) => match next_action(now, s, &identity, params.renew_grace) {
                Action::Claim => {
                    match claim(&api, namespace, &identity, s.lease.clone(), &params).await {
                        Ok(lease) => {
                            failing = false;
                            // `now` precedes the write, so the claim's expiry
                            // on our clock is never later than on a rival's.
                            *s = Seen {
                                lease,
                                since: now,
                                ours: true,
                            };
                            // Re-decide at once, which lands in WaitUntil.
                            Duration::ZERO
                        }
                        Err(e) => {
                            if !is_conflict(&e) && !std::mem::replace(&mut failing, true) {
                                tracing::warn!(
                                    error = &e as &dyn StdError,
                                    "lease claim failed; retrying"
                                );
                            }
                            s.ours = false;
                            stale = true;
                            params.retry
                        }
                    }
                }
                // Wake just past the boundary so the re-decide lands on the
                // other side of it.
                Action::WaitUntil(t) => {
                    t.saturating_duration_since(now) + Duration::from_millis(100)
                }
            },
        };

        publish(&tx, seen.as_ref().is_some_and(|s| s.leads(Instant::now())));

        if !wait.is_zero() {
            tokio::select! {
                biased;
                () = &mut shutdown => break,
                () = tokio::time::sleep(wait) => {}
            }
            // A rival's claim may have been renewed or vacated while we
            // slept; never claim from a stale view.
            if !seen.as_ref().is_some_and(|s| s.leads(Instant::now())) {
                stale = true;
            }
        }
    }

    // Vacate a claim that we wrote, so the peer claims at once instead of
    // waiting out our expiry. Optimistic concurrency keeps this from
    // clearing a rival's newer claim.
    if let Some(s) = seen.filter(|s| s.ours)
        && let Err(e) = vacate(&api, namespace, s.lease, &params.name).await
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

/// Decide from the last-seen lease.
fn next_action(now: Instant, seen: &Seen, identity: &str, renew_grace: Duration) -> Action {
    let Some(holder) = seen.holder() else {
        return Action::Claim;
    };
    // A claim without a duration can't be honored.
    let Some(expiry) = seen.expiry() else {
        return Action::Claim;
    };
    if now >= expiry {
        return Action::Claim;
    }
    if holder != identity {
        return Action::WaitUntil(expiry);
    }
    if !seen.ours {
        return Action::Claim;
    }
    match expiry.checked_sub(renew_grace) {
        Some(renew_at) if now < renew_at => Action::WaitUntil(renew_at),
        _ => Action::Claim,
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
    params: &LeaseParams,
) -> Result<Lease, kube::Error> {
    let now = Timestamp::now();
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

fn publish(tx: &watch::Sender<bool>, leader: bool) {
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
    let _ = getrandom::getrandom(&mut b);
    format!("kinference-{}", u64::from_le_bytes(b))
}

fn hostname() -> std::io::Result<String> {
    std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string())
}

#[cfg(test)]
mod tests;
