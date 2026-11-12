use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use kinference_policy::PoolKey;
use serde::Deserialize;

use kinference_controller::ControllerConfig;
use kinference_controller::leader::{self, LeaderElectionConfig};
use kinference_controller::policy::PolicyConfig;
use kinference_controller::policy::worker_sets::KubernetesConfig;
use kinference_fleet::FleetConfig;
use kinference_fleet::coordinator::WorkerTaskSettings;
use kinference_fleet::discovery::DiscoveryConfig;
use kinference_fleet::ingest::{DEFAULT_REPLAY_TIMEOUT, IngestConfig};
use kinference_fleet::pool::{PoolSpec, ReapPolicy};
use kinference_fleet::profile::ProfileConfig;
use kinference_router::body_store::BodyLimits;
use kinference_router::metrics::MetricsConfig;
use kinference_router::service::RouterConfig;
use kinference_router::tokenizer::TokenizeConfig;

#[derive(Debug, Clone, Deserialize)]
// Unknown top-level keys are a startup error, so a mistyped or renamed key
// can't silently fall back to its default. Not applied to the nested sections
// or to `WorkerConfig`.
#[serde(deny_unknown_fields)]
pub struct Config {
    /// gRPC bind address.
    pub listen: SocketAddr,
    /// Named bundles of routing tuning that worker sets reference by name.
    ///
    /// Part of a profile is calibrated against hardware and workload, so name
    /// profiles accordingly (`qwen32b-h100-tp2`, not `qwen32b`).
    #[serde(default, rename = "profile")]
    pub profiles: Vec<ProfileConfig>,
    /// The worker sets this instance manages. Workers within a set share model,
    /// engine and `kv_block_size`. Requests are routed by `model` through
    /// the model index; when several sets serve one model, all are
    /// scored (see `service::best_group`). At least one is required unless
    /// `kubernetes.enabled` is set.
    #[serde(default, rename = "workerset")]
    pub worker_sets: Vec<WorkerSetConfig>,
    /// Milliseconds between `/metrics` scrapes per worker. Defaults to 1000;
    /// clamped to at least 1.
    #[serde(default)]
    pub metrics_poll_ms: Option<u64>,
    /// Wall-clock budget (ms) for a single `replay_gap` call against the DEALER
    /// socket; events left unreplayed are picked up by later gap detection.
    /// Defaults to 60000. The initial drain can take 40-60s on a long event
    /// history (150-250 events/sec observed).
    #[serde(default)]
    pub replay_timeout_ms: Option<u64>,
    /// Seconds between `/v1/models` refreshes per worker. The list only changes
    /// when LoRA adapters are loaded or unloaded. Defaults to 30.
    #[serde(default)]
    pub models_refresh_secs: Option<u64>,
    /// TTL, in seconds, for a pick's in-flight bookkeeping when the caller
    /// never reports. Must exceed the worst-case *full* response time, since a
    /// non-streaming caller only reports at completion; otherwise its state is
    /// swept mid-flight. Defaults to 600.
    #[serde(default)]
    pub observation_ttl_secs: Option<u64>,
    /// Server-wide ceiling on buffered request bodies (see `kinference_router::body_store`),
    /// including uploads in progress. Once full, pushes are refused with
    /// `RESOURCE_EXHAUSTED`; keep it well under the container memory limit.
    /// Defaults to 262144 KB (256 MiB).
    ///
    /// 1 KB = 1024 bytes. `kinference_body_buffer_bytes` reports in bytes.
    #[serde(default)]
    pub body_max_total_kb: Option<u64>,
    /// Per-request ceiling on a buffered body, checked against
    /// `kinference-declared-length` before the first chunk. A 150k-token prompt
    /// is roughly 1 MiB of JSON. Defaults to 8192 KB (8 MiB).
    #[serde(default)]
    pub body_max_request_kb: Option<u64>,
    /// TTL, in seconds, for a buffered body awaiting `Pick` or `PullBody`. This
    /// is what guarantees release when a caller vanishes, since `Abort` is
    /// best-effort. Defaults to 60.
    #[serde(default)]
    pub body_ttl_secs: Option<u64>,
    /// Maximum seconds between two `PushBody` chunks before the push fails with
    /// `DEADLINE_EXCEEDED`. The push as a whole has no deadline, since it may
    /// span a slow client upload. Defaults to 30.
    #[serde(default)]
    pub body_idle_timeout_secs: Option<u64>,
    /// HTTP bind address for the admin listener: `/readyz`, `/metrics` and `/api`.
    /// Unset disables them.
    #[serde(default)]
    pub admin_listen: Option<SocketAddr>,
    #[serde(default)]
    pub tokenize: TokenizeConfig,
    #[serde(default)]
    pub policy: PolicyConfig,
    /// Labelling of the metrics kinference *publishes* (`kinference_router::metrics`). Not to
    /// be confused with `metrics_poll_ms` and friends, which concern scraping
    /// the engines.
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Derive worker sets from `InferenceWorkerSet` CRs. Independent of
    /// `policy.enabled`, so CRD discovery doesn't require CRD routing rules.
    #[serde(default)]
    pub kubernetes: KubernetesConfig,
    #[serde(default)]
    pub leader_election: LeaderElectionConfig,
    /// The ingest switches from the environment, plus the resolved
    /// `replay_timeout_ms`. Filled by [`Self::read_env`]; not a file key.
    #[serde(skip)]
    pub ingest: IngestConfig,
}

/// A `workerset` entry: one config-declared worker set. The Kubernetes equivalent is an
/// `InferenceWorkerSet`; both reduce to a [`PoolSpec`].
#[derive(Debug, Clone, Deserialize)]
pub struct WorkerSetConfig {
    /// Must be unique.
    pub name: String,
    /// The profile supplying this set's tuning. Required.
    pub profile: String,
    /// KV-cache block size shared by every worker in the set. A worker
    /// reporting a different size is evicted.
    ///
    /// Typical values: `16` for standard vLLM, `64` for Gemma 3/4, `1` for
    /// SGLang with `--page-size 1`.
    pub kv_block_size: u32,
    /// Dedicated `/render` URL (full URL including path). When unset, renders
    /// rotate across the set's own workers.
    #[serde(default)]
    pub render_url: Option<String>,
    #[serde(default)]
    pub discovery: DiscoveryConfig,
}

impl Config {
    /// Load the JSON config file at `path`, as the Helm chart renders it with
    /// `toJson`.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config from {}", path.display()))?;
        let mut cfg: Self = serde_json::from_str(&text)
            .with_context(|| format!("parsing config from {}", path.display()))?;
        cfg.validate()?;
        cfg.read_env();
        Ok(cfg)
    }

    /// Read the environment into the fields the file cannot set. This runs
    /// once, here, so no subsystem reads `std::env` after startup.
    fn read_env(&mut self) {
        self.ingest = IngestConfig {
            disabled: std::env::var("KINFERENCE_DISABLE_INGEST").is_ok(),
            replay: std::env::var("KINFERENCE_ENABLE_REPLAY").is_ok(),
            replay_timeout: self
                .replay_timeout_ms
                .map(Duration::from_millis)
                .unwrap_or(DEFAULT_REPLAY_TIMEOUT),
        };
        self.tokenize.local.hub_token = std::env::var("HUGGING_FACE_HUB_TOKEN")
            .ok()
            .filter(|t| !t.is_empty());
        self.leader_election.identity = leader::identity(std::env::var("POD_NAME").ok());
    }

    /// Reject leader-election timings that can't work. In particular,
    /// `renew_grace >= lease_duration` would make the elector renew in a tight
    /// loop against the apiserver.
    fn validate_leader_election(&self) -> Result<()> {
        let le = &self.leader_election;
        if !le.enabled {
            return Ok(());
        }
        if le.lease_name.trim().is_empty() {
            anyhow::bail!("leader_election.lease_name must be non-empty");
        }
        for (field, v) in [
            ("lease_duration_secs", le.lease_duration_secs),
            ("renew_grace_secs", le.renew_grace_secs),
            ("retry_secs", le.retry_secs),
        ] {
            if v == 0 {
                anyhow::bail!("leader_election.{field} must be > 0");
            }
        }
        if le.renew_grace_secs >= le.lease_duration_secs {
            anyhow::bail!(
                "leader_election.renew_grace_secs ({}) must be < lease_duration_secs ({}). \
                 The holder renews once it is within `renew_grace_secs` of expiry, so a grace \
                 window covering the whole duration means it is always due for renewal: the \
                 elector never sleeps and rewrites the Lease in a tight loop against the \
                 apiserver.",
                le.renew_grace_secs,
                le.lease_duration_secs
            );
        }
        Ok(())
    }

    /// Resolve every `workerset` entry against its profile. An unknown
    /// profile is an error listing the defined ones.
    pub fn pool_specs(&self) -> Result<Vec<PoolSpec>> {
        let by_name: std::collections::HashMap<&str, &ProfileConfig> =
            self.profiles.iter().map(|p| (p.name.as_str(), p)).collect();
        self.worker_sets
            .iter()
            .map(|ws| {
                let profile = by_name.get(ws.profile.as_str()).ok_or_else(|| {
                    let mut defined: Vec<&str> =
                        self.profiles.iter().map(|p| p.name.as_str()).collect();
                    defined.sort_unstable();
                    anyhow::anyhow!(
                        "workerset `{}` names profile `{}`, which is not defined. \
                         Defined profiles: [{}]",
                        ws.name,
                        ws.profile,
                        defined.join(", ")
                    )
                })?;
                Ok(PoolSpec {
                    key: PoolKey::local(&ws.name),
                    name: ws.name.clone(),
                    profile: profile.name.clone(),
                    kv_block_size: ws.kv_block_size,
                    predictor: profile.predictor.clone(),
                    picker: profile.picker.clone(),
                    tokenize_mode: profile.tokenize.mode,
                    render_url: ws.render_url.clone(),
                    discovery: ws.discovery.clone(),
                    // Only CRs can set these.
                    models_refresh_secs: None,
                    static_models: None,
                })
            })
            .collect()
    }

    /// TTL of a pick's in-flight bookkeeping: the pending observation, the
    /// inflight counter and the inflight-prefix entry. Must exceed the
    /// worst-case full response time; see `observation_ttl_secs`.
    fn observation_ttl(&self) -> Duration {
        Duration::from_secs(self.observation_ttl_secs.unwrap_or(600))
    }

    /// The router's settings.
    pub fn router(&self) -> RouterConfig {
        // Config is in KB; everything downstream is in bytes. `saturating_mul`
        // so an absurd value clamps instead of wrapping to a tiny cap.
        const KB: u64 = 1024;
        RouterConfig {
            tokenize: self.tokenize.clone(),
            metrics: self.metrics.clone(),
            bodies: BodyLimits {
                max_total_bytes: self
                    .body_max_total_kb
                    .unwrap_or(256 * 1024)
                    .saturating_mul(KB),
                max_request_bytes: self
                    .body_max_request_kb
                    .unwrap_or(8 * 1024)
                    .saturating_mul(KB),
                // Much shorter than `observation_ttl`: bodies are large and
                // consumed immediately.
                ttl: Duration::from_secs(self.body_ttl_secs.unwrap_or(60)),
                idle_timeout: Duration::from_secs(self.body_idle_timeout_secs.unwrap_or(30)),
            },
            observation_ttl: self.observation_ttl(),
        }
    }

    /// The fleet's settings, with every `workerset` entry resolved.
    pub fn fleet(&self) -> Result<FleetConfig> {
        let observation_ttl = self.observation_ttl();
        Ok(FleetConfig {
            specs: self.pool_specs()?,
            workers: WorkerTaskSettings {
                poll_ms: self.metrics_poll_ms.unwrap_or(1000),
                models_refresh_secs: self.models_refresh_secs.unwrap_or(30),
                ingest: self.ingest,
                observation_ttl,
            },
            reap: ReapPolicy {
                linger: Duration::from_secs(30),
                // A retired pool must outlive observations that may still
                // reference it.
                backstop: observation_ttl.max(Duration::from_secs(600)),
            },
        })
    }

    /// The control plane's settings.
    pub fn controller(&self) -> ControllerConfig {
        ControllerConfig {
            kubernetes: self.kubernetes.clone(),
            policy: self.policy.clone(),
            leader_election: self.leader_election.clone(),
            profiles: self.profiles.clone(),
        }
    }

    /// Validate cross-set invariants.
    fn validate(&self) -> Result<()> {
        self.kubernetes.validate().map_err(anyhow::Error::msg)?;
        if self.worker_sets.is_empty() && !self.kubernetes.enabled {
            anyhow::bail!(
                "at least one workerset is required (or set kubernetes.enabled = true \
                 to derive them from InferenceWorkerSet CRs)"
            );
        }
        if self.kubernetes.enabled && self.profiles.is_empty() {
            // Every InferenceWorkerSet names a profile, so none could be
            // materialized.
            anyhow::bail!(
                "kubernetes.enabled = true but no profile is defined; every \
                 InferenceWorkerSet names a profile, so none could be materialized"
            );
        }
        self.validate_leader_election()?;
        let mut seen_profiles = std::collections::HashSet::new();
        for p in &self.profiles {
            if p.name.trim().is_empty() {
                anyhow::bail!("every profile must have a non-empty `name`");
            }
            if !seen_profiles.insert(p.name.clone()) {
                anyhow::bail!("duplicate profile name: {}", p.name);
            }
        }
        let specs = self.pool_specs()?;
        let mut seen_set_names = std::collections::HashSet::new();
        for spec in &specs {
            if spec.name.trim().is_empty() {
                anyhow::bail!("every workerset must have a non-empty `name`");
            }
            if !seen_set_names.insert(spec.name.clone()) {
                anyhow::bail!("duplicate workerset name: {}", spec.name);
            }
            if spec.kv_block_size == 0 {
                anyhow::bail!("workerset {}: kv_block_size must be > 0", spec.name);
            }
            if spec
                .discovery
                .service_name
                .as_deref()
                .unwrap_or("")
                .is_empty()
            {
                anyhow::bail!(
                    "workerset {}: discovery.service_name is required",
                    spec.name
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
