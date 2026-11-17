//! Worker eviction signaling.
//!
//! A worker is torn down when discovery removes it, or on a homogeneity
//! violation: the metrics poller sees a `/v1/models` list that disagrees with
//! the pool's canonical set.
//!
//! The metrics poller requests eviction by sending an [`EvictionRequest`];
//! `fleet::coordinator` owns the receiver and does the teardown, keeping
//! lifecycle logic in one place.

use tokio::sync::mpsc;

/// One worker to evict, with the reason that triggered it.
#[derive(Debug, Clone)]
pub struct EvictionRequest {
    pub worker_id_num: u64,
    pub reason: EvictionReason,
}

/// Why a worker is being evicted.
#[derive(Debug, Clone)]
pub enum EvictionReason {
    /// The worker's `/v1/models` list differs from the canonical set pinned by
    /// the pool's first non-empty report.
    ModelMismatch {
        expected: Vec<String>,
        observed: Vec<String>,
    },
}

impl EvictionReason {
    /// The `reason` label on `kinference_worker_evictions_total`.
    ///
    /// Separate from `Display`, whose mismatched values are unbounded and must
    /// not reach a label.
    pub fn as_label(&self) -> &'static str {
        match self {
            EvictionReason::ModelMismatch { .. } => "model_mismatch",
        }
    }
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionReason::ModelMismatch { expected, observed } => write!(
                f,
                "served models {observed:?} differ from the worker set's {expected:?}"
            ),
        }
    }
}

/// The eviction channel's sender, cloned into each subsystem that can request
/// an eviction.
pub type EvictionTx = mpsc::UnboundedSender<EvictionRequest>;

/// The eviction channel's receiver, drained by the coordinator.
pub type EvictionRx = mpsc::UnboundedReceiver<EvictionRequest>;

/// Build the eviction channel. Called once at startup.
pub fn channel() -> (EvictionTx, EvictionRx) {
    mpsc::unbounded_channel()
}
