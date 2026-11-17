//! The fleet's own Prometheus families, registered in the registry the
//! server lends, plus the helpers every group registers its families with.

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, core::Collector,
};

/// Unwrap a family's construction. Names and label sets are static
/// literals, so a failure is a typo that every test building the family
/// catches.
pub(crate) fn family<C>(built: prometheus::Result<C>) -> C {
    built.expect("metric names and label sets are static literals")
}

/// Build a family and register a clone of it, so the returned handle and the
/// registry share storage.
fn register<C: Collector + Clone + 'static>(
    registry: &Registry,
    built: prometheus::Result<C>,
) -> C {
    let f = family(built);
    registry
        .register(Box::new(f.clone()))
        .expect("each family is registered once per registry");
    f
}

pub fn int_counter(registry: &Registry, name: &str, help: &str) -> IntCounter {
    register(registry, IntCounter::new(name, help))
}

pub fn int_counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> IntCounterVec {
    register(registry, IntCounterVec::new(Opts::new(name, help), labels))
}

pub fn int_gauge(registry: &Registry, name: &str, help: &str) -> IntGauge {
    register(registry, IntGauge::new(name, help))
}

pub fn int_gauge_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    register(registry, IntGaugeVec::new(Opts::new(name, help), labels))
}

pub fn histogram(registry: &Registry, name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    register(
        registry,
        Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets)),
    )
}

pub fn histogram_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    buckets: Vec<f64>,
    labels: &[&str],
) -> HistogramVec {
    register(
        registry,
        HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels),
    )
}

/// `kinference_index_events_total{kind}`. Source: [`IngestEvent::as_label`].
const INDEX_EVENT_KINDS: [&str; 3] = ["stored", "removed", "cleared"];

/// `kinference_worker_evictions_total{reason}`.
/// Source: `crate::eviction::EvictionReason::as_label`.
///
/// Only evictions kinference decided on; ordinary discovery removals are not
/// counted, so a rolling restart doesn't look like a fault.
const EVICTION_REASONS: [&str; 1] = ["model_mismatch"];

/// One KV-cache event kind, as the `kind` label on
/// `kinference_index_events_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestEvent {
    Stored,
    Removed,
    Cleared,
}

impl IngestEvent {
    pub fn as_label(self) -> &'static str {
        match self {
            IngestEvent::Stored => "stored",
            IngestEvent::Removed => "removed",
            IngestEvent::Cleared => "cleared",
        }
    }
}

/// The fleet's metric families. Clone-free: share via `Arc`.
pub struct FleetMetrics {
    /// `kinference_worker_evictions_total{reason}`: workers kinference removed itself.
    worker_evictions: IntCounterVec,
    /// `kinference_index_events_total{kind}`: KV events applied, by kind.
    index_events: IntCounterVec,
    /// `kinference_kv_events_skipped_total{workerset, reason}`: KV events
    /// that ingest didn't index.
    kv_events_skipped: IntCounterVec,
    /// `kinference_index_blocks_stored_total` / `_removed_total`: block churn.
    index_blocks_stored: IntCounter,
    index_blocks_removed: IntCounter,
}

impl FleetMetrics {
    /// Build the families, register them in `registry`, and seed every
    /// closed label set at zero so `rate()` has a series to read.
    pub fn new(registry: &Registry) -> Self {
        let worker_evictions = int_counter_vec(
            registry,
            "kinference_worker_evictions_total",
            "Workers kinference evicted itself, by reason. An eviction means the \
             worker was discovered and healthy but disagreed with the pool \
             about something the pool cannot vary per worker — so it is \
             removed to keep the rest serving on coherent assumptions. \
             Normal discovery removals are NOT counted: a rolling restart \
             would otherwise read as a fault. Non-zero means a CONFIG \
             mismatch; the WARN log carries both values and the fix",
            &["reason"],
        );
        let index_events = int_counter_vec(
            registry,
            "kinference_index_events_total",
            "KV-cache events applied to the radix tree, by kind (stored, \
             removed, cleared)",
            &["kind"],
        );
        let kv_events_skipped = int_counter_vec(
            registry,
            "kinference_kv_events_skipped_total",
            "KV-cache events that ingest skipped instead of indexing, by \
             worker set and reason. block_size_mismatch means the event's \
             block size differs from the worker set's kvBlockSize, so the \
             router predicts no prefix reuse from that event. \
             implausible_block_size means a block size that no engine uses \
             for attention blocks. nothing_stored means a removal from a \
             worker that has no indexed block. unknown_kind and \
             unlearned_group_idx mean an event whose attention group \
             ingest could not classify. Events of non-main attention \
             groups are not counted",
            &["workerset", "reason"],
        );
        let index_blocks_stored = int_counter(
            registry,
            "kinference_index_blocks_stored_total",
            "Blocks added to the radix tree",
        );
        let index_blocks_removed = int_counter(
            registry,
            "kinference_index_blocks_removed_total",
            "Blocks evicted from the radix tree",
        );
        for reason in EVICTION_REASONS {
            worker_evictions.with_label_values(&[reason]);
        }
        for kind in INDEX_EVENT_KINDS {
            index_events.with_label_values(&[kind]);
        }
        Self {
            worker_evictions,
            index_events,
            kv_events_skipped,
            index_blocks_stored,
            index_blocks_removed,
        }
    }

    /// One worker evicted by kinference's own decision.
    ///
    /// `reason` comes from [`crate::eviction::EvictionReason::as_label`] so the
    /// label and the WARN log cannot name different things.
    pub fn record_worker_eviction(&self, reason: &str) {
        self.worker_evictions.with_label_values(&[reason]).inc();
    }

    /// One KV-cache event that ingest skipped. `reason` must come from a
    /// closed set, since it becomes a label value.
    pub fn record_kv_event_skipped(&self, workerset: &str, reason: &'static str) {
        self.kv_events_skipped
            .with_label_values(&[workerset, reason])
            .inc();
    }

    /// One KV-cache event as ingest decoded it, with the blocks it carried.
    pub fn record_index_event(&self, kind: IngestEvent, blocks: u64) {
        self.index_events
            .with_label_values(&[kind.as_label()])
            .inc();
        match kind {
            IngestEvent::Stored => self.index_blocks_stored.inc_by(blocks),
            IngestEvent::Removed => self.index_blocks_removed.inc_by(blocks),
            IngestEvent::Cleared => {}
        }
    }
}

#[cfg(test)]
mod tests;
