//! The control plane's own Prometheus families, registered in the registry
//! the server lends.

use prometheus::{IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry};

use kinference_fleet::metrics::{int_counter, int_counter_vec, int_gauge, int_gauge_vec};

/// `kinference_workerset_rejected_total{reason}`.
/// Source: `kinference_policy::rejection::RejectReason::as_condition_reason`.
/// CamelCase, since these double as Kubernetes condition reasons.
const WORKERSET_REJECT_REASONS: [&str; 5] = [
    "MissingProfile",
    "UnknownProfile",
    "MissingServiceRef",
    "ModelMissingName",
    "KvEventsMissingPort",
];

/// The control plane's metric families. Clone-free: share via `Arc`.
pub struct ControllerMetrics {
    /// `kinference_workerset_rejected_total{reason}`.
    workerset_rejected: IntCounterVec,
    /// `kinference_leader`: 1 when this replica holds the Lease, else 0.
    ///
    /// Exactly one pod should report 1. None means election is broken or
    /// another deployment holds a same-named Lease (check `holderIdentity`).
    leader: IntGauge,
    /// `kinference_leader_transitions_total`: leadership changes observed by
    /// this replica. A steady rate means flapping.
    leader_transitions: IntCounter,
    /// `kinference_leader_election_errors_total`: failed Lease reads/writes.
    ///
    /// Election fails closed, so rising errors with `kinference_leader` at 0
    /// everywhere means no replica writes status.
    leader_election_errors: IntCounter,
    /// `kinference_policy_warnings{policy,code}`: how many findings each policy
    /// carries in `status.warnings`, by code.
    policy_warnings: IntGaugeVec,
}

impl ControllerMetrics {
    /// Build the families, register them in `registry`, and seed the
    /// rejection reasons at zero so `rate()` has a series to read.
    pub fn new(registry: &Registry) -> Self {
        let workerset_rejected = int_counter_vec(
            registry,
            "kinference_workerset_rejected_total",
            "InferenceWorkerSets that produced no pool, by why",
            &["reason"],
        );
        let leader = int_gauge(
            registry,
            "kinference_leader",
            "1 when this replica holds the leader Lease and writes CRD status, else 0",
        );
        let leader_transitions = int_counter(
            registry,
            "kinference_leader_transitions_total",
            "Leadership transitions observed by this replica",
        );
        let leader_election_errors = int_counter(
            registry,
            "kinference_leader_election_errors_total",
            "Failed leader-election Lease reads or writes",
        );
        let policy_warnings = int_gauge_vec(
            registry,
            "kinference_policy_warnings",
            "Findings an InferencePolicy carries, by warning code — the list in \
             its status.warnings. Non-zero means some part of the policy does not \
             do what it says; the code names which kind. Published by the status \
             writer, so only the leader exports it",
            &["policy", "code"],
        );
        for reason in WORKERSET_REJECT_REASONS {
            workerset_rejected.with_label_values(&[reason]);
        }
        Self {
            workerset_rejected,
            leader,
            leader_transitions,
            leader_election_errors,
            policy_warnings,
        }
    }

    /// A worker set the operator declared but the router could not use.
    /// Counted on the transition into rejection, not per watch tick.
    pub fn record_workerset_rejected(&self, reason: &str) {
        self.workerset_rejected.with_label_values(&[reason]).inc();
    }

    /// Publish this replica's leadership state. Counts a transition only on an
    /// actual change, so the counter measures flapping rather than tick rate.
    pub fn set_leader(&self, is_leader: bool) {
        let want = i64::from(is_leader);
        if self.leader.get() != want {
            self.leader.set(want);
            self.leader_transitions.inc();
        }
    }

    /// A Lease read or write failed.
    pub fn record_leader_election_error(&self) {
        self.leader_election_errors.inc();
    }

    /// Publish per-policy warning counts, replacing the previous pass.
    ///
    /// `reset()` first, so fixed or deleted policies drop out.
    pub fn set_policy_warnings(&self, rows: &[(String, &'static str, i64)]) {
        self.policy_warnings.reset();
        for (policy, code, n) in rows {
            self.policy_warnings
                .with_label_values(&[policy.as_str(), code])
                .set(*n);
        }
    }
}

#[cfg(test)]
mod tests;
