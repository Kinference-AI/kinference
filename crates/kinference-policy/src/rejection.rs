//! Why an `InferenceWorkerSet` produced no pool.
//!
//! The router's `worker_sets::desired_pools` produces these; `resolve`
//! projects them into the reasons status reports.

use super::PoolKey;

/// Why a worker set could not be materialized.
///
/// Every variant is an operator-fixable misconfiguration. They are distinct
/// because the fix differs: an unknown profile means editing the router's
/// config, a missing one means editing the CR.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// `spec.profile` absent. Required by the schema, so this means the object
    /// predates it or bypassed validation.
    MissingProfile,
    /// `spec.profile` names a profile the router does not define. Carries the
    /// defined names, since this is usually a typo.
    UnknownProfile { defined: Vec<String> },
    /// `spec.serviceRef.name` is empty, so there is nothing to watch.
    MissingServiceRef,
    /// A `spec.models` row carries no `name`, so its aliases would resolve to
    /// nothing. Only reachable by an object that bypassed validation.
    ModelMissingName,
    /// `spec.kvEvents` is present but carries no `port`: it declares that the
    /// engines publish events without saying where. Only reachable by an
    /// object that bypassed validation.
    KvEventsMissingPort,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProfile => write!(f, "spec.profile is required"),
            Self::UnknownProfile { defined } => write!(
                f,
                "spec.profile names an undefined routing profile; the router defines [{}]",
                defined.join(", ")
            ),
            Self::MissingServiceRef => write!(f, "spec.serviceRef.name is required"),
            Self::ModelMissingName => write!(
                f,
                "a spec.models row has no name; every row needs the name the \
                 engines report for that model"
            ),
            Self::KvEventsMissingPort => write!(
                f,
                "spec.kvEvents is present but has no port; omit the whole block if these \
                 engines publish no KV events"
            ),
        }
    }
}

/// Short machine-readable form, for a status condition's `reason` and for a
/// metric label. Kubernetes conditions want CamelCase here.
impl RejectReason {
    pub fn as_condition_reason(&self) -> &'static str {
        match self {
            Self::MissingProfile => "MissingProfile",
            Self::UnknownProfile { .. } => "UnknownProfile",
            Self::MissingServiceRef => "MissingServiceRef",
            Self::ModelMissingName => "ModelMissingName",
            Self::KvEventsMissingPort => "KvEventsMissingPort",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    pub key: PoolKey,
    pub reason: RejectReason,
}
