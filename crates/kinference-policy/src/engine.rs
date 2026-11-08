//! Policy evaluation: rule selection, candidate filtering, and ranking.
//!
//! Pure over its inputs. Candidate generation needs tokenization and the KV
//! index, so it is async and lives in the router's `monitor`; this module takes
//! an already-scored candidate set and decides.
//!
//! The caller orchestrates, since `onEmpty: FallThrough` needs a fresh
//! candidate set per rule:
//!
//! ```text
//! for rule in matching_rules(policy, attrs):
//!     models = models_to_score(rule.route, attrs.requested_model)   // may sample by weight
//!     candidates = <async scoring>
//!     match evaluate(rule.route, attrs, candidates):
//!         Chosen  -> done
//!         Empty   -> Reject, or continue to the next rule on FallThrough
//! // no rule matched at all -> spec.unmatched
//! ```
//!
//! Within `evaluate`: the built-in context-window fit, then the author's
//! exclusions, then the ordered `select` tiers, then `tieBreak`.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::sync::Arc;

use super::PoolId;
use super::compile::{
    BackendMatch, CompiledPolicy, CompiledRoute, CompiledRule, ModelChoice, Rank,
};
use super::crd::{BackendType, OnEmpty, TieBreak, Unmatched};
use super::predicate::{CandidateAttrs, RequestAttrs};

/// One `(model, destination)` pair, the unit a policy chooses between, so one
/// decision covers both which model serves the request and where it runs.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Client-facing name: the first name that led to this candidate.
    /// `attrs.model_names` holds every name that led to it.
    pub canonical_model: String,
    /// What the engine actually wants in the body. Differs from
    /// `canonical_model` when an alias resolved, and usually differs for a
    /// provider, whose `models[].name` is the vendor's own spelling.
    pub upstream_model: String,
    /// The destination's own name; `backend.name` in predicates.
    pub backend: String,
    /// Where this would go, and what can be known about it.
    pub target: CandidateTarget,
    /// Precomputed predicate inputs for this candidate.
    pub attrs: CandidateAttrs,
    /// The destination's labels, for `select` tiers that subset by
    /// `selector.matchLabels`. Shared: every candidate from one destination
    /// carries the same map.
    pub labels: Arc<BTreeMap<String, String>>,
}

/// The two kinds of destination.
///
/// A provider candidate has no endpoint, so no per-endpoint signals, no pool
/// (no radix tree or inflight tracking), and no per-request prediction, only an
/// aggregate.
#[derive(Debug, Clone)]
pub enum CandidateTarget {
    Local {
        pool_id: PoolId,
        endpoint: String,
        /// Index into the caller's per-(pool,model) score list, so the winner can
        /// be mapped back to its full scoring state for the critical section.
        score_idx: usize,
        /// `picker::predicted_wait_ms`, the CRD's `PredictedTTFT`.
        predicted_ttft_ms: f32,
    },
    Provider {
        /// The `InferenceProvider`, for attribution.
        provider: ObjectKey,
        /// The `GatewayBackend` the caller must resolve. kinference never reads it.
        backend_ref: ObjectKey,
        /// `InferenceProvider.spec.path`, or `None` to dispatch on the path the
        /// client sent. Carried here so the winner doesn't need a second lookup
        /// against a possibly newer snapshot.
        path: Option<String>,
        /// Trailing-window p95 of caller-reported TTFT. `None` until the first
        /// report, and still eligible then, since a provider can't get an
        /// observation without being picked.
        observed_ttft_ms: Option<f32>,
    },
}

impl Candidate {
    /// Short label for traces: `backend/model@endpoint`, or `backend/model@<provider>`.
    pub fn label(&self) -> String {
        match &self.target {
            CandidateTarget::Local { endpoint, .. } => {
                format!("{}/{}@{}", self.backend, self.upstream_model, endpoint)
            }
            CandidateTarget::Provider { .. } => {
                format!("{}/{}@<provider>", self.backend, self.upstream_model)
            }
        }
    }

    pub fn backend_type(&self) -> BackendType {
        match self.target {
            CandidateTarget::Local { .. } => BackendType::Local,
            CandidateTarget::Provider { .. } => BackendType::Provider,
        }
    }

    /// This candidate's value for a tier's objective, or `None` when it cannot
    /// answer.
    ///
    /// `None` must not be defaulted: `0.0` would be the best possible score.
    pub fn objective_ms(&self, rank: &Rank) -> Option<f32> {
        match (rank, &self.target) {
            (
                Rank::MinimizePredictedTtft | Rank::MinimizeEstimatedTtft,
                CandidateTarget::Local {
                    predicted_ttft_ms, ..
                },
            ) => Some(*predicted_ttft_ms),
            // A provider has nothing to compute PredictedTTFT from. The CRD
            // forbids this for a well-formed policy.
            (Rank::MinimizePredictedTtft, CandidateTarget::Provider { .. }) => None,
            (
                Rank::MinimizeEstimatedTtft,
                CandidateTarget::Provider {
                    observed_ttft_ms, ..
                },
            ) => *observed_ttft_ms,
            (Rank::Order(_), _) => None,
        }
    }
}

/// A namespaced object, named rather than resolved.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectKey {
    pub namespace: String,
    pub name: String,
}

impl ObjectKey {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub fn label(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
}

/// What to score, derived from one rule's `consider`.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoreTarget {
    /// Canonical (client-facing) model name.
    pub model: String,
    /// Which worker sets this target may use.
    pub backends: BackendMatch,
}

/// Why no candidate was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Every candidate was excluded (or none existed) and `onEmpty: Reject`.
    OnEmpty,
    /// No rule matched and `unmatched: Reject`.
    Unmatched,
}

/// Per-decision trace. Since aliasing lets two upstream models share one name,
/// only a trace shows which model and endpoint served a request.
#[derive(Debug, Clone, Default)]
pub struct Trace {
    pub rule: Option<String>,
    /// Candidates generated, before any filtering.
    pub considered: Vec<String>,
    /// `(candidate, what removed it)`. Includes the built-in context-window fit.
    pub eliminated: Vec<(String, String)>,
    /// Candidates still standing when selection ran.
    pub survivors: Vec<String>,
    /// Which `select` tier produced the winner.
    pub tier: Option<usize>,
    /// The winner's objective value.
    pub winner_ttft_ms: Option<f32>,
    /// How many candidates tied with the winner and were broken by `tieBreak`.
    pub tied: usize,
}

impl Trace {
    /// Compact single-line rendering for the debug log.
    pub fn summary(&self) -> String {
        let mut s = format!(
            "rule={} considered={} survivors={}",
            self.rule.as_deref().unwrap_or("<unmatched>"),
            self.considered.len(),
            self.survivors.len()
        );
        if let Some(t) = self.tier {
            s.push_str(&format!(" tier={t}"));
        }
        if let Some(ms) = self.winner_ttft_ms {
            s.push_str(&format!(" objective_ms={ms:.1}"));
        }
        if self.tied > 1 {
            s.push_str(&format!(" tied={}", self.tied));
        }
        for (cand, why) in &self.eliminated {
            s.push_str(&format!(" [-{cand} by {why}]"));
        }
        s
    }
}

/// Outcome of evaluating ONE rule's route against a candidate set.
///
/// `Chosen` is much larger than `Empty` but not boxed: only one exists per
/// request.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Outcome {
    Chosen {
        candidate: Candidate,
        trace: Trace,
    },
    /// Nothing survived. The caller applies `onEmpty`.
    Empty {
        trace: Trace,
    },
}

/// Rules that match this request, in list order.
///
/// The caller normally takes the first; the rest matter only for
/// `onEmpty: FallThrough`. A rule with no `matches` matches everything.
pub fn matching_rules<'a>(
    policy: &'a CompiledPolicy,
    attrs: &RequestAttrs,
) -> Vec<&'a CompiledRule> {
    policy.rules.iter().filter(|r| r.matches(attrs)).collect()
}

/// Rules that could match this request once its token count is known, in
/// list order: [`matching_rules`] with every `inputTokens` term taken as
/// true.
pub fn possibly_matching_rules<'a>(
    policy: &'a CompiledPolicy,
    attrs: &RequestAttrs,
) -> Vec<&'a CompiledRule> {
    policy.rules.iter().filter(|r| r.may_match(attrs)).collect()
}

/// Resolve a route's `consider` into the set of `(model, backends)` pairs to
/// score.
///
/// Weighted groups are sampled first, before exclusions and selection, so a
/// 70/30 split composes with `minimize` picking the best worker inside the
/// chosen group. Unweighted groups union, which is how `consider` adds a model
/// the client didn't ask for.
///
/// When a group takes its models from the request, the target is the one name
/// that the client asked for (`RequestAttrs::requested_model`). The caller
/// resolves that name through each destination's own alias table, so only
/// destinations that declare the name become eligible. The resolved upstream
/// would miss a provider that spells the model differently, and every alias of
/// that upstream would let one worker set serve a name that only another set
/// declares.
pub fn models_to_score(route: &CompiledRoute, requested_model: &str) -> Vec<ScoreTarget> {
    let groups: Vec<&super::compile::CompiledConsiderGroup> = if route.consider.is_empty() {
        // Default: the requested name, on every destination that the pool allows.
        return vec![ScoreTarget {
            model: requested_model.to_string(),
            backends: BackendMatch::default(),
        }];
    } else if route.weighted() {
        match sample_weighted(&route.consider) {
            Some(i) => vec![&route.consider[i]],
            None => return Vec::new(),
        }
    } else {
        route.consider.iter().collect()
    };

    let mut out: Vec<ScoreTarget> = Vec::new();
    for g in groups {
        let models: Vec<String> = match &g.models {
            ModelChoice::FromRequest => vec![requested_model.to_string()],
            ModelChoice::Names(n) => n.clone(),
        };
        for m in models {
            // Dedupe groups naming the same model with the same backend subset,
            // comparing the whole subset including the backend class.
            if out.iter().any(|t| t.model == m && t.backends == g.backends) {
                continue;
            }
            out.push(ScoreTarget {
                model: m,
                backends: g.backends.clone(),
            });
        }
    }
    out
}

/// Sample one group index proportional to weight.
fn sample_weighted(groups: &[super::compile::CompiledConsiderGroup]) -> Option<usize> {
    let total: u64 = groups.iter().map(|g| g.weight as u64).sum();
    if total == 0 {
        return None;
    }
    let mut buf = [0u8; 8];
    let roll = match getrandom::getrandom(&mut buf) {
        Ok(()) => u64::from_le_bytes(buf) % total,
        Err(e) => {
            // Deterministic fallback beats failing the request; log so a broken
            // CSPRNG doesn't silently pin all traffic to one group.
            tracing::warn!(
                error = &e as &dyn StdError,
                "getrandom failed; using the first consider group"
            );
            0
        }
    };
    let mut acc = 0u64;
    for (i, g) in groups.iter().enumerate() {
        acc += g.weight as u64;
        if roll < acc {
            return Some(i);
        }
    }
    Some(groups.len() - 1)
}

/// Apply one route to a candidate set: built-in context fit, then exclusions,
/// then the `select` tiers, then `tieBreak`.
pub fn evaluate(
    route: &CompiledRoute,
    rule_name: Option<&str>,
    attrs: &RequestAttrs,
    candidates: Vec<Candidate>,
) -> Outcome {
    let mut trace = Trace {
        rule: rule_name.map(|s| s.to_string()),
        considered: candidates.iter().map(|c| c.label()).collect(),
        ..Default::default()
    };

    // --- built-in context-window fit (before author exclusions) ---------
    //
    // Uses each endpoint's effective window, so a rollout that shrinks
    // --max-model-len stops attracting long requests instead of failing them.
    // A provider's declared window is not checked.
    let mut survivors: Vec<Candidate> = Vec::with_capacity(candidates.len());
    for c in candidates {
        let need = match c.target {
            CandidateTarget::Local { .. } => required_context(&c.attrs, attrs),
            CandidateTarget::Provider { .. } => None,
        };
        match (need, c.attrs.context_window) {
            (Some(need), Some(window)) if need > window => {
                trace
                    .eliminated
                    .push((c.label(), format!("context-window fit ({need} > {window})")));
            }
            _ => survivors.push(c),
        }
    }

    // --- author exclusions (order-independent set subtraction) -----------
    for ex in &route.exclude {
        // A guard reads each candidate's own `inputTokens`, so it is
        // evaluated per candidate. A candidate with no count, such as a
        // provider, misses an `inputTokens` term.
        let before = std::mem::take(&mut survivors);
        for c in before {
            let guarded = ex
                .if_
                .as_ref()
                .is_none_or(|g| g.matches_with_input_tokens(attrs, c.attrs.input_tokens));
            if guarded && ex.drop.matches(&c.attrs) {
                trace
                    .eliminated
                    .push((c.label(), format!("exclusion `{}`", ex.name)));
            } else {
                survivors.push(c);
            }
        }
    }
    trace.survivors = survivors.iter().map(|c| c.label()).collect();

    if survivors.is_empty() {
        return Outcome::Empty { trace };
    }

    // --- select: ordered tiers, first with a survivor wins outright ------
    for (i, tier) in route.select.iter().enumerate() {
        let mut in_tier: Vec<Candidate> = Vec::new();
        for c in &survivors {
            if !tier
                .backends
                .matches(c.backend_type(), &c.backend, &c.labels)
            {
                continue;
            }
            // A candidate with no value for this tier's objective is dropped
            // from the tier. `Order` ranks by name instead.
            if !matches!(tier.rank, Rank::Order(_)) && c.objective_ms(&tier.rank).is_none() {
                // Except an unobserved provider under EstimatedTTFT, which sorts
                // last instead, since it can't get an observation without being
                // picked.
                let unobserved_provider = matches!(tier.rank, Rank::MinimizeEstimatedTtft)
                    && matches!(c.target, CandidateTarget::Provider { .. });
                if !unobserved_provider {
                    trace.eliminated.push((
                        c.label(),
                        format!("select tier {i}: no value for its objective"),
                    ));
                    continue;
                }
            }
            in_tier.push(c.clone());
        }
        if in_tier.is_empty() {
            continue;
        }
        let (winner, tied) = rank(&tier.rank, in_tier, &route.tie_break);
        trace.tier = Some(i);
        trace.tied = tied;
        // For an `order` tier, log the winner's prediction instead.
        trace.winner_ttft_ms = winner
            .objective_ms(&tier.rank)
            .or_else(|| winner.objective_ms(&Rank::MinimizeEstimatedTtft));
        return Outcome::Chosen {
            candidate: winner,
            trace,
        };
    }

    // No tier admitted any survivor.
    Outcome::Empty { trace }
}

/// Total context a request needs on one candidate: the prompt under that
/// candidate's tokenizer, plus whatever the request may generate.
///
/// `maxTokens` is included because the KV cache has to hold the completion
/// too. `None`, so the prompt is taken to fit, when the candidate has no
/// count of its own.
fn required_context(candidate: &CandidateAttrs, attrs: &RequestAttrs) -> Option<i64> {
    let input = candidate.input_tokens?;
    Some(match attrs.max_tokens {
        None => input,
        Some(max) => input.saturating_add(max),
    })
}

/// Rank within one tier and return `(winner, how_many_tied)`.
///
/// Every candidate handed here is rankable, or is an unobserved provider under
/// `EstimatedTTFT`, which sorts last; `evaluate` has dropped the rest.
fn rank(rank: &Rank, mut candidates: Vec<Candidate>, tie_break: &[TieBreak]) -> (Candidate, usize) {
    // Establish the best key, then collect everything equal to it.
    let best_first = match rank {
        Rank::MinimizePredictedTtft | Rank::MinimizeEstimatedTtft => {
            // No value sorts last, so an unobserved provider is chosen only when
            // nothing better is standing.
            let key = |c: &Candidate| c.objective_ms(rank).unwrap_or(f32::INFINITY);
            candidates.sort_by(|a, b| {
                key(a)
                    .partial_cmp(&key(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            let best = key(&candidates[0]);
            candidates.iter().take_while(|c| key(c) == best).count()
        }
        Rank::Order(order) => {
            let key = |c: &Candidate| {
                order
                    .iter()
                    .position(|n| n == &c.backend)
                    // Unlisted destinations rank last rather than being
                    // dropped; `backends` is the filter.
                    .unwrap_or(usize::MAX)
            };
            candidates.sort_by_key(key);
            let best = key(&candidates[0]);
            candidates.iter().take_while(|c| key(c) == best).count()
        }
    };
    let tied = best_first.max(1);

    // PreferLocal narrows the tie first, then Random breaks what is left.
    if tied > 1 && tie_break.contains(&TieBreak::PreferLocal) {
        let local = candidates[..tied]
            .iter()
            .filter(|c| matches!(c.target, CandidateTarget::Local { .. }))
            .count();
        if local > 0 && local < tied {
            candidates[..tied].sort_by_key(|c| !matches!(c.target, CandidateTarget::Local { .. }));
            return pick_among(candidates, local, tie_break);
        }
    }
    pick_among(candidates, tied, tie_break)
}

/// Choose one of the first `tied` candidates, honoring `tieBreak: Random`.
fn pick_among(
    mut candidates: Vec<Candidate>,
    tied: usize,
    tie_break: &[TieBreak],
) -> (Candidate, usize) {
    let idx = if tied > 1 && tie_break.contains(&TieBreak::Random) {
        let mut buf = [0u8; 8];
        match getrandom::getrandom(&mut buf) {
            Ok(()) => (u64::from_le_bytes(buf) as usize) % tied,
            Err(_) => 0,
        }
    } else {
        // No Random: the first in sorted order.
        0
    };
    (candidates.swap_remove(idx), tied)
}

/// The route to apply when no rule matched.
///
/// `ServeLocal` is fail-open and carries no exclusions, not even a queue
/// threshold.
pub fn unmatched_route(policy: &CompiledPolicy) -> Result<CompiledRoute, RejectReason> {
    match policy.unmatched {
        Unmatched::ServeLocal => Ok(CompiledPolicy::default_route()),
        Unmatched::Reject => Err(RejectReason::Unmatched),
    }
}

/// Whether an empty candidate set should try the next matching rule.
pub fn falls_through(route: &CompiledRoute) -> bool {
    route.on_empty == OnEmpty::FallThrough
}

#[cfg(test)]
mod tests;
