//! The CRD policy driver: running `InferencePolicy` for a request that named
//! an `InferencePool`.
//!
//! `policy_select` applies a compiled policy to a live snapshot of pools,
//! worker sets and providers; `candidates_for` flattens a scored group into
//! per-endpoint candidates. Ranking holds no pool lock; see `policy_select`.

use futures::{FutureExt as _, StreamExt as _};

use super::*;

impl KinferenceService {
    /// Run the CRD policy engine for a request that named an `InferencePool`.
    ///
    /// Candidates may span pools, each with its own `pick_lock`, so ranking is
    /// lock-free (taking two locks in opposite orders would deadlock) and only
    /// the winner's pool is locked, to commit. The ranking may be one
    /// concurrent pick stale; inflight bookkeeping is still exact.
    ///
    /// `PolicySelection::Picker` means the policy had no opinion, e.g. for a
    /// pool kinference doesn't know, and the normal picker chooses (fail
    /// open).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn policy_select(
        &self,
        pref: &kinference_policy::PoolRef,
        snapshot: &kinference_policy::resolve::PolicyIndex,
        // Groups that the caller has already scored. Their models aren't
        // rendered again.
        primary: Option<ScoredGroup>,
        // Passed explicitly, since `primary` may not exist (a provider-only
        // model).
        requested_model: &str,
        spare: Option<ScoreInput>,
        mut attrs: kinference_policy::predicate::RequestAttrs,
    ) -> Result<PolicySelection, PickAndRecordOutcome> {
        use kinference_policy::engine;

        let Some(resolved) = snapshot.lookup(&pref.namespace, &pref.name) else {
            // Both fail open, but are logged differently: before the first list
            // completes, every pool looks unknown.
            if snapshot.listed() {
                let pool = pref.label();
                if self.unknown_pools.first(pool.as_str()) {
                    tracing::warn!(
                        inference_pool = %pool,
                        "unknown InferencePool; routing on load alone"
                    );
                }
                self.prom.record_policy_decision("unknown_pool");
            } else {
                tracing::debug!(
                    inference_pool = %pref.label(),
                    "InferencePools not listed yet; routing on load alone"
                );
                self.prom.record_policy_decision("not_listed");
            }
            return Ok(PolicySelection::Picker { group: primary });
        };

        // Opaque revision: we can tell we differ, not who is behind. Advisory,
        // so record it and proceed.
        if !pref.resource_version.is_empty() && pref.resource_version != resolved.resource_version {
            self.prom.record_policy_revision_mismatch();
            tracing::debug!(
                inference_pool = %pref.label(),
                requested = %pref.resource_version,
                have = %resolved.resource_version,
                "InferencePool revision differs from the gateway's"
            );
        }

        let model = requested_model.to_string();
        // Rules can't reach beyond the pool's destinations.
        if resolved.destinations.is_empty() {
            self.prom.record_policy_decision("no_destinations");
            return Err(PickAndRecordOutcome::PolicyRejected {
                reason: format!(
                    "pool {} lists no destination kinference can route to",
                    pref.label()
                ),
                model,
            });
        }

        let policy = resolved.policy.clone();
        // Before the token count is known, every rule that could match once
        // it is. Then the implicit unmatched route (`None`, traced as
        // `<unmatched>`), which gets its own candidate generation (the
        // requested model, local destinations only), so it can't serve a
        // model an earlier rule scored. Each route's targets are drawn once,
        // since weighted groups are sampled.
        let uses_count = policy.uses_input_tokens();
        let rules = if uses_count {
            engine::possibly_matching_rules(&policy, &attrs)
        } else {
            engine::matching_rules(&policy, &attrs)
        };
        let mut plan: Vec<(
            Option<&kinference_policy::compile::CompiledRule>,
            kinference_policy::compile::CompiledRoute,
            Vec<engine::ScoreTarget>,
        )> = Vec::new();
        // The loop below never gets past a rule that matches whatever the
        // count and doesn't fall through, so nothing after it is rendered.
        let mut reachable = true;
        for r in rules {
            let targets = engine::models_to_score(&r.route, &attrs.requested_model);
            plan.push((Some(r), r.route.clone(), targets));
            // An entry with no `inputTokens` term matches whatever the count.
            let sure = r.matches.is_empty()
                || r.matches
                    .iter()
                    .any(|p| !p.tests_input_tokens() && p.matches(&attrs));
            if sure && !engine::falls_through(&r.route) {
                reachable = false;
                break;
            }
        }
        if reachable && let Ok(route) = engine::unmatched_route(&policy) {
            let targets = engine::models_to_score(&route, &attrs.requested_model);
            plan.push((None, route, targets));
        }
        if plan.is_empty() {
            // No rule can match, and `unmatched: Reject`.
            return Err(self.reject_unmatched(pref, model));
        }

        // Groups scored so far, by (pool, model). Every model that the plan
        // could use starts rendering now, all at once, since render calls are
        // the costliest part of a pick. The pick then waits only for the
        // renders that it needs, when it needs them.
        let mut scored: Vec<ScoredGroup> = primary.into_iter().collect();
        let mut renders = match spare.as_ref() {
            Some(spare) => {
                let targets: Vec<&engine::ScoreTarget> =
                    plan.iter().flat_map(|(_, _, t)| t.iter()).collect();
                self.start_renders(&targets, &resolved.destinations, &scored, spare)
            }
            None => Renders::default(),
        };

        // The count comes from whichever render finishes first. Which model
        // that is is not part of the API.
        if uses_count {
            attrs.input_tokens = renders.first_count(&mut scored, &self.prom).await;
            plan.retain(|(rule, _, _)| rule.is_none_or(|r| r.matches(&attrs)));
            if plan.is_empty() {
                return Err(self.reject_unmatched(pref, model));
            }
        }
        let any_rule_matched = plan.iter().any(|(rule, _, _)| rule.is_some());

        for (rule, route, targets) in &plan {
            let rule_name = rule.map(|r| r.name.clone());
            let needed = local_upstreams(targets.iter(), &resolved.destinations);
            renders.wait_for(&needed, &mut scored, &self.prom).await;
            let mut candidates: Vec<engine::Candidate> = Vec::new();
            // For each (destination, upstream), the indices of the candidates
            // that the loop has built so far. Two targets can lead to the same
            // pair, as when `consider` contains both a model and its alias. A
            // copy per name would record its prediction once per copy and tilt
            // `tieBreak: Random` toward that destination.
            let mut built: HashMap<(CandidateDest, String), Vec<usize>> = HashMap::new();

            for target in targets {
                for dest in &resolved.destinations {
                    if !target
                        .backends
                        .matches(dest.backend_type(), dest.name(), dest.labels())
                    {
                        continue;
                    }
                    // Providers need no scoring; candidates come from the
                    // declared model table.
                    let dest = match dest {
                        kinference_policy::resolve::ResolvedDestination::Local(local) => local,
                        kinference_policy::resolve::ResolvedDestination::Provider(prov) => {
                            let dest =
                                CandidateDest::Provider(prov.namespace.clone(), prov.name.clone());
                            for c in provider_candidates_for(
                                &self.observations.provider_health,
                                prov,
                                &target.model,
                            ) {
                                let key = (dest.clone(), c.upstream_model.clone());
                                if !add_name(&mut candidates, &built, &key, &target.model) {
                                    built.insert(key, vec![candidates.len()]);
                                    candidates.push(c);
                                }
                            }
                            continue;
                        }
                    };
                    // A canonical name can map to several upstream models, each
                    // its own candidate.
                    for upstream in dest.upstreams_for(&target.model) {
                        let key = (CandidateDest::Local(dest.pool_id), upstream.clone());
                        if add_name(&mut candidates, &built, &key, &target.model) {
                            continue;
                        }
                        let idx = match scored
                            .iter()
                            .position(|g| g.pool.id == dest.pool_id && g.model == upstream)
                        {
                            Some(i) => i,
                            None => {
                                let Some(pool) =
                                    self.scoring.pools.snapshot().get(dest.pool_id).cloned()
                                else {
                                    continue;
                                };
                                let Some(spare) = spare.as_ref() else {
                                    // Only reachable if scoring inputs were not
                                    // cloned, which cannot happen when a policy
                                    // is active.
                                    continue;
                                };
                                // Another pool of a model that this pick
                                // already rendered: reuse its tokens, or after
                                // a failed render, score on load alone.
                                let cached = scored
                                    .iter()
                                    .find(|g| g.model == upstream && g.input_tokens.is_some())
                                    .map(|g| g.side.tokens.clone());
                                let failed = scored
                                    .iter()
                                    .any(|g| g.model == upstream && g.render_failed());
                                let (outcome, side) = match cached {
                                    Some(t) => {
                                        self.scoring
                                            .score_workers(
                                                &*self.tokenizer,
                                                &pool,
                                                &upstream,
                                                ScoreInput::tokens(t),
                                                HashMap::new(),
                                            )
                                            .await
                                    }
                                    None if failed => (
                                        self.scoring.score_load_only(&pool, &upstream),
                                        ScoreSideData::default(),
                                    ),
                                    None => {
                                        self.scoring
                                            .score_workers(
                                                &*self.tokenizer,
                                                &pool,
                                                &upstream,
                                                spare.clone(),
                                                HashMap::new(),
                                            )
                                            .await
                                    }
                                };
                                let Some(group) = ScoredGroup::from_outcome(
                                    pool,
                                    upstream.clone(),
                                    outcome,
                                    side,
                                    &self.prom,
                                ) else {
                                    continue;
                                };
                                scored.push(group);
                                scored.len() - 1
                            }
                        };
                        let new = self.candidates_for(&scored[idx], idx, dest, &target.model);
                        built.insert(
                            key,
                            (candidates.len()..candidates.len() + new.len()).collect(),
                        );
                        candidates.extend(new);
                    }
                }
            }

            // Log the cross-group ranking, which the per-group score table
            // can't show. One line per candidate, best first, capped; `rank` and
            // `n` show when the list is truncated.
            if tracing::enabled!(tracing::Level::TRACE) {
                // Providers have no prediction, so they sort last.
                let sort_key = |c: &engine::Candidate| match &c.target {
                    engine::CandidateTarget::Local {
                        predicted_ttft_ms, ..
                    } => *predicted_ttft_ms,
                    engine::CandidateTarget::Provider { .. } => f32::INFINITY,
                };
                let mut ranked: Vec<&engine::Candidate> = candidates.iter().collect();
                ranked.sort_by(|a, b| {
                    sort_key(a)
                        .partial_cmp(&sort_key(b))
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                const SHOWN: usize = 8;
                let total = ranked.len();
                for (i, c) in ranked.iter().take(SHOWN).enumerate() {
                    let (endpoint, predicted) = match &c.target {
                        engine::CandidateTarget::Local {
                            endpoint,
                            predicted_ttft_ms,
                            ..
                        } => (endpoint.clone(), format!("{predicted_ttft_ms:.1}")),
                        engine::CandidateTarget::Provider {
                            backend_ref,
                            observed_ttft_ms,
                            ..
                        } => (
                            backend_ref.label(),
                            observed_ttft_ms
                                .map(|ms| format!("~{ms:.1}"))
                                .unwrap_or_else(|| "unobserved".to_string()),
                        ),
                    };
                    tracing::trace!(
                        rule = rule_name.as_deref().unwrap_or("<unmatched>"),
                        rank = i + 1,
                        n = total,
                        backend = %c.backend,
                        class = %c.backend_type().as_str(),
                        model = %c.upstream_model,
                        endpoint = %endpoint,
                        predicted_ms = %predicted,
                        "policy candidate"
                    );
                }
                if total > SHOWN {
                    tracing::trace!(
                        rule = rule_name.as_deref().unwrap_or("<unmatched>"),
                        elided = total - SHOWN,
                        n = total,
                        "more policy candidates not shown"
                    );
                }
            }

            match engine::evaluate(route, rule_name.as_deref(), &attrs, candidates) {
                engine::Outcome::Chosen { candidate, trace } => {
                    log_winner(&pref.label(), &candidate, &trace);
                    self.prom.record_policy_decision(if rule_name.is_some() {
                        "chosen"
                    } else {
                        "unmatched_served"
                    });
                    if let Some(n) = &rule_name {
                        self.prom.record_policy_rule_match(n);
                    }
                    return Ok(selection(&mut scored, candidate));
                }
                engine::Outcome::Empty { trace } => {
                    tracing::debug!(
                        inference_pool = %pref.label(),
                        trace = %trace.summary(),
                        "no policy candidate survived"
                    );
                    if rule_name.is_some() && engine::falls_through(route) {
                        // FallThrough: try the next matching rule.
                        continue;
                    }
                    let (label, reason) = match rule_name {
                        Some(n) => (
                            "rejected_on_empty",
                            format!("every candidate was excluded by rule `{n}` (onEmpty: Reject)"),
                        ),
                        None => (
                            "unmatched_empty",
                            if any_rule_matched {
                                "every matching rule fell through and the unmatched route had \
                                 no candidate"
                            } else {
                                "no rule matched and the unmatched route had no candidate"
                            }
                            .to_string(),
                        ),
                    };
                    self.prom.record_policy_decision(label);
                    return Err(PickAndRecordOutcome::PolicyRejected { reason, model });
                }
            }
        }

        // Every matching rule fell through and there is no unmatched route to
        // close the plan: `unmatched: Reject`.
        self.prom.record_policy_decision("rejected_unmatched");
        Err(PickAndRecordOutcome::PolicyRejected {
            reason: "every matching rule fell through and unmatched=Reject".to_string(),
            model,
        })
    }

    /// The refusal for a request that no rule matched, under
    /// `unmatched: Reject`.
    fn reject_unmatched(
        &self,
        pref: &kinference_policy::PoolRef,
        model: String,
    ) -> PickAndRecordOutcome {
        tracing::debug!(
            inference_pool = %pref.label(),
            "no policy rule matched; refusing"
        );
        self.prom.record_policy_decision("rejected_unmatched");
        PickAndRecordOutcome::PolicyRejected {
            reason: "no rule matched and unmatched=Reject".to_string(),
            model,
        }
    }

    /// Start one render for each upstream model that `targets` reach on a
    /// local destination and that no group in `scored` has.
    ///
    /// Token ids belong to a model, so each such model costs one render call.
    /// Each model renders on the first destination with a ready worker for
    /// it. The candidate loop scores any other pool that serves the same
    /// model from those tokens, or on load alone when the render failed.
    fn start_renders<'a>(
        &'a self,
        targets: &[&kinference_policy::engine::ScoreTarget],
        destinations: &[kinference_policy::resolve::ResolvedDestination],
        scored: &[ScoredGroup],
        spare: &ScoreInput,
    ) -> Renders<'a> {
        let pools = self.scoring.pools.snapshot();
        let mut renders = Renders::default();
        for (pool_id, upstream) in local_upstreams(targets.iter().copied(), destinations) {
            if renders.pending.contains(&upstream) || scored.iter().any(|g| g.model == upstream) {
                continue;
            }
            let Some(pool) = pools.get(pool_id).cloned() else {
                continue;
            };
            let ready = pool
                .registry
                .workers_for_model(&upstream)
                .iter()
                .any(|(_, w)| w.is_ready());
            if !ready {
                continue;
            }
            let input = spare.clone();
            let model = upstream.clone();
            renders.pending.insert(upstream);
            renders.inflight.push(
                async move {
                    let (outcome, side) = self
                        .scoring
                        .score_workers(&*self.tokenizer, &pool, &model, input, HashMap::new())
                        .await;
                    (pool, model, outcome, side)
                }
                .boxed(),
            );
        }
        renders
    }

    /// Build one candidate per endpoint in a scored group.
    ///
    /// `group_idx` rides on `Candidate::score_idx` so the winner maps back to
    /// the group the critical section must commit with.
    fn candidates_for(
        &self,
        group: &ScoredGroup,
        group_idx: usize,
        dest: &kinference_policy::resolve::ResolvedWorkerSet,
        canonical_model: &str,
    ) -> Vec<kinference_policy::engine::Candidate> {
        let now = Instant::now();
        let ready_endpoints = group.pool.registry.len() as i64;
        let labels = Arc::new(dest.labels.clone());
        let model_names = model_names(
            &group.model,
            dest.aliases_for(&group.model),
            canonical_model,
        );
        // Per endpoint, since endpoints may disagree mid-rollout.
        let context_by_endpoint: HashMap<String, u32> = group
            .pool
            .registry
            .iter_snapshot()
            .into_iter()
            .filter_map(|(num, entry)| {
                let info = self.scoring.scrape.models(num);
                let window = info
                    .models
                    .iter()
                    .find(|m| m.name == group.model)
                    .and_then(|m| m.max_model_len)
                    .or_else(|| info.models.iter().filter_map(|m| m.max_model_len).min())?;
                Some((entry.score_addr, window))
            })
            .collect();
        // The two saturation signals per endpoint, as windowed means rather
        // than `WorkerScore`'s instantaneous queue, which spikes and clears.
        // Only scraped endpoints; the rest would read as all-zero (idle).
        let saturation_by_endpoint: HashMap<String, (f64, f64)> = group
            .pool
            .registry
            .iter_snapshot()
            .into_iter()
            .filter_map(|(num, entry)| {
                let m = self.scoring.scrape.sample(num)?;
                Some((
                    entry.score_addr,
                    (m.waiting_mean as f64, m.kv_cache_usage_mean as f64),
                ))
            })
            .collect();

        group
            .scores
            .iter()
            .map(|s| {
                let summaries: Vec<kinference_fleet::inflight::InflightSummary> = group
                    .pool
                    .inflight_prefixes
                    .dispatches_for(&s.endpoint)
                    .iter()
                    .map(|d| d.summary())
                    .collect();
                let predicted =
                    crate::picker::predicted_wait_ms(s, &group.pool.predictor, &summaries, now)
                        .total_ms();
                // Recorded for every candidate, as a `predictedTTFT` threshold
                // applies to every candidate; winners only would bias it low.
                self.prom
                    .record_predicted_ttft(&s.endpoint, &group.model, predicted as f64);
                let hit_ratio = if s.total_blocks > 0 {
                    s.matched_blocks as f64 / s.total_blocks as f64
                } else {
                    0.0
                };
                kinference_policy::engine::Candidate {
                    canonical_model: canonical_model.to_string(),
                    upstream_model: group.model.clone(),
                    backend: dest.name.clone(),
                    labels: labels.clone(),
                    target: kinference_policy::engine::CandidateTarget::Local {
                        pool_id: group.pool.id,
                        endpoint: s.endpoint.clone(),
                        score_idx: group_idx,
                        predicted_ttft_ms: predicted,
                    },
                    attrs: kinference_policy::predicate::CandidateAttrs {
                        context_window: context_by_endpoint.get(&s.endpoint).map(|v| *v as i64),
                        // `None` after a failed render: the fit then assumes
                        // that the prompt fits.
                        input_tokens: group.input_tokens,
                        predicted_ttft_ms: Some(predicted as f64),
                        prefix_cache_hit_ratio: Some(hit_ratio),
                        // Absent, not zero, when the endpoint has not been
                        // scraped.
                        queued_prompts: saturation_by_endpoint.get(&s.endpoint).map(|(q, _)| *q),
                        kv_cache_usage: saturation_by_endpoint.get(&s.endpoint).map(|(_, k)| *k),
                        model_names: model_names.clone(),
                        backend_name: Some(dest.name.clone()),
                        backend_type: Some(
                            kinference_policy::crd::BackendType::Local
                                .as_str()
                                .to_string(),
                        ),
                        backend_ready_endpoints: Some(ready_endpoints),
                        // Provider-only.
                        backend_observed_ttft_ms: None,
                        backend_budget_exhausted: None,
                    },
                }
            })
            .collect()
    }
}

/// One finished render: the pool and model that it scored, and the outcome.
type Rendered = (Arc<Pool>, String, ScoreOutcome, ScoreSideData);

/// The render calls that a pick has started, one per model.
///
/// The futures run only while the pick polls them, through [`Renders::next`],
/// so the first poll sends every request. Dropping the set cancels the calls
/// that the pick did not wait for.
#[derive(Default)]
struct Renders<'a> {
    inflight: futures::stream::FuturesUnordered<futures::future::BoxFuture<'a, Rendered>>,
    /// The models whose render has not finished.
    pending: std::collections::HashSet<String>,
}

impl Renders<'_> {
    /// Add the next render to finish to `scored`. False when none is left.
    async fn next(&mut self, scored: &mut Vec<ScoredGroup>, prom: &RouterMetrics) -> bool {
        let Some((pool, model, outcome, side)) = self.inflight.next().await else {
            return false;
        };
        self.pending.remove(&model);
        scored.extend(ScoredGroup::from_outcome(pool, model, outcome, side, prom));
        true
    }

    /// Wait until no model in `needed` is still rendering.
    async fn wait_for(
        &mut self,
        needed: &[(kinference_policy::PoolId, String)],
        scored: &mut Vec<ScoredGroup>,
        prom: &RouterMetrics,
    ) {
        while needed.iter().any(|(_, m)| self.pending.contains(m)) {
            if !self.next(scored, prom).await {
                break;
            }
        }
    }

    /// The first token count that any group has, waiting only until one
    /// render yields one. `None` when every render failed.
    async fn first_count(
        &mut self,
        scored: &mut Vec<ScoredGroup>,
        prom: &RouterMetrics,
    ) -> Option<i64> {
        loop {
            if let Some(n) = scored.iter().find_map(|g| g.input_tokens) {
                return Some(n);
            }
            if !self.next(scored, prom).await {
                return None;
            }
        }
    }
}

/// Each `(pool, upstream model)` that `targets` reach on a local
/// destination, in order.
fn local_upstreams<'t>(
    targets: impl Iterator<Item = &'t kinference_policy::engine::ScoreTarget>,
    destinations: &[kinference_policy::resolve::ResolvedDestination],
) -> Vec<(kinference_policy::PoolId, String)> {
    let mut out = Vec::new();
    for target in targets {
        for dest in destinations {
            if !target
                .backends
                .matches(dest.backend_type(), dest.name(), dest.labels())
            {
                continue;
            }
            let kinference_policy::resolve::ResolvedDestination::Local(local) = dest else {
                continue;
            };
            for upstream in local.upstreams_for(&target.model) {
                out.push((local.pool_id, upstream));
            }
        }
    }
    out
}

/// A destination that the router builds candidates for: a local worker set,
/// or a provider by namespace and name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum CandidateDest {
    Local(kinference_policy::PoolId),
    Provider(String, String),
}

/// A candidate's `model.name`: `upstream`, then the `aliases` that its
/// destination declares for it, then `reached_by` if the destination serves
/// that name but does not declare it.
pub(super) fn model_names(upstream: &str, aliases: Vec<String>, reached_by: &str) -> Vec<String> {
    let mut names = vec![upstream.to_string()];
    for name in aliases
        .into_iter()
        .chain(std::iter::once(reached_by.to_string()))
    {
        if !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

/// Add `name` to the candidates that the router already built for `key`,
/// unless they already contain it. Returns `false` if the router has built
/// nothing for `key` yet.
fn add_name(
    candidates: &mut [kinference_policy::engine::Candidate],
    built: &HashMap<(CandidateDest, String), Vec<usize>>,
    key: &(CandidateDest, String),
    name: &str,
) -> bool {
    let Some(indices) = built.get(key) else {
        return false;
    };
    for &i in indices {
        let names = &mut candidates[i].attrs.model_names;
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    true
}
