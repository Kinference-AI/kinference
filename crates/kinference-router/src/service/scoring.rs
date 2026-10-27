//! Scoring: turning a pool's live workers into `WorkerScore` rows.
//!
//! `Scoring` is the read side of a pick: which pools serve a model, and the
//! registry each worker's load and prefix overlap are read from. It makes no
//! routing decision; `lifecycle` does. `degraded_response` is the same with
//! the prefix-cache values zeroed.

use super::*;

/// The read side of a pick: which pools serve a model, and each worker's
/// scraped load and prefix overlap.
pub(crate) struct Scoring {
    /// The pools this fleet manages. Each owns its own predictor, picker,
    /// block size, indexer and registry. Pools come and go, and the service is
    /// shared behind an `Arc`, so the set is swapped rather than edited.
    pub(crate) pools: Arc<kinference_fleet::pool::PoolRegistry>,
    /// Which pools serve each model, read on the pick hot path. Several pools
    /// may serve one model; the pick path ranks across them.
    pub(crate) models: Arc<ModelIndex>,
    /// The fleet's scrape samples and `/v1/models` answers.
    pub(crate) scrape: Arc<MetricsRegistry>,
}

impl Scoring {
    pub(crate) fn new(
        pools: Arc<kinference_fleet::pool::PoolRegistry>,
        models: Arc<ModelIndex>,
        scrape: Arc<MetricsRegistry>,
    ) -> Self {
        Self {
            pools,
            models,
            scrape,
        }
    }

    /// Every pool currently serving `model`, in stable id order.
    ///
    /// Empty when no pool has claimed the model yet, or every claiming pool has
    /// since been removed (ids that resolve to nothing are dropped). The
    /// returned handles keep their pools alive.
    pub(crate) fn resolve_pools(&self, model: &str) -> Vec<Arc<Pool>> {
        let ids = self.models.pools_for(model);
        if ids.is_empty() {
            return Vec::new();
        }
        let snapshot = self.pools.snapshot();
        ids.into_iter()
            .filter_map(|id| snapshot.get(id).cloned())
            .collect()
    }

    /// Resolve the `"auto"` pseudo-model to a concrete model for this prompt.
    ///
    /// For now, uniform random among the models any pool serves; `messages` is
    /// passed for future prompt-aware selection. `None` when no model is known
    /// yet, which the caller treats like an unknown model.
    pub(crate) fn choose_model_for_auto(&self, _messages: &[serde_json::Value]) -> Option<String> {
        let models = self.models.models();
        // Never route "auto" to itself.
        let candidates: Vec<&String> = models.iter().filter(|m| m.as_str() != AUTO_MODEL).collect();
        match candidates.len() {
            0 => None,
            1 => Some(candidates[0].clone()),
            n => {
                let mut buf = [0u8; 8];
                let idx = match getrandom::getrandom(&mut buf) {
                    Ok(()) => (u64::from_le_bytes(buf) as usize) % n,
                    Err(_) => 0,
                };
                Some(candidates[idx].clone())
            }
        }
    }

    /// Build a degraded `ScoreResponse` when tokenize is unavailable: load
    /// fields as usual, `total_blocks` and `matched_blocks` zeroed.
    pub(crate) fn degraded_response(&self, workers: &[(u64, WorkerEntry)]) -> ScoreResponse {
        let mut scores = Vec::with_capacity(workers.len());
        for (num, entry) in workers {
            let sample = self.scrape.latest(*num);
            scores.push(worker_score(entry, &sample, entry.block_size, 0, 0));
        }
        ScoreResponse { scores }
    }

    /// Score one pool's workers for one model.
    ///
    /// `tokenizer` is passed in, since it is the service's own part.
    /// `chat_kwargs` applies to Chat and Messages input only.
    pub(crate) async fn score_workers(
        &self,
        tokenizer: &dyn crate::tokenizer::Tokenizer,
        pool: &Pool,
        model: &str,
        input: ScoreInput,
        chat_kwargs: HashMap<String, String>,
    ) -> (ScoreOutcome, ScoreSideData) {
        let mut side = ScoreSideData {
            tokens: Vec::new(),
            hashes_by_block_size: HashMap::new(),
        };

        // 1. Ready workers serving this model (see `WorkerEntry::is_ready`).
        //    None means `NoWorkers`, typically during cold start.
        let workers: Vec<_> = pool
            .registry
            .workers_for_model(model)
            .into_iter()
            .filter(|(_, w)| w.is_ready())
            .collect();
        if workers.is_empty() {
            return (ScoreOutcome::NoWorkers, side);
        }

        let ScoreInput {
            input,
            chat_extras,
            raw_body,
        } = input;
        let ChatExtras {
            tools,
            tool_choice,
            add_generation_prompt,
        } = chat_extras;

        // 2. Get tokens, pre-supplied or by tokenizing. Total failure
        //    degrades rather than erroring.
        let token_source: TokenSource = match input {
            Input::Tokens(t) => TokenSource::Direct(t),
            Input::Raw(s) => TokenSource::NeedsTokenize(TokenizeInput::Raw(s)),
            Input::Chat(s) => TokenSource::NeedsTokenize(TokenizeInput::Chat {
                messages: vec![serde_json::json!({ "role": "user", "content": s })],
                kwargs: chat_kwargs,
                tools,
                tool_choice,
                add_generation_prompt,
            }),
            Input::Messages(messages) => {
                // Already parsed by the caller.
                TokenSource::NeedsTokenize(TokenizeInput::Chat {
                    messages,
                    kwargs: chat_kwargs,
                    tools,
                    tool_choice,
                    add_generation_prompt,
                })
            }
        };

        let tokens: Vec<u32> = match token_source {
            TokenSource::Direct(t) => t,
            TokenSource::NeedsTokenize(tok_input) => {
                let tok_req = crate::tokenizer::TokenizeRequest {
                    mode: pool.tokenize_mode,
                    render_url: pool.tokenize_render_url.as_deref(),
                    workers: &workers,
                    model,
                    input: tok_input,
                    raw_body: raw_body.as_deref(),
                };
                match tokenizer.tokenize(tok_req).await {
                    crate::tokenizer::TokenizeOutcome::Ok(tokens) => tokens,
                    crate::tokenizer::TokenizeOutcome::Unavailable { reason, attempts } => {
                        tracing::error!(
                            model = model,
                            attempts,
                            reason = %reason,
                            "tokenize unavailable across the fleet; returning degraded ScoreResponse"
                        );
                        let degraded = self.degraded_response(&workers);
                        return (
                            ScoreOutcome::Unavailable(degraded, Degraded::Tokenize),
                            side,
                        );
                    }
                }
            }
        };
        // So callers scoring the same model in other pools needn't re-tokenize.
        side.tokens = tokens.clone();

        // 3. Group workers by block_size, score each group separately.
        let mut by_block: HashMap<u32, Vec<(u64, &WorkerEntry)>> = HashMap::new();
        for w in &workers {
            by_block
                .entry(w.1.block_size)
                .or_default()
                .push((w.0, &w.1));
        }

        let mut out: HashMap<u64, WorkerScore> = HashMap::new();
        for (block_size, group) in by_block {
            let hashes =
                compute_block_hash_for_seq(&tokens, block_size, BlockHashOptions::default());
            let total_blocks = hashes.len() as u32;

            // Keep the hashes for the pick path before `find_matches` consumes
            // them.
            let hashes_u64: Vec<u64> = hashes.iter().map(|h| h.0).collect();

            tracing::trace!(
                block_size,
                total_blocks,
                first_tokens = ?tokens.iter().take(8).collect::<Vec<_>>(),
                first_hashes = ?hashes_u64.iter().take(4).collect::<Vec<_>>(),
                "score: computed block hashes"
            );

            side.hashes_by_block_size.insert(block_size, hashes_u64);

            let overlap = match pool.indexer.find_matches(hashes).await {
                Ok(o) => o,
                Err(e) => {
                    // Degrade to load-only, reported as a prefix-index (not
                    // tokenize) failure.
                    tracing::error!(
                        pool = %pool.name,
                        block_size,
                        error = %e,
                        "prefix index query failed; degrading this pick to load-only"
                    );
                    let degraded = self.degraded_response(&workers);
                    return (
                        ScoreOutcome::Unavailable(degraded, Degraded::PrefixIndex),
                        side,
                    );
                }
            };

            let mut matched: HashMap<u64, u32> = HashMap::new();
            for (k, v) in overlap.scores {
                *matched.entry(k.worker_id).or_insert(0) += v;
            }

            for (num, entry) in &group {
                let sample = self.scrape.latest(*num);

                // Raw radix-tree match; the pick path layers in expected
                // overlap later, inside `pick_lock`.
                let kv_match = matched.get(num).copied().unwrap_or(0);

                out.insert(
                    *num,
                    worker_score(entry, &sample, block_size, kv_match, total_blocks),
                );
            }
        }

        let scores: Vec<WorkerScore> = out.into_values().collect();
        (ScoreOutcome::Ok(ScoreResponse { scores }), side)
    }
}
