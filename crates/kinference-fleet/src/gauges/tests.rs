use super::*;
use std::time::Instant as WallInstant;

use prometheus::{Encoder, TextEncoder};

use crate::pool::{Pool, PoolMap, test_pool};
use crate::scrape::MetricsSample;
use crate::workers::WorkerEntry;

struct Harness {
    pools: Arc<PoolRegistry>,
    scrape: Arc<MetricsRegistry>,
    registry: Registry,
    gauges: FleetGauges,
}

fn harness() -> Harness {
    let pools = PoolRegistry::new();
    let scrape = Arc::new(MetricsRegistry::new());
    let registry = Registry::new();
    let gauges = FleetGauges::new(pools.clone(), scrape.clone(), &registry);
    Harness {
        pools,
        scrape,
        registry,
        gauges,
    }
}

impl Harness {
    /// What a scraper would read, minus the handler's dump refresh.
    fn render(&self) -> String {
        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buf)
            .unwrap();
        String::from_utf8(buf).unwrap()
    }

    fn publish(&self, pools: Vec<Arc<Pool>>) {
        self.pools.publish(Arc::new(PoolMap::from_pools(pools)));
    }

    fn scraped_now(&self, id: u64, waiting: f32, kv: f32) {
        self.scrape.record_sample_for_test(
            id,
            MetricsSample {
                scraped_at: Some(WallInstant::now()),
                waiting_mean: waiting,
                kv_cache_usage_mean: kv,
                ..Default::default()
            },
        );
    }

    /// A worker whose last scrape is older than the unresponsive bound.
    fn scraped_stale(&self, id: u64) {
        let age = Duration::from_millis(UNRESPONSIVE_AFTER_MS + 1000);
        self.scrape.record_sample_for_test(
            id,
            MetricsSample {
                scraped_at: Some(WallInstant::now().checked_sub(age).unwrap()),
                ..Default::default()
            },
        );
    }
}

fn entry(num: u64, models: &[&str]) -> WorkerEntry {
    WorkerEntry {
        worker_id_str: format!("w-{num}"),
        model_name_hint: None,
        current_models: models.iter().map(|m| m.to_string()).collect(),
        http_url: None,
        indexed_group: Some(crate::workers::IndexedGroup {
            group_idx: 0,
            kind: None,
            block_size: 16,
        }),
        initial_sync_complete: true,
        kv_events_observed: true,
        score_addr: format!("10.0.0.{num}:8000"),
        engine: None,
        pod_name: None,
        namespace: None,
        node_name: None,
        node_ip: None,
        pod_ip: None,
        availability_zone: None,
        instance_type: None,
    }
}

/// Whole-line match, so `... 0` doesn't match `0.5`.
fn has_line(out: &str, line: &str) -> bool {
    out.lines().any(|l| l == line)
}

/// Series lines of one family.
fn series<'a>(out: &'a str, family: &str) -> Vec<&'a str> {
    out.lines()
        .filter(|l| !l.starts_with('#') && l.starts_with(family))
        .collect()
}

#[tokio::test]
async fn a_gather_before_any_refresh_renders_every_family_at_zero() {
    let h = harness();
    let out = h.render();
    assert!(
        has_line(&out, r#"kinference_workers{state="ok"} 0"#),
        "{out}"
    );
    assert!(
        has_line(&out, r#"kinference_workers{state="unresponsive"} 0"#),
        "{out}"
    );
    assert!(has_line(&out, "kinference_index_blocks 0"), "{out}");
    assert!(has_line(&out, "kinference_index_workers 0"), "{out}");
    assert!(
        series(&out, "kinference_workers_by_model").is_empty(),
        "{out}"
    );
}

/// A worker is unresponsive once its last scrape is over 5s old, and a
/// recovery brings the count back down.
#[tokio::test]
async fn health_splits_on_scrape_age_and_recovers() {
    let h = harness();
    let pool = test_pool("a");
    pool.registry.register(1, entry(1, &["gemma"]));
    pool.registry.register(2, entry(2, &["gemma"]));
    h.publish(vec![pool]);
    h.scraped_now(1, 0.0, 0.0);
    h.scraped_stale(2);

    let out = h.render();
    assert!(
        has_line(&out, r#"kinference_workers{state="ok"} 1"#),
        "{out}"
    );
    assert!(
        has_line(&out, r#"kinference_workers{state="unresponsive"} 1"#),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            r#"kinference_workers_by_model{model="gemma",state="ok"} 1"#
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            r#"kinference_workers_by_model{model="gemma",state="unresponsive"} 1"#
        ),
        "{out}"
    );

    h.scraped_now(2, 0.0, 0.0);
    let out = h.render();
    assert!(
        has_line(&out, r#"kinference_workers{state="ok"} 2"#),
        "{out}"
    );
    assert!(
        has_line(&out, r#"kinference_workers{state="unresponsive"} 0"#),
        "{out}"
    );
}

/// `kinference_workers` stays the fleet total; the per-model family counts a
/// multi-model worker under each model.
#[tokio::test]
async fn the_total_is_independent_of_the_per_model_split() {
    let h = harness();
    let pool = test_pool("a");
    pool.registry.register(1, entry(1, &["gemma", "qwen"]));
    pool.registry.register(2, entry(2, &["gemma"]));
    pool.registry.register(3, entry(3, &["gemma"]));
    pool.registry.register(4, entry(4, &["qwen"]));
    h.publish(vec![pool]);
    for id in 1..=4 {
        h.scraped_now(id, 0.0, 0.0);
    }
    let out = h.render();
    assert!(
        has_line(&out, r#"kinference_workers{state="ok"} 4"#),
        "fleet total unchanged:\n{out}"
    );
    assert!(
        has_line(
            &out,
            r#"kinference_workers_by_model{model="gemma",state="ok"} 3"#
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            r#"kinference_workers_by_model{model="qwen",state="ok"} 2"#
        ),
        "{out}"
    );
}

/// A model nobody serves any more leaves the per-model family rather than
/// freezing at its last count.
#[tokio::test]
async fn a_model_that_disappears_leaves_the_per_model_family() {
    let h = harness();
    let both = test_pool("a");
    both.registry.register(1, entry(1, &["gemma", "qwen"]));
    h.publish(vec![both]);
    h.scraped_stale(1);
    let out = h.render();
    assert!(
        has_line(
            &out,
            r#"kinference_workers_by_model{model="qwen",state="unresponsive"} 1"#
        ),
        "{out}"
    );

    let gemma_only = test_pool("a");
    gemma_only.registry.register(1, entry(1, &["gemma"]));
    h.publish(vec![gemma_only]);
    let out = h.render();
    assert!(
        !out.contains(r#"model="qwen""#),
        "qwen must be gone:\n{out}"
    );
    assert!(
        has_line(
            &out,
            r#"kinference_workers_by_model{model="gemma",state="unresponsive"} 1"#
        ),
        "{out}"
    );
}

/// The saturation gauges carry each worker's windowed means per model, and a
/// departed worker's rows go with it.
#[tokio::test]
async fn saturation_rows_follow_the_live_workers() {
    let h = harness();
    let pool = test_pool("a");
    pool.registry.register(1, entry(1, &["gemma"]));
    h.publish(vec![pool]);
    h.scraped_now(1, 2.5, 0.75);

    let out = h.render();
    assert!(
        has_line(
            &out,
            r#"kinference_worker_queued_prompts_mean30s{model="gemma",worker="10.0.0.1:8000"} 2.5"#
        ),
        "{out}"
    );
    assert!(
        has_line(
            &out,
            r#"kinference_worker_kv_cache_usage_mean30s{model="gemma",worker="10.0.0.1:8000"} 0.75"#
        ),
        "{out}"
    );
    assert_eq!(
        series(&out, "kinference_worker_queued_prompts_mean30s").len(),
        1,
        "{out}"
    );

    h.publish(vec![]);
    let out = h.render();
    assert!(
        series(&out, "kinference_worker_queued_prompts_mean30s").is_empty(),
        "{out}"
    );
    assert!(
        series(&out, "kinference_worker_kv_cache_usage_mean30s").is_empty(),
        "{out}"
    );
}

/// The first refresh dumps at once; inside the interval later refreshes reuse
/// it; past the interval the next one dumps again.
#[tokio::test(start_paused = true)]
async fn the_index_dump_is_read_at_once_and_reused_inside_the_interval() {
    let h = harness();
    let pool = test_pool("a");
    h.publish(vec![pool.clone()]);
    pool.indexer
        .apply_event(test_stored_event(7, &[1, 2]))
        .await;

    h.gauges.refresh_index().await;
    let out = h.render();
    assert!(has_line(&out, "kinference_index_blocks 2"), "{out}");
    assert!(has_line(&out, "kinference_index_workers 1"), "{out}");

    pool.indexer
        .apply_event(test_stored_event(8, &[3, 4, 5]))
        .await;
    h.gauges.refresh_index().await;
    let out = h.render();
    assert!(
        has_line(&out, "kinference_index_blocks 2"),
        "a second refresh inside the interval must reuse the dump:\n{out}"
    );

    tokio::time::advance(INDEX_DUMP_INTERVAL).await;
    h.gauges.refresh_index().await;
    let out = h.render();
    assert!(has_line(&out, "kinference_index_blocks 5"), "{out}");
    assert!(has_line(&out, "kinference_index_workers 2"), "{out}");
}

/// With no pool to dump, the index gauges hold their last value.
#[tokio::test(start_paused = true)]
async fn the_index_gauges_hold_their_last_value_when_no_pool_answers() {
    let h = harness();
    let pool = test_pool("a");
    h.publish(vec![pool.clone()]);
    pool.indexer
        .apply_event(test_stored_event(7, &[1, 2]))
        .await;
    h.gauges.refresh_index().await;
    assert!(has_line(&h.render(), "kinference_index_blocks 2"));

    h.publish(vec![]);
    tokio::time::advance(INDEX_DUMP_INTERVAL).await;
    h.gauges.refresh_index().await;
    let out = h.render();
    assert!(has_line(&out, "kinference_index_blocks 2"), "{out}");
    assert!(has_line(&out, "kinference_index_workers 1"), "{out}");
}
