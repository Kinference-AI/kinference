use super::*;

use std::sync::Arc;

use dynamo_kv_router::protocols::{BlockHashOptions, compute_block_hash_for_seq};
use dynamo_kv_router::zmq_wire::BlockHashValue;
use prometheus::{Encoder, Registry, TextEncoder};
use tokio_util::sync::CancellationToken;

use crate::discovery::WorkerConfig;
use crate::eviction::{self, EvictionRx};
use crate::metrics::FleetMetrics;
use crate::models::ModelIndex;
use crate::pool::{Pool, test_pool};
use crate::workers::WorkerEntry;

const FULL: Option<KvCacheSpecKind> = Some(KvCacheSpecKind::FullAttention);

/// The context of worker `num`, registered in `pool`.
fn worker_context(pool: &Pool, num: u64, prom: &Arc<FleetMetrics>) -> (WorkerContext, EvictionRx) {
    let cfg = WorkerConfig {
        worker_id_str: format!("w-{num}"),
        worker_id_num: num,
        ..Default::default()
    };
    pool.registry.register(num, WorkerEntry::from_config(&cfg));
    let (eviction_tx, evictions) = eviction::channel();
    let ctx = WorkerContext {
        cfg,
        endpoint_key: format!("10.0.0.{num}:8000"),
        pool_id: pool.id,
        pool_name: pool.name.clone(),
        registry: pool.registry.clone(),
        indexer: pool.indexer.clone(),
        inflight_prefixes: pool.inflight_prefixes.clone(),
        prom: prom.clone(),
        models: Arc::new(ModelIndex::new()),
        eviction_tx,
        cancel: CancellationToken::new(),
    };
    (ctx, evictions)
}

/// The context of worker 1 in the worker set "set-a".
fn context() -> (WorkerContext, Registry, EvictionRx) {
    let pool = test_pool("set-a");
    let registry = Registry::new();
    let prom = Arc::new(FleetMetrics::new(&registry));
    let (ctx, evictions) = worker_context(&pool, 1, &prom);
    (ctx, registry, evictions)
}

/// The first `n` token IDs of the prompt that starts at `first`.
fn prompt(first: u32, n: usize) -> Vec<u32> {
    (first..first + n as u32).collect()
}

/// One `BlockStored` event with a single block: the first `block_size`
/// tokens of the prompt that starts at `first`.
fn stored_in(
    hash: u64,
    first: u32,
    block_size: usize,
    group_idx: Option<u32>,
    kind: Option<KvCacheSpecKind>,
) -> RawKvEvent {
    RawKvEvent::BlockStored {
        block_hashes: vec![BlockHashValue::Unsigned(hash)],
        parent_block_hash: None,
        token_ids: prompt(first, block_size),
        block_size,
        medium: None,
        lora_name: None,
        block_mm_infos: None,
        is_eagle: None,
        group_idx,
        kv_cache_spec_kind: kind,
        kv_cache_spec_sliding_window: None,
    }
}

/// A `BlockStored` event that names no group, as an older vLLM sends it.
fn stored(hash: u64, block_size: usize, kind: Option<KvCacheSpecKind>) -> RawKvEvent {
    stored_in(hash, 0, block_size, None, kind)
}

fn removed_in(hash: u64, group_idx: Option<u32>, kind: Option<KvCacheSpecKind>) -> RawKvEvent {
    RawKvEvent::BlockRemoved {
        block_hashes: vec![BlockHashValue::Unsigned(hash)],
        medium: None,
        group_idx,
        kv_cache_spec_kind: kind,
        kv_cache_spec_sliding_window: None,
    }
}

fn removed(hash: u64) -> RawKvEvent {
    removed_in(hash, None, None)
}

async fn apply(ctx: &WorkerContext, memory: &mut WorkerMemory, events: Vec<RawKvEvent>) {
    let mut state = ListenerState {
        cursor: CursorState::Initial,
        memory,
        warned: ListenerWarnings::default(),
    };
    let batch = KvEventBatch {
        ts: 0.0,
        events,
        data_parallel_rank: None,
    };
    apply_batch(ctx, &mut state, 0, batch).await;
}

/// How many blocks of the prompt that starts at `first` the radix tree
/// holds for the worker, when the lookup cuts the prompt at `block_size`.
async fn matched(ctx: &WorkerContext, first: u32, block_size: u32) -> u32 {
    ctx.indexer.flush().await;
    let hashes =
        compute_block_hash_for_seq(&prompt(first, 256), block_size, BlockHashOptions::default());
    let overlap = ctx.indexer.find_matches(hashes).await.unwrap();
    overlap
        .scores
        .iter()
        .filter(|(w, _)| w.worker_id == ctx.cfg.worker_id_num)
        .map(|(_, n)| *n)
        .sum()
}

fn indexed_group(ctx: &WorkerContext) -> Option<IndexedGroup> {
    ctx.registry
        .lookup(ctx.cfg.worker_id_num)
        .unwrap()
        .indexed_group
}

fn render(registry: &Registry) -> String {
    let mut buf = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut buf)
        .unwrap();
    String::from_utf8(buf).unwrap()
}

fn has_line(registry: &Registry, line: &str) -> bool {
    render(registry).lines().any(|l| l == line)
}

/// `classify` follows the rule for the indexed group: the lowest group index
/// wins, and the group's largest block size wins.
#[test]
fn classify_selects_the_lowest_group_at_its_largest_block_size() {
    let indexed = Some(IndexedGroup {
        group_idx: 1,
        kind: None,
        block_size: 32,
    });

    assert_eq!(classify(None, 1, 32), Verdict::Select);
    assert_eq!(classify(indexed, 1, 32), Verdict::Index);
    assert_eq!(
        classify(indexed, 1, 16),
        Verdict::Skip(SKIP_BLOCK_SIZE_MISMATCH)
    );
    assert_eq!(classify(indexed, 1, 64), Verdict::Select);
    // A lower group takes over at any block size.
    assert_eq!(classify(indexed, 0, 16), Verdict::Select);
    assert_eq!(classify(indexed, 0, 64), Verdict::Select);
    // A higher group is skipped at any block size.
    assert_eq!(
        classify(indexed, 2, 16),
        Verdict::Skip(SKIP_UNINDEXED_GROUP)
    );
    assert_eq!(
        classify(indexed, 2, 64),
        Verdict::Skip(SKIP_UNINDEXED_GROUP)
    );
}

/// The worker's first `BlockStored` event sets the worker's block size, and
/// ingest indexes the event at that size.
#[tokio::test]
async fn the_first_stored_event_sets_the_block_size() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    assert_eq!(indexed_group(&ctx), None);

    apply(&ctx, &mut memory, vec![stored(1, 32, None)]).await;

    let group = IndexedGroup {
        group_idx: 0,
        kind: None,
        block_size: 32,
    };
    assert_eq!(indexed_group(&ctx), Some(group));
    assert_eq!(matched(&ctx, 0, 32).await, 1);
    assert!(!render(&registry).contains("kinference_kv_events_skipped_total{"));
}

/// Ingest skips and counts an event of the indexed group at a smaller block
/// size, keeps the worker's block size and requests no eviction.
#[tokio::test]
async fn a_smaller_block_size_is_skipped_and_counted() {
    let (ctx, registry, mut evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![
        stored(1, 32, None),
        stored(2, 16, None),
        stored(3, 16, None),
    ];
    apply(&ctx, &mut memory, events).await;

    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"block_size_mismatch\",workerset=\"set-a\"} 2"
    ));
    assert!(has_line(
        &registry,
        "kinference_index_events_total{kind=\"stored\"} 1"
    ));
    assert_eq!(indexed_group(&ctx).unwrap().block_size, 32);
    assert!(evictions.try_recv().is_err());
}

/// A larger block size of the indexed group replaces the worker's block
/// size, and ingest first clears the worker's blocks.
#[tokio::test]
async fn a_larger_block_size_clears_the_worker_and_takes_over() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    apply(&ctx, &mut memory, vec![stored_in(1, 0, 16, Some(0), FULL)]).await;
    assert_eq!(matched(&ctx, 0, 16).await, 1);

    apply(
        &ctx,
        &mut memory,
        vec![stored_in(2, 1000, 64, Some(0), FULL)],
    )
    .await;

    assert_eq!(indexed_group(&ctx).unwrap().block_size, 64);
    assert_eq!(matched(&ctx, 0, 16).await, 0);
    assert_eq!(matched(&ctx, 1000, 64).await, 1);
    assert!(has_line(
        &registry,
        "kinference_index_events_total{kind=\"cleared\"} 1"
    ));
}

/// The first `BlockStored` event of a group with a lower index makes that
/// group the indexed group, and ingest then skips the other group's events.
#[tokio::test]
async fn a_lower_group_takes_over() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    apply(&ctx, &mut memory, vec![stored_in(1, 0, 16, Some(1), FULL)]).await;
    assert_eq!(indexed_group(&ctx).unwrap().group_idx, 1);

    let events = vec![
        stored_in(2, 1000, 64, Some(0), FULL),
        stored_in(3, 2000, 16, Some(1), FULL),
    ];
    apply(&ctx, &mut memory, events).await;

    let group = IndexedGroup {
        group_idx: 0,
        kind: Some(AttentionKind::Full),
        block_size: 64,
    };
    assert_eq!(indexed_group(&ctx), Some(group));
    assert_eq!(matched(&ctx, 0, 16).await, 0);
    assert_eq!(matched(&ctx, 1000, 64).await, 1);
    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"unindexed_attention_group\",workerset=\"set-a\"} 1"
    ));
}

/// A stored block and a removal of a second main attention group leave the
/// indexed group's block in the radix tree, and both events are counted.
/// vLLM gives the block of both groups the same engine hash.
#[tokio::test]
async fn events_of_a_second_main_group_are_skipped_and_counted() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![
        stored_in(1, 0, 16, Some(0), FULL),
        stored_in(1, 0, 64, Some(1), FULL),
    ];
    apply(&ctx, &mut memory, events).await;
    assert_eq!(matched(&ctx, 0, 16).await, 1);

    apply(&ctx, &mut memory, vec![removed_in(1, Some(1), FULL)]).await;

    assert_eq!(matched(&ctx, 0, 16).await, 1);
    assert_eq!(matched(&ctx, 0, 64).await, 0);
    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"unindexed_attention_group\",workerset=\"set-a\"} 2"
    ));
    assert!(has_line(
        &registry,
        "kinference_index_blocks_removed_total 0"
    ));

    apply(&ctx, &mut memory, vec![removed_in(1, Some(0), FULL)]).await;
    assert_eq!(matched(&ctx, 0, 16).await, 0);
}

/// Two workers of one worker set publish different block sizes, and the
/// radix tree holds each worker's blocks at the worker's own size.
#[tokio::test]
async fn two_workers_of_one_worker_set_keep_their_own_block_sizes() {
    let pool = test_pool("set-a");
    let prom = Arc::new(FleetMetrics::new(&Registry::new()));
    let (small, _e1) = worker_context(&pool, 1, &prom);
    let (large, _e2) = worker_context(&pool, 2, &prom);

    let mut memory = WorkerMemory::default();
    apply(&small, &mut memory, vec![stored(1, 16, None)]).await;
    let mut memory = WorkerMemory::default();
    apply(&large, &mut memory, vec![stored(1, 64, None)]).await;

    assert_eq!(indexed_group(&small).unwrap().block_size, 16);
    assert_eq!(indexed_group(&large).unwrap().block_size, 64);
    assert_eq!(matched(&small, 0, 16).await, 1);
    assert_eq!(matched(&large, 0, 16).await, 0);
    assert_eq!(matched(&small, 0, 64).await, 0);
    assert_eq!(matched(&large, 0, 64).await, 1);
}

/// An event of a non-main attention group is skipped by design, so the
/// skip counter doesn't count it and the worker gets no block size from it.
#[tokio::test]
async fn a_non_main_attention_event_is_not_counted() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![stored(1, 4, Some(KvCacheSpecKind::SlidingWindow))];
    apply(&ctx, &mut memory, events).await;

    assert!(!render(&registry).contains("kinference_kv_events_skipped_total{"));
    assert_eq!(indexed_group(&ctx), None);
}

/// An event of an attention group that ingest can't classify counts under
/// the normalizer's reason.
#[tokio::test]
async fn an_unknown_kind_event_is_counted_under_its_own_reason() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![stored(1, 16, Some(KvCacheSpecKind::Unknown))];
    apply(&ctx, &mut memory, events).await;

    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"unknown_kind\",workerset=\"set-a\"} 1"
    ));
}

/// An implausible block size counts under its own reason and gives the
/// worker no block size.
#[tokio::test]
async fn an_implausible_block_size_is_counted_under_its_own_reason() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    apply(
        &ctx,
        &mut memory,
        vec![stored(1, 1568, None), stored(2, 0, None)],
    )
    .await;

    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"implausible_block_size\",workerset=\"set-a\"} 2"
    ));
    assert!(memory.warned_implausible_block_size);
    assert_eq!(indexed_group(&ctx), None);
}

/// Ingest skips a removal from a worker that has no indexed block, and
/// applies a removal once a stored block is indexed, across a reconnect.
#[tokio::test]
async fn a_removal_is_skipped_until_a_stored_block_is_indexed() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    apply(&ctx, &mut memory, vec![removed(1)]).await;

    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"nothing_stored\",workerset=\"set-a\"} 1"
    ));
    assert!(has_line(
        &registry,
        "kinference_index_blocks_removed_total 0"
    ));

    apply(&ctx, &mut memory, vec![stored(2, 16, None)]).await;
    // A second `apply` builds a new listener state, as a reconnect does.
    apply(&ctx, &mut memory, vec![removed(2)]).await;
    assert!(has_line(
        &registry,
        "kinference_index_blocks_removed_total 1"
    ));
}

/// After a reconnect, ingest still applies a removal that names the indexed
/// group by index alone, when that index is above 0.
#[tokio::test]
async fn a_removal_without_a_kind_is_applied_after_a_reconnect() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![
        stored_in(1, 0, 4, Some(0), Some(KvCacheSpecKind::SlidingWindow)),
        stored_in(1, 0, 16, Some(1), FULL),
    ];
    apply(&ctx, &mut memory, events).await;
    assert_eq!(indexed_group(&ctx).unwrap().group_idx, 1);
    assert_eq!(matched(&ctx, 0, 16).await, 1);

    // A second `apply` builds a new listener state, as a reconnect does.
    apply(&ctx, &mut memory, vec![removed_in(1, Some(1), None)]).await;

    assert_eq!(matched(&ctx, 0, 16).await, 0);
    assert!(!render(&registry).contains("kinference_kv_events_skipped_total{"));
}

#[test]
fn parse_seq_payload_extracts_seq_and_payload() {
    let msg = vec![
        b"topic".to_vec(),
        42u64.to_be_bytes().to_vec(),
        b"hello".to_vec(),
    ];
    let (seq, payload) = parse_seq_payload(&msg).expect("3-frame msg parses");
    assert_eq!(seq, 42);
    assert_eq!(payload, b"hello");
}

#[test]
fn parse_seq_payload_rejects_wrong_frame_count() {
    assert!(parse_seq_payload(&[]).is_none());
    assert!(parse_seq_payload(&[b"a".to_vec()]).is_none());
    assert!(parse_seq_payload(&[b"a".to_vec(), b"b".to_vec()]).is_none());
    let four = vec![b"a".to_vec(); 4];
    assert!(parse_seq_payload(&four).is_none());
}

#[test]
fn parse_seq_payload_rejects_malformed_seq() {
    let msg = vec![
        b"topic".to_vec(),
        vec![1, 2, 3], // wrong length, not 8 bytes
        b"payload".to_vec(),
    ];
    assert!(parse_seq_payload(&msg).is_none());
}

/// Cursor state machine: a fresh listener starts at `Initial`;
/// after applying seq=10, it's `Live(10)`; seq=11 is
/// `Contiguous`; seq=9 is `Stale`; seq=20 reports a `Gap`.
#[test]
fn cursor_state_transitions_through_initial_live_gap_stale() {
    let mut cursor = CursorState::Initial;
    // First observation: Initial { got: 10 } when starting fresh
    // at a non-zero seq.
    assert!(matches!(
        cursor.observe(10),
        CursorObservation::Initial { got: 10 }
    ));
    // After applying, advance to Live(10).
    cursor = cursor.advance_to(10);
    assert_eq!(cursor, CursorState::Live(10));

    // Contiguous next.
    assert!(matches!(
        cursor.observe(11),
        CursorObservation::Contiguous { got: 11 }
    ));
    cursor = cursor.advance_to(11);

    // Earlier seq → Stale.
    assert!(matches!(
        cursor.observe(9),
        CursorObservation::Stale {
            got: 9,
            last_applied: Some(11)
        }
    ));
    // Cursor not advanced after Stale.
    assert_eq!(cursor, CursorState::Live(11));

    // Skip ahead → Gap.
    assert!(matches!(
        cursor.observe(20),
        CursorObservation::Gap {
            expected: 12,
            got: 20
        }
    ));
}

/// After a barrier (cache Cleared event) the cursor is
/// invalidated; the next live frame observation reports
/// `FreshAfterBarrier` rather than `Gap`, so we don't trigger an
/// inline replay against a barrier-invalidated history.
#[test]
fn barrier_invalidates_cursor_and_fresh_observation_is_not_gap() {
    let cursor = CursorState::Live(50);
    let invalidated = cursor.invalidate_by_barrier();
    assert!(matches!(
        invalidated.observe(100),
        CursorObservation::FreshAfterBarrier {
            got: 100,
            last_before_barrier: Some(50)
        }
    ));
    // Frames at or before the barrier are stale (don't reapply).
    assert!(matches!(
        invalidated.observe(50),
        CursorObservation::Stale {
            got: 50,
            last_applied: Some(50)
        }
    ));
}
