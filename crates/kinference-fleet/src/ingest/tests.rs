use super::*;

use std::sync::Arc;

use dynamo_kv_router::zmq_wire::{BlockHashValue, KvCacheSpecKind};
use prometheus::{Encoder, Registry, TextEncoder};
use tokio_util::sync::CancellationToken;

use crate::discovery::WorkerConfig;
use crate::eviction::{self, EvictionRx};
use crate::metrics::FleetMetrics;
use crate::models::ModelIndex;
use crate::pool::test_pool;

/// A worker context in the worker set "set-a", whose `kvBlockSize` is 16.
fn context() -> (WorkerContext, Registry, EvictionRx) {
    let pool = test_pool("set-a");
    let registry = Registry::new();
    let (eviction_tx, evictions) = eviction::channel();
    let ctx = WorkerContext {
        cfg: WorkerConfig {
            worker_id_str: "w-1".into(),
            worker_id_num: 1,
            ..Default::default()
        },
        endpoint_key: "10.0.0.1:8000".into(),
        pool_id: pool.id,
        pool_name: pool.name.clone(),
        block_size: pool.block_size,
        registry: pool.registry.clone(),
        indexer: pool.indexer.clone(),
        inflight_prefixes: pool.inflight_prefixes.clone(),
        prom: Arc::new(FleetMetrics::new(&registry)),
        models: Arc::new(ModelIndex::new()),
        eviction_tx,
        cancel: CancellationToken::new(),
    };
    (ctx, registry, evictions)
}

/// One `BlockStored` event with a single block of `block_size` tokens.
fn stored(hash: u64, block_size: usize, kind: Option<KvCacheSpecKind>) -> RawKvEvent {
    RawKvEvent::BlockStored {
        block_hashes: vec![BlockHashValue::Unsigned(hash)],
        parent_block_hash: None,
        token_ids: (0..block_size as u32).collect(),
        block_size,
        medium: None,
        lora_name: None,
        block_mm_infos: None,
        is_eagle: None,
        group_idx: None,
        kv_cache_spec_kind: kind,
        kv_cache_spec_sliding_window: None,
    }
}

fn removed(hash: u64) -> RawKvEvent {
    RawKvEvent::BlockRemoved {
        block_hashes: vec![BlockHashValue::Unsigned(hash)],
        medium: None,
        group_idx: None,
        kv_cache_spec_kind: None,
        kv_cache_spec_sliding_window: None,
    }
}

async fn apply(ctx: &WorkerContext, memory: &mut WorkerMemory, events: Vec<RawKvEvent>) {
    let mut state = ListenerState {
        cursor: CursorState::Initial,
        normalizer: ZmqEventNormalizer::new(ctx.block_size),
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

/// Ingest skips and counts events at another block size, requests no
/// eviction, and indexes the matching event that follows.
#[tokio::test]
async fn a_block_size_mismatch_is_skipped_and_counted() {
    let (ctx, registry, mut evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![
        stored(1, 32, None),
        stored(2, 32, None),
        stored(3, 32, None),
        stored(4, 32, None),
        stored(5, 16, None),
    ];
    apply(&ctx, &mut memory, events).await;

    assert!(has_line(
        &registry,
        "kinference_kv_events_skipped_total{reason=\"block_size_mismatch\",workerset=\"set-a\"} 4"
    ));
    assert!(has_line(
        &registry,
        "kinference_index_events_total{kind=\"stored\"} 1"
    ));
    assert!(evictions.try_recv().is_err());
}

/// An event of a non-main attention group is skipped by design, so the
/// skip counter doesn't count it.
#[tokio::test]
async fn a_non_main_attention_event_is_not_counted() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    let events = vec![stored(1, 4, Some(KvCacheSpecKind::SlidingWindow))];
    apply(&ctx, &mut memory, events).await;

    assert!(!render(&registry).contains("kinference_kv_events_skipped_total{"));
    assert!(memory.warned_block_sizes.is_empty());
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

/// Each mismatching block size is recorded for the WARN line once, up to
/// the cap.
#[tokio::test]
async fn each_mismatching_block_size_warns_once_up_to_the_cap() {
    let (ctx, _registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    apply(
        &ctx,
        &mut memory,
        vec![
            stored(1, 32, None),
            stored(2, 32, None),
            stored(3, 64, None),
        ],
    )
    .await;
    assert_eq!(memory.warned_block_sizes, [32, 64]);

    let garbage = (100..120).map(|n| stored(n, n as usize, None)).collect();
    apply(&ctx, &mut memory, garbage).await;
    assert_eq!(memory.warned_block_sizes.len(), MAX_WARNED_BLOCK_SIZES);
    assert!(memory.warned_block_size_cap);
}

/// An implausible block size counts under its own reason and takes no slot
/// in the list of warned sizes.
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
    assert!(memory.warned_block_sizes.is_empty());
}

/// Ingest skips a removal from a worker that has no indexed block, and
/// applies a removal once a stored block is indexed, across a reconnect.
#[tokio::test]
async fn a_removal_is_skipped_until_a_stored_block_is_indexed() {
    let (ctx, registry, _evictions) = context();
    let mut memory = WorkerMemory::default();
    apply(&ctx, &mut memory, vec![stored(1, 32, None), removed(1)]).await;

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
