use super::*;
use crate::picker::WorkerScore;
use crate::picker::predicted_wait_ms;
use std::sync::{Arc, Mutex};

fn synthetic_record() -> PendingObservation {
    let worker = WorkerScore {
        name: "qwen-vllm-1".to_string(),
        endpoint: "10.0.0.1:8000".to_string(),
        matched_blocks: 2,
        total_blocks: 4,
        queue_waiting: 2,
        queue_running: 1,
        queue_swapped: 0,
        kv_cache_usage: 0.23,
        completion_rate_per_sec: 2.5,
        block_size: Some(16),
    };
    let predictor_cfg = PredictorConfig::default();
    let picker_cfg = PickerConfig::default();
    let prediction = predicted_wait_ms(&worker, &predictor_cfg, &[], Instant::now());
    PendingObservation {
        request_id: 42,
        workerset: "set-a".to_string(),
        profile: "h100-tp2".to_string(),
        inflight: InflightHandles {
            inflight: std::sync::Arc::new(kinference_fleet::inflight::InflightRegistry::new()),
            prefixes: std::sync::Arc::new(kinference_fleet::inflight::InflightPrefixes::new()),
        },
        picked_at: Instant::now(),
        chosen_worker: worker,
        prediction,
        q_inflight: 3,
        kv_blocks: 2,
        inflight_workload_blocks: 17,
        q_ms_constant: 1000.0,
        q_ms_workload: 250.0,
        inflight_overlap_blocks: vec![5, 3],
        expected_overlap_alpha: 1.0,
        predictor_cfg,
        picker_cfg,
        external_request_id: Some("trace-abc-123".to_string()),
        replication_fired: false,
        model: "m".to_string(),
        streaming: true,
        qos: Qos::Interactive,
        tenant: "acme".to_string(),
    }
}

/// Collects the JSON log lines that a subscriber writes.
#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Finalize `record` under a JSON subscriber that enables only the
/// observation target, and return the parsed events.
fn finalize_capturing(record: PendingObservation) -> Vec<serde_json::Value> {
    let buf = LogBuf::default();
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let (_registry, prom) = crate::tokenizer::test_support::router_metrics();
    let obs = Observations::new(
        Duration::from_secs(30),
        Arc::new(kinference_policy::provider_health::ProviderHealthStore::new()),
        prom,
    );
    let id = record.request_id;
    obs.pending.insert(record);
    tracing::subscriber::with_default(subscriber, || {
        assert!(obs.finalize(id, 523, 200));
    });
    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    out.lines()
        .map(|l| serde_json::from_str::<serde_json::Value>(l).expect("one JSON object per line"))
        .filter(|e| e["target"] == OBSERVATION_TARGET)
        .collect()
}

#[test]
fn finalize_emits_one_observation_event_with_named_fields() {
    let events = finalize_capturing(synthetic_record());
    assert_eq!(events.len(), 1, "{events:?}");
    let e = &events[0];
    assert_eq!(e["level"], "DEBUG");
    assert_eq!(e["response_id"], 42);
    assert_eq!(e["req"], "trace-abc-123");
    assert_eq!(e["workerset"], "set-a");
    assert_eq!(e["profile"], "h100-tp2");
    assert_eq!(e["endpoint"], "10.0.0.1:8000");
    assert_eq!(e["upstream_status"], 200);
    assert_eq!(e["observed_ms"], 523);
    let predicted = e["predicted_ms"].as_f64().unwrap();
    let residual = e["residual_ms"].as_f64().unwrap();
    assert!((523.0 - predicted - residual).abs() < 1e-3, "{e}");
    assert_eq!(e["q_inflight"], 3);
    assert_eq!(e["q_ms_constant"], 1000.0);
    assert_eq!(e["inflight_dispatches"], 2);
    let overlap: Vec<u32> =
        serde_json::from_str(e["inflight_overlap_blocks"].as_str().unwrap()).unwrap();
    assert_eq!(overlap, [5, 3]);
    assert_eq!(e["replication_fired"], false);
    assert_eq!(
        e["queue_ms_per_entry"].as_f64().unwrap() as f32,
        PredictorConfig::default().queue_ms_per_entry
    );
    assert_eq!(e["top_k_min"], PickerConfig::default().top_k_min);
}

#[test]
fn finalize_logs_an_empty_overlap_list_when_nothing_is_in_flight() {
    let mut r = synthetic_record();
    r.inflight_overlap_blocks = vec![];
    let events = finalize_capturing(r);
    assert_eq!(events[0]["inflight_dispatches"], 0);
    assert_eq!(events[0]["inflight_overlap_blocks"], "[]");
}

#[test]
fn finalize_omits_req_when_the_caller_sent_none() {
    let mut r = synthetic_record();
    r.external_request_id = None;
    let events = finalize_capturing(r);
    assert_eq!(events.len(), 1);
    assert!(events[0].get("req").is_none(), "{}", events[0]);
}

#[test]
fn finalize_emits_no_observation_event_for_a_non_streaming_request() {
    let mut r = synthetic_record();
    r.streaming = false;
    assert!(finalize_capturing(r).is_empty());
}

#[test]
fn pending_observations_take_returns_inserted_entry() {
    let pending = PendingObservations::new(Duration::from_secs(30));
    let r = synthetic_record();
    let id = r.request_id;
    pending.insert(r);
    assert_eq!(pending.len(), 1);
    let taken = pending.take(id);
    assert!(taken.is_some());
    assert_eq!(pending.len(), 0);
    // Second take returns None: the entry was consumed.
    assert!(pending.take(id).is_none());
}

#[test]
fn pending_observations_take_returns_none_for_unknown_id() {
    let pending = PendingObservations::new(Duration::from_secs(30));
    assert!(pending.take(999).is_none());
}

#[test]
fn pending_observations_sweep_evicts_expired() {
    let pending = PendingObservations::new(Duration::from_millis(50));
    let mut r = synthetic_record();
    r.picked_at = Instant::now() - Duration::from_secs(1);
    pending.insert(r);
    assert_eq!(pending.len(), 1);
    let evicted = pending.take_expired();
    assert_eq!(evicted.len(), 1);
    assert_eq!(pending.len(), 0);
}

#[test]
fn pending_observations_sweep_keeps_fresh() {
    let pending = PendingObservations::new(Duration::from_secs(30));
    pending.insert(synthetic_record());
    assert_eq!(pending.take_expired().len(), 0);
    assert_eq!(pending.len(), 1);
}

/// `RequestLabels` round-trips the labels a feedback RPC reads back.
///
/// `ReportFinalToken` uses these to label the token and ITL metrics. Both
/// local and provider picks carry the caller's real `qos`.
#[test]
fn request_labels_round_trip_the_metric_labels() {
    let labels = RequestLabels::new(Duration::from_secs(600));

    // A local pick.
    labels.insert(
        1,
        RequestMeta {
            model: "qwen-2.5".to_string(),
            qos: Qos::Batch,
            tenant: "acme".to_string(),
            streaming: true,
            picked_at: Instant::now(),
            provider: None,
        },
    );
    let got = labels.take(1).expect("just inserted");
    assert_eq!(got.model, "qwen-2.5");
    assert_eq!(got.qos, Qos::Batch, "the caller's qos, not a default");
    assert_eq!(got.tenant, "acme");
    assert!(got.provider.is_none());

    // Taking consumes, so a second ReportFinalToken finds nothing.
    assert!(labels.take(1).is_none(), "take must consume the entry");

    // A provider pick carries the same request-side labels plus `provider`.
    // `Deferrable`, since it is neither the default nor the first variant.
    labels.insert(
        2,
        RequestMeta {
            model: "llama-3".to_string(),
            qos: Qos::Deferrable,
            tenant: "globex".to_string(),
            // Non-streaming, whose reported first-token time is total latency.
            streaming: false,
            picked_at: Instant::now(),
            provider: Some(kinference_policy::engine::ObjectKey {
                namespace: "default".to_string(),
                name: "together".to_string(),
            }),
        },
    );
    // A peek must not consume, since provider feedback arrives in stages.
    assert!(labels.provider_pick_for(2).is_some());
    // The peek carries `streaming`, which the provider feedback path needs.
    let peek = labels.provider_pick_for(2).expect("still present");
    assert!(!peek.streaming, "the peek must carry the streaming flag");
    assert_eq!(peek.model, "llama-3");

    let got = labels.take(2).expect("peek must not have consumed it");
    assert_eq!(got.qos, Qos::Deferrable, "off-cluster picks keep their qos");
    assert_eq!(got.tenant, "globex");
    assert!(!got.streaming);
}
