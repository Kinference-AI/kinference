use super::*;

fn entry(num: u64, model: &str) -> WorkerEntry {
    WorkerEntry {
        worker_id_str: format!("w-{num}"),
        model_name_hint: Some(model.to_string()),
        current_models: vec![model.to_string()],
        http_url: Some(format!("http://10.0.0.{num}:8000/")),
        indexed_group: None,
        initial_sync_complete: false,
        kv_events_observed: false,
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

/// A worker has no block size until ingest records its indexed group.
#[test]
fn the_indexed_group_gives_the_worker_its_block_size() {
    let reg = WorkerRegistry::new();
    reg.register(1, entry(1, "qwen"));
    assert_eq!(reg.lookup(1).unwrap().block_size(), None);

    let group = IndexedGroup {
        group_idx: 0,
        kind: Some(AttentionKind::Full),
        block_size: 64,
    };
    reg.set_indexed_group(1, group);
    assert_eq!(reg.lookup(1).unwrap().indexed_group, Some(group));
    assert_eq!(reg.lookup(1).unwrap().block_size(), Some(64));
}

#[test]
fn worker_is_not_ready_at_construction() {
    let e = entry(1, "qwen");
    // entry() sets current_models; reset it to mirror a fresh worker.
    let fresh = WorkerEntry {
        current_models: Vec::new(),
        initial_sync_complete: false,
        kv_events_observed: false,
        ..e
    };
    assert!(!fresh.is_ready());
}

/// The most common block size ignores workers without one, and the smaller
/// size wins a tie.
#[test]
fn the_most_common_block_size_breaks_a_tie_towards_the_smaller_size() {
    let at = |num, block_size: Option<u32>| WorkerEntry {
        indexed_group: block_size.map(|block_size| IndexedGroup {
            group_idx: 0,
            kind: None,
            block_size,
        }),
        ..entry(num, "qwen")
    };
    assert_eq!(most_common_block_size(&[at(1, None)]), None);

    let mut workers = vec![at(1, Some(64)), at(2, Some(16)), at(3, None)];
    assert_eq!(most_common_block_size(&workers), Some(16));
    workers.push(at(4, Some(64)));
    assert_eq!(most_common_block_size(&workers), Some(64));
}

#[test]
fn worker_ready_requires_both_signals() {
    // Readiness is `initial_sync_complete && !current_models.is_empty()`.
    let cases: Vec<(&str, bool, bool, bool)> = vec![
        // (label, sync, models_present, expected_ready)
        ("none", false, false, false),
        ("models only", false, true, false),
        ("sync only", true, false, false),
        ("both", true, true, true),
    ];
    for (label, sync, models, expected) in cases {
        let mut e = entry(1, "qwen");
        e.initial_sync_complete = sync;
        e.current_models = if models {
            vec!["qwen".into()]
        } else {
            Vec::new()
        };
        assert_eq!(e.is_ready(), expected, "case `{label}`");
    }
}

#[test]
fn set_initial_sync_complete_updates_existing_entry() {
    let reg = WorkerRegistry::new();
    reg.register(1, entry(1, "qwen"));
    assert!(!reg.lookup(1).unwrap().initial_sync_complete);
    assert!(reg.set_initial_sync_complete(1));
    assert!(reg.lookup(1).unwrap().initial_sync_complete);
    // Idempotent: setting again is a no-op (still true).
    assert!(reg.set_initial_sync_complete(1));
    assert!(reg.lookup(1).unwrap().initial_sync_complete);
}

#[test]
fn set_initial_sync_complete_on_missing_worker_returns_false() {
    let reg = WorkerRegistry::new();
    assert!(!reg.set_initial_sync_complete(999));
}

#[test]
fn any_ready_returns_true_when_at_least_one_ready() {
    let reg = WorkerRegistry::new();
    reg.register(1, entry(1, "qwen"));
    reg.register(2, entry(2, "gemma"));
    // current_models is set via entry(); flip sync on worker 2 only.
    reg.set_initial_sync_complete(2);
    assert!(
        !reg.lookup(1).unwrap().is_ready(),
        "worker 1 missing sync → not ready"
    );
    assert!(
        reg.lookup(2).unwrap().is_ready(),
        "worker 2 has both signals → ready"
    );
    assert!(
        reg.any_ready(),
        "fleet has at least one ready worker → any_ready true"
    );
}

#[test]
fn any_ready_returns_false_when_none_ready() {
    let reg = WorkerRegistry::new();
    reg.register(1, entry(1, "qwen"));
    // No worker has set initial_sync_complete yet.
    assert!(!reg.any_ready(), "no ready workers → any_ready false");
}

/// A WorkerEntry with empty `current_models`, so the first
/// `set_served_models` call pins canonical.
fn empty_entry(num: u64) -> WorkerEntry {
    WorkerEntry {
        current_models: Vec::new(),
        model_name_hint: None,
        ..entry(num, "ignored")
    }
}

#[test]
fn set_served_models_first_call_pins_canonical_and_returns_accepted() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    let verdict = reg.set_served_models(1, &["gemma".into()]);
    assert_eq!(verdict, ServedModelsVerdict::Accepted);
    // The canonical set must reflect the first non-empty report.
    let canonical = reg.canonical_models.lock().unwrap().clone();
    let expected: BTreeSet<String> = ["gemma".to_string()].into_iter().collect();
    assert_eq!(canonical, Some(expected));
}

#[test]
fn set_served_models_matching_returns_accepted() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.register(2, empty_entry(2));
    assert_eq!(
        reg.set_served_models(1, &["gemma".into()]),
        ServedModelsVerdict::Accepted
    );
    // Order doesn't matter (set semantics).
    assert_eq!(
        reg.set_served_models(2, &["gemma".into()]),
        ServedModelsVerdict::Accepted
    );
}

/// A worker that gained a model (e.g. a LoRA adapter loaded at runtime)
/// widens canonical rather than being evicted.
#[test]
fn a_worker_that_gained_a_model_widens_the_set_instead_of_being_evicted() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.register(2, empty_entry(2));
    assert_eq!(
        reg.set_served_models(1, &["base".into()]),
        ServedModelsVerdict::Accepted
    );

    // Adapter loaded on worker 2.
    assert_eq!(
        reg.set_served_models(2, &["base".into(), "adapter".into()]),
        ServedModelsVerdict::Widened {
            added: vec!["adapter".to_string()]
        }
    );
    // Canonical followed, so the next worker reporting the same set is not a
    // conflict.
    assert_eq!(
        reg.set_served_models(1, &["base".into(), "adapter".into()]),
        ServedModelsVerdict::Accepted
    );
    // And the widened model is routable on the worker that has it.
    assert!(
        reg.workers_for_model("adapter")
            .iter()
            .any(|(n, _)| *n == 2)
    );
}

/// A worker that hasn't caught up with a rollout stays routable for what it
/// serves, and canonical is not narrowed.
#[test]
fn a_worker_missing_a_model_is_kept_and_canonical_is_not_narrowed() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.register(2, empty_entry(2));
    reg.set_served_models(1, &["base".into(), "adapter".into()]);

    assert_eq!(
        reg.set_served_models(2, &["base".into()]),
        ServedModelsVerdict::Narrowed {
            missing: vec!["adapter".to_string()]
        }
    );
    // Routable for `base`, not for the model it lacks.
    assert!(reg.workers_for_model("base").iter().any(|(n, _)| *n == 2));
    assert!(
        !reg.workers_for_model("adapter")
            .iter()
            .any(|(n, _)| *n == 2)
    );
    // Canonical still holds both, so worker 1 remains Accepted.
    assert_eq!(
        reg.set_served_models(1, &["base".into(), "adapter".into()]),
        ServedModelsVerdict::Accepted
    );
}

/// A conflict, where each side has something the other lacks, is fatal: two
/// different fleets merged into one pool.
#[test]
fn a_conflicting_model_set_is_still_a_mismatch() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.register(2, empty_entry(2));
    reg.set_served_models(1, &["gemma".into(), "shared".into()]);

    let v = reg.set_served_models(2, &["qwen".into(), "shared".into()]);
    match v {
        ServedModelsVerdict::Mismatch { expected, observed } => {
            assert_eq!(expected, ["gemma", "shared"]);
            assert_eq!(observed, ["qwen", "shared"]);
        }
        other => panic!("a set differing in BOTH directions must be a mismatch: {other:?}"),
    }
    // And by_model was not updated for the conflicting worker.
    assert!(!reg.workers_for_model("qwen").iter().any(|(n, _)| *n == 2));
}

#[test]
fn set_served_models_mismatch_returns_mismatch_and_skips_by_model() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.register(2, empty_entry(2));
    reg.set_served_models(1, &["gemma".into()]);
    let verdict = reg.set_served_models(2, &["qwen".into()]);
    match verdict {
        ServedModelsVerdict::Mismatch { expected, observed } => {
            assert_eq!(expected, vec!["gemma".to_string()]);
            assert_eq!(observed, vec!["qwen".to_string()]);
        }
        other => panic!("expected Mismatch, got {other:?}"),
    }
    // by_model must not have routed `qwen` to worker 2.
    let workers_for_qwen = reg.workers_for_model("qwen");
    assert!(
        workers_for_qwen.is_empty(),
        "rejected worker must not be indexed by its mismatched model"
    );
    // Worker 1 is still indexed under the canonical model.
    let workers_for_gemma = reg.workers_for_model("gemma");
    assert_eq!(workers_for_gemma.len(), 1);
    assert_eq!(workers_for_gemma[0].0, 1);
}

#[test]
fn set_served_models_empty_returns_empty_without_changing_canonical() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.set_served_models(1, &["gemma".into()]);
    // Worker 2 reports empty (engine not ready yet): no change, no eviction.
    reg.register(2, empty_entry(2));
    assert_eq!(reg.set_served_models(2, &[]), ServedModelsVerdict::Empty);
    let canonical = reg.canonical_models.lock().unwrap().clone();
    let expected: BTreeSet<String> = ["gemma".to_string()].into_iter().collect();
    assert_eq!(canonical, Some(expected));
}

/// Editing a Static set's `spec.models` reaches already-registered workers,
/// since the pool update keeps this registry.
#[test]
fn reset_declared_models_repins_and_reroutes_existing_workers() {
    let reg = WorkerRegistry::new();
    reg.register(1, entry(1, "old-model"));
    reg.register(2, entry(2, "old-model"));
    assert!(matches!(
        reg.set_served_models(1, &["old-model".to_string()]),
        ServedModelsVerdict::Accepted
    ));
    assert!(matches!(
        reg.set_served_models(2, &["old-model".to_string()]),
        ServedModelsVerdict::Accepted
    ));

    let touched = reg.reset_declared_models(Some(&["new-model".to_string()]));
    assert_eq!(touched, 2);

    let by_model = reg.by_model.lock().unwrap();
    assert!(
        !by_model.contains_key("old-model"),
        "the old name must stop resolving, or requests route to workers that \
         no longer serve it"
    );
    assert_eq!(by_model.get("new-model").map(|s| s.len()), Some(2));
    drop(by_model);

    // And the pin moved with it, so a worker joining later is accepted.
    reg.register(3, entry(3, "new-model"));
    assert!(matches!(
        reg.set_served_models(3, &["new-model".to_string()]),
        ServedModelsVerdict::Accepted
    ));
}

/// Going back to endpoint discovery drops the pin so the next scrape can
/// establish it.
#[test]
fn reset_declared_models_with_none_only_drops_the_pin() {
    let reg = WorkerRegistry::new();
    reg.register(1, entry(1, "m"));
    reg.set_served_models(1, &["m".to_string()]);
    assert!(reg.canonical_models.lock().unwrap().is_some());

    reg.reset_declared_models(None);
    assert!(reg.canonical_models.lock().unwrap().is_none());
    assert!(
        reg.by_model.lock().unwrap().contains_key("m"),
        "routing must keep working until a scrape says otherwise"
    );
}

#[test]
fn canonical_models_cleared_when_registry_becomes_empty() {
    let reg = WorkerRegistry::new();
    reg.register(1, empty_entry(1));
    reg.set_served_models(1, &["gemma".into()]);
    assert!(reg.canonical_models.lock().unwrap().is_some());
    // Unregistering the last worker clears canonical, so a rolling model
    // swap can re-pin it.
    reg.unregister(1);
    assert!(reg.canonical_models.lock().unwrap().is_none());
}
