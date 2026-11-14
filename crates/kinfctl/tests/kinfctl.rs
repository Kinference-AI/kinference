//! `kinfctl` end to end: the real binary, real files, real exit statuses.
//!
//! The checks are unit-tested in `kinference-policy`'s `check/tests.rs`. These
//! cover the contract scripts rely on: which inputs are read, stdout versus
//! stderr, and the exit statuses for "clean", "findings" and "could not check".

use std::io::Write;
use std::process::{Command, Output, Stdio};

const KINFCTL: &str = env!("CARGO_BIN_EXE_kinfctl");

// The demo fleet: one pool over three local worker sets and one provider, one
// policy written against it, and two policies with deliberate mistakes. These
// are the manifests several tests share; one only a single test needs is
// written inline in that test.

const POOL_ALL: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePool
metadata:
  name: inference-all
  namespace: default
spec:
  destinations:
    - {kind: InferenceWorkerSet, name: gemma-4-12b}
    - {kind: InferenceWorkerSet, name: qwen-2-5-32b}
    - {kind: InferenceWorkerSet, name: qwen-3-6-35b-a3b}
    - {kind: InferenceProvider,  name: databricks}
"#;

const WORKERSET_GEMMA_4: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceWorkerSet
metadata:
  name: gemma-4-12b
  namespace: default
spec:
  profile: default
  kvBlockSize: 64
  serviceRef:
    name: gemma-4-12b-vllm
    port: http
  kvEvents:
    port: kv-pub
  models:
    - {name: google/gemma-4-12B-it, aliases: [gemma-4]}
"#;

const WORKERSET_QWEN_2_5: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceWorkerSet
metadata:
  name: qwen-2-5-32b
  namespace: default
spec:
  profile: default
  kvBlockSize: 16
  serviceRef:
    name: qwen-vllm
    port: http
  kvEvents:
    port: kv-pub
  models:
    - name: Qwen/Qwen2.5-32B-Instruct-AWQ
      aliases: [qwen-2.5, qwen-simple]
"#;

const WORKERSET_QWEN_3_6_35B_A3B: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceWorkerSet
metadata:
  name: qwen-3-6-35b-a3b
  namespace: default
spec:
  profile: default
  kvBlockSize: 16
  serviceRef:
    name: qwen-3-6-35b-a3b-vllm
    port: http
  kvEvents:
    port: kv-pub
  models:
    - {name: Qwen/Qwen3.6-35B-A3B, aliases: [qwen3.6]}
"#;

const PROVIDER_DATABRICKS: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceProvider
metadata:
  name: databricks
  namespace: default
spec:
  backendRef:
    name: databricks
  api: OpenAI
  path: /serving-endpoints/vllm-endpoint/invocations
  models:
    - name: Qwen/Qwen2.5-32B-Instruct-Turbo
      aliases: [qwen-2.5, qwen-simple, HuggingFaceTB/SmolLM2-135M-Instruct]
      contextWindow: 32768
"#;

const POLICY_1: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: policy-1, namespace: default}
spec:
  rules:
  - name: batch-goes-to-gemma
    matches:
    - model: qwen-2.5
      qos: [batch]
    route:
      consider:
      - models: {names: [gemma-4]}
      select: [{type: Local, minimize: PredictedTTFT}]
"#;

/// Deliberately wrong in every way `kinfctl check` should catch without a
/// cluster. Each mistake is one an author, or an assistant guessing names,
/// makes easily, and none of them fails at `kubectl apply`.
const BROKEN_POLICY: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: broken, namespace: default}
spec:
  rules:
  - name: burst-to-cloud
    matches:
    - tennant: acme                    # misspelled: the API server would drop it
    route:
      exclude:
      - name: shed-saturated
        drop:
          backend:      {type: Local}
          kvCacheUsage: {greaterThan: 80}  # a percentage against a 0-1 fraction
      select:
      - {type: Local, order: [qwen-2-5-32b, gemma-4-12b]}  # no Random tie-break
      - {type: Provider, order: [databriks]}                # misspelled provider
"#;

/// A value the schema's enum rejects. `kubectl apply` would refuse this one,
/// but only after the round trip; `kinfctl check` says where, before.
const BAD_ENUM_POLICY: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: bad-enum, namespace: default}
spec:
  rules:
  - name: r
    route:
      consider:
      - type: local
      select:
      - {minimize: PredictedTTFT}
"#;

fn kinfctl(args: &[&str]) -> Output {
    Command::new(KINFCTL)
        .args(args)
        .output()
        .expect("kinfctl runs")
}

fn kinfctl_with_stdin(args: &[&str], stdin: &str) -> Output {
    let mut child = Command::new(KINFCTL)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("kinfctl runs");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

/// The demo fleet on disk, since `kinfctl` reads files. The shared manifests
/// are written under their names on creation; a test writes its own with
/// [`Demo::write`]. Removed with the test.
struct Demo {
    dir: tempfile::TempDir,
}

impl Demo {
    fn new() -> Self {
        let demo = Self {
            dir: tempfile::tempdir().expect("temp dir"),
        };
        std::fs::create_dir(demo.dir.path().join("databricks")).unwrap();
        for (name, text) in [
            ("pool-all.yaml", POOL_ALL),
            ("workerset-gemma-4.yaml", WORKERSET_GEMMA_4),
            ("workerset-qwen-2-5.yaml", WORKERSET_QWEN_2_5),
            (
                "workerset-qwen-3-6-35b-a3b.yaml",
                WORKERSET_QWEN_3_6_35B_A3B,
            ),
            ("databricks/provider.yaml", PROVIDER_DATABRICKS),
            ("policy-1.yaml", POLICY_1),
            ("broken-policy.yaml", BROKEN_POLICY),
            ("bad-enum-policy.yaml", BAD_ENUM_POLICY),
        ] {
            demo.write(name, text);
        }
        demo
    }

    /// Write `text` as `name` and return its path.
    fn write(&self, name: &str, text: &str) -> String {
        let path = self.dir.path().join(name);
        std::fs::write(&path, text).unwrap();
        path.display().to_string()
    }

    fn path(&self, name: &str) -> String {
        self.dir.path().join(name).display().to_string()
    }

    /// The pool and every destination's own manifest.
    fn destinations(&self) -> Vec<String> {
        [
            "pool-all.yaml",
            "workerset-gemma-4.yaml",
            "workerset-qwen-2-5.yaml",
            "workerset-qwen-3-6-35b-a3b.yaml",
            "databricks/provider.yaml",
        ]
        .map(|name| self.path(name))
        .to_vec()
    }
}

/// What `kubectl get infpool,iws,infprov -o yaml` prints for the demo fleet:
/// one `kind: List` of the objects as the API server returns them, with no
/// `---` at the end. Their status is deliberately not one this build's types
/// accept: it is the server's, and kinfctl must not trip over it.
fn live_objects() -> String {
    let mut items = Vec::new();
    for text in [
        POOL_ALL,
        WORKERSET_GEMMA_4,
        WORKERSET_QWEN_2_5,
        WORKERSET_QWEN_3_6_35B_A3B,
        PROVIDER_DATABRICKS,
    ] {
        for mut obj in serde_saphyr::from_multiple::<serde_json::Value>(text).unwrap() {
            if !obj["kind"]
                .as_str()
                .is_some_and(|k| k.starts_with("Inference"))
            {
                continue;
            }
            let meta = obj["metadata"].as_object_mut().unwrap();
            meta.entry("namespace").or_insert("default".into());
            meta.insert("resourceVersion".into(), "123456".into());
            meta.insert("uid".into(), "6f1c2b9e-0000-4000-8000-000000000000".into());
            obj["status"] = serde_json::json!({
                "conditions": [{"type": "Accepted", "status": "True"}],
            });
            items.push(obj);
        }
    }
    serde_saphyr::to_string(&serde_json::json!({
        "apiVersion": "v1",
        "items": items,
        "kind": "List",
        "metadata": {"resourceVersion": ""},
    }))
    .unwrap()
}

/// Clean is quiet: an `ok` line per policy and nothing else.
#[test]
fn a_clean_fleet_prints_one_ok_line_per_policy() {
    let demo = Demo::new();
    let policy_3 = demo.write(
        "policy-3.yaml",
        r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: policy-3, namespace: default}
spec:
  rules:
  - name: burst-to-cloud
    matches:
      - tenant: ""
    route:
      exclude:
      - name: drop-by-queue-size
        drop:
          backend:       {type: Local}
          queuedPrompts: {greaterThan: 4}
      select:
      - {type: Local,    minimize: PredictedTTFT}
      - {type: Provider, order: [databricks]}
"#,
    );
    let mut files = demo.destinations();
    files.push(demo.path("policy-1.yaml"));
    files.push(policy_3);
    let mut args = vec!["check", "--pool", "default/inference-all"];
    args.extend(files.iter().map(String::as_str));
    let out = kinfctl(&args);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "{stdout}");
    assert!(lines.iter().all(|l| l.ends_with(": ok")), "{stdout}");
}

/// What limited the checks is said once, before the results, not per policy.
#[test]
fn skipped_name_checks_are_noted_once_at_the_top() {
    let demo = Demo::new();
    let one = demo.path("policy-1.yaml");
    let two = demo.write(
        "policy-2.yaml",
        r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: policy-2, namespace: default}
spec:
  rules:
  - name: batch-can-go-to-gemma-if-faster
    matches:
    - model: qwen-2.5
      qos: [batch]
    route:
      consider:
      - models: {names: [gemma-4, qwen-2.5]}
      select: [{type: Local, minimize: PredictedTTFT}]
"#,
    );
    let out = kinfctl(&["check", one.as_str(), two.as_str()]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(
        out.status.code(),
        Some(0),
        "skipping is not a finding: {stdout}"
    );
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 3, "{stdout}");
    assert_eq!(
        lines[0],
        "Note: Skipping name checks. Add InferencePool manifests (and if necessary, --pool) to \
         enable.",
        "{stdout}"
    );
    assert!(lines[1..].iter().all(|l| l.ends_with(": ok")), "{stdout}");
}

/// Given only the pool, a correct policy is clean: the destinations the pool
/// lists count by name, and the checks that need their manifests say, once,
/// that they were skipped.
#[test]
fn a_pool_alone_checks_backend_names_and_says_what_it_skipped() {
    let demo = Demo::new();
    // A local tier, a provider tier, and `databricks` by name: all of it is
    // right for the pool, and none of it may look wrong without the manifests.
    let policy = demo.write(
        "policy-4.yaml",
        r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: policy-4, namespace: default}
spec:
  rules:
  - name: local-first-with-provider
    matches:
      - tenant: ""
    route:
      exclude:
      - name: drop-backed-up-endpoints
        drop:
          backend:       {type: Local}
          queuedPrompts: {greaterThan: 4}
      - name: restricted-stays-local
        if:   { tag: [restricted] }
        drop: { backend: {type: Provider} }
      select:
      - {type: Local,    minimize: PredictedTTFT}
      - {type: Provider, order: [databricks]}
"#,
    );
    let pool = demo.path("pool-all.yaml");
    let out = kinfctl(&[
        "check",
        "--pool",
        "default/inference-all",
        policy.as_str(),
        pool.as_str(),
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(0), "{stdout}");
    assert_eq!(
        stdout,
        format!(
            "Note: Skipping model and label checks. Add InferenceWorkerSet and InferenceProvider \
             manifests to enable.\n{policy}: default/policy-4: ok\n"
        )
    );
}

/// Every mistake in the broken policy is reported with its path and code: as
/// `E` when it is wrong whatever pool serves the policy, `W` when it is wrong
/// for this pool. In text, and in JSON an agent can act on, and the status
/// says there is something to fix.
#[test]
fn a_broken_draft_reports_each_mistake_and_exits_1() {
    let demo = Demo::new();
    let broken = demo.path("broken-policy.yaml");
    let destinations = demo.destinations();
    let mut args = vec!["check", "--pool", "default/inference-all", &broken];
    args.extend(destinations.iter().map(String::as_str));

    let out = kinfctl(&args);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{stdout}");
    let findings: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with("E   ") || l.starts_with("W   "))
        .collect();
    assert_eq!(findings.len(), 5, "{stdout}");
    assert!(
        findings.contains(
            &"W   spec.rules[0].route.select[1].order[0]: UnknownBackend: rule `burst-to-cloud`: \
              `databriks` is not a destination of pool default/inference-all, so nothing matches \
              it. Did you mean `databricks`? Known destinations: databricks, gemma-4-12b, \
              qwen-2-5-32b, qwen-3-6-35b-a3b"
        ),
        "{stdout}"
    );
    assert!(
        findings.iter().any(|l| l.starts_with(
            "E   spec.rules[0].matches[0].tennant: UnknownField: not part of the \
             InferencePolicy schema"
        )),
        "{stdout}"
    );

    args.extend(["-o", "json"]);
    let out = kinfctl(&args);
    assert_eq!(out.status.code(), Some(1));
    let report: serde_json::Value = serde_json::from_slice(&out.stdout).expect("JSON on stdout");
    assert_eq!(report["ok"], false);
    let p = &report["policies"][0];
    assert_eq!(p["policy"], "default/broken");
    let found = |kind: &str| -> Vec<(String, String)> {
        p[kind]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| {
                (
                    f["code"].as_str().unwrap().to_string(),
                    f["path"].as_str().unwrap().to_string(),
                )
            })
            .collect()
    };
    let (errors, warnings) = (found("errors"), found("warnings"));
    for (code, path) in [
        ("UnknownField", "spec.rules[0].matches[0].tennant"),
        ("EmptyPredicate", "spec.rules[0].matches[0]"),
        (
            "ThresholdOutOfRange",
            "spec.rules[0].route.exclude[0].drop.kvCacheUsage",
        ),
        ("OrderWithoutRandom", "spec.rules[0].route.select[0].order"),
    ] {
        assert!(
            errors.contains(&(code.to_string(), path.to_string())),
            "missing error {code} at {path}: {errors:?}"
        );
    }
    assert_eq!(
        warnings,
        [(
            "UnknownBackend".to_string(),
            "spec.rules[0].route.select[1].order[0]".to_string()
        )]
    );
    assert_eq!(p["errors"][0]["suggestion"], "tenant");
}

#[test]
fn standard_input_is_read_with_a_dash() {
    let out = kinfctl_with_stdin(&["check", "-"], BAD_ENUM_POLICY);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains("<stdin>: default/bad-enum"), "{stdout}");
    assert!(
        stdout.contains(
            "E   spec.rules[0].route.consider[0].type: SchemaError: unknown variant `local`"
        ),
        "{stdout}"
    );
}

/// The documented way to check a draft against the cluster: the live objects
/// on stdin, the draft as its own argument. The tempting alternative,
/// `{ kubectl get … -o yaml; cat policy.yaml; } | kinfctl check -`, joins two
/// manifests with no `---` between them, and must say that is what happened.
#[test]
fn live_objects_on_stdin_are_checked_beside_a_draft() {
    let demo = Demo::new();
    let live = live_objects();
    let policy = demo.path("policy-1.yaml");

    let out = kinfctl_with_stdin(
        &[
            "check",
            "--pool",
            "default/inference-all",
            "-",
            policy.as_str(),
        ],
        &live,
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{stdout}{stderr}");
    assert_eq!(stdout, format!("{policy}: default/policy-1: ok\n"));

    let joined = live + POLICY_1;
    let out = kinfctl_with_stdin(&["check", "--pool", "default/inference-all", "-"], &joined);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(
        stderr.contains("starts another manifest with no `---` line before it"),
        "{stderr}"
    );
}

/// One of our kinds under a misspelled API group is a finding, never a
/// silently skipped document.
#[test]
fn a_misspelled_api_group_is_a_finding() {
    let demo = Demo::new();
    let policy = demo.path("policy-1.yaml");
    let typo = POLICY_1.replace("kinference.buoyant.io", "kinference.buoyant.com");
    let out = kinfctl_with_stdin(&["check", policy.as_str(), "-"], &typo);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{stdout}");
    assert!(
        stdout.contains(
            "E <stdin> (document 1, InferencePolicy): UnknownApiGroup: apiVersion \
             `kinference.buoyant.com/v1alpha1` is not ours"
        ),
        "{stdout}"
    );

    // Alone, it still exits 1 with the finding rather than "no InferencePolicy".
    let out = kinfctl_with_stdin(&["check", "-"], &typo);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert_eq!(out.status.code(), Some(1), "{stdout}");
    assert!(stdout.contains("UnknownApiGroup"), "{stdout}");

    // Another project's kind of the same name is skipped, as before.
    let other = POLICY_1.replace(
        "kinference.buoyant.io/v1alpha1",
        "inference.networking.k8s.io/v1",
    );
    let out = kinfctl_with_stdin(&["check", policy.as_str(), "-"], &other);
    assert_eq!(out.status.code(), Some(0));
}

/// Input that cannot be checked is exit 2 with the reason on stderr, never
/// mixed into the report on stdout.
#[test]
fn unusable_input_exits_2_with_the_reason_on_stderr() {
    let demo = Demo::new();
    let dir = demo.dir.path().display().to_string();
    let pool = demo.path("pool-all.yaml");
    let policy = demo.path("policy-1.yaml");
    let cases: [(Vec<&str>, &str); 4] = [
        (vec!["check", &dir], "is a directory"),
        (
            vec!["check", "/nonexistent/policy.yaml"],
            "/nonexistent/policy.yaml",
        ),
        (vec!["check", &pool], "no InferencePolicy"),
        (
            vec!["check", "--pool", "default/missing", &policy],
            "no InferencePool by that name",
        ),
    ];
    for (args, says) in cases {
        let out = kinfctl(&args);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(2), "{args:?}: {stderr}");
        assert!(stderr.contains(says), "{args:?}: {stderr}");
        assert!(out.stdout.is_empty(), "{args:?}: nothing on stdout");
    }
}

/// `--help` is how an assistant learns a CLI, so the examples must be there.
#[test]
fn check_help_carries_examples_and_exit_statuses() {
    let out = kinfctl(&["check", "--help"]);
    let help = String::from_utf8_lossy(&out.stdout);
    assert!(
        help.contains("kubectl get infpool,iws,infprov,refgrant -A -o yaml"),
        "{help}"
    );
    assert!(
        help.contains("Exit status: 0 nothing to fix, 1 findings, 2"),
        "{help}"
    );
}
