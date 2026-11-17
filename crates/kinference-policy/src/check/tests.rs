use super::*;
use crate::warning::WarningCode;

fn input(source: &str, text: &str) -> Input {
    Input {
        source: source.into(),
        text: text.into(),
    }
}

fn opts() -> Options {
    Options {
        namespace: "default".into(),
        pool: None,
    }
}

// The demo fleet: one pool over three local worker sets and one provider, and
// the policies written against it. These are the manifests several tests
// share; one only a single test needs is written inline in that test.

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

const POLICY_3: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
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
      - name: drop-by-kvcache-usage
        drop:
          backend:       {type: Local}
          kvCacheUsage:  {greaterThan: 0.5}
      select:
      - {type: Local,    minimize: PredictedTTFT}
      - {type: Provider, order: [databricks]}
"#;

const POLICY_4: &str = r#"apiVersion: kinference.buoyant.io/v1alpha1
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
      - name: drop-endpoints-with-no-kv-headroom
        drop:
          backend:       {type: Local}
          kvCacheUsage:  {greaterThan: 0.1}
      - name: restricted-stays-local
        if:   { tag: [restricted] }
        drop: { backend: {type: Provider} }
      select:
      - {type: Local,    minimize: PredictedTTFT}
      - {type: Provider, order: [databricks]}
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

fn pool_all() -> Input {
    input("pool-all.yaml", POOL_ALL)
}

fn gemma() -> Input {
    input("workerset-gemma-4.yaml", WORKERSET_GEMMA_4)
}

fn qwen_2_5() -> Input {
    input("workerset-qwen-2-5.yaml", WORKERSET_QWEN_2_5)
}

fn qwen_3_6() -> Input {
    input(
        "workerset-qwen-3-6-35b-a3b.yaml",
        WORKERSET_QWEN_3_6_35B_A3B,
    )
}

fn provider() -> Input {
    input("databricks/provider.yaml", PROVIDER_DATABRICKS)
}

fn policy_1() -> Input {
    input("policy-1.yaml", POLICY_1)
}

fn policy_3() -> Input {
    input("policy-3.yaml", POLICY_3)
}

fn policy_4() -> Input {
    input("policy-4.yaml", POLICY_4)
}

fn broken() -> Input {
    input("broken-policy.yaml", BROKEN_POLICY)
}

/// The whole demo fleet, including the manifests only the whole-fleet tests
/// need: a second pool, a fourth worker set, two more policies, and the
/// gateway's half of the provider, which is not a kinference kind and must be
/// passed over.
fn fleet() -> Vec<Input> {
    vec![
        pool_all(),
        input(
            "pool-local.yaml",
            r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePool
metadata:
  name: inference-local
  namespace: default
spec:
  destinations:
    - {kind: InferenceWorkerSet, name: gemma-4-12b}
    - {kind: InferenceWorkerSet, name: qwen-2-5-32b}
    - {kind: InferenceWorkerSet, name: qwen-3-6-35b-a3b}
    - {kind: InferenceWorkerSet, name: dummy}
"#,
        ),
        gemma(),
        qwen_2_5(),
        qwen_3_6(),
        input(
            "workerset-dummy.yaml",
            r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferenceWorkerSet
metadata:
  name: dummy
  namespace: default
spec:
  profile: default
  kvBlockSize: 128
  serviceRef:
    name: dummy-vllm
    port: http
  kvEvents:
    port: kv-pub
  models:
    - {name: HuggingFaceTB/SmolLM2-135M-Instruct, aliases: [dummy]}
"#,
        ),
        provider(),
        input(
            "databricks/gateway-backend.yaml",
            r#"apiVersion: gateway.linkerd.io/v1alpha1
kind: GatewayBackend
metadata:
  name: databricks
  namespace: default
spec:
  baseURL: https://dbc-8f15bc20-a291.cloud.databricks.com
  auth:
    type: OAuth2
    oauth2:
      tokenURL: https://${DATABRICKS_SUBDOMAIN}.cloud.databricks.com/oidc/v1/token
      clientID: ${DATABRICKS_CLIENT_ID}
      clientSecretRef:
        name: databricks-oauth
        key: client-secret
      scopes: [all-apis]
      clientAuthMethod: ClientSecretBasic
"#,
        ),
        policy_1(),
        input(
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
        ),
        policy_3(),
        policy_4(),
        input(
            "policy-5.yaml",
            r#"apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: policy-5, namespace: default}
spec:
  rules:
  - name: batch-only-where-nothing-is-queued
    matches:
    - qos: [batch]
    route:
      consider:
      - {type: Local}
      exclude:
      - name: require-an-empty-queue
        drop:
          backend:       {type: Local}
          queuedPrompts: {greaterThan: 0}
      select:
      - {minimize: PredictedTTFT}
      onEmpty: Reject
"#,
        ),
    ]
}

/// The known-keys table matches the CRD schema exactly, in both directions:
/// every table position exists with the same keys, and every structural object
/// in the schema has a table entry.
#[test]
fn known_keys_match_the_crd_schema() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../charts/kinference-crds/templates/kinference.buoyant.io_inferencepolicies.yaml"
    ))
    .expect("the CRD is the source of truth");
    let crd: Value = serde_saphyr::from_str(&text).expect("CRD parses");
    let spec = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"];
    assert!(spec.is_object(), "schema moved?");

    let mut found: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    fn walk(node: &Value, position: &str, found: &mut BTreeMap<String, BTreeSet<String>>) {
        let node = if node["type"] == "array" {
            &node["items"]
        } else {
            node
        };
        if node.get("x-kubernetes-preserve-unknown-fields") == Some(&Value::Bool(true)) {
            return;
        }
        let Some(props) = node.get("properties").and_then(Value::as_object) else {
            return;
        };
        found.insert(position.to_string(), props.keys().cloned().collect());
        for (k, child) in props {
            walk(child, &format!("{position}.{k}"), found);
        }
    }
    walk(spec, "spec", &mut found);

    let table: BTreeMap<String, BTreeSet<String>> = SPEC_KEYS
        .iter()
        .map(|(p, keys)| (p.to_string(), keys.iter().map(|k| k.to_string()).collect()))
        .collect();
    assert_eq!(
        table, found,
        "SPEC_KEYS has drifted from the CRD schema; update one to match the other"
    );
}

#[test]
fn unknown_fields_are_found_at_any_depth_with_a_suggestion() {
    let r = check(
        &[input(
            "p.yaml",
            r#"
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: p}
spec:
  unmatchd: Reject
  rules:
  - name: r
    matches: [{tennant: acme}]
    route:
      consider: [{models: {fromRequset: true}}]
      exclude:
      - name: e
        drop: {backend: {readyEndpoint: {lessThan: 2}}, kvCacheUsage: {greaterThan: 0.9}}
      select: [{minimize: PredictedTTFT, backends: {selector: {matchLabels: {any-key: ok}}}}]
"#,
        )],
        &opts(),
    )
    .unwrap();
    let got: Vec<(&str, Option<&str>)> = r.policies[0]
        .errors
        .iter()
        .filter(|f| f.code == UNKNOWN_FIELD)
        .map(|u| (u.path.as_str(), u.suggestion.as_deref()))
        .collect();
    assert_eq!(
        got,
        [
            ("spec.rules[0].matches[0].tennant", Some("tenant")),
            (
                "spec.rules[0].route.consider[0].models.fromRequset",
                Some("fromRequest")
            ),
            (
                "spec.rules[0].route.exclude[0].drop.backend.readyEndpoint",
                Some("readyEndpoints")
            ),
            ("spec.unmatchd", Some("unmatched")),
        ],
        "matchLabels keys and predicate values are data, never flagged"
    );
}

#[test]
fn a_deserialization_error_is_located_and_stops_further_checks() {
    let r = check(
        &[input(
            "p.yaml",
            "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
             metadata: {name: p}\nspec:\n  rules:\n  - name: r\n    route:\n      \
             consider: [{type: local}]\n      select: [{minimize: PredictedTTFT}]\n",
        )],
        &opts(),
    )
    .unwrap();
    let p = &r.policies[0];
    assert_eq!(
        p.errors.len(),
        1,
        "nothing compiles from a spec that does not parse: {p:#?}"
    );
    assert_eq!(p.errors[0].code, SCHEMA_ERROR);
    assert_eq!(p.errors[0].path, "spec.rules[0].route.consider[0].type");
    assert!(
        p.errors[0].message.contains("unknown variant `local`"),
        "{p:#?}"
    );
    assert!(p.warnings.is_empty());
    assert!(!r.ok);
}

/// Errors are wrong whatever pool serves the policy; warnings are wrong for
/// the pools it was checked against.
#[test]
fn spec_findings_are_errors_and_name_findings_warnings() {
    let r = check(
        &[
            broken(),
            pool_all(),
            provider(),
            gemma(),
            qwen_2_5(),
            qwen_3_6(),
        ],
        &Options {
            namespace: "default".into(),
            pool: Some("default/inference-all".into()),
        },
    )
    .unwrap();
    let p = &r.policies[0];
    let codes = |fs: &[Finding]| fs.iter().map(|f| f.code).collect::<Vec<_>>();
    assert_eq!(
        codes(&p.errors),
        [
            UNKNOWN_FIELD,
            "EmptyPredicate",
            "ThresholdOutOfRange",
            "OrderWithoutRandom"
        ],
        "{p:#?}"
    );
    assert_eq!(codes(&p.warnings), ["UnknownBackend"], "{p:#?}");
}

/// `kubectl get -o yaml` output is one `kind: List`; its items must be read,
/// and an object without a namespace takes `-n`'s.
#[test]
fn a_kubectl_list_is_unwrapped_and_namespaces_default() {
    let r = check(
        &[input(
            "<stdin>",
            r#"
apiVersion: v1
kind: List
items:
- apiVersion: kinference.buoyant.io/v1alpha1
  kind: InferencePool
  metadata: {name: pool}
  spec:
    destinations: [{kind: InferenceWorkerSet, name: ws}]
    policyRef: {name: p}
- apiVersion: kinference.buoyant.io/v1alpha1
  kind: InferenceWorkerSet
  metadata: {name: ws}
  spec: {serviceRef: {name: ws}}
---
apiVersion: kinference.buoyant.io/v1alpha1
kind: InferencePolicy
metadata: {name: p}
spec:
  rules:
  - name: r
    route: {select: [{order: [ws, wz]}], tieBreak: [Random]}
"#,
        )],
        &Options {
            namespace: "team-a".into(),
            pool: None,
        },
    )
    .unwrap();
    let p = &r.policies[0];
    assert_eq!(p.policy, "team-a/p");
    assert_eq!(p.checked_against_pools, ["team-a/pool"]);
    assert!(p.name_checks.ran);
    let w: Vec<_> = p
        .warnings
        .iter()
        .map(|w| (w.code, w.path.as_str()))
        .collect();
    assert_eq!(
        w,
        [(
            WarningCode::UnknownBackend.as_str(),
            "spec.rules[0].route.select[0].order[1]"
        )],
        "{p:#?}"
    );
}

#[test]
fn without_a_pool_name_checks_are_skipped_and_say_how_to_run_them() {
    let r = check(&[policy_3()], &opts()).unwrap();
    let p = &r.policies[0];
    assert!(!p.name_checks.ran);
    assert_eq!(p.name_checks.skipped, Some(NameCheckSkip::NoPool));
    assert!(r.ok, "skipping is not a finding");
}

/// `policy` checked against the demo pool `default/inference-all`, with
/// `files` from the demo fleet beside it.
fn against_pool_all(policy: Input, files: Vec<Input>) -> Report {
    let mut inputs = vec![policy];
    inputs.extend(files);
    let opts = Options {
        namespace: "default".into(),
        pool: Some("default/inference-all".into()),
    };
    check(&inputs, &opts).unwrap()
}

/// The pool with every destination's own manifest beside it.
fn all_destinations() -> Vec<Input> {
    vec![pool_all(), gemma(), qwen_2_5(), qwen_3_6(), provider()]
}

/// A pool that names its destinations says what each is called and which
/// class it is, so backend names and classes can be checked from it alone. A
/// destination missing from the inputs is not missing from the pool.
#[test]
fn a_pool_alone_is_enough_for_backend_names_and_classes() {
    // policy-4 is right for its pool: a local tier, a provider tier, and
    // `databricks` by name. Given only the pool, none of that may look wrong.
    let r = against_pool_all(policy_4(), vec![pool_all()]);
    let p = &r.policies[0];
    assert!(p.is_clean(), "{p:#?}");
    assert!(p.name_checks.ran);
    assert_eq!(
        p.name_checks.skipped,
        Some(NameCheckSkip::NoDestinationManifests)
    );

    // And a misspelled name still does.
    let r = against_pool_all(broken(), vec![pool_all()]);
    let w: Vec<&str> = r.policies[0]
        .warnings
        .iter()
        .map(|w| w.message.as_str())
        .collect();
    assert_eq!(w.len(), 1, "{w:?}");
    assert!(
        w[0].contains("`databriks`") && w[0].contains("Did you mean `databricks`?"),
        "{w:?}"
    );
}

/// Models and labels are declared by the worker sets and providers, not the
/// pool, so without their manifests they go unchecked rather than flagged.
#[test]
fn models_and_labels_need_the_destinations_manifests() {
    let draft = || {
        input(
            "draft.yaml",
            "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
             metadata: {name: draft}\nspec:\n  rules:\n  - name: r\n    \
             matches: [{model: gemma-4-12b}]\n    route:\n      \
             consider: [{backends: {selector: {matchLabels: {gpu: h100}}}}]\n      \
             select: [{minimize: PredictedTTFT}]\n",
        )
    };
    let codes = |r: &Report| -> Vec<&'static str> {
        r.policies[0].warnings.iter().map(|w| w.code).collect()
    };
    assert_eq!(
        codes(&against_pool_all(draft(), all_destinations())),
        ["UnknownModel", "SelectorMatchesNothing"],
        "`gemma-4-12b` names the worker set, not its model, and no destination is labelled gpu"
    );
    assert!(
        codes(&against_pool_all(draft(), vec![pool_all()])).is_empty(),
        "without the manifests there is nothing to check them against"
    );
}

/// A pool that chooses its destinations by label, or takes every worker set
/// in its namespace, says nothing about them until their manifests are passed.
#[test]
fn a_pool_that_does_not_name_its_destinations_needs_their_manifests() {
    let pool = || {
        input(
            "pool.yaml",
            "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePool\n\
             metadata: {name: everything}\nspec: {}\n",
        )
    };
    let o = Options {
        namespace: "default".into(),
        pool: Some("default/everything".into()),
    };
    let policy = policy_1;
    let r = check(&[policy(), pool()], &o).unwrap();
    assert!(!r.policies[0].name_checks.ran);
    assert_eq!(
        r.policies[0].name_checks.skipped,
        Some(NameCheckSkip::DestinationsNotListed)
    );
    assert!(r.ok, "skipping is not a finding");

    let r = check(&[policy(), pool(), gemma()], &o).unwrap();
    assert!(r.policies[0].name_checks.ran);
    assert_eq!(r.policies[0].name_checks.skipped, None);
}

/// With worker sets and providers among the inputs, a destination the pool
/// lists but the inputs lack still counts by name. Its models are unknown, so
/// a model only it serves is flagged, and the destination is recorded to say
/// why.
#[test]
fn a_listed_destination_without_its_manifest_counts_by_name_only() {
    let draft = input(
        "draft.yaml",
        "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
         metadata: {name: draft}\nspec:\n  rules:\n  - name: r\n    \
         matches: [{model: Qwen/Qwen3.6-35B-A3B}]\n    route:\n      \
         select: [{order: [qwen-3-6-35b-a3b]}]\n      tieBreak: [Random]\n",
    );
    let r = against_pool_all(draft, vec![pool_all(), gemma(), qwen_2_5(), provider()]);
    let p = &r.policies[0];
    let codes: Vec<&str> = p.warnings.iter().map(|w| w.code).collect();
    assert_eq!(codes, ["UnknownModel"], "{p:#?}");
    assert!(
        p.name_checks
            .unresolved
            .iter()
            .any(|u| u.name == "qwen-3-6-35b-a3b" && u.reason == "NotFound"),
        "{p:#?}"
    );
}

#[test]
fn input_that_cannot_be_checked_is_an_error_not_a_finding() {
    assert_eq!(
        check(&[input("x", "a: [unclosed\n")], &opts()).unwrap_err(),
        InputError::Parse {
            source: "x".into(),
            message: check(&[input("x", "a: [unclosed\n")], &opts())
                .unwrap_err()
                .to_string()
                .trim_start_matches("x: not valid YAML: ")
                .to_string(),
        }
    );
    assert_eq!(
        check(&[pool_all()], &opts()).unwrap_err(),
        InputError::NoPolicies
    );
    let o = Options {
        namespace: "default".into(),
        pool: Some("default/nope".into()),
    };
    assert_eq!(
        check(&[policy_1()], &o).unwrap_err(),
        InputError::PoolNotFound("default/nope".into())
    );
}

/// Two manifests joined with no `---` between them are one document with
/// every key twice, and the error says so.
#[test]
fn manifests_joined_without_a_separator_get_a_hint() {
    let joined = "apiVersion: v1\nkind: List\nitems: []\nmetadata:\n  resourceVersion: \"\"\n\
                  apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n";
    let err = check(&[input("x", joined)], &opts())
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("hint: line 6 starts another manifest"),
        "{err}"
    );
    assert!(!err.contains("DuplicateKeyPolicy"), "{err}");

    // A key repeated inside one manifest is a plain mistake, not a join.
    let repeated = "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
                    metadata:\n  name: a\n  name: b\n";
    let err = check(&[input("x", repeated)], &opts())
        .unwrap_err()
        .to_string();
    assert!(err.contains("duplicate mapping key `name`"), "{err}");
    assert!(!err.contains("hint:"), "{err}");
    assert!(!err.contains("DuplicateKeyPolicy"), "{err}");
}

#[test]
fn the_same_policy_twice_is_an_error_on_the_second() {
    let p = policy_1();
    let again = input("copy.yaml", &p.text);
    let r = check(&[p, again], &opts()).unwrap();
    assert!(r.policies[0].errors.is_empty());
    assert_eq!(r.policies[1].errors[0].code, DUPLICATE_POLICY);
    assert!(
        r.policies[1].errors[0].message.contains("also defined in"),
        "{:#?}",
        r.policies[1]
    );
}

/// Every demo policy checks clean (spec, unknown fields and names) against
/// the demo pool.
#[test]
fn every_demo_policy_checks_clean_against_its_pool() {
    let r = check(
        &fleet(),
        &Options {
            namespace: "default".into(),
            pool: Some("default/inference-all".into()),
        },
    )
    .unwrap();
    assert_eq!(r.policies.len(), 5, "the demo policies changed?");
    for p in &r.policies {
        assert!(p.name_checks.ran, "{}: {:?}", p.policy, p.name_checks);
        assert!(p.is_clean(), "{p:#?}");
    }
    assert!(r.ok, "{:#?}", r.other_errors);
}

/// And each demo policy on its own: the spec checks need no pool, so nothing
/// escapes them by only ever being checked beside one.
#[test]
fn every_demo_policy_passes_the_spec_checks_alone() {
    let mut checked = 0;
    for manifest in fleet() {
        // The parse decides what is a policy, not the file name.
        let source = manifest.source.clone();
        let r = match check(&[manifest], &opts()) {
            Err(InputError::NoPolicies) => continue,
            other => other.unwrap_or_else(|e| panic!("{source}: {e}")),
        };
        for p in &r.policies {
            checked += 1;
            assert!(p.is_clean(), "{p:#?}");
        }
    }
    assert_eq!(checked, 5, "the demo policies changed?");
}

/// The YAML parser buffers at most 32 comment events by default; 40 comment
/// lines above a policy rule still parse.
#[test]
fn a_long_comment_block_does_not_stop_the_parse() {
    let comments: String = (0..40)
        .map(|i| format!("    # line {i} of a long explanation\n"))
        .collect();
    let text = format!(
        "apiVersion: kinference.buoyant.io/v1alpha1\nkind: InferencePolicy\n\
         metadata: {{name: p}}\nspec:\n  rules:\n{comments}  - name: r\n    route:\n      \
         select: [{{minimize: PredictedTTFT}}]\n"
    );
    let r = check(&[input("p.yaml", &text)], &opts()).expect("parses despite the comments");
    assert_eq!(r.policies.len(), 1);
    assert!(r.ok, "{r:#?}");
}
