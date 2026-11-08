//! Checks on `InferencePolicy` manifests before they are applied; the engine
//! behind `kinfctl check`.
//!
//! Pure over its inputs: text in, a [`Report`] out. The CLI does the reading.
//!
//! Every finding has a path into the manifest and a stable code.
//!
//! Errors are wrong whatever pool serves the policy, in the order to fix them:
//!
//! 1. `SchemaError`: the spec does not deserialize, located with
//!    `serde_path_to_error`.
//! 2. `UnknownField`: keys the CRD schema does not declare. The API server
//!    silently prunes these on apply, so a misspelled `tennant:` makes the rule
//!    broader; this is the only place it can be caught.
//! 3. The compile checks of [`super::compile`].
//!
//! Warnings are the name checks of [`super::lint`], against the pools the
//! policy is attached to, when those are among the inputs. Resolution uses
//! the server's own [`super::resolve::resolve`]. A name one pool lacks may be
//! right for another, so these are warnings.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use kube::ResourceExt;
use serde::Serialize;
use serde_json::Value;

use super::crd::{
    BackendType, InferencePolicy, InferencePolicySpec, InferencePool, InferenceProvider,
    InferenceWorkerSet, ReferenceGrant,
};
use super::resolve::{PoolIndex, ResolvedPool};
use super::warning::{PolicyWarning, did_you_mean};

/// One input: a file, or standard input.
pub struct Input {
    /// How to name it in findings: a path, or `<stdin>`.
    pub source: String,
    pub text: String,
}

/// Knobs for [`check`].
pub struct Options {
    /// Namespace for objects that do not set one, as with `kubectl apply -n`.
    pub namespace: String,
    /// `NAMESPACE/NAME` of a pool to check every policy against, as if
    /// attached to it. For a draft no pool references yet.
    pub pool: Option<String>,
}

/// Input that cannot be checked at all: exit status 2, not a finding.
#[derive(Debug, PartialEq, Eq)]
pub enum InputError {
    /// Not YAML (or JSON) at all.
    Parse { source: String, message: String },
    /// Nothing to check.
    NoPolicies,
    /// `--pool` names a pool that is not among the inputs.
    PoolNotFound(String),
    /// `--pool` is not `NAMESPACE/NAME`.
    BadPoolRef(String),
}

impl std::fmt::Display for InputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InputError::Parse { source, message } => {
                write!(f, "{source}: not valid YAML: {message}")
            }
            InputError::NoPolicies => f.write_str(
                "no InferencePolicy among the inputs. Pass the policy's manifest; pools, worker \
                 sets and providers are only read to check the names a policy uses",
            ),
            InputError::PoolNotFound(p) => write!(
                f,
                "--pool {p}: no InferencePool by that name among the inputs. Pass the pool's \
                 manifest, and its destinations', alongside the policy"
            ),
            InputError::BadPoolRef(p) => write!(f, "--pool {p}: expected NAMESPACE/NAME"),
        }
    }
}

/// The result of a check. `ok` is true when there is nothing at all to fix.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub ok: bool,
    pub policies: Vec<PolicyReport>,
    /// Supporting objects (pools, worker sets, providers, grants) that could
    /// not be read. They cannot take part in name checks.
    pub other_errors: Vec<DocumentError>,
    /// Documents of kinds `check` does not read, for the record.
    pub skipped_documents: Vec<SkippedDocument>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyReport {
    pub source: String,
    /// `NAMESPACE/NAME`.
    pub policy: String,
    /// The pools whose destinations the name checks used.
    pub checked_against_pools: Vec<String>,
    pub name_checks: NameChecks,
    /// Wrong whatever pool serves the policy.
    pub errors: Vec<Finding>,
    /// Wrong for the pools it was checked against: the name checks.
    pub warnings: Vec<Finding>,
}

impl PolicyReport {
    pub fn is_clean(&self) -> bool {
        self.errors.is_empty() && self.warnings.is_empty()
    }

    /// File a compile or name-check finding as an error or a warning.
    fn add(&mut self, w: PolicyWarning) {
        if w.code.is_name_check() {
            self.warnings.push(w.into());
        } else {
            self.errors.push(w.into());
        }
    }
}

/// Whether name checks ran, and what limited them.
#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NameChecks {
    /// Whether any ran.
    pub ran: bool,
    /// What did not run, and why.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped: Option<NameCheckSkip>,
    /// Why model names went unchecked when the rest ran.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub models_unchecked: Option<String>,
    /// Destinations of the pools checked against that did not resolve, when
    /// model and label checks ran. A model or label only they would supply
    /// counts as unknown, so these can explain a warning that looks wrong.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub unresolved: Vec<UnresolvedDestination>,
}

/// Name checks that did not run, by what was missing from the inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum NameCheckSkip {
    /// No pool among the inputs serves the policy, and `--pool` named none.
    /// None ran.
    NoPool,
    /// A pool chooses its destinations by label or by namespace, and no
    /// worker set or provider manifests were passed to say which. None ran.
    DestinationsNotListed,
    /// No worker set or provider manifests were passed. Backend names and
    /// classes were checked against the pool's list; model names and labels,
    /// which only those manifests declare, were not.
    NoDestinationManifests,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedDestination {
    /// `NAMESPACE/NAME` of the pool that lists it.
    pub pool: String,
    pub kind: String,
    pub name: String,
    /// `NotFound` usually means its manifest is not among the inputs.
    pub reason: String,
}

/// One thing to fix.
#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Finding {
    /// Where in the manifest, e.g. `spec.rules[0].matches[1].tenant`.
    pub path: String,
    /// A [`super::warning::WarningCode`], or one only a manifest check has:
    /// [`SCHEMA_ERROR`], [`UNKNOWN_FIELD`] or [`DUPLICATE_POLICY`].
    pub code: &'static str,
    pub message: String,
    /// For an `UnknownField`, the declared key it most likely meant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suggestion: Option<String>,
}

impl From<PolicyWarning> for Finding {
    fn from(w: PolicyWarning) -> Self {
        Self {
            path: w.path,
            code: w.code.as_str(),
            message: w.message,
            suggestion: None,
        }
    }
}

/// The spec, or a supporting object, does not match its kind's schema.
pub const SCHEMA_ERROR: &str = "SchemaError";
/// A key the schema does not declare, which the API server drops on apply.
pub const UNKNOWN_FIELD: &str = "UnknownField";
/// A policy's `namespace/name` appears twice among the inputs.
pub const DUPLICATE_POLICY: &str = "DuplicatePolicy";
/// One of our kinds under an API group that looks like a typo of ours. The
/// API server would store it as some other resource, or reject it.
pub const UNKNOWN_API_GROUP: &str = "UnknownApiGroup";

const GROUP: &str = "kinference.buoyant.io";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentError {
    pub source: String,
    /// 1-based position of the document within its source.
    pub document: usize,
    pub kind: String,
    /// [`SCHEMA_ERROR`] or [`UNKNOWN_API_GROUP`].
    pub code: &'static str,
    pub message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SkippedDocument {
    pub source: String,
    pub document: usize,
    pub kind: String,
}

/// The request fields a `matches` entry or an `if:` accepts.
const REQUEST_KEYS: &[&str] = &["model", "inputTokens", "maxTokens", "tenant", "qos", "tag"];

/// The keys the CRD schema declares at each OBJECT position of a policy spec,
/// positions spelled as dotted paths with list indices removed.
///
/// A key anywhere else is pruned by the API server on apply. Positions not
/// listed hold free-form values (predicate values, `matchLabels`) whose keys
/// are data, not schema, and are not checked. Pinned to the CRD by the test
/// `known_keys_match_the_crd_schema`, which fails on any drift either way.
pub(crate) const SPEC_KEYS: &[(&str, &[&str])] = &[
    ("spec", &["unmatched", "rules"]),
    ("spec.rules", &["name", "matches", "route"]),
    ("spec.rules.matches", REQUEST_KEYS),
    (
        "spec.rules.route",
        &["consider", "exclude", "select", "tieBreak", "onEmpty"],
    ),
    (
        "spec.rules.route.consider",
        &["name", "type", "weight", "models", "backends"],
    ),
    (
        "spec.rules.route.consider.models",
        &["fromRequest", "names"],
    ),
    (
        "spec.rules.route.consider.backends",
        &["type", "names", "selector"],
    ),
    (
        "spec.rules.route.consider.backends.selector",
        &["matchLabels"],
    ),
    ("spec.rules.route.exclude", &["name", "if", "drop"]),
    ("spec.rules.route.exclude.if", REQUEST_KEYS),
    (
        "spec.rules.route.exclude.drop",
        &[
            "contextWindow",
            "predictedTTFT",
            "prefixCacheHitRatio",
            "queuedPrompts",
            "kvCacheUsage",
            "model",
            "backend",
        ],
    ),
    ("spec.rules.route.exclude.drop.model", &["name"]),
    (
        "spec.rules.route.exclude.drop.backend",
        &[
            "type",
            "name",
            "readyEndpoints",
            "observedTTFTP95",
            "budgetExhausted",
        ],
    ),
    (
        "spec.rules.route.select",
        &["type", "backends", "minimize", "order"],
    ),
    (
        "spec.rules.route.select.backends",
        &["type", "names", "selector"],
    ),
    (
        "spec.rules.route.select.backends.selector",
        &["matchLabels"],
    ),
];

fn known_keys(position: &str) -> Option<&'static [&'static str]> {
    SPEC_KEYS
        .iter()
        .find(|(p, _)| *p == position)
        .map(|(_, keys)| *keys)
}

/// Every key in `v` that the schema does not declare at `position`.
fn unknown_fields(v: &Value, position: &str, path: &str, out: &mut Vec<Finding>) {
    let (Some(known), Value::Object(map)) = (known_keys(position), v) else {
        return;
    };
    for (k, child) in map {
        let child_path = format!("{path}.{k}");
        if !known.contains(&k.as_str()) {
            let suggestion = did_you_mean(k, known.iter().copied()).map(str::to_string);
            let hint = suggestion
                .as_deref()
                .map(|s| format!(" Did you mean `{s}`?"))
                .unwrap_or_default();
            out.push(Finding {
                path: child_path,
                code: UNKNOWN_FIELD,
                message: format!(
                    "not part of the InferencePolicy schema — the API server drops it on \
                     apply, silently.{hint}"
                ),
                suggestion,
            });
            continue;
        }
        let child_position = format!("{position}.{k}");
        match child {
            Value::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    unknown_fields(item, &child_position, &format!("{child_path}[{i}]"), out);
                }
            }
            other => unknown_fields(other, &child_position, &child_path, out),
        }
    }
}

/// A document's `kind`, or `(none)`.
fn kind_of(v: &Value) -> String {
    v.get("kind")
        .and_then(Value::as_str)
        .unwrap_or("(none)")
        .to_string()
}

/// Whether a pool names every destination, rather than choosing some by label
/// or, with none listed, taking every worker set in its namespace.
fn names_all(pool: &InferencePool) -> bool {
    let d = &pool.spec.destinations;
    !d.is_empty() && d.iter().all(|d| d.name.is_some())
}

/// The destinations a pool names, each with the class its `kind` gives.
fn named(pool: &InferencePool) -> impl Iterator<Item = (String, BackendType)> + '_ {
    pool.spec.destinations.iter().filter_map(|d| {
        let class = match d.kind.as_deref()? {
            "InferenceWorkerSet" => BackendType::Local,
            "InferenceProvider" => BackendType::Provider,
            _ => return None,
        };
        Some((d.name.clone()?, class))
    })
}

/// Whether `v` claims `group` in its `apiVersion`, so a same-named kind from
/// another API group is not mistaken for ours.
fn in_group(v: &Value, group: &str) -> bool {
    v.get("apiVersion")
        .and_then(Value::as_str)
        .is_some_and(|a| a.split('/').next() == Some(group))
}

/// The API group of `v`, when it is close to ours but not ours.
fn group_typo(v: &Value) -> Option<&str> {
    let group = v.get("apiVersion")?.as_str()?.split('/').next()?;
    did_you_mean(group, [GROUP]).map(|_| group)
}

/// Parse a multi-document YAML stream into JSON values.
///
/// The parser refuses more than 32 consecutive comment lines before a list
/// entry, a fixed limit that well-commented manifests hit. On that error,
/// parse again with every full-line comment blanked (not deleted, so line
/// numbers still match). Blank lines don't end a block scalar, so nothing
/// structural changes.
fn parse_yaml(text: &str) -> Result<Vec<Value>, String> {
    let err = match serde_saphyr::from_multiple(text) {
        Ok(docs) => return Ok(docs),
        Err(e) if e.to_string().contains("too many consecutive comments") => {
            let blanked: String = text
                .lines()
                .map(|l| {
                    if l.trim_start().starts_with('#') {
                        ""
                    } else {
                        l
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            match serde_saphyr::from_multiple(&blanked) {
                Ok(docs) => return Ok(docs),
                Err(e) => e,
            }
        }
        Err(e) => e,
    };
    Err(describe_parse_error(&err))
}

/// A parse error, worded for whoever is running kinfctl.
///
/// Uses serde-saphyr's `UserMessageFormatter`, whose wording is for end
/// users. A duplicate `apiVersion` or `kind` usually means two manifests were
/// concatenated without a `---` between them.
fn describe_parse_error(e: &serde_saphyr::Error) -> String {
    let mut message = e.render_with_formatter(&serde_saphyr::UserMessageFormatter);
    if let serde_saphyr::Error::DuplicateMappingKey {
        key: Some(key),
        location,
    } = e.without_snippet()
        && location.column() == 1
        && matches!(key.as_str(), "apiVersion" | "kind")
    {
        message.push_str(&format!(
            "\nhint: line {} starts another manifest with no `---` line before it. Put `---` \
             between manifests, or pass each file as its own argument (`-` reads standard input)",
            location.line()
        ));
    }
    message
}

/// A policy manifest as read, before anything is decided about it.
struct PolicyDoc {
    source: String,
    namespace: String,
    name: String,
    value: Value,
}

/// Check `inputs`. `Err` only for input that cannot be checked at all.
pub fn check(inputs: &[Input], opts: &Options) -> Result<Report, InputError> {
    let pool_ref = match &opts.pool {
        None => None,
        Some(p) => match p.split_once('/') {
            Some((ns, name)) if !ns.is_empty() && !name.is_empty() && !name.contains('/') => {
                Some((ns.to_string(), name.to_string()))
            }
            _ => return Err(InputError::BadPoolRef(p.clone())),
        },
    };

    let mut policies: Vec<PolicyDoc> = Vec::new();
    let mut pools: Vec<Arc<InferencePool>> = Vec::new();
    let mut worker_sets: Vec<Arc<InferenceWorkerSet>> = Vec::new();
    let mut providers: Vec<Arc<InferenceProvider>> = Vec::new();
    let mut grants: Vec<Arc<ReferenceGrant>> = Vec::new();
    let mut other_errors = Vec::new();
    let mut skipped_documents = Vec::new();

    for input in inputs {
        let docs: Vec<Value> = parse_yaml(&input.text).map_err(|message| InputError::Parse {
            source: input.source.clone(),
            message,
        })?;
        // `kubectl get -o yaml` wraps everything in one `kind: List`; unwrap it
        // so the cluster's own objects pipe straight in.
        let objects = docs.into_iter().enumerate().flat_map(|(i, d)| {
            let n = i + 1;
            match d {
                Value::Object(ref m) if m.get("kind").and_then(Value::as_str) == Some("List") => m
                    .get("items")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(|item| (n, item))
                    .collect::<Vec<_>>(),
                Value::Null => Vec::new(),
                other => vec![(n, other)],
            }
        });
        for (n, mut obj) in objects {
            // Default the namespace the way `kubectl apply -n` would, so
            // same-namespace references resolve.
            if let Some(meta) = obj.get_mut("metadata").and_then(Value::as_object_mut)
                && meta.get("namespace").and_then(Value::as_str).is_none()
            {
                meta.insert("namespace".into(), Value::String(opts.namespace.clone()));
            }
            // Drop status, so objects from `kubectl get -o yaml` read the same
            // as the files that created them.
            if let Some(m) = obj.as_object_mut() {
                m.remove("status");
            }
            let kind = kind_of(&obj);
            let ours = in_group(&obj, GROUP);
            let typed_err = |e: serde_json::Error| DocumentError {
                source: input.source.clone(),
                document: n,
                kind: kind.clone(),
                code: SCHEMA_ERROR,
                message: e.to_string(),
            };
            match kind.as_str() {
                "InferencePolicy" if ours => {
                    let meta = obj.get("metadata");
                    let field = |f: &str| {
                        meta.and_then(|m| m.get(f))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    };
                    policies.push(PolicyDoc {
                        source: input.source.clone(),
                        namespace: field("namespace").unwrap_or_else(|| opts.namespace.clone()),
                        name: field("name").unwrap_or_else(|| "<unnamed>".to_string()),
                        value: obj,
                    });
                }
                "InferencePool" if ours => match serde_json::from_value(obj) {
                    Ok(p) => pools.push(Arc::new(p)),
                    Err(e) => other_errors.push(typed_err(e)),
                },
                "InferenceWorkerSet" if ours => match serde_json::from_value(obj) {
                    Ok(w) => worker_sets.push(Arc::new(w)),
                    Err(e) => other_errors.push(typed_err(e)),
                },
                "InferenceProvider" if ours => match serde_json::from_value(obj) {
                    Ok(p) => providers.push(Arc::new(p)),
                    Err(e) => other_errors.push(typed_err(e)),
                },
                "ReferenceGrant" if in_group(&obj, "gateway.networking.k8s.io") => {
                    match serde_json::from_value(obj) {
                        Ok(g) => grants.push(Arc::new(g)),
                        Err(e) => other_errors.push(typed_err(e)),
                    }
                }
                "InferencePolicy" | "InferencePool" | "InferenceWorkerSet"
                | "InferenceProvider"
                    if group_typo(&obj).is_some() =>
                {
                    let api_version = obj["apiVersion"].as_str().unwrap_or_default();
                    other_errors.push(DocumentError {
                        source: input.source.clone(),
                        document: n,
                        kind,
                        code: UNKNOWN_API_GROUP,
                        message: format!(
                            "apiVersion `{api_version}` is not ours, so the document was not \
                             checked. Did you mean `{GROUP}/v1alpha1`?"
                        ),
                    });
                }
                _ => skipped_documents.push(SkippedDocument {
                    source: input.source.clone(),
                    document: n,
                    kind,
                }),
            }
        }
    }
    // A policy under a misspelled group is a finding, not an empty input.
    if policies.is_empty() && !other_errors.iter().any(|e| e.kind == "InferencePolicy") {
        return Err(InputError::NoPolicies);
    }

    // --- per policy: unknown fields, then deserialize --------------------------
    let mut reports = Vec::with_capacity(policies.len());
    let mut parsed: Vec<Option<InferencePolicySpec>> = Vec::with_capacity(policies.len());
    let mut seen: BTreeMap<(String, String), String> = BTreeMap::new();
    for doc in &policies {
        let mut report = PolicyReport {
            source: doc.source.clone(),
            policy: format!("{}/{}", doc.namespace, doc.name),
            checked_against_pools: Vec::new(),
            name_checks: NameChecks::default(),
            errors: Vec::new(),
            warnings: Vec::new(),
        };
        let key = (doc.namespace.clone(), doc.name.clone());
        if let Some(first) = seen.get(&key) {
            report.errors.push(Finding {
                path: "metadata.name".into(),
                code: DUPLICATE_POLICY,
                message: format!(
                    "{} is also defined in {first}; applying both would keep only the last",
                    report.policy
                ),
                suggestion: None,
            });
            reports.push(report);
            parsed.push(None);
            continue;
        }
        seen.insert(key, doc.source.clone());
        let spec_value = doc.value.get("spec").cloned().unwrap_or(Value::Null);
        let mut unknown = Vec::new();
        unknown_fields(&spec_value, "spec", "spec", &mut unknown);
        match serde_path_to_error::deserialize::<_, InferencePolicySpec>(&spec_value) {
            Ok(spec) => parsed.push(Some(spec)),
            Err(e) => {
                let at = e.path().to_string();
                report.errors.push(Finding {
                    path: if at == "." {
                        "spec".into()
                    } else {
                        format!("spec.{at}")
                    },
                    code: SCHEMA_ERROR,
                    message: e.into_inner().to_string(),
                    suggestion: None,
                });
                parsed.push(None);
            }
        }
        // After a schema error, which is the first thing to fix.
        report.errors.extend(unknown);
        reports.push(report);
    }

    // --- resolve, as the server does ------------------------------------------
    //
    // Offline there is no discovery, so treat every declared worker set as
    // bound to a pool; the question is what names refer to.
    let bound: PoolIndex = worker_sets
        .iter()
        .filter_map(|w| {
            Some((
                super::PoolKey::namespaced(&w.namespace()?, w.name_any()),
                super::PoolId::next(),
            ))
        })
        .collect();
    let policy_objects: Vec<Arc<InferencePolicy>> = policies
        .iter()
        .zip(&parsed)
        .filter_map(|(doc, spec)| {
            Some(Arc::new(InferencePolicy {
                metadata: kube::core::ObjectMeta {
                    name: Some(doc.name.clone()),
                    namespace: Some(doc.namespace.clone()),
                    ..Default::default()
                },
                spec: spec.clone()?,
                status: None,
            }))
        })
        .collect();
    let index = super::resolve::resolve(
        &pools,
        &policy_objects,
        &worker_sets,
        &providers,
        &[],
        &grants,
        &bound,
        &Default::default(),
    );
    let forced: Option<&Arc<ResolvedPool>> = match &pool_ref {
        None => None,
        Some((ns, name)) => Some(
            index
                .lookup(ns, name)
                .ok_or_else(|| InputError::PoolNotFound(format!("{ns}/{name}")))?,
        ),
    };

    // --- compile checks, then name checks ------------------------------------
    //
    // Offline, a destination that did not resolve has usually just not been
    // passed. So backend names and classes come from what a pool lists, and
    // the destinations' own manifests, when there are any, add their models
    // and labels. With none at all, those go unchecked rather than flagged.
    let have_destination_manifests = !worker_sets.is_empty() || !providers.is_empty();
    for ((report, doc), spec) in reports.iter_mut().zip(&policies).zip(&parsed) {
        let Some(spec) = spec else { continue };
        let Some(resolved) = index
            .policies()
            .find(|p| p.namespace == doc.namespace && p.name == doc.name)
        else {
            continue;
        };
        for w in resolved.policy.warnings.iter().cloned() {
            report.add(w);
        }

        let attached: Vec<&ResolvedPool> = match forced {
            Some(p) => vec![p.as_ref()],
            None => resolved
                .referenced_by
                .iter()
                .filter_map(|pool| index.lookup(&doc.namespace, pool))
                .map(Arc::as_ref)
                .collect(),
        };
        if attached.is_empty() {
            report.name_checks.skipped = Some(NameCheckSkip::NoPool);
            continue;
        }
        // The pools as written: a pool that names its destinations says what
        // each is called and its class, with or without their manifests.
        let manifests: Vec<&InferencePool> = attached
            .iter()
            .filter_map(|r| {
                pools.iter().find(|p| {
                    p.namespace().as_deref() == Some(r.namespace.as_str()) && p.name_any() == r.name
                })
            })
            .map(Arc::as_ref)
            .collect();
        let names_known = have_destination_manifests
            || (manifests.len() == attached.len() && manifests.iter().all(|p| names_all(p)));
        if !names_known {
            report.name_checks.skipped = Some(NameCheckSkip::DestinationsNotListed);
            continue;
        }
        let offline = super::lint::Offline {
            listed: manifests.iter().flat_map(|p| named(p)).collect(),
            details: have_destination_manifests,
        };
        let names = super::lint::reference_warnings_offline(spec, &attached, &offline);
        report.name_checks.ran = true;
        report.checked_against_pools = attached
            .iter()
            .map(|p| format!("{}/{}", p.namespace, p.name))
            .collect();
        if have_destination_manifests {
            report.name_checks.unresolved = attached
                .iter()
                .flat_map(|pool| {
                    pool.unresolved.iter().map(|u| UnresolvedDestination {
                        pool: format!("{}/{}", pool.namespace, pool.name),
                        kind: u.kind.to_string(),
                        name: u.name.clone(),
                        reason: u.reason.as_str().to_string(),
                    })
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
        } else {
            report.name_checks.skipped = Some(NameCheckSkip::NoDestinationManifests);
        }
        report.name_checks.models_unchecked = names.models_unchecked;
        for w in names.warnings {
            report.add(w);
        }
    }

    let ok = other_errors.is_empty() && reports.iter().all(PolicyReport::is_clean);
    Ok(Report {
        ok,
        policies: reports,
        other_errors,
        skipped_documents,
    })
}

#[cfg(test)]
mod tests;
