//! `InferencePolicy` to `CompiledPolicy`.
//!
//! Runs once per policy revision. Predicates become typed ASTs, defaults are
//! resolved, and anything suspect becomes a warning in `status.warnings`. The
//! checks focus on policies admission accepts but that silently do nothing.

use std::collections::BTreeMap;

use super::crd::{
    BackendSelector, BackendType, ConsiderGroup, InferencePolicySpec, Minimize, OnEmpty,
    RequestPredicate, Route, Rule, TieBreak, Unmatched,
};
use super::predicate::{
    AttrValue, CandidateAttr, CompiledCandidatePredicate, CompiledRequestPredicate, Matcher,
    RequestAttr, TermIssue,
};
use super::warning::{PolicyWarning, WarningCode, did_you_mean};

/// How a `consider` group picks models.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelChoice {
    /// The model the request asked for; the default when `consider` is omitted.
    FromRequest,
    /// An explicit set, which is the only way to route to a model the client
    /// did not ask for.
    Names(Vec<String>),
}

/// A subset of the pool's destinations, by class, name or label.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendMatch {
    /// Backend class. `None` (an omitted `type:`) admits both.
    pub backend_type: Option<BackendType>,
    pub names: Vec<String>,
    pub match_labels: BTreeMap<String, String>,
}

impl BackendMatch {
    fn compile(sel: &BackendSelector) -> Self {
        Self {
            backend_type: sel.type_,
            names: sel.names.clone(),
            match_labels: sel
                .selector
                .as_ref()
                .map(|s| s.match_labels.clone())
                .unwrap_or_default(),
        }
    }

    /// True when a destination is in this subset. An empty match (an omitted
    /// `backends:`) is everything. `type` ANDs with the name/label test.
    pub fn matches(
        &self,
        backend_type: BackendType,
        name: &str,
        labels: &BTreeMap<String, String>,
    ) -> bool {
        if self.backend_type.is_some_and(|t| t != backend_type) {
            return false;
        }
        if self.names.is_empty() && self.match_labels.is_empty() {
            return true;
        }
        if self.names.iter().any(|n| n == name) {
            return true;
        }
        !self.match_labels.is_empty()
            && self
                .match_labels
                .iter()
                .all(|(k, v)| labels.get(k) == Some(v))
    }

    /// Whether this admits every destination, so a tier carrying it can never
    /// hand anything on to the next one.
    fn is_empty(&self) -> bool {
        self.backend_type.is_none() && self.names.is_empty() && self.match_labels.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct CompiledConsiderGroup {
    pub name: Option<String>,
    /// Relative weight, `1` when unset. Only meaningful when the route
    /// partitions; see [`CompiledRoute::weighted`].
    pub weight: u32,
    /// Whether the YAML said `weight:`. Unset unions; set to 1 partitions.
    pub explicit_weight: bool,
    pub models: ModelChoice,
    pub backends: BackendMatch,
}

#[derive(Debug, Clone)]
pub struct CompiledExclusion {
    pub name: String,
    /// Request-scope guard. `None` always applies.
    pub if_: Option<CompiledRequestPredicate>,
    pub drop: CompiledCandidatePredicate,
}

/// How one `select` tier ranks its candidates.
///
/// Both objectives are partial: a candidate with no value is dropped from the
/// tier rather than defaulted, since `0` would be the best possible score.
#[derive(Debug, Clone)]
pub enum Rank {
    /// A calibrated per-request prediction. Local candidates only; the CRD
    /// rejects a tier that pairs this with a candidate set spanning both classes.
    MinimizePredictedTtft,
    /// `PredictedTTFT` for local candidates, the observed p95 for providers.
    /// Ranks across classes at the cost of comparing a prediction against an
    /// aggregate.
    MinimizeEstimatedTtft,
    /// Destinations in preference order.
    Order(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct CompiledSelectTier {
    pub backends: BackendMatch,
    pub rank: Rank,
}

#[derive(Debug, Clone)]
pub struct CompiledRoute {
    /// Empty means the default: `[{models: {fromRequest: true}}]`.
    pub consider: Vec<CompiledConsiderGroup>,
    pub exclude: Vec<CompiledExclusion>,
    pub select: Vec<CompiledSelectTier>,
    pub tie_break: Vec<TieBreak>,
    pub on_empty: OnEmpty,
}

impl CompiledRoute {
    /// True when the groups partition traffic stochastically rather than
    /// unioning.
    ///
    /// A route where any group carries a weight is sampled (one group, then
    /// exclude/select within it); one where none does is unioned.
    pub fn weighted(&self) -> bool {
        self.consider.iter().any(|g| g.explicit_weight)
    }
}

impl CompiledConsiderGroup {
    fn new(raw: &ConsiderGroup, path: &str, rule: &str, warnings: &mut Vec<PolicyWarning>) -> Self {
        let models = match raw.models.as_ref() {
            None => ModelChoice::FromRequest,
            Some(m) => match (m.from_request, m.names.is_empty()) {
                (Some(true), true) => ModelChoice::FromRequest,
                (None, false) | (Some(false), false) => ModelChoice::Names(m.names.clone()),
                (Some(true), false) => {
                    warnings.push(PolicyWarning::new(
                        WarningCode::ConsiderModelChoice,
                        format!("{path}.models"),
                        format!(
                            "rule `{rule}`: consider group sets both `fromRequest` and `names`; \
                             using `names`"
                        ),
                    ));
                    ModelChoice::Names(m.names.clone())
                }
                (Some(false), true) => {
                    warnings.push(PolicyWarning::new(
                        WarningCode::ConsiderModelChoice,
                        format!("{path}.models"),
                        format!(
                            "rule `{rule}`: `fromRequest: false` with no `names` selects no \
                             model at all; defaulting to the requested model. Set `names`, or \
                             `fromRequest: true`"
                        ),
                    ));
                    ModelChoice::FromRequest
                }
                (None, true) => {
                    warnings.push(PolicyWarning::new(
                        WarningCode::ConsiderModelChoice,
                        format!("{path}.models"),
                        format!(
                            "rule `{rule}`: `models` sets neither `fromRequest` nor `names`; \
                             defaulting to the requested model"
                        ),
                    ));
                    ModelChoice::FromRequest
                }
            },
        };
        Self {
            name: raw.name.clone(),
            weight: raw.weight.unwrap_or(1),
            explicit_weight: raw.weight.is_some(),
            models,
            backends: {
                let mut backends = raw
                    .backends
                    .as_ref()
                    .map(BackendMatch::compile)
                    .unwrap_or_default();
                // `BackendMatch::compile` only sees the `backends` block, so
                // fold in the `type:` shorthand here, or `{type: Provider}`
                // would admit every candidate.
                if backends.backend_type.is_none() {
                    backends.backend_type = raw.backend_type();
                }
                backends
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompiledRule {
    pub name: String,
    /// Entries are ORed. Empty matches everything.
    pub matches: Vec<CompiledRequestPredicate>,
    pub route: CompiledRoute,
}

impl CompiledRule {
    /// True when this rule matches the request. Entries OR; an empty `matches`
    /// is a catch-all.
    pub fn matches(&self, attrs: &super::predicate::RequestAttrs) -> bool {
        self.matches.is_empty() || self.matches.iter().any(|p| p.matches(attrs))
    }

    /// [`Self::matches`] with every `inputTokens` term taken as true.
    pub fn may_match(&self, attrs: &super::predicate::RequestAttrs) -> bool {
        self.matches.is_empty() || self.matches.iter().any(|p| p.may_match(attrs))
    }
}

#[derive(Debug, Clone)]
pub struct CompiledPolicy {
    pub name: String,
    pub unmatched: Unmatched,
    pub rules: Vec<CompiledRule>,
    /// Warnings from the spec alone. The name checks that need the attached
    /// pools live in [`super::lint`].
    pub warnings: Vec<PolicyWarning>,
}

impl CompiledPolicy {
    /// True when a rule's `matches` tests `inputTokens`. Only then does the
    /// router need a token count before it knows which rules match. An
    /// exclusion's `if:` reads each candidate's own count instead.
    pub fn uses_input_tokens(&self) -> bool {
        self.rules
            .iter()
            .any(|r| r.matches.iter().any(|p| p.tests_input_tokens()))
    }

    /// The implicit policy for a pool with no `policyRef`, and for traffic that
    /// matches no rule: serve as asked, with no exclusions (not even a queue
    /// threshold).
    pub fn serve_as_asked(name: &str) -> Self {
        Self {
            name: name.to_string(),
            unmatched: Unmatched::ServeLocal,
            rules: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// The route applied to traffic that matched no rule (`unmatched:
    /// ServeLocal`), equivalent to
    /// `consider: [{models: {fromRequest: true}, backends: {type: Local}}]`
    /// + `select: [{type: Local, minimize: PredictedTTFT}]`.
    ///
    /// `Local` is stated explicitly rather than relying on `PredictedTTFT`
    /// being Local-only, so changing the objective can't send unmatched
    /// traffic off-cluster. A provider is never reached by default.
    pub fn default_route() -> CompiledRoute {
        let local = BackendMatch {
            backend_type: Some(BackendType::Local),
            ..Default::default()
        };
        CompiledRoute {
            consider: vec![CompiledConsiderGroup {
                name: None,
                weight: 1,
                explicit_weight: false,
                models: ModelChoice::FromRequest,
                // Filters at generation, so no provider candidate is built.
                backends: local.clone(),
            }],
            exclude: Vec::new(),
            select: vec![CompiledSelectTier {
                backends: local,
                rank: Rank::MinimizePredictedTtft,
            }],
            tie_break: vec![TieBreak::Random],
            on_empty: OnEmpty::Reject,
        }
    }

    pub fn compile(name: &str, spec: &InferencePolicySpec) -> Self {
        let mut warnings = Vec::new();
        let rules: Vec<CompiledRule> = spec
            .rules
            .iter()
            .enumerate()
            .map(|(i, r)| compile_rule(i, r, spec.rules.len(), &mut warnings))
            .collect();
        duplicate_rule_names(spec, &mut warnings);
        identical_matches(spec, &mut warnings);
        Self {
            name: name.to_string(),
            unmatched: spec.unmatched,
            rules,
            warnings,
        }
    }
}

/// The request fields a `matches` entry or `if:` accepts, for hints.
const REQUEST_FIELDS: &str = "model, inputTokens, maxTokens, tenant, qos, tag";
/// The candidate fields a `drop:` accepts, for hints.
const DROP_FIELDS: &str = "contextWindow, predictedTTFT, prefixCacheHitRatio, queuedPrompts, \
                           kvCacheUsage, model.name, backend.type, backend.name, \
                           backend.readyEndpoints, backend.observedTTFTP95, \
                           backend.budgetExhausted";
/// Hint for an empty predicate. The API server prunes unknown fields, so a
/// misspelled key (`if: {tennant: acme}`) arrives as `if: {}`.
const PRUNED_HINT: &str = "If you wrote conditions here, none were recognized: the API server \
                           silently drops fields it doesn't know, so a misspelled key simply \
                           disappears. `kinfctl check` reports unknown fields before you apply.";

/// Where a predicate sits, which decides what an invalid term does.
#[derive(Clone, Copy)]
enum PredicateSite {
    MatchesEntry,
    ExclusionIf,
    ExclusionDrop,
}

impl PredicateSite {
    /// What an unsatisfiable predicate at this site amounts to.
    fn never(self) -> &'static str {
        match self {
            PredicateSite::MatchesEntry => "this `matches` entry never matches",
            PredicateSite::ExclusionIf => {
                "this exclusion never fires, and the candidates it would drop are still \
                 selectable"
            }
            PredicateSite::ExclusionDrop => "this exclusion never drops anything",
        }
    }

    /// What a predicate that is true for everything amounts to here.
    fn always(self) -> &'static str {
        match self {
            PredicateSite::MatchesEntry => "this term constrains nothing",
            PredicateSite::ExclusionIf => "the exclusion's guard constrains nothing",
            PredicateSite::ExclusionDrop => {
                "this term drops every candidate that carries the signal at all"
            }
        }
    }
}

/// Turn per-term issues into warnings, now that the path and rule are known.
fn term_warnings(
    issues: Vec<TermIssue>,
    path: &str,
    rule: &str,
    site: PredicateSite,
    warnings: &mut Vec<PolicyWarning>,
) {
    for i in issues {
        let at = format!("{path}.{}", i.attr);
        if i.fatal {
            warnings.push(PolicyWarning::new(
                WarningCode::InvalidTerm,
                at,
                format!(
                    "rule `{rule}`: `{}`: {}. An invalid term fails closed, so {}",
                    i.attr,
                    i.message,
                    site.never()
                ),
            ));
        } else {
            warnings.push(PolicyWarning::new(
                WarningCode::CoercedValue,
                at,
                format!("rule `{rule}`: `{}`: {}", i.attr, i.message),
            ));
        }
    }
}

/// True when a request predicate names no field at all.
fn is_blank(p: &RequestPredicate) -> bool {
    p.model.is_none()
        && p.input_tokens.is_none()
        && p.max_tokens.is_none()
        && p.tenant.is_none()
        && p.qos.is_none()
        && p.tag.is_none()
}

/// The values a numeric attribute can take, for spotting comparisons that are
/// constant over all of them.
#[derive(Clone, Copy)]
struct Domain {
    lo: f64,
    hi: f64,
    /// How the attribute is measured, for the message.
    what: &'static str,
    /// Suffix for rendering a threshold; durations compile to milliseconds.
    unit: &'static str,
}

impl Domain {
    const COUNT: Domain = Domain {
        lo: 0.0,
        hi: f64::INFINITY,
        what: "a count, never negative",
        unit: "",
    };
    const FRACTION: Domain = Domain {
        lo: 0.0,
        hi: 1.0,
        what: "a 0–1 fraction (0.8 means 80%)",
        unit: "",
    };
    const DURATION: Domain = Domain {
        lo: 0.0,
        hi: f64::INFINITY,
        what: "a duration, never negative",
        unit: "ms",
    };

    fn request(attr: &RequestAttr) -> Option<Domain> {
        match attr {
            RequestAttr::InputTokens | RequestAttr::MaxTokens => Some(Domain::COUNT),
            _ => None,
        }
    }

    fn candidate(attr: CandidateAttr) -> Option<Domain> {
        match attr {
            CandidateAttr::KvCacheUsage | CandidateAttr::PrefixCacheHitRatio => {
                Some(Domain::FRACTION)
            }
            CandidateAttr::ContextWindow
            | CandidateAttr::QueuedPrompts
            | CandidateAttr::BackendReadyEndpoints => Some(Domain::COUNT),
            CandidateAttr::PredictedTtft | CandidateAttr::BackendObservedTtftP95 => {
                Some(Domain::DURATION)
            }
            _ => None,
        }
    }
}

/// `Some(true)` when `m` holds for every value in `d`, `Some(false)` when it
/// holds for none, `None` when it depends on the value.
fn constant_over(m: &Matcher, d: Domain) -> Option<bool> {
    let (lo, hi) = (d.lo, d.hi);
    let decide = |never: bool, always: bool| {
        if never {
            Some(false)
        } else if always {
            Some(true)
        } else {
            None
        }
    };
    match m {
        Matcher::GreaterThan(n) => decide(*n >= hi, *n < lo),
        Matcher::GreaterOrEqual(n) => decide(*n > hi, *n <= lo),
        Matcher::LessThan(n) => decide(*n <= lo, *n > hi),
        Matcher::LessOrEqual(n) => decide(*n < lo, *n >= hi),
        Matcher::Equals(v) => v.as_f64().filter(|x| *x < lo || *x > hi).map(|_| false),
        Matcher::OneOf(vs) if !vs.is_empty() => vs
            .iter()
            .all(|v| v.as_f64().is_some_and(|x| x < lo || x > hi))
            .then_some(false),
        Matcher::Not(inner) => constant_over(inner, d).map(|b| !b),
        _ => None,
    }
}

/// The comparison as an author would have written it, for messages.
fn render(m: &Matcher, unit: &str) -> String {
    let num = |n: &f64| format!("{n}{unit}");
    match m {
        Matcher::GreaterThan(n) => format!("{{greaterThan: {}}}", num(n)),
        Matcher::GreaterOrEqual(n) => format!("{{greaterThanOrEqual: {}}}", num(n)),
        Matcher::LessThan(n) => format!("{{lessThan: {}}}", num(n)),
        Matcher::LessOrEqual(n) => format!("{{lessThanOrEqual: {}}}", num(n)),
        Matcher::Equals(v) => v.as_f64().map(|x| num(&x)).unwrap_or_default(),
        Matcher::OneOf(vs) => format!(
            "[{}]",
            vs.iter()
                .filter_map(|v| v.as_f64().map(|x| num(&x)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Matcher::Not(inner) => format!("{{not: {}}}", render(inner, unit)),
        _ => String::new(),
    }
}

/// The threshold a single comparison names, for the percentage hint.
fn threshold(m: &Matcher) -> Option<f64> {
    match m {
        Matcher::GreaterThan(n)
        | Matcher::GreaterOrEqual(n)
        | Matcher::LessThan(n)
        | Matcher::LessOrEqual(n) => Some(*n),
        Matcher::Equals(v) => v.as_f64(),
        Matcher::Not(inner) => threshold(inner),
        _ => None,
    }
}

/// Warn when a numeric term is decided by its threshold alone.
///
/// Mostly for fractions: `kvCacheUsage` is 0 to 1, so `{greaterThan: 80}`
/// (meaning 80%) is never true.
fn range_warning(
    name: &str,
    m: &Matcher,
    d: Domain,
    path: &str,
    rule: &str,
    site: PredicateSite,
    warnings: &mut Vec<PolicyWarning>,
) {
    let Some(always) = constant_over(m, d) else {
        return;
    };
    let hint = match threshold(m) {
        Some(n) if d.hi == 1.0 && n > 1.0 && n <= 100.0 => format!(" Did you mean {}?", n / 100.0),
        _ => String::new(),
    };
    let consequence = if always { site.always() } else { site.never() };
    warnings.push(PolicyWarning::new(
        WarningCode::ThresholdOutOfRange,
        path.to_string(),
        format!(
            "rule `{rule}`: `{name}` is {}, so `{name}: {}` is {} true; {consequence}.{hint}",
            d.what,
            render(m, d.unit),
            if always { "always" } else { "never" },
        ),
    ));
}

/// Which backend classes a `drop`'s `backend.type` term admits, found by
/// evaluating the compiled matcher on both values. `None` when the drop has no
/// `backend.type` term.
fn admitted_classes(drop: &CompiledCandidatePredicate) -> Option<Vec<BackendType>> {
    let (_, m) = drop
        .terms
        .iter()
        .find(|(a, _)| matches!(a, CandidateAttr::BackendType))?;
    Some(
        [BackendType::Local, BackendType::Provider]
            .into_iter()
            .filter(|t| m.eval(Some(&AttrValue::Str(t.as_str().to_string()))))
            .collect(),
    )
}

fn compile_rule(
    index: usize,
    raw: &Rule,
    total: usize,
    warnings: &mut Vec<PolicyWarning>,
) -> CompiledRule {
    let name = raw.name.clone();
    let rule_path = format!("spec.rules[{index}]");
    let mut matches: Vec<CompiledRequestPredicate> = Vec::with_capacity(raw.matches.len());
    for (j, m) in raw.matches.iter().enumerate() {
        let path = format!("{rule_path}.matches[{j}]");
        if is_blank(m) {
            warnings.push(PolicyWarning::new(
                WarningCode::EmptyPredicate,
                path.clone(),
                format!(
                    "rule `{name}`: `matches[{j}]` has no conditions, so it matches every \
                     request. {PRUNED_HINT} Valid fields: {REQUEST_FIELDS}"
                ),
            ));
        }
        let mut issues = Vec::new();
        let p = CompiledRequestPredicate::compile(m, &mut issues);
        term_warnings(issues, &path, &name, PredicateSite::MatchesEntry, warnings);
        for (attr, matcher) in &p.terms {
            if let Some(d) = Domain::request(attr) {
                range_warning(
                    &attr.name(),
                    matcher,
                    d,
                    &format!("{path}.{}", attr.name()),
                    &name,
                    PredicateSite::MatchesEntry,
                    warnings,
                );
            }
        }
        matches.push(p);
    }

    // A catch-all rule that cannot fall through makes every later rule dead.
    // A rule is a catch-all when `matches` is omitted or any entry has no
    // terms (entries OR). An entry whose terms were all invalid is not, since
    // it compiles to never-matching terms. `onEmpty: FallThrough` doesn't
    // shadow, since the next rule still runs on an empty candidate set.
    let unconditional = raw.matches.is_empty() || matches.iter().any(|p| p.is_unconditional());
    if unconditional && raw.route.on_empty != OnEmpty::FallThrough && index + 1 < total {
        let (how, path) = if raw.matches.is_empty() {
            ("has no `matches`", rule_path.clone())
        } else {
            (
                "has an empty `matches` entry",
                format!("{rule_path}.matches"),
            )
        };
        warnings.push(PolicyWarning::new(
            WarningCode::ShadowedRule,
            path,
            format!(
                "rule `{name}` {how} so it matches everything, and its `onEmpty: {}` stops \
                 evaluation there — so the {} later rule(s) can never be reached. Move it last, \
                 give it `onEmpty: FallThrough`, or express it as `spec.unmatched`",
                on_empty_str(raw.route.on_empty),
                total - index - 1
            ),
        ));
    }

    CompiledRule {
        name: name.clone(),
        matches,
        route: compile_route(&name, &format!("{rule_path}.route"), &raw.route, warnings),
    }
}

/// The CRD spelling of an `onEmpty` value.
fn on_empty_str(o: OnEmpty) -> &'static str {
    match o {
        OnEmpty::Reject => "Reject",
        OnEmpty::FallThrough => "FallThrough",
    }
}

/// Two rules with the same name are indistinguishable everywhere a rule is
/// reported by name: traces, the policy explorer, and the `rule` label of
/// `kinference_policy_rule_matches_total`, where their counts merge.
fn duplicate_rule_names(spec: &InferencePolicySpec, warnings: &mut Vec<PolicyWarning>) {
    let mut first: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, r) in spec.rules.iter().enumerate() {
        match first.get(r.name.as_str()) {
            Some(&j) => warnings.push(PolicyWarning::new(
                WarningCode::DuplicateName,
                format!("spec.rules[{i}].name"),
                format!(
                    "rule name `{}` is already used by spec.rules[{j}]. Traces, the policy \
                     explorer and `kinference_policy_rule_matches_total{{rule}}` identify rules \
                     by name, so the two are indistinguishable there",
                    r.name
                ),
            )),
            None => {
                first.insert(&r.name, i);
            }
        }
    }
}

/// A rule whose `matches` repeat, verbatim, those of an earlier rule that does
/// not fall through can never be reached: the earlier one claims every request
/// it would. Usually a copy-paste that was meant to be edited.
///
/// Verbatim only; containment would need predicate implication. An earlier
/// catch-all is reported on the catch-all itself, so it is skipped here.
fn identical_matches(spec: &InferencePolicySpec, warnings: &mut Vec<PolicyWarning>) {
    let raw: Vec<serde_json::Value> = spec
        .rules
        .iter()
        .map(|r| serde_json::to_value(&r.matches).unwrap_or_default())
        .collect();
    for (j, later) in spec.rules.iter().enumerate() {
        if later.matches.is_empty() {
            continue;
        }
        let shadow = spec.rules[..j].iter().enumerate().find(|(i, earlier)| {
            !earlier.matches.is_empty()
                && !earlier.matches.iter().any(is_blank)
                && earlier.route.on_empty != OnEmpty::FallThrough
                && raw[*i] == raw[j]
        });
        if let Some((i, earlier)) = shadow {
            warnings.push(PolicyWarning::new(
                WarningCode::ShadowedRule,
                format!("spec.rules[{j}].matches"),
                format!(
                    "rule `{}` has the same `matches` as rule `{}` (spec.rules[{i}]), which comes \
                     first and does not fall through, so rule `{}` can never be reached. Change \
                     one of them, merge them, or give rule `{}` `onEmpty: FallThrough`",
                    later.name, earlier.name, later.name, earlier.name
                ),
            ));
        }
    }
}

fn compile_route(
    rule: &str,
    route_path: &str,
    raw: &Route,
    warnings: &mut Vec<PolicyWarning>,
) -> CompiledRoute {
    let consider: Vec<CompiledConsiderGroup> = raw
        .consider
        .iter()
        .enumerate()
        .map(|(i, g)| {
            CompiledConsiderGroup::new(g, &format!("{route_path}.consider[{i}]"), rule, warnings)
        })
        .collect();

    // Weighting some groups samples all of them, with unset as 1, so
    // `[{weight: 70}, {}]` is a 70:1 split.
    let weighted = raw.consider.iter().filter(|g| g.weight.is_some()).count();
    if weighted > 0 && weighted < raw.consider.len() {
        let unset: Vec<String> = raw
            .consider
            .iter()
            .enumerate()
            .filter(|(_, g)| g.weight.is_none())
            .map(|(i, g)| match &g.name {
                Some(n) => format!("`{n}`"),
                None => format!("consider[{i}]"),
            })
            .collect();
        let split = raw
            .consider
            .iter()
            .map(|g| g.weight.unwrap_or(1).to_string())
            .collect::<Vec<_>>()
            .join(":");
        warnings.push(PolicyWarning::new(
            WarningCode::PartialWeights,
            format!("{route_path}.consider"),
            format!(
                "rule `{rule}`: {} set no `weight` while other groups do. Any weight turns the \
                 groups into a weighted split — one group is sampled per request — and an unset \
                 weight counts as 1, so this split is {split}. Set `weight` on every group, or \
                 on none to union them",
                unset.join(", ")
            ),
        ));
    }
    duplicate_names(
        raw.consider
            .iter()
            .enumerate()
            .filter_map(|(i, g)| g.name.as_deref().map(|n| (i, n))),
        &format!("{route_path}.consider"),
        "consider group",
        rule,
        warnings,
    );
    duplicate_names(
        raw.exclude
            .iter()
            .enumerate()
            .map(|(i, e)| (i, e.name.as_str())),
        &format!("{route_path}.exclude"),
        "exclusion",
        rule,
        warnings,
    );

    let exclude: Vec<CompiledExclusion> = raw
        .exclude
        .iter()
        .enumerate()
        .map(|(k, e)| {
            let path = format!("{route_path}.exclude[{k}]");
            let if_ = e.if_.as_ref().map(|g| {
                let if_path = format!("{path}.if");
                if is_blank(g) {
                    warnings.push(PolicyWarning::new(
                        WarningCode::EmptyPredicate,
                        if_path.clone(),
                        format!(
                            "rule `{rule}` exclusion `{}`: `if` has no conditions, so the \
                             exclusion applies to every request. {PRUNED_HINT} Valid fields: \
                             {REQUEST_FIELDS}",
                            e.name
                        ),
                    ));
                }
                let mut issues = Vec::new();
                let p = CompiledRequestPredicate::compile(g, &mut issues);
                term_warnings(issues, &if_path, rule, PredicateSite::ExclusionIf, warnings);
                for (attr, matcher) in &p.terms {
                    if let Some(d) = Domain::request(attr) {
                        range_warning(
                            &attr.name(),
                            matcher,
                            d,
                            &format!("{if_path}.{}", attr.name()),
                            rule,
                            PredicateSite::ExclusionIf,
                            warnings,
                        );
                    }
                }
                p
            });

            let drop_path = format!("{path}.drop");
            let mut issues = Vec::new();
            let drop = CompiledCandidatePredicate::compile(&e.drop, &mut issues);
            term_warnings(
                issues,
                &drop_path,
                rule,
                PredicateSite::ExclusionDrop,
                warnings,
            );
            if drop.terms.is_empty() {
                warnings.push(PolicyWarning::new(
                    WarningCode::EmptyDrop,
                    drop_path.clone(),
                    format!(
                        "rule `{rule}` exclusion `{}`: `drop` names no usable signal, so it \
                         removes nothing. {PRUNED_HINT} Valid fields: {DROP_FIELDS}",
                        e.name
                    ),
                ));
            }
            for (attr, matcher) in &drop.terms {
                if let Some(d) = Domain::candidate(*attr) {
                    range_warning(
                        attr.name(),
                        matcher,
                        d,
                        &format!("{drop_path}.{}", attr.name()),
                        rule,
                        PredicateSite::ExclusionDrop,
                        warnings,
                    );
                }
            }

            // `backend.type` is compared as a plain string, and the schema
            // admits any value, so `local` never matches.
            if let Some((_, m)) = drop
                .terms
                .iter()
                .find(|(a, _)| matches!(a, CandidateAttr::BackendType))
            {
                let mut named = Vec::new();
                m.string_operands(&mut named);
                for v in named {
                    if v != "Local" && v != "Provider" {
                        let hint = did_you_mean(&v, ["Local", "Provider"])
                            .map(|s| format!(" Did you mean `{s}`?"))
                            .unwrap_or_default();
                        warnings.push(PolicyWarning::new(
                            WarningCode::InvalidEnumValue,
                            format!("{drop_path}.backend.type"),
                            format!(
                                "rule `{rule}` exclusion `{}`: `{v}` is not a backend type — \
                                 valid values are `Local` and `Provider`, compared \
                                 case-sensitively — so a comparison against it never \
                                 matches.{hint}",
                                e.name
                            ),
                        ));
                    }
                }
            }

            // A signal only one class carries can't drop the other class, since
            // absence never matches.
            let classes = admitted_classes(&drop);
            for (attr, _) in &drop.terms {
                let Some(class) = attr.scoped_to() else {
                    continue;
                };
                let message = match &classes {
                    // An empty set means `backend.type` itself never matches;
                    // InvalidEnumValue has already said so.
                    Some(c) if c.is_empty() || c.as_slice() == [class] => continue,
                    Some(c) if !c.contains(&class) => format!(
                        "`{}` exists only for {} candidates, but `backend.type` admits only \
                         {} ones, so this exclusion never drops anything",
                        attr.name(),
                        class.as_str(),
                        c.iter().map(|t| t.as_str()).collect::<Vec<_>>().join(", ")
                    ),
                    _ => format!(
                        "`{}` exists only for {} candidates, and this exclusion does not pin \
                         `backend.type` to {}, so it silently never drops a candidate of the \
                         other class. Add `backend: {{type: {}}}` to say what you mean",
                        attr.name(),
                        class.as_str(),
                        class.as_str(),
                        class.as_str()
                    ),
                };
                warnings.push(PolicyWarning::new(
                    WarningCode::DropTermWrongClass,
                    format!("{drop_path}.{}", attr.name()),
                    format!("rule `{rule}` exclusion `{}`: {message}", e.name),
                ));
            }
            if drop
                .terms
                .iter()
                .any(|(a, _)| matches!(a, CandidateAttr::BackendBudgetExhausted))
            {
                warnings.push(PolicyWarning::new(
                    WarningCode::NoSignalSource,
                    format!("{drop_path}.backend.budgetExhausted"),
                    format!(
                        "rule `{rule}` exclusion `{}`: `backend.budgetExhausted` has no source — \
                         UsageBudget is not implemented — so this exclusion never fires and the \
                         candidates it would drop are still selectable",
                        e.name
                    ),
                ));
            }
            CompiledExclusion {
                name: e.name.clone(),
                if_,
                drop,
            }
        })
        .collect();

    // Local candidates can reach a tier only if some consider group admits
    // them. No `consider` means the default, which admits everything.
    let route_admits_local = raw.consider.is_empty()
        || raw
            .consider
            .iter()
            .any(|g| g.backend_type() != Some(BackendType::Provider));

    let mut select = Vec::new();
    for (i, tier) in raw.select.iter().enumerate() {
        let tier_path = format!("{route_path}.select[{i}]");
        let mut backends = tier
            .backends
            .as_ref()
            .map(BackendMatch::compile)
            .unwrap_or_default();
        // Fold in the `type:` shorthand, which `BackendMatch::compile` doesn't
        // see; otherwise `{type: Provider, order: [...]}` would admit local
        // candidates. The CRD rejects setting both.
        if backends.backend_type.is_none() {
            backends.backend_type = tier.backend_type();
        }
        let rank = match (tier.minimize, tier.order.is_empty()) {
            (Some(Minimize::PredictedTTFT), true) => Rank::MinimizePredictedTtft,
            (Some(Minimize::EstimatedTTFT), true) => Rank::MinimizeEstimatedTtft,
            (None, false) => Rank::Order(tier.order.clone()),
            (Some(m), false) => {
                warnings.push(PolicyWarning::new(
                    WarningCode::SelectRankAmbiguous,
                    tier_path.clone(),
                    format!(
                        "rule `{rule}` select tier {i}: sets both `minimize` and `order`; using \
                         `minimize`"
                    ),
                ));
                match m {
                    Minimize::PredictedTTFT => Rank::MinimizePredictedTtft,
                    Minimize::EstimatedTTFT => Rank::MinimizeEstimatedTtft,
                }
            }
            (None, true) => {
                warnings.push(PolicyWarning::new(
                    WarningCode::SelectRankAmbiguous,
                    tier_path.clone(),
                    format!(
                        "rule `{rule}` select tier {i}: ranks by neither `minimize` nor \
                         `order`; defaulting to minimize PredictedTTFT"
                    ),
                ));
                Rank::MinimizePredictedTtft
            }
        };
        // A tier with no `backends` subset keeps whatever survived, so nothing
        // after it can run.
        if backends.is_empty() && i + 1 < raw.select.len() {
            warnings.push(PolicyWarning::new(
                WarningCode::UnreachableTier,
                tier_path.clone(),
                format!(
                    "rule `{rule}` select tier {i} has no `backends` subset, so it always has a \
                     surviving candidate and the {} tier(s) after it are unreachable",
                    raw.select.len() - i - 1
                ),
            ));
        }
        // `order` ranks by name and ignores load, so every endpoint of the
        // top-ranked backend ties. With no random tie-break, `pick_among` takes
        // the first of them every time. Providers are exempt, with nothing to
        // spread.
        if matches!(rank, Rank::Order(_))
            && backends.backend_type != Some(BackendType::Provider)
            && route_admits_local
            && !raw.tie_break.contains(&TieBreak::Random)
        {
            warnings.push(PolicyWarning::new(
                WarningCode::OrderWithoutRandom,
                format!("{tier_path}.order"),
                format!(
                    "rule `{rule}` select tier {i} ranks by `order`, which ignores load: every \
                     endpoint of the first-listed backend ties, and without `tieBreak: [Random]` \
                     the same endpoint wins every time, so one pod takes all of this tier's \
                     traffic. Add `tieBreak: [Random]` to the route, or rank with `minimize: \
                     PredictedTTFT`. If this tier only ever holds providers, say `type: \
                     Provider`: a provider has one candidate per model, so nothing ties"
                ),
            ));
        }
        select.push(CompiledSelectTier { backends, rank });
    }
    if select.is_empty() {
        // `select` is required with minItems 1.
        warnings.push(PolicyWarning::new(
            WarningCode::EmptySelect,
            format!("{route_path}.select"),
            format!("rule `{rule}`: `select` is empty; defaulting to minimize PredictedTTFT"),
        ));
        select.push(CompiledSelectTier {
            backends: BackendMatch::default(),
            rank: Rank::MinimizePredictedTtft,
        });
    }

    CompiledRoute {
        consider,
        exclude,
        select,
        tie_break: raw.tie_break.clone(),
        on_empty: raw.on_empty,
    }
}

/// Warn on every name in `named` already used by an earlier entry.
fn duplicate_names<'a>(
    named: impl Iterator<Item = (usize, &'a str)>,
    list_path: &str,
    what: &str,
    rule: &str,
    warnings: &mut Vec<PolicyWarning>,
) {
    let mut first: BTreeMap<&str, usize> = BTreeMap::new();
    for (i, n) in named {
        match first.get(n) {
            Some(&j) => warnings.push(PolicyWarning::new(
                WarningCode::DuplicateName,
                format!("{list_path}[{i}].name"),
                format!(
                    "rule `{rule}`: {what} name `{n}` is already used by {list_path}[{j}], so the \
                     two are indistinguishable in traces and the policy explorer"
                ),
            )),
            None => {
                first.insert(n, i);
            }
        }
    }
}

#[cfg(test)]
mod tests;
