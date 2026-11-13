//! Rust types for the `kinference.buoyant.io/v1alpha1` custom resources, plus
//! the parts of the Gateway API kinference reads and writes.
//!
//! These mirror `charts/kinference-crds/templates/*.yaml`, which is the source
//! of truth; no schema is generated from these types. The `JsonSchema` derive
//! exists only because `kube`'s `CustomResource` macro requires it.
//!
//! Predicate values (`matches`, `if:`, `drop:`) arrive as untyped
//! `serde_json::Value`, since the value's shape selects the comparison (scalar
//! for equality, array for membership, map for an operator). `super::predicate`
//! compiles them once per policy revision.
//!
//! Fields the schema requires are often `Option` here: `kube::runtime::watcher`
//! deserializes list pages wholesale, so one nonconforming object (applied with
//! `--validate=false`, or predating a schema change) would fail the whole page.
//! The router enforces them instead and reports violations in status.
//!
//! Status object fields serialize empty lists as `[]` and absent optional values
//! as `null`. JSON merge patches leave omitted fields unchanged. Fields nested
//! inside arrays may still skip, since the parent array is replaced wholesale.
//!
//! Each status's `state` is a comma-separated string for a printer column: the
//! primary verdict, then any qualifiers that apply. It is a string because a
//! `[*]` printer column renders only the first array element, and it is never
//! empty once written, so a blank column means no status was written.

use std::collections::BTreeMap;

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Untyped predicate payload, compiled by `super::predicate`.
pub type RawPredicateValue = serde_json::Value;

// ---------------------------------------------------------------------------
// InferenceWorkerSet
// ---------------------------------------------------------------------------

/// A set of local model-server pods, named indirectly through the Service that
/// fronts them.
///
/// A Service rather than a pod selector, so discovery can rely on its
/// EndpointSlices' `ready` state: a terminating endpoint keeps its in-flight
/// requests but gets no new ones.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferenceWorkerSet",
    plural = "inferenceworkersets",
    shortname = "iws",
    namespaced,
    status = "InferenceWorkerSetStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceWorkerSetSpec {
    /// The Service fronting these model servers. Same-namespace only, so no
    /// `ReferenceGrant` is involved.
    pub service_ref: ServiceRef,
    /// Which profile in kinference's config supplies this set's predictor
    /// coefficients, picker policy, and tokenize mode.
    ///
    /// Required by the schema; see the module docs for why it is `Option`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    /// Tokens per block in the KV-cache events these engines publish.
    ///
    /// Not necessarily the engine's `--block-size`. They coincide for standard
    /// vLLM (16), but Gemma 4 reports `block_size="64"` in
    /// `vllm:cache_config_info` while publishing 256-token blocks, and SGLang
    /// `--page-size 1` publishes 1. The log line `Block not published. Block
    /// size must be N tokens to be published. Block size is: M` gives the right
    /// value, M.
    ///
    /// With a wrong value, mismatched blocks are dropped, so the prefix tree
    /// stays empty and picks degrade to load-only with no error. After
    /// `BLOCK_SIZE_MISMATCH_EVICT_THRESHOLD` consecutive mismatched events the
    /// endpoint is evicted; one matching event resets the count.
    ///
    /// Required by the schema; see the module docs for why it is `Option`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_block_size: Option<u32>,
    /// Where Prometheus metrics are served, if not on the OpenAI port.
    ///
    /// Unset uses the port named `metrics` if the Service has one, else
    /// `serviceRef.port`. vLLM and SGLang both serve `/metrics` on the OpenAI
    /// port.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_port: Option<IntOrString>,
    /// Where these servers publish KV cache events, on the same Service as
    /// [`Self::service_ref`]. Without events there is no prefix model, so
    /// `prefixCacheHitRatio` is 0 everywhere and `predictedTTFT` loses most of
    /// its accuracy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_events: Option<KvEvents>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_discovery: Option<ModelDiscovery>,
    /// Optional aliases. Two entries sharing an alias declare their upstream
    /// models interchangeable, so a request for that alias fans out into
    /// multiple candidates.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ModelAlias>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceRef {
    pub name: String,
    /// The OpenAI API's Service port name (preferred) or number.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<IntOrString>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct KvEvents {
    /// Port name or number on `serviceRef`'s Service, for the publish socket.
    ///
    /// Required by the schema; see the module docs for why it is `Option`.
    /// Rejected as `KvEventsMissingPort` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<IntOrString>,
    /// The engine's ZMQ event replay socket, read at startup to recover events
    /// published before the router connected.
    ///
    /// Unset uses the port named `kv-replay` if the Service has one, and
    /// otherwise disables replay: an endpoint's existing cache contents stay
    /// invisible until they are re-stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_port: Option<IntOrString>,
}

/// Minimal stand-in for `k8s.io/apimachinery` `IntOrString`, which
/// `k8s-openapi` does expose but not with a `JsonSchema` impl.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum IntOrString {
    Int(i64),
    Str(String),
}

impl IntOrString {
    /// The port name, when the reference used one.
    pub fn as_name(&self) -> Option<&str> {
        match self {
            IntOrString::Str(s) => Some(s.as_str()),
            IntOrString::Int(_) => None,
        }
    }

    /// The port number, when the reference used one or a numeric string.
    pub fn as_number(&self) -> Option<u16> {
        match self {
            IntOrString::Int(n) => u16::try_from(*n).ok(),
            IntOrString::Str(s) => s.parse().ok(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelDiscovery {
    #[serde(default = "default_discovery_mode")]
    pub mode: ModelDiscoveryMode,
    /// Duration string (`30s`, `1m`).
    #[serde(default = "default_models_interval")]
    pub interval: String,
}

impl Default for ModelDiscovery {
    fn default() -> Self {
        Self {
            mode: default_discovery_mode(),
            interval: default_models_interval(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ModelDiscoveryMode {
    /// Poll `/v1/models` on each ready endpoint.
    #[default]
    Endpoint,
    /// Trust `spec.models` alone; no polling.
    Static,
}

fn default_discovery_mode() -> ModelDiscoveryMode {
    ModelDiscoveryMode::Endpoint
}
fn default_models_interval() -> String {
    "30s".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelAlias {
    /// What this set reports on `/v1/models`, and what gets written into the
    /// outbound body when this candidate wins. Always routable under itself.
    ///
    /// Required by the schema; see the module docs for why it is `Option`.
    /// Rejected as `ModelMissingName` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Client-facing names that also resolve to [`Self::name`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferenceWorkerSetStatus {
    /// See the module docs.
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub models: Vec<WorkerSetModelStatus>,
    /// Pools listing this set in `spec.destinations`, sorted.
    ///
    /// Empty doesn't mean unreachable: a `Pick` naming no pool can still route
    /// here. It means no pool-scoped policy applies.
    #[serde(default)]
    pub referenced_by: Vec<String>,
    #[serde(default)]
    pub endpoints: Option<EndpointCounts>,
    #[serde(default)]
    pub kv_events: Option<KvEventsStatus>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// Whether a prefix model exists for this set. Otherwise invisible, since a
/// set with no event stream still serves traffic.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KvEventsStatus {
    /// Mirrors `spec.kvEvents` being set.
    #[serde(default)]
    pub configured: Option<bool>,
    /// Endpoints kinference has received KV events from.
    #[serde(default)]
    pub subscribed_endpoints: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSetModelStatus {
    /// What this set's engines report on `/v1/models`.
    ///
    /// One entry per distinct model, so LoRA adapters and multiple
    /// `--served-model-name` values each get one, and `ready_endpoints` shows a
    /// partial rollout.
    pub upstream_name: String,
    /// Client-facing names `spec.models` maps onto this one, if any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_endpoints: Option<i64>,
    /// Effective context window (vLLM `max_model_len`), as a range since
    /// endpoints may disagree mid-rollout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<ContextWindow>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct ContextWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i64>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema)]
pub struct EndpointCounts {
    #[serde(default)]
    pub ready: Option<i64>,
}

// ---------------------------------------------------------------------------
// InferenceProvider
// ---------------------------------------------------------------------------

/// A destination outside the cluster: wire format and model aliasing.
///
/// `baseURL`, credentials and TLS live on the `gateway.linkerd.io`
/// `GatewayBackend` this points at, which kinference doesn't read or hold RBAC
/// for; it names the backend in the pick response and the caller resolves it.
/// Everything inference-specific the caller needs is flattened into
/// `ProviderTarget`, so the gateway never reads this kind and kinference never
/// holds a vendor key.
///
/// So kinference can't tell whether `backendRef` resolves. A dangling ref or a
/// missing Secret looks healthy until the caller reports otherwise via
/// `Abort(BACKEND_UNRESOLVED)`.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferenceProvider",
    plural = "inferenceproviders",
    shortname = "infprov",
    namespaced,
    status = "InferenceProviderStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderSpec {
    /// The `GatewayBackend` carrying URL and credentials. Cross-namespace
    /// needs a `ReferenceGrant` in the target namespace, which kinference
    /// checks without reading the backend.
    ///
    /// Required by the schema; see the module docs for why it is `Option`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_ref: Option<BackendObjectRef>,
    /// Wire format. kinference only names it; the caller translates.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<ApiFormat>,
    /// Path the caller must dispatch on, replacing the one the client sent.
    /// Absent leaves the request line alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Declared rather than discovered, since kinference can't reach the
    /// vendor's `/v1/models`.
    ///
    /// For a request to span local and remote candidates, a name here must
    /// match what the worker sets expose. Nothing validates that;
    /// `InferencePool.status.models[].servedBy` shows every claim.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<ProviderModel>,
}

/// A reference to an object in another API group, so `group` and `kind` are
/// meaningful rather than implied.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendObjectRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Wire format an upstream speaks.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum ApiFormat {
    /// The only pass-through.
    #[default]
    OpenAI,
    Anthropic,
    Bedrock,
    VertexAI,
    AzureOpenAI,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProviderModel {
    /// What this vendor calls it: an endpoint name, an ARN, a repo path.
    /// Written into the body when this provider wins, and routable under itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Client-facing names that also resolve to [`Self::name`].
    ///
    /// This is where a name shared with a local worker set goes, so one
    /// request name can reach both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
    /// Declared, since vendors rarely advertise one. Requests longer than this
    /// won't be sent here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderStatus {
    /// See the module docs.
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

// ---------------------------------------------------------------------------
// ReferenceGrant (gateway.networking.k8s.io/v1beta1), read-only
// ---------------------------------------------------------------------------

/// Just enough `ReferenceGrant` to answer "may namespace A reference this
/// object in namespace B".
///
/// Checked for a pool's `destinations[].namespace` and an
/// `InferenceProvider`'s `backendRef.namespace`; the latter stops one namespace
/// using another's vendor credential.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1beta1",
    kind = "ReferenceGrant",
    plural = "referencegrants",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceGrantSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub from: Vec<ReferenceGrantFrom>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub to: Vec<ReferenceGrantTo>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceGrantFrom {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReferenceGrantTo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Absent means every object of that kind in this namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

// ---------------------------------------------------------------------------
// InferencePool
// ---------------------------------------------------------------------------

/// The HTTPRoute backendRef: which destinations traffic may reach, and which
/// policy chooses among them.
///
/// A policy cannot reach a destination the pool does not list, so tenants can
/// own policies while pools stay platform-owned.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferencePool",
    plural = "inferencepools",
    shortname = "infpool",
    namespaced,
    status = "InferencePoolStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferencePoolSpec {
    /// Omit to mean every `InferenceWorkerSet` in this namespace.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub destinations: Vec<Destination>,
    /// At most one policy. Omitting it means "serve every request as asked".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_ref: Option<PolicyRef>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Destination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Exactly one of `name` or `selector` is set (admission-enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct LabelSelector {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub match_labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PolicyRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferencePoolStatus {
    /// See the module docs.
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub destination_count: Option<i64>,
    /// The pool's own admission state, as distinct from the per-parent
    /// handshake in `parents[].conditions`. A pool with no `HTTPRoute` has no
    /// parents.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// What traffic on this pool can reach, aggregated across destinations.
    #[serde(default)]
    pub models: Vec<PoolModelStatus>,
    #[serde(default)]
    pub unresolved_destinations: Vec<UnresolvedDestination>,
    /// Gateway API status contract, per parent Gateway. This is why kinference
    /// reads HTTPRoutes.
    #[serde(default)]
    pub parents: Vec<ParentStatus>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PoolModelStatus {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub served_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<ContextWindow>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedDestination {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Why it didn't resolve: `NotFound`, `NoMatchingPool`, etc.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParentStatus {
    pub parent_ref: ParentRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub controller_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParentRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// `metav1.Condition`. Hand-rolled rather than reused from `k8s-openapi`
/// because that type has no `JsonSchema` impl.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    pub r#type: String,
    /// `"True"` | `"False"` | `"Unknown"`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    pub last_transition_time: String,
    pub reason: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// InferencePolicy
// ---------------------------------------------------------------------------

/// How to choose, given the worker sets an `InferencePool` allows. The only
/// object a tenant needs write access to. Rules are first-match-wins in list
/// order.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "kinference.buoyant.io",
    version = "v1alpha1",
    kind = "InferencePolicy",
    plural = "inferencepolicies",
    shortname = "infpol",
    namespaced,
    status = "InferencePolicyStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct InferencePolicySpec {
    /// `ServeLocal` (default) is fail-open. A rule set meant to enumerate must
    /// say `Reject`, or an unlisted tenant is served rather than refused.
    #[serde(default)]
    pub unmatched: Unmatched,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<Rule>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum Unmatched {
    #[default]
    ServeLocal,
    Reject,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rule {
    pub name: String,
    /// Entries are ORed; fields within one entry are ANDed. No `matches` means
    /// "matches everything".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matches: Vec<RequestPredicate>,
    pub route: Route,
}

/// Request-scope predicate. Values are untyped; see the module docs.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RequestPredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<RawPredicateValue>,
    /// Always present on the request, so a predicate here always evaluates.
    /// `tenant: ""` matches traffic the caller could not attribute.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<RawPredicateValue>,
    /// Array-only, over `interactive | batch | deferrable`, as the schema
    /// enforces. Untyped here so a nonconforming value degrades to a warning
    /// rather than failing deserialization of the whole policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<RawPredicateValue>,
    /// A set of opaque tags attached to the request. A scalar means contains,
    /// an array means intersects, `{containsAll: [...]}` means all of them,
    /// and `{not: t}` means `t` is absent. The router sends an empty set for
    /// now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<RawPredicateValue>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    /// Candidate generation. Defaults to the requested model on every
    /// destination the pool allows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub consider: Vec<ConsiderGroup>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<Exclusion>,
    /// Required. Ordered tiers; the first with a surviving candidate wins.
    pub select: Vec<SelectTier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tie_break: Vec<TieBreak>,
    #[serde(default)]
    pub on_empty: OnEmpty,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConsiderGroup {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Shorthand for `backends.type`, as in `SelectTier`; setting both is
    /// admission-rejected.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<BackendType>,
    /// Relative, not a percentage. Groups are sampled by weight before
    /// exclusions and selection run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub models: Option<ModelSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<BackendSelector>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ModelSelector {
    /// Exactly one of `from_request` or `names` (admission-enforced).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_request: Option<bool>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendSelector {
    /// Semantic class, not a Kind name, so `Local` could later cover more than
    /// `InferenceWorkerSet`.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<BackendType>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub names: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<LabelSelector>,
}

/// Which class of destination a selector or tier admits.
///
/// A local candidate carries live per-endpoint telemetry; a provider candidate
/// carries only an empirical aggregate. Signals are scoped by this.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum BackendType {
    #[default]
    Local,
    Provider,
}

impl BackendType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local => "Local",
            Self::Provider => "Provider",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Exclusion {
    pub name: String,
    /// Optional request-scope guard, since first-match-wins rules can't compose
    /// exclusions.
    #[serde(default, rename = "if", skip_serializing_if = "Option::is_none")]
    pub if_: Option<RequestPredicate>,
    pub drop: CandidatePredicate,
}

/// Candidate-scope predicate. A predicate over a signal the candidate does not
/// carry does not match.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidatePredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_window: Option<RawPredicateValue>,
    /// Renamed explicitly: `rename_all` would give `predictedTtft`, and serde
    /// would silently ignore the schema's `predictedTTFT`. Pinned by
    /// `every_candidate_attr_name_round_trips_through_serde`.
    #[serde(
        default,
        rename = "predictedTTFT",
        skip_serializing_if = "Option::is_none"
    )]
    pub predicted_ttft: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_cache_hit_ratio: Option<RawPredicateValue>,
    /// Mean queued prompts over the last 30s, so fractional: 5 prompts queued
    /// for two seconds reads about 0.3, and `greaterThan: 0` means any queueing
    /// in the window.
    ///
    /// Excluding every backed-up endpoint empties the candidate set, and
    /// `onEmpty` applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_prompts: Option<RawPredicateValue>,
    /// Mean KV-cache utilization over the last 30s, 0.0..1.0.
    ///
    /// Unlike `queuedPrompts` it rises monotonically with pressure, so it is
    /// the one to threshold for "this GPU is nearly full".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_cache_usage: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<CandidateModelPredicate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<CandidateBackendPredicate>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidateModelPredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<RawPredicateValue>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CandidateBackendPredicate {
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<RawPredicateValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<RawPredicateValue>,
    /// Local-only, so it never drops a provider candidate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_endpoints: Option<RawPredicateValue>,
    /// Provider-only. An aggregate, so it is a gate and never a ranking key.
    #[serde(
        default,
        rename = "observedTTFTP95",
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_ttft_p95: Option<RawPredicateValue>,
    /// Provider-only, sourced from a `UsageBudget`, which does not exist yet.
    /// Parsed so using it produces a warning rather than being silently ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_exhausted: Option<RawPredicateValue>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SelectTier {
    /// Shorthand for `backends.type`; setting both is admission-rejected.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<BackendType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<BackendSelector>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimize: Option<Minimize>,
    /// Destinations, in preference order. Exactly one of `minimize` or `order`
    /// (admission-enforced).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order: Vec<String>,
}

impl ConsiderGroup {
    /// The group's backend-class constraint, from either spelling.
    pub fn backend_type(&self) -> Option<BackendType> {
        self.type_
            .or_else(|| self.backends.as_ref().and_then(|b| b.type_))
    }
}

impl SelectTier {
    /// The tier's backend-class constraint, from either spelling.
    pub fn backend_type(&self) -> Option<BackendType> {
        self.type_
            .or_else(|| self.backends.as_ref().and_then(|b| b.type_))
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum Minimize {
    /// A per-request prediction over prompt, prefix overlap and worker state.
    /// Local-only, so the CRD rejects it on a tier that could span both
    /// classes.
    #[default]
    PredictedTTFT,
    /// `PredictedTTFT` where available, `observedTTFTP95` otherwise. This ranks
    /// a per-request prediction against an empirical aggregate; use separate
    /// tiers to avoid mixing them.
    EstimatedTTFT,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TieBreak {
    /// Prefer a local candidate over a provider one. Only matters within a
    /// tier spanning both classes, and only among ties.
    PreferLocal,
    #[default]
    Random,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum OnEmpty {
    /// Fail the request when every candidate has been excluded.
    #[default]
    Reject,
    /// Continue evaluating subsequent rules; if none yields a candidate, apply
    /// `spec.unmatched`.
    FallThrough,
}

// ---------------------------------------------------------------------------
// HTTPRoute (gateway.networking.k8s.io/v1), read-only
// ---------------------------------------------------------------------------

/// Just enough `HTTPRoute` to answer "which Gateways route to this pool".
///
/// Needed because `InferencePool` status reports conditions per parent
/// Gateway. kinference never writes HTTPRoute status.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "HTTPRoute",
    plural = "httproutes",
    namespaced
)]
#[serde(rename_all = "camelCase")]
pub struct HTTPRouteSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_refs: Vec<ParentRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<HTTPRouteRule>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct HTTPRouteRule {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backend_refs: Vec<BackendRef>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl BackendRef {
    /// True when this ref targets an `kinference.buoyant.io` `InferencePool`.
    pub fn is_inference_pool(&self) -> bool {
        self.group.as_deref() == Some("kinference.buoyant.io")
            && self.kind.as_deref() == Some("InferencePool")
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct InferencePolicyStatus {
    /// `InForce` or `NotReferenced`, then `HasWarnings` if there are any. See
    /// the module docs. Warnings don't affect the `Accepted` condition.
    #[serde(default)]
    pub state: Option<String>,
    /// Pools whose `policyRef` names this policy. Empty means nothing routes
    /// through it.
    #[serde(default)]
    pub referenced_by: Vec<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

// ---------------------------------------------------------------------------
// BackendBinding (gateway.linkerd.io)
// ---------------------------------------------------------------------------

/// The gateway's stand-in for a backendRef of a kind it has no schema for.
///
/// A `BackendBinding` claims a `(group, kind, name)` triple and says what that
/// ref resolves to in kinds the gateway does resolve: a core `Service` or a
/// `GatewayBackend`. The gateway never reads the claimed object. The router
/// writes one per `InferencePool`.
///
/// Unlike the other kinds here, the schema ships with the gateway
/// (`deploy/chart/gateway/crds/backendbinding.yaml` there). Nothing catches
/// drift between the two, so check against it when touching either.
#[derive(CustomResource, Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "gateway.linkerd.io",
    version = "v1alpha1",
    kind = "BackendBinding",
    plural = "backendbindings",
    namespaced,
    status = "BackendBindingStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct BackendBindingSpec {
    /// The foreign backendRef this binding answers for, matched as opaque
    /// strings against route refs in the binding's own namespace.
    pub claim: BindingClaim,
    /// What the claimed ref resolves to. Native kinds only.
    pub backend_refs: Vec<BindingBackendRef>,
    /// `Extension` objects prepended to the filter chain of any rule whose ref
    /// this binding claims.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<ExtensionReference>,
}

/// An `Extension` a binding lists. One in another namespace is used only if
/// its `allowedReferences` admits the binding's namespace.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct ExtensionReference {
    pub name: String,
    /// Absent means the binding's own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// The `(group, kind, name)` of the backendRef a binding answers for, held as
/// opaque strings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct BindingClaim {
    pub group: String,
    pub kind: String,
    pub name: String,
}

/// One destination a binding resolves to.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BindingBackendRef {
    /// Identifies the entry within the binding. kinference uses the
    /// destination's name, which is unique within a pool.
    pub name: String,
    /// The target. Absent `group` and `kind` mean a core `Service`.
    #[serde(rename = "ref")]
    pub target: BindingObjectReference,
}

/// A typed reference to a native backend kind.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BindingObjectReference {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    /// Absent means the binding's own namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Service port the traffic goes to. Absent for a `GatewayBackend`, which
    /// carries its port in `baseURL` and is rejected if given one here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// Status of one `BackendBinding`, written by the gateway. Read so kinference
/// can log an `Accepted=False` verdict.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BackendBindingStatus {
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

#[cfg(test)]
mod tests;
