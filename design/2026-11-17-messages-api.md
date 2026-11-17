---
status: draft
date: 2026-11-17
author: Zahari Dichev
issue: 117
---

# Route Anthropic Messages requests

## Summary

Kinference will pick an endpoint for `/v1/messages` and
`/v1/messages/count_tokens` requests. The gateway will name the API
call of each request in `PickRequest`. Kinference will get the token
ids of a Messages body from vLLM's `/v1/messages/render`, so that a
Messages pick uses the KV cache as a chat-completion pick does.

## Why

A client such as Claude Code speaks the Anthropic Messages API. An
operator who serves such a client through the gateway can't use
Kinference today. Kinference reads every body as an OpenAI request,
and the gateway has no way to name another format.

The body alone doesn't settle the format. The body
`{"model": "m", "max_tokens": 64, "messages": [{"role": "user",
"content": "hi"}]}` is valid in both schemas.

## Today

- `PickRequest` has no field for the API format, in
  `crates/kinference-router/proto/kinference.proto`.
- The pick reads every body as a chat-completion request, in
  `crates/kinference-router/src/service/lifecycle.rs`. `pick_body`
  sets `stream_options.include_usage` on every streaming body.
- The render backend posts the body to `/v1/chat/completions/render`
  on the worker set's workers, in
  `crates/kinference-router/src/tokenizer/http.rs`. When the operator
  sets `render_url`, the backend posts the body to that one full URL
  and never tries the workers.
- `resolve_provider` rejects every `InferenceProvider` whose `api` is
  not `OpenAI`, in `crates/kinference-policy/src/resolve.rs`.
- The gateway reports the first token at the first body chunk and
  scans the response for the OpenAI usage fields.

No workaround exists.

## Proposal

### The gateway names the API call

```proto
enum ApiCall {
  CHAT_COMPLETIONS = 0;
  MESSAGES = 1;
  MESSAGES_COUNT_TOKENS = 2;
}
ApiCall api_call = 6;  // in PickRequest
```

The gateway derives the value from the path of the client's request,
without the query. The proto states the rule.

- A path that ends in `/v1/messages/count_tokens` is
  `MESSAGES_COUNT_TOKENS`.
- A path that ends in `/v1/messages` is `MESSAGES`.
- Every other path is `CHAT_COMPLETIONS`.

Kinference never guesses the format from the body. `Pick` returns the
gRPC status `INVALID_ARGUMENT` for an unknown value.

### A Messages pick

The five steps of `pick_and_record` stay.

- Parse reads `model`, `stream` and `max_tokens`, as today.
- Resolve writes the resolved model into the top-level `model` field.
  `pick_body` adds no `stream_options`, because vLLM and Anthropic
  report usage on every Messages stream.
- Score posts the body unchanged to `/v1/messages/render` and matches
  the token ids against the radix tree. vLLM converts the body with
  the converter that `/v1/messages` uses, so Kinference never repeats
  the conversion.
- Select drops each provider that doesn't speak the format. The
  Providers section has the rule.
- Commit doesn't change.

A Messages request always uses the render backend, whatever tokenize
mode the profile sets. When the render call fails, the pick routes on
load alone and returns `DEGRADED`, as a chat-completion pick does.
The latch of the WARN line gains the API call in its key.

### One base URL for a render service

A worker set in the config gains `render_base_url`. The render
backend appends the render path of the API call to the base URL and
calls the result once. `render_url` keeps its meaning for a
chat-completion request. The config check rejects a worker set
that has both fields.

A Messages render goes to one of three places.

- With `render_base_url`, the render goes to the base URL.
- With `render_url` alone, Kinference derives the URL. When
  `render_url` ends in `/v1/chat/completions/render`, Kinference
  swaps that suffix for `/v1/messages/render`. For any other
  `render_url`, Kinference makes no render call. The pick returns
  `DEGRADED`, and Kinference logs one WARN line that names
  `render_base_url`.
- With neither field, the render goes to the set's own workers. Every
  worker set that an `InferenceWorkerSet` declares is in this case.

### A token-count pick

`/v1/messages/count_tokens` runs the engine's tokenizer and never the
model.

- Score makes no render call and scores every worker on load alone.
  The pick returns `OK`.
- The policy gets no input token count, so a predicate on
  `inputTokens` doesn't match.
- Commit writes no in-flight state, no prediction and no prefix-match
  count, and leaves no observation. A provider pick keeps the
  per-pick record that the provider's health reads.
- The gateway sends `ReportFirstToken` with the upstream status when
  the response ends, and no `ReportFinalToken`. For a provider pick,
  Kinference reads the status and releases the per-pick record. For a
  local pick, Kinference ignores the report.

### Providers

`resolve_provider` accepts `api: Anthropic` beside `api: OpenAI`, and
`ResolvedProvider` gains the provider's `api`. `kinference-policy`
gains one pure check of a provider against an API call. A Messages
request and a token-count request match only an Anthropic provider. A
chat-completion request matches only an OpenAI provider. The router's
policy driver, the router's `provider_serves` and the admin policy
API drop a provider that fails the check. The explain request gains
the API call.

Every worker set stays a candidate for every API call. Kinference
can't tell whether a worker serves the Messages routes, so the
operator sends Messages requests only to pools whose workers do.
Kinference translates nothing.

One `spec.path` can't name both Messages paths. For a token-count
pick, Kinference returns an empty `ProviderTarget.path`, and the
gateway keeps the path that the client sent.

### What the gateway reports for a Messages response

The proto documents how the gateway fills the two reports from a
Messages response. The report messages gain no field.

- The gateway sends exactly one `ReportFirstToken` for each
  dispatched pick. The TTFT clock starts when `Pick` returns, as
  today.
- `model` is the `model` of the response.
- `prompt_tokens` is `input_tokens` plus `cache_read_input_tokens`
  plus `cache_creation_input_tokens`. An absent or `null` field
  counts as 0.
- `cached_tokens` is `cache_read_input_tokens`, and stays unset when
  that field is absent or `null`.
- `completion_tokens` is `output_tokens`.
- A response without a stream has the counts in its top-level
  `usage`. In a stream, the gateway reads the counts from
  `message_start`. A count in a later `message_delta` that is not
  `null` replaces the earlier count.
- The first token is the first `content_block_delta` event, and an
  inter-token gap is the time between two such events. When a
  response has no such event, the first token is the end of the
  response.
- An `error` event before the first token is an upstream failure.
  The gateway sends `ReportFirstToken` with the status 500.

### Metrics

`kinference_picks_total`, `kinference_pick_duration_ms`,
`kinference_tokenize_total` and `kinference_tokenize_duration_ms`
gain the label `api`, with the values `chat_completions`, `messages`
and `messages_count_tokens`.

### Invariants

- The picker protocol gains one enum field with a zero default, and
  comments. A gateway on the previous protocol keeps working.
- `grpc` holds no logic. The router converts the enum beside its
  other proto conversions, and `grpc` maps the error to a status.
- The picker stays pure. The API call is an input.
- `resolve` stays pure, so `kinfctl` resolves an Anthropic provider
  as the router does.
- Only `commit` writes state. A local token-count pick writes none.
- Import direction: `kinference-policy` defines the API call type and
  the provider check, and the router imports both.

### Out of scope

- In-process tokenization of a Messages body.
- Translation between formats.
- A render URL on `InferenceWorkerSet`.
- The gateway's own changes and the Messages cases of the e2e suite.
  Both land in their own repositories. No client reaches a Messages
  pick before the gateway change lands. The e2e cases land after the
  two routing pull requests, because the `e2e` check runs the suite
  on every pull request. The last pull request of the Break-down
  waits for the e2e cases.

## Alternatives considered

**Detect the format from the body.** A minimal body is valid in both
schemas, so detection fails for the simplest request.

**Send the client's path to Kinference.** The rule for the path
would then live in Kinference, away from the routes that it depends
on. The gateway owns the routes and can later take the value from a
route's config without a protocol change.

**Start the enum with `UNSPECIFIED = 0`.** A gateway on the previous
protocol sends 0, so Kinference would have to read 0 as a chat
completion anyway.

**Convert the Messages body in process.** vLLM's converter changed in
at least seven releases between v0.18.0 and v0.34.0, and depends on
probes of the worker's chat template. llm-d-router shipped such a
converter and replaced it two months later.

**Turn `render_url` into an object with one URL per route.** Each new
render route would add a key. A render service serves every route on
one host, so one base URL holds the same information.

**Try the workers when `render_url` has another path.** A worker set
with `render_url` never falls back to its workers today. A Messages
pick keeps that rule.

## Prior art

Sources read on 2026-11-17.

**vLLM**, `vllm-project/vllm` at `61dc78b`, and pull requests 45803
and 55176. `/v1/messages/render` ships since v0.29.0 and returns the
object that the chat render route returns. From v0.30.0,
`--enable-scale-out` gates every render route. Render and
`/v1/messages` disagree for `thinking: disabled` before v0.33.0. The
converter drops the billing block that Claude Code changes per
session, so the block never reaches the token ids. `count_tokens`
never calls the engine. vLLM reports the cache fields only with
`--enable-prompt-tokens-details`.

**llm-d-router**, `llm-d/llm-d-router` at `8837883`, the tokenizer
README and the decision record on native render endpoints.
llm-d-router posts the raw Messages body to `/v1/messages/render` and
rejects translation in the router. llm-d-router takes the format from
the `:path` that ext_proc delivers. Kinference's picker gets no path,
so the format arrives as a field. llm-d-router counts a token-count
request as load. Kinference doesn't, because the request never
reaches the model.

**NVIDIA Dynamo**, `lib/llm/src/http/service/anthropic.rs` at
`772e7c3`. Dynamo translates a Messages request to a chat completion
in its frontend and answers `count_tokens` itself. Kinference is not
in the data path.

**AIBrix**, `pkg/plugins/gateway/util.go` at `81840c6`. AIBrix hashes
the plain text of a Messages body. A text hash can't match the KV
events that Kinference indexes.

**agentgateway**, `crates/protos/proto/resource.proto` at `bdb36de`.
Its route types name the same three calls, and it takes the first
token from `content_block_delta`. agentgateway sends no route type to
an endpoint picker.

**Envoy AI Gateway**, `api/v1alpha1/ai_service_backend.go` and
`internal/metrics/genai.go` at `f99b015`. Each backend declares one
schema, and the metrics separate `chat`, `messages` and
`count_tokens`. Envoy AI Gateway has no KV-aware routing.

**OpenTelemetry GenAI conventions**, `docs/gen-ai/anthropic.md` at
`e6658f1`. The page requires the sum of the three input fields. The
conventions have no operation name for Messages, so Kinference uses
its own label.

**Anthropic API reference**, the streaming, prompt-caching and
token-counting pages. The pages confirm the sum and the `error` event
inside a 200 response. Some content blocks arrive with no
`content_block_delta`.

**Buoyant gateway**, `crates/gateway-extension-impls/src/kinference.rs`
at `ba46405`. The selector holds the request head when it builds
`PickRequest`, so the selector can read the path. Its usage scan
needs an SSE event scanner for a Messages response.

## Operator-visible changes

- Proto: `PickRequest.api_call`, and comments on the report messages.
- Config: `render_base_url` on a worker set.
- An `InferenceProvider` with `api: Anthropic` resolves. The CRD
  schema doesn't change.
- The explain request of the admin policy API takes the API call.
- The `api` label on the four metric families.
- A worker needs vLLM v0.29.0 or later and, from v0.30.0,
  `--enable-scale-out`. Without the render route, a Messages pick
  routes on load alone. Before v0.33.0, a request with
  `thinking: disabled` gets wrong token ids and no error.
  `cached_tokens` needs `--enable-prompt-tokens-details`.
- A pool with the `local` tokenize mode makes a render call for each
  Messages pick.

## Compatibility and rollout

An existing config file loads unchanged. A gateway on the previous
protocol sends no `api_call`, and Kinference reads its requests as
chat completions. An `InferenceProvider` with `api: Anthropic` moves
from unresolved to resolved and receives only Messages requests.

The operator upgrades the workers, then every Kinference replica,
then the gateway, and adds the route for the Messages paths last. An
older Kinference ignores `api_call` and reads a Messages body as a
chat completion.

A rollback reverses the order. An older Kinference rejects a config
file with `render_base_url`, so the operator removes the field first.
An older Kinference also returns each Anthropic provider to
unresolved.

## Break-down

- Add `render_base_url` to a worker set. Done: a worker set with
  `render_base_url` renders a chat-completion body at
  `<base>/v1/chat/completions/render`, and the config check rejects a
  worker set with both fields.
- Add `api_call` to `PickRequest`, with the report mapping and the
  `api` metric label. Done: the proto states the source of each
  report field in a Messages response. A pick with a Messages value
  or an unknown value returns `INVALID_ARGUMENT`. The four metric
  families show `api="chat_completions"`.
- Resolve Anthropic providers and check providers against the API
  call. Done: an `InferenceProvider` with `api: Anthropic` shows as
  resolved, a chat-completion pick never returns that provider, and
  the explain request accepts an API call.
- Route Messages requests. Done: a pick with `MESSAGES` returns a
  worker with cached-prefix overlap or an Anthropic provider, in a
  pool of any tokenize mode. The pulled body has no `stream_options`.
  A failed render returns `DEGRADED`. A worker set whose `render_url`
  ends in `/v1/chat/completions/render` renders at the derived URL. A
  pick with
  `MESSAGES_COUNT_TOKENS` still returns `INVALID_ARGUMENT`.
- Route token-count requests on load alone. Done: a pick with
  `MESSAGES_COUNT_TOKENS` returns `OK`, makes no render call and
  leaves the in-flight gauges unchanged. A provider pick has an empty
  `path`.
- Document Messages support. Done: the e2e suite has a case for a
  Messages pick and a case for a token-count pick, and the `e2e`
  check on the pull request runs both. The README no longer lists
  `/v1/messages` as missing, `docs/request-flow.md` describes the
  three API calls, and `tests/e2e/smoke.sh` checks
  `/v1/messages/render` on the dummy vLLM.

## Open questions

Does a failure after the first token need a report field? An `error`
event can arrive late, and `ReportFinalTokenRequest` has no field for
a failure. The maintainers decide in the review of this design.

Does every `content_block_delta` count as a token? A
`signature_delta` and an empty delta hold no generated text. The
first token of a chat completion also stays the first body chunk, so
the observed TTFT of the two formats differs in meaning. The
maintainers decide in the review of this design.

Should Kinference stop calling a render route that answers 403 or
404? A worker set without the route costs up to three failed calls
for each Messages pick. llm-d-router probes the route once. The
maintainers decide in the review of the pull request that routes
Messages requests.

Should `kinfctl check` warn about `spec.path` on an Anthropic
provider? The empty path for a token-count pick breaks when the
operator uses `spec.path` for a prefix. The maintainers decide in the
review of this design.
