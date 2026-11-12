# Prior art

The projects whose design decisions bear on Kinference's own: the
engines it reads, the proxies it serves, the routers that have faced
some of the same questions, and the libraries it builds on. None of
them solves Kinference's problem, and each one made choices that
Kinference has to interoperate with, learn from or reject. Read the
ones that bear on a change before a design or an issue settles a
choice, and cite the file or document with its date. Add a project
here when a design cites it for the first time, and keep each entry to
what it is and where its design decisions are written down.

## The gateway

- Buoyant gateway, `BuoyantIO/gateway`. The proxy that calls
  Kinference's `Picker` over gRPC and resolves the `BackendBinding`
  that the controller writes. Its `design/` directory holds the
  extension and binding designs, and
  `documents/kinference-manual-verification.md` the full request path.

## Inference routing

- llm-d, `llm-d/llm-d` and `llm-d/llm-d-router`. The Kubernetes
  inference scheduler whose router is the external endpoint picker for
  the Gateway API Inference Extension. Designs live in `proposals/` in
  the first repo; the router's scoring, KV indexer and flow control in
  the second. Bears on scoring, the KV index and the block hash.
- Gateway API Inference Extension, `kubernetes-sigs/gateway-api-inference-extension`.
  The `InferencePool` API and the ext_proc protocol between a proxy and
  an endpoint picker. Designs in `docs/proposals/`. Bears on the
  picker contract and on the CRDs.
- NVIDIA Dynamo, `ai-dynamo/dynamo` and `ai-dynamo/enhancements`. The
  KV-aware router whose `dynamo-kv-router` crate Kinference depends on
  for the radix tree. Designs in the enhancements repo under `deps/`.
  Bears on the index and on KV event ingest.
- AIBrix, `vllm-project/aibrix`. A control plane for vLLM on
  Kubernetes with its own gateway plugins and routing. Designs in
  `docs/source/designs/`. Bears on the controller and the CRDs.

## Engines

- vLLM, `vllm-project/vllm`. The engine that publishes the KV events
  and the metrics that the fleet ingests and scrapes. Changes arrive as
  RFC issues. Bears on `scrape`, `ingest` and the tokenize modes.
- SGLang, `sgl-project/sglang`. The second engine kind on the roadmap,
  with its own KV event format. Bears on `ingest`.

## Proxies and the Gateway API

- Envoy, `envoyproxy/envoy`. The ext_proc filter is the protocol that
  every other endpoint picker speaks. Its docs under `docs/root/` and
  the api protos describe the body modes and the per-stage metadata.
  Bears on the picker contract and the body store.
- agentgateway, `agentgateway/agentgateway`. A Rust proxy that
  implements the client side of the inference extension. Designs in
  `design/`. Bears on the picker contract.
- kgateway, `kgateway-dev/kgateway`. An Envoy-based gateway with the
  inference extension. Designs in `design/`. Bears on the
  `BackendBinding` and the route attachment.
- Gateway API, `kubernetes-sigs/gateway-api`. The route and backend
  model that the gateway implements. GEPs in `geps/`. Bears on the
  CRDs and on how a route names a pool.

## Kubernetes in Rust

- kube-rs, `kube-rs/kube`. The client, the runtime and the controller
  patterns that `kinference-controller` uses. Bears on the watch, the
  reconcile and leader election.
