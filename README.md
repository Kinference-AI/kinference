# Kinference

🎈 Welcome to Kinference! 👋

Kinference is an inference _control plane_ for Kubernetes and vLLM.

At its core, Kinference selects among inference endpoints, applying policies,
routing, and load balancing as appropriate. It exposes a gRPC interface that
takes an inference request (e.g. in OpenAI chat-completions or Anthropic
Messages format) and returns the endpoint for that request, as well as the
appropriate-mutated body.

To do this, Kinference monitors vLLM KV-cache events and engine metrics.
Kinference currently uses NVIDIA's
[dynamo-kv-router](https://github.com/ai-dynamo/dynamo) crate to handle cache
events.

Kinference also monitors and sets the status on a set of Kubernetes CRDs that
configure its behavior.

Kinference is designed to be paired with a proxy. For example, it pairs with the
[Buoyant Gateway](https://github.com/BuoyantIO/gateway/).

## Quick start

See [the Kinference getting started
guide](https://kinference.ai/docs/getting-started/).

## Community

Please use [GitHub
discussions](https://github.com/Kinference-AI/kinference/discussions)!

## Documentation

- [Architecture](ARCHITECTURE.md): Overall project architecture.
- [Request flow](docs/request-flow.md): How requests flow through the system as
  a whole.
- [Local development](docs/local-development.md): How to build, install, and run
  locally.
- [Contributing](CONTRIBUTING.md): How to contribute the project.

## CRDs

Kinference primarily uses Kubernetes Custom Resource Definitions (CRDs) for
configuration. The CRD schemas are in
[charts/kinference-crds](charts/kinference-crds/templates). See [the Kinference
CRD reference docs](https://kinference.ai/docs/reference/crds/) for documentation.

## kinfctl

This repo includes a `kinfctl` CLI tool. This CLI is designed to make it easy
for you (or your agents) to validate policies before sending them to the server.
See [the Kinference kinfctl reference](https://kinference.ai/docs/reference/kinfctl/)
for more.

To build the CLI from source, run `just install-cli`.

## Known limitations

Kinference is alpha software. It has not seen sustained production exposure. Run
at your own risk.

The current Kinference implementation lacks several features that are in the
full scope of the project, including:

* Rate limiting
* Semantic classifiers
* Support for multiple replicas

These will be addressed shortly.

## Copyright and license

Kinference is copyright 2026 Buoyant, Inc.

Kinference is licensed under the Apache License, Version 2.0 (the "License");
you may not use these files except in compliance with the License. You may
obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 .

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
