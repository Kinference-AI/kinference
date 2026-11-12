# Local development

How to run Kinference on a kind cluster on your own machine, point it at
a real vLLM worker, and watch Kinference discover the worker, ingest the
KV-cache events that the worker publishes, and write status. CI runs the
same loop, through the recipes in the [justfile](../justfile).

## What you need

`docker`, `kind`, `kubectl`, `helm` and `just`. Docker builds the image,
so you need the Rust toolchain only for `just lint` and `just test-unit`;
on the host those two also need `g++`.

The first `just image` compiles the `dynamo-kv-router` git dependency and
takes a while; BuildKit cache mounts keep later builds incremental. The
worker below pulls a CPU vLLM image of about 600 MB and requests 500m CPU
and 1 GiB of memory.

## Bring it up

```sh
just up
```

The recipe builds `kinference:latest`, creates the kind cluster
`kinference-smoke`, loads the image into the cluster, installs
`charts/kinference-crds` and `charts/kinference` from source into the
`default` namespace, and waits until the Deployment is Available. kind
makes `kind-kinference-smoke` the current kubectl context.

The recipe installs the chart with `extension.enabled` off, so you need
no gateway. Without a gateway, nothing sends requests through Kinference;
the section "See it work" lists what you can see anyway, and the section
"A full request path" says how to put a gateway in front.

## Add a worker

```sh
just test-smoke
```

The recipe applies the manifests in
[tests/e2e/manifests](../tests/e2e/manifests) and runs
[tests/e2e/smoke.sh](../tests/e2e/smoke.sh). The manifests are:

- `dummy-vllm.yaml`: vLLM on CPU with `--load-format dummy`, serving
  `HuggingFaceTB/SmolLM2-135M-Instruct` with random weights. The responses
  are garbage, but `/metrics`, `/v1/models` and the KV events on port 5557
  are real. The CPU backend uses 128-token blocks.
- `workerset.yaml`: the `dummy` InferenceWorkerSet. It points at the
  `dummy-vllm` Service, sets `kvBlockSize: 128` and declares the alias
  `dummy`.
- `pool.yaml`: the `smoke` InferencePool, with that worker set as its one
  destination.

The script then waits for the worker, port-forwards Kinference and the
worker, checks that `/readyz` reports a ready endpoint, sends one long
completion so that vLLM stores full blocks and publishes events, waits
until `kinference_index_blocks` rises above zero, and checks that the
worker set is `Accepted`. Every wait is bounded; when a step fails, the
script says which step timed out.

You can also apply the manifests yourself and do the rest by hand:

```sh
kubectl apply -f tests/e2e/manifests
kubectl rollout status deployment/dummy-vllm --timeout=420s
```

## See it work

Port-forward both admin ports:

```sh
kubectl port-forward svc/kinference 9001:admin &
kubectl port-forward svc/dummy-vllm 8000:http &
```

`/readyz` reports the fleet:

```sh
curl -s localhost:9001/readyz
```

```
ready
worker sets: 1, with ready endpoints: 1
```

The log shows discovery and each worker that Kinference adds. At
`debug` it also shows the first scrape, and at `trace` every KV event:

```sh
kubectl logs deployment/kinference -f
```

Send a prompt that is longer than one block, then watch the index fill.
A short prompt publishes nothing, because vLLM stores only full blocks,
and a prompt that the engine has already seen publishes nothing either,
because its blocks are cached already. The smoke test has sent one
prompt, so use different text:

```sh
PROMPT=$(printf 'Every good boy deserves fudge and the cat sat on the mat by the fire tonight. %.0s' {1..60})
curl -s localhost:8000/v1/chat/completions -H 'content-type: application/json' \
  -d "{\"model\":\"HuggingFaceTB/SmolLM2-135M-Instruct\",\"max_tokens\":8,\"messages\":[{\"role\":\"user\",\"content\":\"$PROMPT\"}]}" > /dev/null
curl -s localhost:9001/metrics | grep -E '^kinference_(workers|index_blocks|index_events_total)[ {]'
```

Kinference refreshes `kinference_index_blocks` from a throttled dump of
the radix tree, so allow about ten seconds.

The custom resources and the read-only policy API show the controller's
view:

```sh
kubectl get inferenceworkersets,inferencepools
kubectl get inferenceworkersets dummy -o yaml
curl -s localhost:9001/api/policy/context | jq
```

`context` lists every pool with its destinations and its live candidates,
which are the inputs that an `InferencePolicy` is written against.
`kinfctl check` reads the same resources from the cluster to check a
policy draft; `kinfctl check --help` shows the invocation.

## Iterate

A code change means a new image, which you load into the cluster, and a
restart. The chart pulls `IfNotPresent` on a fixed tag, so loading the
same tag again replaces the image:

```sh
just image
kind load docker-image kinference:latest --name kinference-smoke
kubectl rollout restart deployment/kinference
```

To change the config, upgrade the chart. The config is the `config` block
of [charts/kinference/values.yaml](../charts/kinference/values.yaml),
which the chart renders to JSON. A checksum annotation on the pod restarts
the pod when the rendered config changes:

```sh
helm upgrade kinference charts/kinference --reuse-values \
  --set config.metrics_poll_ms=500
```

The same command sets the chart's other knobs: `logLevel` sets `RUST_LOG`
(`kinference=trace,dynamo_kv_router=trace` for KV-event debugging),
`logFormat` sets `KINFERENCE_LOG_FORMAT` to `text` or `json`, and
`enableReplay` makes Kinference fetch the event history on attach.

The unit tests and the lints run on the host, outside the cluster:

```sh
just test-unit
just lint
```

## Read the observations

For each streaming request, Kinference logs one event with the predicted
and observed time to first token. The event goes to the
`kinference::observation` target at debug level, so it is off by default.
Each event names its worker set and profile, and it includes the profile's
predictor and picker settings. These events are the input for refitting a
profile's predictor coefficients. Turn them on with JSON logs, because the
text format does not quote its fields reliably. Helm splits `--set` values
on commas, so the commas inside `logLevel` need a backslash:

```sh
helm upgrade kinference charts/kinference --reuse-values \
  --set logFormat=json \
  --set logLevel='kinference=info\,kinference::observation=debug\,dynamo_kv_router::indexer::radix_tree=error\,dynamo_kv_router=warn'
```

The events go wherever the cluster's log pipeline sends the pod's output.
A pipeline may sample, rate-limit or drop lines, so the events are a
sample of the traffic rather than a complete record. To read the events
from the current pod and write them to a file:

```sh
kubectl logs deploy/kinference \
  | jq -cR 'fromjson? | select(.target == "kinference::observation")' \
  > observations.jsonl
```

`observations::log_observation` in the router lists every field. Every
field is a number, a boolean or a string. `inflight_overlap_blocks` is a
list with one entry for each request in flight on the chosen worker. Each
entry counts the blocks that the new request shares with that in-flight
request. A log field can't hold a list. The event therefore records
the list as a JSON array inside a string, such as `"[5,3]"`, and
`inflight_dispatches` records the length of the list as a number. To
read the list as an array:

```sh
jq -c '.inflight_overlap_blocks | fromjson' observations.jsonl
```

## When it does not

- `dummy-vllm` stays Pending: the node has less than the 500m CPU and
  1 GiB that the worker requests. kind's own pods reserve most of a
  two-core machine.
- `/readyz` says `with ready endpoints: 0`: the worker is not Ready, or
  the Service's port names are not the names that the worker set
  declares. `kubectl get endpointslices -l
  kubernetes.io/service-name=dummy-vllm` shows what discovery sees.
- `/readyz` returns 503: an enabled worker-set or policy watch has not
  completed its initial list. Check the CRDs chart and list/watch RBAC.
- Startup reports `invalid kubernetes.selector`: fix the label selector
  in the config before restarting the deployment.
- `kinference_index_blocks` stays 0: the prompt was shorter than a block;
  or `kvBlockSize` disagrees with the block size that the engine
  publishes, which Kinference logs as a block size mismatch and the
  engine logs as `Block not published. Block size must be N tokens`; or
  someone set `KINFERENCE_DISABLE_INGEST`.

## A full request path

To send real requests, put Kinference behind the gateway as the
`Picker` extension. The gateway repo's
`documents/kinference-manual-verification.md` walks through it: that
repo's `just up` brings up a kind cluster with the gateway; you load this
repo's image into that cluster, install the chart with
`extension.enabled=true`, apply the same worker set and pool as above,
name the pool in an `HTTPRoute`, and `curl` the gateway. The request
lands on the worker through a pick.

## Tear down

```sh
just down
```
