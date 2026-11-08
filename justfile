set shell := ["bash", "-eo", "pipefail", "-c"]

IMAGE_REPO := "kinference"
IMAGE_TAG := "latest"
IMAGE := IMAGE_REPO + ":" + IMAGE_TAG
# Baked into both binaries' `--version`.
KINFERENCE_VERSION := IMAGE_TAG

default: list

list:
    @just --list

[group('check')]
check-fmt:
    cargo fmt --all -- --check

[group('check')]
clippy:
    cargo clippy --locked --all-targets -- -D warnings

[group('check')]
lint-chart:
    helm lint charts/kinference-crds charts/kinference
    helm template charts/kinference-crds > /dev/null
    helm template charts/kinference > /dev/null

[group('check')]
lint: check-fmt clippy lint-chart

[group('test')]
test-unit:
    cargo test --locked

[group('build')]
image:
    docker build --build-arg KINFERENCE_VERSION={{KINFERENCE_VERSION}} -t {{IMAGE}} .

# install `kinfctl`, the InferencePolicy checker, into ~/.cargo/bin
[group('build')]
install-cli:
    cargo install --path crates/kinfctl --locked

# `--target` is always passed so the binary lands at the same path even for
# the host target.

# build `kinfctl` for one target into target/cli/kinfctl-<version>-<os>-<arch> (+ .sha256)
[group('build')]
cli-bin target="":
    #!/usr/bin/env bash
    set -euo pipefail
    target='{{target}}'
    target=${target:-$(rustc -vV | sed -n 's/^host: //p')}
    KINFERENCE_VERSION='{{KINFERENCE_VERSION}}' \
        cargo build --release --locked -p kinfctl --target "$target"
    case $target in
      aarch64-apple-darwin)      os_arch=darwin-arm64 ;;
      x86_64-apple-darwin)       os_arch=darwin-amd64 ;;
      x86_64-unknown-linux-gnu)  os_arch=linux-amd64 ;;
      aarch64-unknown-linux-gnu) os_arch=linux-arm64 ;;
      *) echo "cli-bin: no asset name for target $target" >&2; exit 1 ;;
    esac
    mkdir -p target/cli
    asset="kinfctl-{{KINFERENCE_VERSION}}-$os_arch"
    cp "target/$target/release/kinfctl" "target/cli/$asset"
    (cd target/cli && shasum -a 256 "$asset" > "$asset.sha256")
    echo "target/cli/$asset"

CLUSTER := "kinference-smoke"

# build the image, create a kind cluster and install both charts
[group('e2e')]
up: image
    kind create cluster --name {{CLUSTER}}
    kind load docker-image {{IMAGE}} --name {{CLUSTER}}
    helm install kinference-crds charts/kinference-crds --kube-context kind-{{CLUSTER}}
    helm install kinference charts/kinference --kube-context kind-{{CLUSTER}} \
        --set image.repository={{IMAGE_REPO}} --set image.tag={{IMAGE_TAG}}
    kubectl --context kind-{{CLUSTER}} wait --for=condition=available deployment/kinference --timeout=180s

# deploy a CPU dummy-weights vLLM and check discovery, KV ingest and CRD status
[group('e2e')]
test-smoke:
    kubectl --context kind-{{CLUSTER}} apply -f tests/e2e/manifests
    KIND_CONTEXT=kind-{{CLUSTER}} tests/e2e/smoke.sh

[group('e2e')]
down:
    kind delete cluster --name {{CLUSTER}}
