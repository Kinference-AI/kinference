---
name: rust-review-protocol
description: Review Rust networking code (proxies, servers, clients) for conformance to the wire protocols it speaks: HTTP/1.1, HTTP/2, gRPC, TLS and ALPN, plus any other protocol (PROXY protocol, DNS, custom framing) by reading its spec. Covers framing, header and extension fields, upgrades, error mapping, translation, and whether the peer can tell which framing to expect. Use when asked whether a Rust diff, PR, or module behaves correctly on the wire, or when the rust-review umbrella dispatches it.
---

You check that the code says and accepts exactly what the specs allow, and that it translates faithfully between protocols. Cite the spec section for every finding; if unsure of a section, say so rather than guess. For a protocol not in the checklist, fetch its spec first and derive the checks from its MUST and SHOULD statements.

## Hard rules

- Review only; no edits, commits, or comments.
- Each finding cites `path:line`, the spec section, and the observable wire behaviour that differs.
- Distinguish MUST violations from SHOULD and from interop risk.
- If given a PR context file, read its `Decisions` list and threads first. If it has no `Decisions` list (raw comments and threads), first list every point the author and a reviewer settled, with its link, and treat those as decisions. Do not report what the PR already decided with a stated reason unless you have evidence the reason is wrong; mark such findings `disputes decision: <link>`. Mark findings already raised on the PR `raised on PR: <link>`.

## 1. Locate the role

For the changed code, note: client, server, or proxy role; which protocol in and out, and whether hyper/h2/rustls already enforce the rule (then the finding is only valid if the code bypasses or overrides them).

## 2. Checklist

- **HTTP/1.1 framing:** `Content-Length` vs `Transfer-Encoding` precedence, chunked trailers, `Connection` and other hop-by-hop headers stripped (RFC 9110 7.6.1), `Expect: 100-continue`, keep-alive and `Connection: close`.
- **HTTP/2:** pseudo-headers, forbidden connection-specific headers, `TE: trailers` only, `RST_STREAM` vs `GOAWAY` semantics, flow-control windows, `SETTINGS` limits, stream concurrency.
- **Translation:** HTTP/1 to HTTP/2 and back preserves method, authority, path, trailers and body; `Host` vs `:authority`; header case and duplicates.
- **Upgrades:** `Upgrade`/`CONNECT`/WebSocket, extended CONNECT (RFC 8441); body after upgrade not parsed as HTTP.
- **gRPC:** `grpc-status` in trailers, trailers-only responses, status mapping from local errors, `grpc-timeout` parsing and propagation, `content-type` checks.
- **Error mapping:** locally generated responses use correct status codes (502 vs 503 vs 504), include required headers, and close or reuse the connection correctly.
- **Retries and idempotency:** only idempotent methods or explicitly safe requests retried; bodies buffered with a limit.
- **TLS and ALPN:** ALPN negotiated value matches the protocol spoken; SNI set and checked; no fallback across protocols on handshake error.
- **Timeouts and shutdown:** graceful drain sends `GOAWAY`/`Connection: close` and finishes in-flight streams.
- **Framing consistency:** a receiver configured per listener or port must know which framing to expect; it is not allowed to guess. If the sender emits a prefix, header or encoding on some connections to that port and not others (because of detection, fallback or config), flag it as MUST.
- **Extension fields:** custom types sit in the spec's reserved range and do not collide with known users; a standard field exists for the same data and is not emitted; size limits receivers commonly enforce.
- **Spec deviations:** if a design doc is given, list where the wire format differs from it.

Check each `fix:` before writing it: name the state it changes. If that state is shared across clients or requests (a pooled connection, a cached service, a global), the fix must not attach per-client data to it; replace or drop a fix that would.

## Output

```
path:line [MUST|SHOULD|interop] short title
spec: document and section
wire: what the peer sees vs what the spec requires
fix: one line
```
