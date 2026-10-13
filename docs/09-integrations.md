# Integrations and resource ownership

Integrations execute through SDK capabilities and the current context. Their
resources retain event, activation and execution-domain authority. Design
timeouts, cancellation, payload bounds and external consistency at each edge.

![Integration boundaries](diagrams/architecture.svg)

| Surface | Builder guidance | Source / example |
| --- | --- | --- |
| HTTP and bodies | Use owned request/response APIs, explicit deadlines and bounded parsing | [HTTP](../crates/cellar/application/http.rs), [services](../crates/cellar/application/service.rs) |
| Byte streams | Prefer streaming for large payloads; cancel abandoned streams; bound buffered bytes | [streams](../crates/cellar/application/streams.rs) |
| Blob | Use streaming/ranges, conditional options and bounded paginated listings | [Blob](../crates/cellar/application/blob.rs) |
| Assets and cache | Stage assets with the package; define cache freshness and rebuild behavior | [assets/cache](../crates/cellar/application/assets.rs), [counter](../examples/native-counter-project.json) |
| HTML rewriting | Transform streaming markup within the event lifetime | [rewriter](../crates/cellar/application/html_rewriter.rs) |
| WebSockets | Authenticate upgrades; define reconnect/replay and bounded fanout | [WebSocket example](../examples/native_websocket.rs) |
| TCP/TLS | Select transport explicitly; own readers/writers, close and handle cancellation | [sockets](../crates/cellar/application/sockets.rs) |
| Containers | A cell owns the lifecycle; wait for port readiness and budget resource use | [container example](../examples/native-container/README.md) |
| Facets | Use cell-owned facet lifecycle and properties; test parent cancellation | [facets](../crates/cellar/application/facets.rs) |
| Compiled loader | Load registered native application objects with host quotas | [loader](../crates/cellar/application/loader.rs) |

## WebSockets and containers

The SDK includes socket pairs, ordinary and hibernating acceptance, text/binary
sends, attachments, tags and cell socket enumeration. Handle open/message/close
events explicitly. Preserve session facts that must survive reactivation in
durable state or the supported attachment mechanism. A socket is not a global
durable message log; reconnecting clients need an application sequence/catch-up
protocol. Test disconnects during writes and recovery during active sessions.

Cell-owned containers expose start, monitor, destroy, signal, inactivity policy,
ports, execution and forwarding. The container adapter needs an explicitly
managed Docker/Podman environment. The host's native executable, container image,
container filesystem and cell SQLite have different durability boundaries.
Do not assume a writable container layer is replicated by Cellar. Define
storage mounts, credentials, image promotion and process failure behavior.

## Blob and database consistency

Blob objects and SQLite mutations do not form one transaction merely because
both APIs are available in one handler. A proposed upload flow writes an object
under a unique immutable key, verifies it, commits the reference in cell state,
then reconciles abandoned objects. Deletes require a similar reference/retention
policy. Bound multipart work, abort incomplete uploads, and handle conditional
read/write outcomes. Large KV values may use Blob-backed storage; validate the
combined failure and retention behavior for your workload.

## Outbound behavior

Use the framework path for egress tied to a state mutation so it retains gate
and event authority. Raw network operations in application code can bypass
those lifecycle protections. External destinations still need authentication,
authorization and idempotency. Restrict outbound targets for user-supplied URLs
at the host/network boundary and validate redirects and resolved addresses.

Test integration cancellation, slow consumers, partial bodies, malformed input,
deadline expiry, stale handles, limits and dependency failure. SDK coverage is
listed in [API map](22-api-reference.md); production acceptance belongs to
[testing and qualification](19-testing-qualification.md).
