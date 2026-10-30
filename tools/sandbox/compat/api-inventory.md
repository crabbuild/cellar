<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Pinned API requirements and implementation gaps

The byte identities in [manifest.json](manifest.json) define this inventory:
celld commit `8702f6d186c77698dbfb30cf29e3b1922a88f008`, Containers 0.3.7,
Sandbox 0.12.9 and its paired `linux/amd64` image digest. This is a source audit;
none of the rows is a runtime compatibility pass. Requalify changed inputs.

## Runtime and authority requirements

| Published code requires | Native building block | Compatibility work still required |
| --- | --- | --- |
| `cloudflare:workers` `DurableObject`, `WorkerEntrypoint`, `RpcTarget`, `tracing` | Optional JS adapters; activation-owned objects | Versioned module exports, RPC target semantics and explicit tracing behavior |
| Standard `Request`, `Response`, `Headers`, `URL`, streams, timers, abort signals and cryptography | Native HTTP/stream/WS bridge; Node web globals | Workers-specific semantics, bounds and cancellation; embedded V8 web globals |
| Sandbox `node:path/posix` import | Node builtins | Explicit optional module profile for bundled execution |
| `ctx.id`, namespace `idFromName`, `get`, class exports and direct stub method calls | Native class/name identity and namespace fetch | Opaque object IDs, correct binding admission and bounded DO RPC dispatch |
| `ctx.blockConcurrencyWhile` from constructors | Native activation ownership and event admission | Run initialization before events; reject failed initialization; avoid borrowing later invocation authority |
| `ctx.storage.sql.exec` iterable synchronous rows | Bounded invocation-owned synchronous SQL bridge | Cloudflare cursor values/metadata, parameter conversion and transaction behavior |
| `ctx.storage.kv.get` / `put` and async storage `get` / `put` / `delete` | Native KV snapshots and atomic batches | Correct missing values, supported serialization, multi-key operations and API-level limits |
| Storage `transaction`, alarms, `sync` | Native synchronous checked batches, alarms and durability proof | Callback transaction semantics, rollback/conflict behavior, alarm delivery and published package scheduling |
| Stable `ctx.container.running` property and synchronous queued `start` | Authoritative synchronous snapshot; bounded one-shot admission and native run identity | Root-event and concurrency-block controls execute in Node/V8; blocks retain native admission through queued completion and refuse inherited transactions/late continuations. Node hook/error/nonzero-exit slices pass; complete timeout, cancellation and recovery qualification remains required |
| Container `getTcpPort`, HTTP/WS forwarding and readiness | Native port, TCP and WS capabilities; incremental Node port responses, container/local-pair upgrades, Worker/DO forwarding, bounded upload and abortable native fetch | Embedded web globals, complete transfer/upgrade/fault qualification, port switching and complete timeout behavior |
| Background monitor/lifetime work | Native cancellation, ownership, inactivity and cleanup | Workers `waitUntil`/event lifetime behavior without detached native SDK work |
| Outbound interception and `ctx.exports.ContainerProxy` | Native internet policy | Interception/proxy implementation or explicit profile refusal; no silent policy bypass |
| Sandbox Cap'n Web `RpcSession`, `RpcTarget` and HTTP transport | Actual RPC session and gated binary SDK upload over native WS/local pairs | Complete target lifetime, stream errors, reconnect/disconnect and failure qualification |

Synchronous native calls are private runtime plumbing. They do not supply the
Workers module exports, storage facade, SQL cursor or constructor admission.
All compatibility effects must still cross the original native turn and its
ownership, epoch, operation/byte budgets and durability/output gates.

## Unchanged upstream application paths

| Fixture path | Actual package behavior to exercise | Current execution evidence |
| --- | --- | --- |
| Container `/container/<name>` and `/singleton` | `getContainer`, class construction, `onStart` / `onStop` / `onError`, port 8080 fetch | Node native module test passes against an engine protocol peer; image execution remains pending |
| Container `/lb` | `getRandom` namespace routing to three named objects | Node native module test passes against an engine protocol peer; image execution remains pending |
| Container `sleepAfter = "2m"` | SQL schedule table, activity renewal, alarm delivery, stop and wake | Constructor/scheduling and explicit start/stop peer tests pass; two-minute sleep/wake qualification remains pending |
| Sandbox `/exec` | `getSandbox`, direct RPC, control-server startup, shell command result | Pinned SDK/image passes stdout, stderr and nonzero child exit assertions |
| Sandbox `/write` | Real SDK `writeFile` / `readFile` and image filesystem | Exact contents survive a later request, including the trailing newline |
| Sandbox `/serve` | Real `startProcess` with a persistent Node HTTP process | Pinned SDK/image starts the process and returns its function-bearing object through native RPC |
| Sandbox `/processes` | Real `listProcesses`, durable process/runtime identity and response conversion | A later request discovers the same live process; restart/fencing qualification remains pending |

An additional probe subclasses the published Sandbox class and consumes its real
`execStream()` through the pinned image. A file handshake releases the command
only after stdout is observed, then verifies final stdout/stderr and exit code 7.
The test also forwards the SDK SSE body through DO and Worker fetches to an
external HTTP client, which must receive stdout before an independent request
releases the command. An additional route calls the unchanged `stub.execStream()`
method directly: its returned readable byte stream crosses native RPC before
becoming the Worker response. It passes the same handshake, final output and
exit-code checks. Generic returned writable streams also run in a separate native
fixture. Generic readable/writable RPC inputs now run in a native-host fixture,
including returned-function arguments and cross-DO forwarding; this does not
alone qualify the SDK's separate Cap'n Web transport. A dedicated RPC-image test
now runs actual SDK `writeFile` with a 2 MiB + 17 byte readable input, observing
the server's atomic temporary-file prefix before release and verifying exact
final bytes through an independent SDK execution session. The same scenario
round-trips those bytes through the unchanged binary `readFile` method and its
returned content stream. A separate native fixture covers returned HTTP response
metadata, streaming, cancellation and cleanup. Node Request results and HTTP-message
arguments have their own native fixture; complete reconnect/fault semantics remain
unqualified.

Selected HTTP/WebSocket preview routing now passes the pinned image scenario.
The complete baseline still needs HTTP/RPC failure, lifetime, ownership and
teardown qualification required by SP-04. Tunnels,
bucket mounts, backup/snapshot helpers and outbound credentials require separate
routing/storage/credential policy. They cannot inherit a pass from basic exec.

Sandbox 1.x, Dynamic Workers, hosted Cloudflare execution and Vercel/E2B/Daytona/
Modal are separate profiles or providers. This inventory makes no claim that
those SDKs execute unchanged or that all provider capabilities are equivalent.
See the [implementation ledger](../../../docs/design/sandbox-provider-progress.md).
