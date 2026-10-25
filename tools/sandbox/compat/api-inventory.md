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
| Stable `ctx.container.running` property and synchronous queued `start` | Native async lifecycle and observed run identity | Queue/admission timing, event-owned authority, monitor callbacks and stopped state recovery |
| Container `getTcpPort`, HTTP/WS forwarding and readiness | Native port, TCP and WS capabilities | Fetch/port switching, stream/upgrade conversion and exact package timeout/abort behavior |
| Background monitor/lifetime work | Native cancellation, ownership, inactivity and cleanup | Workers `waitUntil`/event lifetime behavior without detached native SDK work |
| Outbound interception and `ctx.exports.ContainerProxy` | Native internet policy | Interception/proxy implementation or explicit profile refusal; no silent policy bypass |
| Sandbox Cap'n Web `RpcSession`, `RpcTarget` and HTTP transport | Native streaming and WS transport | Actual package RPC sessions, target lifetime, stream errors and cleanup |

Synchronous native calls are private runtime plumbing. They do not supply the
Workers module exports, storage facade, SQL cursor or constructor admission.
All compatibility effects must still cross the original native turn and its
ownership, epoch, operation/byte budgets and durability/output gates.

## Unchanged upstream application paths

| Fixture path | Actual package behavior to exercise | Current execution evidence |
| --- | --- | --- |
| Container `/container/<name>` and `/singleton` | `getContainer`, class construction, `onStart` / `onStop` / `onError`, port 8080 fetch | Inputs bundle; not executed |
| Container `/lb` | `getRandom` namespace routing to three named objects | Inputs bundle; not executed |
| Container `sleepAfter = "2m"` | SQL schedule table, activity renewal, alarm delivery, stop and wake | Constructor SQL requirement audited; no package lifecycle test yet |
| Sandbox `/exec` | `getSandbox`, direct RPC, control-server startup, shell command result | Inputs bundle; no package/image command run yet |
| Sandbox `/write` | Real SDK `writeFile` / `readFile` and image filesystem | Inputs bundle; no package/image file round trip yet |
| Sandbox `/serve` | Real `startProcess` with a persistent Node HTTP process | Inputs bundle; no background process lifetime qualification yet |
| Sandbox `/processes` | Real `listProcesses`, durable process/runtime identity and response conversion | Inputs bundle; no later-event/recovery qualification yet |

The baseline additionally needs streamed command output, preview routing, HTTP
and RPC failure paths, shutdown and teardown tests required by SP-04. Tunnels,
bucket mounts, backup/snapshot helpers and outbound credentials require separate
routing/storage/credential policy. They cannot inherit a pass from basic exec.

Sandbox 1.x, Dynamic Workers, hosted Cloudflare execution and Vercel/E2B/Daytona/
Modal are separate profiles or providers. This inventory makes no claim that
those SDKs execute unchanged or that all provider capabilities are equivalent.
See the [implementation ledger](../../../plans/sandbox-provider-progress.md).
