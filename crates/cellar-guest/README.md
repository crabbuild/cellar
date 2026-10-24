<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# cellar-guest: shared SDK operations for language adapters

`cellar-guest` translates SDK calls from Python and JavaScript applications into
native Cellar operations. It validates inputs, converts buffered HTTP values,
applies checked storage batches, and produces bounded success or failure replies.
Both language adapters use it so their calls follow the same native storage and
failure rules.

Here, **guest** means application code running in a language runtime. This crate
contains the shared Rust operation layer; interpreter startup, transport, and
invocation lifetime belong to the adapters. It embeds no Python or JavaScript
engine.

Application authors should start with the [JavaScript/TypeScript SDK guide](../../packages/cellar-sdk/README.md)
or [Python API guide](../cellar-python/README.md#python-api). This README is for
contributors working on the adapters or their shared protocol.

## Where this crate fits

```text
Application code
  Python SDK facade              JavaScript / TypeScript SDK facade
         |                                    |
         v                                    v
  cellar-python                      cellar-javascript
  CPython + asyncio                  Embedded V8 or Node.js
         |                                    |
         +---------- SDK operation -----------+
                     name + owned values
                              |
                              v
             Original Rust invocation future
             (retains its sdk::Context)
                              |
                  +-----------v------------+
                  |      cellar-guest      |
                  | validate and dispatch  |
                  | convert values/results |
                  +-----------+------------+
                              |
                              v
                      Native Cellar SDK
                storage, cells, KV, SQL, queues
                              |
                  bounded result or failure
                              |
                              +---> adapter ---> awaiting application
```

**Execute guest SDK operations inside the original Rust invocation.** Poll
`operations::execute` and `operations::fetch_bytes` with that invocation's
`sdk::Context`; never move their futures onto a language runtime thread. The
native context carries the execution domain and authority under which Cellar
checks ownership, activation, and durability.

| Layer | Responsibility |
| --- | --- |
| Language SDK facade | Present typed application APIs and encode calls. |
| Python / JavaScript adapter | Own runtime startup and shutdown, invocation identity, cancellation, admission limits, and transport. |
| `cellar-guest` | Validate shared operation inputs, call the native SDK, and encode results with consistent limits and failure classification. |
| Native Cellar SDK and host | Enforce storage transactions, routing, ownership, and required durability proofs. |

The adapters enforce the number and concurrency of submitted operations.
`operations::is_read` supplies the shared classification: known reads may overlap
a pending mutation, while arbitrary SQL and nested cell fetches conservatively
count as mutations. This crate does not schedule those calls or retry them.

## Follow one storage update

The examples below show the **internal protocol**, after the language facade has
encoded a call. Applications normally use `ctx.storage` instead of constructing
these messages directly.

First, `storage.read` receives this payload:

```json
{"key": "count"}
```

If the stored counter is `7`, its success envelope is:

```json
{"ok": true, "value": {"present": true, "value": 7}}
```

Presence is explicit: a missing key has `present: false`, while a key containing
JSON `null` has `present: true` and `value: null`.

To increment that snapshot, submit `storage.apply` with:

```json
{
  "checks": {"count": {"present": true, "value": 7}},
  "puts": {"count": 8}
}
```

```text
storage.apply
     |
     v
Validate batch shape, keys, size, and alarm options
     |
     v
One native transaction
     |
     +-- Does every stored value match its snapshot?
            |
            +-- No  --> Conflict / not_committed
            |           No mutations from this batch are applied.
            |
            +-- Yes --> Apply puts, deletes, and alarm changes atomically
                        Return {"ok": true, "value": null}
```

The batch can include `checks`, `puts`, `deletes`, `alarm_at_ms`, and
`delete_alarm`. Checks run before any mutations. Keys must be nonempty, at most
2,048 bytes, and outside the reserved `__sys_` prefix. Duplicate deletes, a put
and delete of the same key, and conflicting alarm changes are rejected.

`storage.apply` performs the checked transaction. A separate `storage.sync`
call, with payload `{}`, awaits the native storage durability barrier. The
native host's output gates still apply; the guest layer cannot replace a
required durability proof with a successful JSON reply.

## Supported operations

The names below are the operation names accepted by `operations::execute`.
Bindings are resolved from the original native context and deployment.

| Area | Operation names | Behavior |
| --- | --- | --- |
| Context | `clock.now`, `var` | Read host wall-clock milliseconds or a configured variable. |
| Cell storage | `storage.read`, `storage.apply`, `storage.sync`, `storage.sql`, `alarm.get` | Read snapshots, apply checked batches, await durability, execute local SQL, or read the alarm. |
| Other cells | `cell.fetch` | Resolve a namespace binding and cell name, then perform a buffered HTTP call. |
| KV bindings | `kv.get`, `kv.put`, `kv.delete`, `kv.list` | Read/write JSON values and list keys with optional pagination. |
| SQL bindings | `sql.query` | Bind JSON parameters and execute a query through the configured SQL service. |
| Queues | `queue.send`, `queue.settle` | Send a JSON message or acknowledge/retry messages in the current batch. |
| Scheduled work | `cron.no_retry` | Disable retries for the current scheduled invocation. |
| Containers | `container.running`, `start`, `monitor`, `destroy`, `signal`, `set_inactivity_timeout` (all under `container.`) | Root-cell lifecycle through the native durability gates. |
| Container exec | `container.exec`, `container.exec_output`, `container.exec.*` | Bounded output or invocation-owned stdin/stdout/stderr, wait and signals. |
| Container ports | `container.port.wait_ready`, `fetch`, `connect`, `connect_websocket` (all under `container.port.`) | Readiness, buffered HTTP, TCP and event-owned WebSockets. |
| Container resources | `container.socket.*`, `container.websocket.*`, `container.release` | Invocation-local opaque handles, byte limits and deterministic release. |

`operations::State` carries the current queue or scheduled event and the
invocation's container resource table. Each table holds at most 64 live exec,
TCP and WebSocket handles; native handles never cross the bridge. Drop the state
on completion/cancellation. Resource IDs have meaning only within that state.
Container commands remain native gated effects. Monitor, exec wait and stream
reads are observational operations so they can progress alongside control calls.
Read failures can follow consumed bytes and must not be treated as replayable.
The [container guide](../../docs/09-integrations.md#how-cell-owned-containers-work)
describes each facade and the engine requirements.

`operations::State` also supplies event authority. Queue
settlement and scheduled retry decisions require the corresponding event;
having a binding alone does not supply it.

Python's `storage.read_many` is implemented in its own bounded
[read-batch adapter](../cellar-python/read_batch.rs). The shared `is_read`
classifier recognizes it, but the shared `execute` dispatcher does not handle it.

## HTTP values and limits

The buffered `cell.fetch` operation collects bounded response bodies. The
invocation-owned `transport::Session` also exposes live response forwarding,
backpressured streaming HTTP/SSE and native WebSocket capabilities. See
[streaming and WebSockets](../../docs/27-streaming-and-websockets.md). Container
ports also provide an explicit event-owned WebSocket API through their provider.

```text
                    Same native HTTP conversion rules
                                  |
                 +----------------+----------------+
                 |                                 |
         JSON transport                    Separate byte transport
         (JavaScript)                      (Python)
                 |                                 |
  {metadata, body: base64(bytes)}       ({metadata, body: ""}, bytes)
                 |                                 |
     request / input / output          request_parts / input_body /
     response                          output_body / response_parts
                 |                                 |
                 +-------- bounded HTTP -----------+
```

The byte path avoids a base64 encode/decode pass. `operations::fetch_bytes`
provides the corresponding nested-cell call. Both paths share body and header
checks. For result budgeting, a separate body is charged as though it were
base64 in the JSON envelope, so choosing the byte path does not bypass the limit.

| Limit | Enforced behavior |
| --- | --- |
| `Limits::max_body_bytes` | Bounds buffered bodies, individual stream chunks/frames, and aggregate retained response prefixes. |
| `Limits::max_batch_operations` | Counts storage batch operations; also bounds live response/socket capabilities per invocation. |
| HTTP headers | At most 128 pairs and 32,768 total bytes across names and values. |
| Guest response status | Must be an integer from 200 through 599. |
| JSON nesting | Depth at most 64, with the root at depth zero. |
| Encoded result | The adapter passes a byte limit to `failure::result` or `result_parts`; allow at least `MIN_RESULT_BYTES` (128) for fixed failure envelopes. |

For a binary result, charged size is `encoded_metadata_bytes + 4 * ceil(body_bytes / 3)`.
Oversized or invalid results can be detected **after** a native effect; rejecting
the result does not establish that the effect rolled back.

## Read failures without guessing whether a write committed

Replies use a success envelope (`ok` and `value`) or a failure envelope:

```json
{
  "ok": false,
  "error": "Ambiguous",
  "code": "Ambiguous",
  "outcome": "unknown"
}
```

`code` describes the failure; `outcome` describes what is known about the
operation's commit status. Fixed failure codes keep backend exception text,
SQL, request contents, and local paths out of replies.

| Failure | Code / outcome | Meaning |
| --- | --- | --- |
| Shared validation fails before native effects | `ValidationError / not_committed` | The operation was rejected before execution. |
| A checked batch finds a changed snapshot | `Conflict / not_committed` | This batch applied no mutations; reconsider it using fresh state. |
| Native routing reports lost ownership | `OwnershipLost / unknown` | Ownership loss does not prove rollback. |
| A submitted call reports capacity or application overload | `Overloaded / unknown` | A nested call may have been refused after earlier effects committed. |
| Durability is unproven, or an otherwise unclassified mutation/result fails | `Ambiguous / unknown` | Reconcile the outcome before deciding whether to retry. |
| An otherwise unclassified known-read operation/result fails | `HostError / unknown` | No usable result was obtained. |

```text
Native mutation             Result encoding / delivery          Caller
      |                                  |                        |
      +-- effect may commit ------------>X result unavailable     |
                                         +----------------------->|
                                               outcome: unknown

An unavailable result is not evidence that the mutation was undone.
```

Only attach the `failure::Validation` marker to errors proven to occur before
effects. Wrapping an arbitrary backend error with it would incorrectly claim
`not_committed`. Adapter-level refusals such as expired contexts are handled by
the owning adapter; see its guide for that additional error contract.

## Find the implementation and tests

| File | Responsibility |
| --- | --- |
| [lib.rs](lib.rs) | Public modules and shared `Limits`. |
| [operations.rs](operations.rs) | Operation dispatch, payload validation, read classification, and event state. |
| [containers.rs](containers.rs) | Container operation validation, native handles and invocation resource cleanup. |
| [storage.rs](storage.rs) | Checked batch validation and one-transaction application. |
| [values.rs](values.rs) | Buffered HTTP conversion, byte accounting, JSON depth, and queue body conversion. |
| [failure.rs](failure.rs) | Bounded reply encoding, error classification, and failure-provenance tests. |
| [values_tests.rs](values_tests.rs) | Exact body/header boundaries and response-stream cleanup tests. |

Run the shared boundary tests from the repository root, using a checkout-specific
`CARGO_TARGET_DIR` as described in [AGENTS.md](../../AGENTS.md):

```sh
cargo test --locked -p cellar-guest
python3 tools/docs/check.py
```

Runtime identity, cancellation, and shutdown tests belong to
[cellar-python](../cellar-python/README.md) and
[cellar-javascript](../cellar-javascript/README.md). When changing the protocol,
check both adapters and the shared boundary: a local conversion change can alter
another language's failure classification or byte budget.

Container operations use [`cellar-container`](../cellar-container/README.md),
which owns provider selection, invocation sessions and the native implementation.
The guest boundary preserves its typed pre-effect validation marker and error
provenance when encoding failures.
