# Architecture and ownership boundaries

Cellar separates decisions from effects. `cellar-logic::on_event` takes a state
and event and returns effects. The decision core has no I/O, async work, ambient
clock, randomness, locks or dependencies. A serial actor owns it; adapters
perform effects and submit versioned completion events to its mailbox.

![Cellar architecture](diagrams/architecture.svg)

## Components

| Component | Responsibility | Boundary to preserve |
| --- | --- | --- |
| `cellar-logic` | Lifecycle, ownership, scheduling, routing, proof decisions | Policy lives here; adapter futures do not mutate core state |
| `cellar/actor` | Serialize events and execute returned effects | Completions retain versions, epochs and request ownership |
| `cellar/execution` | Execution domains and injected services | Tasks retain their domain; closure cancels rather than proves a drain |
| `cellar/runtime` | Generations, pools, cells, invocation gates | Event and activation authority outlive neither owner |
| `cellar/application` | Native contracts and SDK binding adapters | `sdk` is the application-facing facade |
| `cellar-app` | SQL, KV, Blob, queue, workflow and cron contracts, codecs and configuration | No host dependency or ambient execution services |
| `cellar/storage` + `cellar-ltx` | SQLite, WAL/LTX capture, restore, Blob/store clients | Replication mechanics do not define fleet authority |
| `cellar/durability` + `cellar/fleet` | Logs, proof, leases, handoff, wakes | Recovery pointers and predecessor logs survive transitions |
| `cellar-native` | Owned values, structured clone, crypto/web primitives | No JavaScript engine dependency |
| `cellar/node` + `cellar/transport` | Startup, ingress, supervision, network adapters | Listeners, sockets and streams retain invocation/domain ownership |

The physical component layout does not rename public Rust modules. `lib.rs`
uses `#[path]` declarations; for example `cellar::daemon` resides in
`node/mod.rs`. [API reference](22-api-reference.md) links through the public
facade and actual files.

`cellar` depends on `cellar-app`, which uses `cellar-native` value primitives and
`cellar-logic` policies. Existing SDK paths re-export the shared types. Runtime
binding handles, service cells and their authority checks stay in the host.
See the [building-block guide](../crates/cellar-app/README.md).

## A request and a write

Ingress admits an event to a compiled application generation. The application
derives a cell identity from a namespace. The actor resolves its owner, forwards
to a peer or activates it locally after ownership/recovery. The cell executes
under an activation epoch. SQLite commits create a write position captured in
LTX. The output gate requires the appropriate durability and ownership proof
before releasing externally visible success.

A single-node bucket posture waits for object-store proof. A fleet can use
follower disk proof before bucket upload, with fallback to bucket proof. A lost
proof or fence fails the operation. An application-level timeout can still be
ambiguous about whether state changed; use a command ID to resolve it.
Explore [the write flow](diagrams/write-flow.svg).

## Four lifetimes

The node's execution domain owns its tasks and policies. An application
generation owns its compiled workers and pools. A cell activation owns state
authority, resources and an epoch. An event owns its context, driver and
background work. These are related lifetimes, not interchangeable handles.
New generations can coexist with draining events from old generations.

Retiring an activation invalidates its state handles and wakes/cancels waiting
work. Shutdown drains full event drivers, including `wait_until` and nested
SDK calls, before changing cell lifecycle. The caller keeps supplied executors
alive until the node closes and joins its work.

## Extending Cellar

Put a new decision in the core, an external mechanism in its adapter, and
application semantics in the application. Search persisted-field readers and
writers before changing formats. Keep one `DurabilityOwner` for coupled log and
replication tasks. Do not create a competing lease authority, bypass the output
gate, mutate a cell database behind its live owner, or move process environment
parsing into library defaults.

Sources: [repository guidance](../AGENTS.md), [logic](../crates/cellar-logic/lib.rs),
[actor](../crates/cellar/actor/mod.rs), [SDK](../crates/cellar/sdk.rs).
