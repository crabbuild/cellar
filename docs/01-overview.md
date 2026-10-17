# Overview and application fit

Cellar is a Rust framework for applications built from durable business cells,
such as accounts, rooms and workflows. Each cell owns SQLite state and has one
authoritative owner. Nodes sharing an object-store bucket form a fleet that
routes calls, replicates writes and recovers state before takeover.

Start with the [counter quickstart](02-quickstart.md) to run an application,
or use the capability map below to assess fit.

![Architecture](diagrams/architecture.svg)

## What an application supplies

Implement `cellar::sdk::Application` and `Cell`, or use `Registry` for handler
and factory registration. Compile these factories into a native executable.
Use the CLI's `Host` runner or embed `sdk::Node` with explicit configuration,
listeners, executors, clients, peer authentication and optional fleet services.
The generic `cellar` executable packages and manages applications; it cannot
serve factories that were never compiled into it.

| Need | Cellar surface | Application responsibility |
| --- | --- | --- |
| Entity state | Cell-local SQLite, KV-shaped storage, SQL, transactions | Aggregate boundaries, schema, invariants, migrations |
| Calls | Named namespaces, HTTP and structured RPC, service bindings | Authorization, API compatibility, idempotency |
| Durable execution | Alarms, wakes, Queue, Workflow, cron | Replay-safe side effects, retry limits and reconciliation |
| External systems | HTTP, Blob, streams, sockets, containers | Credentials, timeouts, resource budgets and external consistency |
| Fleet lifecycle | Ownership, fencing, replication, recovery, drain reports | Infrastructure, supervision, capacity and recovery qualification |

The workspace excludes JavaScript engines even with all features enabled.
Source JavaScript, Node.js modules and former Python/Wasm wrappers are not
executable application formats. A Python process inside the container example
is a container workload; it is not a Cellar application loader.

## Choose cell boundaries before choosing fleet size

Good candidates include accounts, projects, rooms, devices and workflows with
clear per-entity invariants. Independent entities can distribute work across
nodes. A global singleton cell concentrates contention; adding nodes does not
split its authority or transaction boundary. A requirement for an atomic
transaction across arbitrary cells needs application coordination or a
different data architecture. No cross-cell ACID guarantee is established here.

Native in-process application code is trusted host code. Loader quotas and
execution domains provide lifetime and resource controls; they are not evidence
of a security sandbox for untrusted tenant code. Tenant isolation and public
authentication must be designed by the host and application.

## Evidence and development status

The repository contains broad SDK integration tests and bounded local recovery
reports. The 10,000-cell report used three nodes in one VM, small overwritten
values, closed-loop load and an earlier image. It is useful recovery evidence,
not a supported maximum fleet size or a production service-level objective (SLO).
See [the evidence ledger](21-production-readiness.md) before making scale claims.

Cellar starts with fresh configuration and state. Celld aliases and persisted
state migration are unsupported. Reserve the `__sys_` SQLite prefix for the
framework, and keep persisted cell identity stable across releases.

Next: [quickstart](02-quickstart.md), [cell modeling](04-cell-modeling.md), and
[architecture](03-architecture.md). Implementation:
[SDK facade](../crates/cellar/sdk.rs), [application contracts](../crates/cellar/application/mod.rs).
