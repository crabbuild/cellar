# Cellar documentation

Build durable applications, understand their failure boundaries, and operate a
Cellar fleet with an explicit evidence trail.

**Baseline:** `c24feb25057793e6d6b18502c96eccbfcac2bdf4`, reviewed 2026-10-12.
This library documents the checked-in native Rust framework. Application
patterns and operating procedures are recommendations. A documentation set does
not certify production scale; [production readiness](21-production-readiness.md)
records the remaining release-specific gates. [Sources](sources.md) map claims
to implementation and retained validation reports.

![Cellar architecture](diagrams/architecture.svg)

## Choose a reading path

| Reader | Recommended path | Deliverable |
| --- | --- | --- |
| Application builder | 01 → 02 → 04 → 05 → 06 → 07 → 08 → 09 | A tested native application with stable identities and retry semantics |
| Platform engineer | 03 → 10 → 11 → 12 → 13 → 14 → 16 | An explicit host, secure infrastructure, and measured capacity envelope |
| Operator / SRE | 15 → 17 → 18 → 19 → 21 | Dashboards, recovery drills, runbooks, and release qualification evidence |
| Team lead | 20 → 21 → ADRs → templates | Ownership, contracts, change review, and a go/no-go decision |

## Complete document set

“Guide” describes implemented behavior; “practice” proposes an application or
team convention; “gate” defines evidence required before a deployment claim.
Documents can contain all three, with that distinction made in their text.

| Document | Purpose | Primary owner | Kind |
| --- | --- | --- | --- |
| [01 · Overview](01-overview.md) | Product boundaries, fit, capability map | Architecture | Guide |
| [02 · Quickstart](02-quickstart.md) | Run, inspect, and package the counter | Application | Guide |
| [03 · Architecture](03-architecture.md) | Decision core, adapters, runtime, data paths | Platform | Guide |
| [04 · Cell modeling](04-cell-modeling.md) | Identity, partitioning, invariants, hot cells | Application | Practice |
| [05 · SDK and lifetimes](05-sdk-and-lifetimes.md) | Handler contracts, authority, concurrency | Application | Guide |
| [06 · Storage and transactions](06-storage-transactions.md) | SQLite, transaction scope, migrations, outbox | Application | Guide / practice |
| [07 · Routing and RPC](07-routing-rpc.md) | Owner routing, structured calls, retry contracts | Application | Guide / practice |
| [08 · Asynchronous work](08-asynchronous-work.md) | Alarms, wakes, queues, workflow replay, cron | Application | Guide / practice |
| [09 · Integrations](09-integrations.md) | HTTP, streams, Blob, sockets, containers, facets | Application | Guide |
| [10 · Embedded host](10-embedding.md) | Application-owned Node and supervision | Platform | Guide |
| [11 · Configuration](11-configuration.md) | Explicit policies, defaults, CLI differences | Platform | Guide |
| [12 · Packaging and deployment](12-packaging-deployment.md) | Native artifacts, promotion, compatibility, rollback | Release | Guide / practice |
| [13 · Fleet topology](13-fleet-topology.md) | Placement, failure domains, network and store assumptions | Platform | Practice / gate |
| [14 · Security and tenancy](14-security-tenancy.md) | Threat model, identity, credentials, isolation | Security | Practice / gate |
| [15 · Observability](15-observability.md) | Health, outcomes, telemetry, SLO signals | SRE | Guide / practice |
| [16 · Capacity and scaling](16-capacity-scaling.md) | Admission, memory, hot partitions, benchmark interpretation | Performance | Practice / gate |
| [17 · Operations](17-operations.md) | Startup, rollout, drain, fencing, incidents | SRE | Practice |
| [18 · Recovery and backup](18-recovery-backup.md) | Predecessor recovery, recovery sets, restore drills | SRE | Guide / practice / gate |
| [19 · Testing and qualification](19-testing-qualification.md) | CI, conformance, load and failure matrix | Quality | Guide / gate |
| [20 · Team delivery](20-team-delivery.md) | Team boundaries, dependencies, reviews, change control | Engineering lead | Practice |
| [21 · Production readiness](21-production-readiness.md) | Evidence ledger, release gates, scope and gaps | Release owner | Gate |
| [22 · API map](22-api-reference.md) | SDK entry points linked to actual source | Application | Reference |
| [23 · CLI reference](23-cli-reference.md) | Verified command syntax and operator surfaces | Platform | Reference |
| [24 · Glossary](24-glossary.md) | Shared domain language | Architecture | Reference |
| [25 · Documentation maintenance](25-documentation-maintenance.md) | Update rules, visual tooling and verification | Documentation | Practice |
| [26 · Event delivery](26-event-delivery.md) | Outbox, durable streams, resumable subscriptions and outcomes | Application / SRE | Guide / practice |
| [Sources and evidence](sources.md) | Baseline, implementation links, validation limitations | Release owner | Reference |
| [Architecture decisions](adr/README.md) | Documented constraints and future decisions | Architecture | Decisions |
| [Application brief](templates/application-brief.md) | Define boundaries and measurable requirements | Application | Template |
| [Service contract](templates/service-contract.md) | Define identity, calls, compatibility, failure semantics | API owner | Template |
| [Release checklist](templates/release-checklist.md) | Record artifacts and gate evidence | Release owner | Template |
| [Incident record](templates/incident-record.md) | Preserve evidence and recovery decisions | Incident lead | Template |

## Visuals and interactive flows

Open [index.html](index.html) directly in a browser. It works offline without a
build, package install, or external font. Use the role filter and document
search, choose a flow, inspect a node, step forward/back, or select its failure
branch. Every flow is also an independently interactive SVG:

| Visual | Read it to understand |
| --- | --- |
| [Architecture](diagrams/architecture.svg) · [PNG](diagrams/architecture@2x.png) | Core decisions, effect adapters, native handlers, durability |
| [Cell boundaries](diagrams/cell-boundaries.svg) | Transactions inside cells and coordination across cells |
| [Fleet topology](diagrams/fleet-topology.svg) | Proposed failure domains and shared dependencies |
| [Team delivery](diagrams/team-delivery.svg) | Contract → implementation → qualification → operation |
| [Request routing](diagrams/routing-flow.svg) | Cell identity, route lookup, ownership and dispatch |
| [Write durability](diagrams/write-flow.svg) | SQLite commit → LTX → proof → output gate |
| [Recovery](diagrams/recovery-flow.svg) | Predecessor log → authority → restore → readiness |
| [Deployment](diagrams/deployment-flow.svg) | Build → verify → publish → install → rollout |
| [Asynchronous delivery](diagrams/async-flow.svg) | Intent → delivery → deduplication → side effect → acknowledgement |
| [Shutdown](diagrams/shutdown-flow.svg) | Ingress → events → cells → durability → report |

Markdown image previews are static because image renderers disable SVG scripts.
Open an SVG as a document, or use the explorer, for interaction. SVG links lead
back to the relevant guide. The catalog is also available as
[catalog.json](catalog.json) for tools.

## First team milestone

Run the counter, choose an application aggregate, write its contract, and build
a small vertical slice with idempotent commands. Then qualify that exact
release under representative traffic and failures. Team documents should
record concrete owners, environments, and measured thresholds using the
templates; this library does not silently choose those business requirements.
