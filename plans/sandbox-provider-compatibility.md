<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Sandbox providers and Cloudflare compatibility

Status: accepted architecture direction and proposed delivery contract. Native
Docker/compatible Podman operations are implemented in `cellar-container`.
Experimental E2B/Vercel providers and local Cloudflare compatibility slices are
implemented. Full target qualification and the sandbox extensions below remain
pending. Naming a target does not enable that backend or SDK profile.

Implementation evidence and remaining requirements are tracked in the
[implementation ledger](sandbox-provider-progress.md).

## Decision: follow celld's application model, add execution providers

Prioritize self-hosted Workers/Durable Object container and Sandbox API
compatibility, using the celld pattern. The default executor is the existing
local native engine. Additional providers sit behind the application-facing API.
Calling hosted Cloudflare services is an optional later provider, not a
prerequisite for running compatible applications on Cellar.

The design separates three responsibilities:

1. **Application API:** native Rust/Python/JS/TS sandbox controls and an optional
   Cloudflare-compatible Workers/Durable Object and package surface.
2. **Cellar authority:** durable cell identity, ownership, storage, alarms,
   admission, output gates, resource limits and cleanup.
3. **Execution provider:** local Docker/Podman initially; adapters for Vercel,
   E2B, Daytona, Modal and optional hosted Cloudflare execution.

A supported application API operates on a provider's declared capabilities.
Keep business code and durable cell state independent of the selected executor.
Provider selection belongs in host configuration. The public API must not become
one application-specific client per vendor.

## What celld establishes

A declared container class gives each object a `ctx.container` that controls one
container. The owning node runs it through Docker/Podman. Images travel through
the deployment bucket; application state remains in object storage/bindings.
Idle eviction can retain the container, while ownership changes require cleanup.
Celld exposes lifecycle, exec and port primitives through its Workers runtime,
allowing published Cloudflare packages to use that local implementation.

This is the architectural reference, not evidence that Cellar already runs the
same packages or that celld supports every current Cloudflare API. Celld's guide
lists unsupported operations and marks containers experimental.

The upstream examples provide concrete first conformance inputs:

| Input | Observed upstream declaration | Cellar requirement |
| --- | --- | --- |
| Container example | `@cloudflare/containers: ^0.3.7` | Resolve and pin the exact package and lockfile before qualification |
| Sandbox example | `@cloudflare/sandbox: 0.12.9` | Test this explicit compatibility baseline |
| Sandbox image | `cloudflare/sandbox:0.12.9`, port 3000 | Pin its matching platform and image digest; retain package/image pairing |
| Sandbox source | `getSandbox`, published `Sandbox` class, exec, files and background processes | Run the published package through Cellar's runtime/DO surface, not a rewritten application wrapper |

Use vendored or checksummed fixtures with preserved licenses and recorded source
revisions. Package drift must invalidate the corresponding compatibility result.

## Targets and status

| Target | Application contract | Execution path | Status |
| --- | --- | --- | --- |
| Docker/compatible Podman | Existing Cellar container facades | Existing local native engine | Implemented |
| Cloudflare Workers/Durable Objects | Versioned modules, web APIs, bindings, lifecycle/storage/alarms and required RPC | Optional language runtime adapter above native Cellar | Partial runtime slices, including Node native service/loopback entrypoints, props and value RPC; full profile pending |
| Cloudflare Containers | `ctx.container` and published `@cloudflare/containers` profile | Local provider first; capable remote providers later | Partial local package profile |
| Cloudflare Sandbox 0.12.9 | Published Sandbox class and matching control-server image | Local provider first; package requirements govern other providers | Basic routes, in-object/HTTP/direct-RPC incremental execStream, RPC binary SDK upload and selected HTTP/WebSocket preview paths pass; complete lifetime/fault qualification pending |
| Cloudflare Sandbox 1.x | Direct container API plus matching package helpers/shim | Independently versioned profile | Selected Node exec/Files operations and automatic byte-source stdin pass with the pinned 1.0.3 helper/image; full profile partial |
| Vercel Sandbox / E2B | Portable Cellar sandbox operations; selected Cloudflare surface where capabilities permit | Provider adapters and durable remote bindings | Experimental bounded exec/files; live-account qualification pending |
| Daytona / Modal | Same portable operations plus declared extensions | Provider adapters and durable remote bindings | Planned |
| Dynamic Workers | Isolated module execution with explicit bindings and limits | Separate optional runtime capability; hosted Worker gateway if selected | Planned extension |
| Hosted Cloudflare Containers/Sandbox | Portable operations through a configured service gateway | Worker/Durable Object gateway and Rust client | Optional later backend |

Vercel/E2B/Daytona/Modal backend support does not itself promise unchanged vendor
SDKs will run against Cellar. Add such compatibility profiles separately when
required. Cloudflare package compatibility is an explicit target because it is
part of the chosen celld-style application model.

## Cloudflare API compatibility

### Workers and Durable Objects

Define a versioned profile for module exports, HTTP primitives, streams,
WebSockets, event lifetime, service bindings, object identity, storage and alarms.
Implement the Durable Object RPC dispatch needed by the actual container and
Sandbox packages. Existing Cellar language facades and HTTP transport do not
alone provide that runtime, RPC or storage contract.

Pin compatibility dates/flags and required `cloudflare:workers`/Node module
behavior. Unsupported bindings and APIs must fail explicitly. Keep the runtime
optional: native-only hosts and the pure decision core remain free of a
JavaScript engine. Rust, Python, JavaScript and TypeScript applications must also
retain access to native sandbox controls without adopting the Workers API.

### Containers

Map the public container lifecycle, exec and port APIs to the selected provider
through Cellar's ownership and output gates. Cloudflare's `running` is a property
and its `start()` queues startup after validation; Cellar's current facades use a
method and await startup. Preserve the selected compatibility contract instead
of silently equating similarly named methods.

Qualify published Container class hooks, `sleepAfter` alarms, object routing,
port switching, HTTP/WebSocket forwarding and the required process semantics.
Declare newer image selection, inspect, PTY/resize, snapshots and outbound
interception independently. A remote provider that cannot satisfy a required
operation must refuse admission to that profile rather than silently weaken it.

### Sandbox SDK versions

Start with celld's pinned 0.12.9 SDK/image baseline. Exercise HTTP and RPC paths,
file round trips, streamed/buffered commands, background process records, preview
routing and teardown with actual package code. Preview URLs, tunnels and bucket
mounts need their own routing, image and credential configuration.

Sandbox 1.x is a separate profile: its helpers operate over a running container,
while the application Durable Object owns lifecycle policy. Qualify the matching
`sandbox-shim`, package version, image architecture and runtime modules together.
Do not treat the older control-server image or SDK-class lifecycle as equivalent
to the 1.x model. Snapshot restoration must state whether it restores files,
memory or running processes; command cancellation cannot establish rollback.

### Dynamic Workers and hosted Cloudflare

Worker isolates and Linux environments are different execution kinds. An isolate
supports module entry points and explicitly granted bindings, not a generic
Linux process tree or filesystem. A local Worker-loader implementation requires
its own isolation and limit tests. A hosted loader uses a configured Worker with
a loader binding; hosted containers use a Worker/Durable Object gateway. Neither
hosted path is needed for local package compatibility.

## Shared provider contracts to extend

The existing `Provider`, `ProviderSession` and operation capability list are the
foundation. Extend them in tested slices when an actual backend needs them:

- **Configuration and discovery:** named bindings select provider, environment,
  image/template/module identity, region, limits, timeouts and network policy.
  All four language facades discover usable operations without exposing host
  credentials. Capability availability may depend on region, template or plan.
- **Portable operations:** create/connect, inspect, lifecycle, bounded commands,
  concurrent stdout/stderr, file read/write/list/stat and service forwarding.
  Worker module invocation is a separate operation family.
- **Optional features:** distinguish filesystem/memory snapshots, pause/resume,
  fork, PTY/resize, volumes, GPUs, raw TCP, HTTP previews and outbound credential
  brokering. Do not claim equal semantics merely because names match.
- **Durable bindings:** persist provider identity, remote resource ID, generation,
  configuration digest, lifecycle intent and reconciliation state. Bind leases
  to current cell ownership. Temporary process/stream handles remain event-owned;
  a configured persistent sandbox may reconnect during a later event.
- **Failure provenance:** use stable request IDs and parameter digests where a
  provider or gateway can enforce idempotency. Reconcile unresolved create, exec
  and delete outcomes. Never automatically replay an accepted mutation because
  its response was lost.
- **Ownership and cleanup:** remote fencing must be enforced by the service or
  a broker, not only the caller. If a backend cannot provide the requested
  guarantee, reject that profile or explicitly expose a weaker contract. Work
  accepted before a fence may continue; expiry and reconciliation must account
  for it. Dropping streams cannot replace sandbox lifecycle cleanup.

## Delivery slices and acceptance

SP-01 input identities and the API/gap inventory are implemented; the
[ledger](sandbox-provider-progress.md) records the evidence and partial runtime
work. The complete runtime/provider profiles below still require their acceptance
checks. No cloud account is needed to prove the initial local SDK profiles.

| Slice | Deliverable | Required evidence |
| --- | --- | --- |
| SP-01 | Exact celld reference fixtures and compatibility inventory | Pin package/image/source versions; list required APIs and explicit gaps |
| SP-02 | Optional Workers/DO runtime surface needed by the fixtures | Module imports, HTTP/streams/WS, object storage/alarms and required RPC; native-only dependency guard |
| SP-03 | Local published Containers package | Actual Container class hooks, routing, sleep/wake, process/port behavior and owned cleanup |
| SP-04 | Local published Sandbox 0.12.9 package and image | HTTP/RPC, exec, files, background processes, previews and teardown; no rewritten SDK substitutes |
| SP-05 | Rich provider capabilities, files and durable bindings | Four-language discovery, pre-effect refusal, lost responses, ownership takeover, expiry and orphan cleanup |
| SP-06 | Vercel/E2B execution adapters | Same Cellar application contracts; provider fixtures plus real-account tests; declare Cloudflare profile gaps |
| SP-07 | Current Cloudflare container/Sandbox 1.x profile | Matched helper/image and exact API inventory, streams, errors, cancellation and supported persistence |
| SP-08 | Daytona/Modal and additional capabilities | Separate provider qualification for processes, files, storage, PTY/GPU and snapshots |
| SP-09 | Dynamic Workers and optional hosted Cloudflare | Module isolation/limits, authenticated gateways, explicit bindings, outbound policy and owned cleanup |
| SP-10 | Release compatibility manifests and failure qualification | Version/operation matrix, four-language tests, resource bounds, fencing, uncertain outcomes and joined shutdown |

SP-01/SP-02 enable the local package profiles. SP-05 supports additional execution
providers; their implementation need not wait for unrelated Workers bindings.
SP-07 must not replace the celld baseline without a distinct migration profile.
Hosted deployment and live-account qualification are explicit delivery steps.
Fixture passes are not live cloud qualification or full Workers compatibility.

## Production targets supplied by the user

These targets extend the original goal; they are acceptance requirements, not
claims about the current implementation.

| Area | Required target | Current gap / delivery rule |
| --- | --- | --- |
| Sandbox concurrency | A business cell can allocate as many named sandboxes as its workload requires | Native currently owns one container per execution cell. Optional atomic host instance/memory admission now precedes Engine effects and retains uncertain reservations. The durable named catalog, authenticated child execution and scheduled explicit-retirement cleanup are implemented; per-class native fleet placement is implemented; four-language dynamic facades, deployment-removal cleanup and durable cell erasure are implemented; qualify saturation/recovery; scale by available capacity rather than an unbounded in-memory map or a fixed single sandbox |
| Lifetime | No user-specified maximum | Do not invent a global job lifetime. Publish effective provider limits and support long-lived ownership/renewal where available; ownership loss, explicit destruction and orphan cleanup still apply |
| Transfer defaults | 64 KiB streaming frames and 16 MiB multipart parts | Chosen defaults for common transfer paths. Pinned SDK/shim profiles retain their published geometry when it is not configurable and must record/qualify it separately. Qualify exact-byte 64 MiB and 256 MiB transfers, cancellation and recovery with bounded memory and temporary storage; larger objects remain subject to explicit host/provider quotas |
| Temporary storage | Preserve explicit host limits | Existing defaults reserve at most 1 GiB of streamed-part scratch space and 64 files. Admission must retain reservations for uncertain/entered work until joined cleanup; overload reports backpressure/refusal |
| Recovery | At most 300 seconds per sandbox; aim for 60 seconds or less | Measure from failure detection until a usable, correctly owned sandbox is available, including every state restoration promised by its profile. Never substitute a fresh empty sandbox when durable restoration was promised |

The named allocation boundary must preserve scope/epoch checks, independent run
identity, capability discovery, no replay of uncertain mutations, parent/cell
removal cleanup and the celld-compatible single-container execution-cell path.
The execution topology is a business cell coordinating independently owned
sandbox execution cells, retaining one native container per execution cell.
The native [allocation catalog](../crates/cellar-container/allocations.md)
now reserves named child identities with durable generations and parent
retirement barriers. Managed allocations use authenticated child RPC, bounded
provider execution and scheduled cleanup after explicit parent retirement.
Native resource deletion verifies exact ID absence. Child and opt-in parent
SQLite/replica erasure, removed-class orphan discovery and four-language allocation
facades are implemented. Broader capacity-placement qualification remains open;
manual catalog reservations alone do not allocate compute.

Qualification should increase concurrent allocations until the configured host
capacity is reached, verify bounded queuing/refusal, then inject owner loss and
restart while measuring each sandbox's recovery against the target. No numeric
unlimited-concurrency throughput claim is implied by "as many as needed".

## Next feature priorities

The [current ledger](sandbox-provider-progress.md#requirement-audit) separates
implemented slices from full-profile acceptance. Streaming SDK upload/download
and returned RPC responses now have pinned-image/native evidence. The follow-up
fixes requested V8 shutdown classification, and the selected native/image campaign
passes without changing existing budgets. Request results and HTTP-message
arguments now pass their Node/native slice. Async alarm controls also resolve the
reproduced overlapping callback deadlock at the original deadlines; broader fault
qualification remains open.

| Order | Remaining delivery | Why next |
| --- | --- | --- |
| 1 — qualification | Free-threaded typed RPC, peer-proxy lifetime, cold recovery, cold-first-call and heartbeat | Current main's RPC admission/idle-reuse protection is merged. At head `9dc616ec`, Linux JavaScript/Node fails accepted-create owner-expiry recovery; its exact local test and full five-case fleet suite pass, with new bounded failure observations at unchanged budgets. The same head's outbound workflow and free-threaded typed RPC pass, while GIL typed RPC fails with a closed owner and `NodeLeaseExpired`. The renewal cause, earlier free-threaded finding, intermittent local 250 ms dispatch failure and heartbeat finding remain unproven; successful controls do not repair those qualification gaps. The later `7a7644b4` JavaScript/Node, outbound, Python and typed-RPC workflows pass; those controls do not establish the earlier failures’ causes |
| 2 — feature delivery | Faulted fleet recovery, cleanup throughput and stronger provider lifecycle guarantees | Native currently has one container per execution cell; the user requires scalable named sandboxes per business cell with bounded capacity admission. Running-node and native fleet peers now cover expiry, lost responses, backend loss, cold restart, removed classes and accepted-create owner expiry. Provider API outage through repeated cold replacement and owner-specific store isolation now have native peer coverage; named allocation now has Python/JS/TS facades, deployment-removal cleanup and child/opt-in-parent SQLite/replica erasure with permanent external barriers. Broader outages, partitions, fleet-scale erasure audits and provider-enforced fencing remain open |
| 3 | Complete the separately pinned Sandbox 1.0.3 profile; additional providers and Dynamic Workers | Known-length native part staging and persisted local expiry are delivered. The matching published SDK/image now covers selected lost-create acknowledgement and hard-crash recovery: a replacement process reclaims unpublished rows, reaps the exact crashed application/helper resources and restores the earlier archive. The optional native HTTPS profile now has per-run CA injection and matching-image TLS routing evidence. Next deliver S3 mounts and qualify wider TLS/fault/client behavior, lost transport responses, larger/multi-part and verified-swap cancellation and broader crash boundaries. Remote-provider expiry, additional providers and Dynamic Workers remain open; the evidence ledger preserves the detailed operation/profile inventory |

Live E2B/Vercel qualification waits for the user's configuration. Local
Cloudflare and shared-framework delivery can continue independently. SP-02–SP-06
and the selected SP-07 delivery remain partial; SP-08–SP-09 and full SP-10
release qualification remain open.

## Sources and implementation entry points

Reviewed against upstream documentation and examples on 2026-10-24. Pin exact
source revisions and dependencies during implementation; review dates are not
package versions or conformance results.

- [Current adapter and protocol](../crates/cellar-container/README.md)
- [Cellar language containers](../docs/09-integrations.md#apis-in-each-language)
- [Cellar streaming and WebSockets](../docs/27-streaming-and-websockets.md)
- [celld container architecture and limits](https://github.com/denoland/celld/blob/main/docs/services/containers.md)
- [celld Container example](https://github.com/denoland/celld/blob/main/examples/container/index.js)
- [celld Container package declaration](https://github.com/denoland/celld/blob/main/examples/container/package.json)
- [celld Sandbox example](https://github.com/denoland/celld/blob/main/examples/sandbox/index.js)
- [celld Sandbox package pin](https://github.com/denoland/celld/blob/main/examples/sandbox/package.json)
- [celld Sandbox image pin](https://github.com/denoland/celld/blob/main/examples/sandbox/Dockerfile)
- [Cloudflare Workers runtime APIs](https://developers.cloudflare.com/workers/runtime-apis/)
- [Cloudflare Dynamic Workers](https://developers.cloudflare.com/dynamic-workers/)
- [Cloudflare sandbox environments](https://developers.cloudflare.com/sandbox/concepts/)
- [Cloudflare Durable Object Container API](https://developers.cloudflare.com/containers/api/durable-object-container/)
- [Cloudflare Sandbox package requirements](https://developers.cloudflare.com/sandbox/reference/)
- [Cloudflare Sandbox 1.0 changes](https://developers.cloudflare.com/sandbox/sdk/migrate/changes-in-1-0/)
- [Vercel Sandbox](https://vercel.com/docs/sandbox)
- [E2B](https://docs.e2b.dev/)
- [Daytona](https://www.daytona.io/docs/en/)
- [Modal Sandboxes](https://modal.com/docs/guide/sandboxes)
