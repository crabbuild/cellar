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
| Cloudflare Workers/Durable Objects | Versioned modules, web APIs, bindings, lifecycle/storage/alarms and required RPC | Optional language runtime adapter above native Cellar | Partial runtime slices; full profile pending |
| Cloudflare Containers | `ctx.container` and published `@cloudflare/containers` profile | Local provider first; capable remote providers later | Partial local package profile |
| Cloudflare Sandbox 0.12.9 | Published Sandbox class and matching control-server image | Local provider first; package requirements govern other providers | Basic routes, in-object/HTTP/direct-RPC incremental execStream and RPC binary SDK upload pass; HTTP preview routing passes; upgraded previews and complete lifetime/fault qualification pending |
| Cloudflare Sandbox 1.x | Direct container API plus matching package helpers/shim | Independently versioned profile | Planned next profile |
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

## Next feature priorities

The [current ledger](sandbox-provider-progress.md#requirement-audit) separates
implemented slices from full-profile acceptance. Streaming SDK upload/download
and returned RPC responses now have pinned-image/native evidence. The follow-up
fixes requested V8 shutdown classification, and the selected native/image campaign
passes without changing existing budgets. Request results and HTTP-message
arguments now pass their Node/native slice; broader fault qualification remains
open.

| Order | Remaining delivery | Why next |
| --- | --- | --- |
| 1 | Overlapping callback/alarm resumes, remaining Containers hooks and upgraded Worker/DO routing | Idle lifecycle and local HTTP previews now have selected-path evidence; finish continuation/fault qualification |
| 2 | Named provider bindings, reconnect/expiry and orphan reconciliation | Make the common framework useful across events and ownership changes |
| 3 | Separately pinned Sandbox 1.x; additional providers and Dynamic Workers | Extend only with explicit operation/version and failure evidence |

Live E2B/Vercel qualification waits for the user's configuration. Local
Cloudflare and shared-framework delivery can continue independently. SP-02–SP-06
remain partial; SP-07–SP-09 and full SP-10 release qualification remain open.

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
