<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Published sandbox compatibility inputs

This directory pins real celld examples, published Cloudflare packages, their
runtime import graph and a matching immutable container image. It establishes
SP-01 input identity. It does **not** certify that those packages already execute
on Cellar. Every runtime requirement in `manifest.json` remains pending until
its actual end-to-end tests pass.

```sh
npm ci --ignore-scripts --prefix tools/sandbox/compat
npm run inventory --prefix tools/sandbox/compat
npm test --prefix tools/sandbox/compat
```

The inventory bundles the unchanged upstream examples against the actual npm
packages, without evaluating them. It checks exact lock integrity/version pins,
upstream Git blob hashes, runtime dependency bytes and external module imports.
It verifies the captured OCI manifest against its digest and package/image version
pairing. No container or cloud resource is started by these commands.

Reference source is celld commit
`8702f6d186c77698dbfb30cf29e3b1922a88f008`. The files under `fixtures/celld`
are preserved verbatim under their upstream Apache-2.0 license; see
[`fixtures/celld/LICENSE`](fixtures/celld/LICENSE). A changed SDK, imported file,
reference example or lock refuses verification and requires a reviewed inventory
update. The sandbox tag is resolved to a `linux/amd64` image digest; compatibility
runs must use the digest, rather than re-resolving the mutable tag.

The [API inventory](api-inventory.md) maps required runtime APIs and unchanged
fixture routes to native building blocks and explicit implementation gaps. The
first audit establishes these runtime requirements:

- `cloudflare:workers` base classes, RPC targets and Sandbox tracing hooks.
- Synchronous Durable Object SQL cursors and synchronous storage KV access.
- Constructor admission through `blockConcurrencyWhile`, storage transactions,
  alarms and container lifecycle/port capabilities.
- The real Sandbox package's Cap'n Web RPC transport and `node:path/posix`.

These requirements must cross Cellar's native turn, ownership and durability
boundaries. Replacing synchronous SQL with a dummy cursor, replacing the package
with a hand-written wrapper, or allowing direct language-side disk writes cannot
qualify the profile. Package imports compiling is evidence about dependencies;
it is not evidence about RPC, storage durability, command execution or isolation.

See the [delivery plan](../../../plans/sandbox-provider-compatibility.md) and
[implementation ledger](../../../plans/sandbox-provider-progress.md) for the full
objective and current evidence.

## Native constructor/storage execution fixtures

`node tools/sandbox/compat/build.mjs` verifies the original inventory and builds
separate runtime fixtures against `packages/cellar-cloudflare`. The Rust
`cellar-javascript` `workers` integration target rebuilds these inputs itself
before running, preventing reuse of stale executable fixtures. The original
`fixtures/celld` files remain unchanged.

These tests execute actual published Container construction/scheduling in Node
and V8, and Sandbox construction/configuration in Node, through real native
SQLite and an activation-bound synchronous provider running property. Node/V8
fixtures check native startup/teardown observations against the engine protocol
peer and refuse foreign-cell property access. Root-event queued lifecycle tests
cover blocked startup with storage progress, explicit destroy/monitor outcomes
and submitted startup failures without replay. Node forwards incremental port
and Worker/DO HTTP responses with native cancellation; embedded web globals,
Worker/DO upgrade forwarding and callback-owned queued lifecycle remain incomplete. These tests
qualify those paths only. The unchanged Container module now runs in the separate Node `workers_module`
target: named/singleton/random routing, separate native container ownership,
delayed destroy acknowledgement and explicit cleanup use an engine protocol
peer. The separate image target below qualifies selected Sandbox HTTP/RPC paths;
complete sleep/wake/hooks and cloud-provider qualification remain required. The
source inventory continues to report `conformance: not_run` for that complete baseline.

## Run the pinned image through Cellar

The opt-in `sandbox_image` integration target uses the **unchanged** pinned
Sandbox application and published SDK. Its wrapper adds teardown and explicit
stream probes; the original application routes remain unchanged.
Cellar starts the real image through its selected native engine; no protocol
peer supplies command or file results.

| Prerequisite | Configuration |
| --- | --- |
| Isolated Docker/Podman daemon | Explicit Unix socket; this test creates containers, networks and native fence rules |
| Sandbox image | Preload the exact digest/platform in `manifest.json`; tags are not substituted |
| Fence image | Preload the native `cellar-fence` image built by the normal deployment tooling |
| Optional runtime | Node.js and the `cellar-javascript/node` feature |

```sh
export CELLAR_SANDBOX_ENGINE_SOCKET=/path/to/isolated/docker.sock
export CELLAR_SANDBOX_FENCE_IMAGE="sha256:<preloaded-fence-image-id>"
cargo test --locked -p cellar-javascript --features node \
  --test sandbox_image -- --ignored --nocapture
```

The test checks stdout/stderr and nonzero exit status, a file read during a later
event, background process discovery, explicit SDK destroy and joined native
shutdown. It verifies that no containers with its unique host label remain and
removes only that host's empty, labelled bridges so repeated runs cannot exhaust
the daemon's subnet pool.
It shuts down its node after application failures too. This test does not remove
unrelated daemon resources; use a disposable daemon for network/fence isolation.
Passing it qualifies these routes for the pinned baseline. Generic stream inputs
have a separate native fixture; SDK RPC uploads now have their own real-image
scenario. SP-04 previews, restart/fencing and live-cloud
requirements remain incomplete.

The same `sandbox_image` test target also contains a **separate portable-file
scenario**. It uses Cellar's `ctx.container` file API against the real image,
checks bytes/permissions independently through a container process, reads during
a later event, refuses oversized archives/listings, and drains shutdown. That
scenario does not substitute for the unchanged published SDK route test.

### Current live qualification

| Route / boundary | Evidence on 2026-10-25 |
| --- | --- |
| `/exec` | Real SDK/image stdout, stderr and child-command exit code 7 match assertions |
| `/write`, then a later `/exec` | Real write/read and exact JSON-encoded file contents, including the trailing newline, match assertions |
| `/serve` | Actual background process starts and its function-bearing result crosses native leased RPC |
| `/processes` | A later request finds the same process ID with a running/starting status |
| In-object `execStream()` | A stdout event arrives before release; final stdout/stderr data events and exit code 7 follow |
| Worker/DO HTTP `execStream()` | A network client receives actual SDK SSE stdout before a second HTTP request releases the command; final data events and exit code 7 follow |
| Direct RPC `execStream()` | The unchanged SDK method returns its readable byte stream across native RPC; the network client verifies stdout before releasing the command, final stdout/stderr and exit code 7 |
| Teardown / shutdown | Explicit SDK destroy removes owned containers; serialized alarm callbacks allow native cell/application drainage without a timeout |

**The complete opt-in route test passes.** RPC functions retain their original
native event and use bounded, revocable handles; methods remain on SDK return
objects. The source inventory keeps its broader stream-input/preview/fault baseline
at `conformance: not_run`.

In the local Colima run, automatic VM ports initially collided with an unrelated
host service. Qualification uses an isolated task VM whose Docker ephemeral
port range was moved to observed-free host ports. Verify the expected protocol,
not just HTTP status. This is local macOS/amd64-emulation evidence; Linux network
policy, other architectures and live cloud services remain separately qualified.

### Native writable-result fixture

`web-writable.mjs` exercises ordinary Workers DO methods through Cellar, separately
from the pinned Sandbox image. The native `workers_writable` target checks a
4 KiB binary transfer through a 1 KiB message budget, independently held sink
write/close acknowledgements, mixed readable/writable identity-transform results
and aliases, accepted prefixes after failure, abort reasons, unconsumed imports,
stale writers and zero bridge reservations after joined shutdown.

This is generic returned-writer evidence. It does not substitute for the
published SDK's stream-input/Cap'n Web upload by itself. The dedicated image
scenario below supplies that separate positive upload evidence; full fault
qualification remains pending.

## Common RPC stream input test

```sh
cargo test --locked -p cellar-javascript --features node \
  --test workers_inputs -- --test-threads=1
```

This native-host fixture proves incremental read/write arguments, aliases,
root-Worker and returned-function calls, cross-DO forwarding, captured inputs,
accepted prefixes after source failure, cancellation, local quota/encoding
refusal and unused-session cleanup. Shutdown must drain both application work
and native bridge reservations. It does not run SDK uploads or a container.

## Actual SDK RPC files

The `published_sandbox_rpc_files_use_the_pinned_image` image test selects the
declared `SANDBOX_TRANSPORT=rpc` binding and calls unchanged `getSandbox`,
`writeFile` with a byte stream, binary `readFile`, `exec` and `destroy`. A separate execution session
observes an actual temporary-file prefix before source release; the destination
must remain unpublished until EOF. The final 2 MiB + 17 bytes and SDK byte count
must match exactly. Binary `readFile({ encoding: "none" })` then returns a file
result whose streamed content must match every uploaded byte. Shutdown must
leave no native bridge reservations.

This exercises the published Containers local WebSocketPair proxy and Cap'n Web
session over Cellar's native port socket. It does not replace SDK transport code.
New stream/WS tests retain explicit handshake budgets; existing image deadlines
and assertions remain unchanged. Complete reconnect, abandoned-stream and
fault qualification remain required.

## Returned RPC response fixture

`web-messages.mjs` and the native `workers_messages` test exercise real HTTP
responses returned from DO methods: metadata and aliases, a 3 KiB body through a
2 KiB bridge budget, delivery before source release, errors after an accepted
prefix, client cancellation and unconsumed-body disposal. Shutdown must drain
application work and bridge reservations. Full HTTP-message compatibility remains
pending; the Request/argument slice below has separate evidence.

## RPC Request and HTTP-message argument fixture

`http-rpc.mjs` and `workers_messages` use real Node Request/Response classes over
native CellRpc routing. The test sends 32 KiB + 17 binary bytes through an 8 KiB
bridge limit, observes a prefix before releasing the source, then checks that EOF
does not discard the live abort signal. It also checks HTTP argument aliases,
Response metadata/body, pre-abort refusal before target mutation, a returned
Request's later-event abort after storage proof, result-to-argument forwarding
and unused-message cleanup.

The owned protocol tests separately prove that a later abort waits for that
caller's storage proof while source cancellation runs in its original scope.
This is a bounded Node profile, not full Workers conformance or an embedded web
bootstrap.


## Published idle lifecycle and HTTP previews

`container-lifecycle.mjs` observes the actual Containers idle alarm and hooks
against the native engine peer. The peer models graceful SIGTERM exit; state
and counters must survive sleep/wake, and explicit teardown must drain resources.

`sandbox-preview.mjs` uses the unchanged Sandbox preview helper and matching
image. It verifies binary HTTP forwarding, forged-header removal, invalid-token
refusal, stale URLs after stop/replacement, explicit re-exposure and revocation.
The image keeps its pinned digest; the host declares additional port 8080 with
`exposed_ports`. No external domain or hosted account is contacted.

See the [preview guide](../../../packages/cellar-cloudflare/previews.md) for app
wiring and the [open callback/alarm qualification gap](../../../plans/sandbox-provider-progress.md#open-callbackalarm-qualification-failure)
for the extra overlapping-barrier probe. HTTP preview acceptance does not qualify
that gap, Worker/DO upgraded routing or complete SDK conformance.
