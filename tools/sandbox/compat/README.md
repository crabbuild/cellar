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
and submitted startup failures without replay. The Node port facade adds bounded
buffered HTTP with native cancellation; embedded web globals, streaming/upgrades
and callback-owned queued lifecycle remain incomplete. These tests qualify
those paths only. The
unchanged Container module now runs in the separate Node `workers_module`
target: named/singleton/random routing, separate native container ownership,
delayed destroy acknowledgement and explicit cleanup use an engine protocol
peer. Actual image execution, complete sleep/wake/hooks, Sandbox HTTP/RPC and
cloud providers remain required. The source inventory continues to report
`conformance: not_run` for that complete baseline.

## Run the pinned image through Cellar

The opt-in `sandbox_image` integration target uses the **unchanged** pinned
Sandbox application and published SDK. Its wrapper adds only a teardown route.
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
shutdown. It verifies that no containers with its unique host label remain.
It shuts down its node after application failures too. This test does not remove
unrelated daemon resources; use a disposable daemon for network/fence isolation.
Passing it qualifies these routes for the pinned baseline, not the full SP-04
streaming, preview, restart/fencing or live-cloud requirements.

### Current live qualification

| Route / boundary | Evidence on 2026-10-25 |
| --- | --- |
| `/exec` | Real SDK/image stdout, stderr and child-command exit code 7 match assertions |
| `/write`, then a later `/exec` | Real write/read and exact JSON-encoded file contents, including the trailing newline, match assertions |
| `/serve` | Actual background process starts, but its function-bearing result cannot cross the value-only RPC codec; the submitted outcome remains unknown |
| `/processes` | Not reached by the full test; returned process objects require the same function-delegate support |
| Teardown / shutdown | Explicit SDK destroy removes owned containers; serialized alarm callbacks allow native cell/application drainage without a timeout |

**The complete opt-in image test still fails at `/serve`.** Implement bounded
RPC function delegates and their ownership/disposal lifetimes; do not strip
methods from SDK return objects to turn this into a value-only test. The source
inventory keeps its complete baseline at `conformance: not_run`.

In the local Colima run, automatic VM ports initially collided with an unrelated
host service. Qualification uses an isolated task VM whose Docker ephemeral
port range was moved to observed-free host ports. Verify the expected protocol,
not just HTTP status. This is local macOS/amd64-emulation evidence; Linux network
policy, other architectures and live cloud services remain separately qualified.
