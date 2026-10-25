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
and submitted startup failures without replay. Callback-owned queued lifecycle
and port methods remain incomplete. These tests qualify those paths only. The
unchanged upstream applications, actual image, HTTP/RPC, monitor hooks and cloud
providers remain required. The source inventory continues to report
`conformance: not_run` for that complete baseline.
