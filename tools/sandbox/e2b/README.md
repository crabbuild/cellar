<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# E2B protocol reference

| Input | Pin |
| --- | --- |
| Upstream source | [e2b-dev/E2B at a2b2cae](https://github.com/e2b-dev/E2B/tree/a2b2caea4498766aee8fd33d3280b56744070e99) |
| SDK source reviewed | JavaScript SDK `2.59.0`; no SDK runtime is embedded |
| Vendored inputs | Control OpenAPI, envd gateway OpenAPI, process/filesystem protobufs, repository license |
| Integrity | Exact Git blob identities in [manifest.json](manifest.json) |
| Qualification | Local HTTP/Connect peer; live E2B and published SDK conformance remain `not_run` |

```sh
node tools/sandbox/e2b/check.mjs
node --test tools/sandbox/e2b/check.test.mjs
cargo test --locked -p cellar-container --test e2b
cargo test --locked -p cellar-javascript --features node,embed --test e2b
cargo test --locked -p cellar-python --features embed --test e2b
```

The peer checks native invocation routing, control/envd credential separation,
durable create/delete reconciliation, metadata ownership, binary process and
file framing, bounds and stream cleanup. It executes no VM or real commands.
Keep upstream inputs byte-for-byte; do not run a formatter on `reference/`.
The process protobuf retains upstream trailing whitespace; whitespace checks of
Cellar changes exclude `reference/`, while the identity check covers every byte.

See the [provider guide](../../../crates/cellar-container/e2b.md) for configuration,
the weaker ownership profile and unsupported operations.
