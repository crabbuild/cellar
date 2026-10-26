<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Vercel protocol reference and helper acceptance

| Input | Pin / scope |
| --- | --- |
| Source | [vercel/sandbox at 8910e62](https://github.com/vercel/sandbox/tree/8910e6205b1c72e1ad6ebaa2ceae236a3aa4d87f) |
| SDK reference | `@vercel/sandbox` `3.9.0`; native provider does not embed the SDK |
| Files | API client, validators, base client, archive writer, package metadata and Apache-2.0 license |
| Integrity | Exact Git blob identities in [manifest.json](manifest.json) |
| Live qualification | `not_run` |

```sh
node tools/sandbox/vercel/check.mjs
node --test tools/sandbox/vercel/check.test.mjs
python3 tools/sandbox/vercel/helper_test.py
cargo test --locked -p cellar-container --test vercel
cargo test --locked -p cellar-javascript --features node,embed --test vercel
cargo test --locked -p cellar-python --features embed --test vercel
```

The HTTP/NDJSON peer checks the published protocol through native invocations;
it executes no VM or user command. A separate local test runs the actual
[helper](../../../crates/cellar-container/vercel/helper.py) with owned temporary
files and controlled subprocesses. Live VM, image and account qualification are
separate requirements.

Upstream TypeScript retains its original bytes and is excluded from the owned
formatter. The integrity tests reject tampering/missing sources. See the
[provider guide](../../../crates/cellar-container/vercel.md) for guarantees,
configuration and unsupported operations.
