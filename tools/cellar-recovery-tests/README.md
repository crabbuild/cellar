<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Native recovery qualification

This private test package runs the public SDK's large-image recovery tests with
the shared fleet fixtures in `crates/cellar/tests/support`. Keeping its entry
point outside the host package avoids building unrelated example and CLI
executables for each focused recovery campaign. It uses the workspace's ordinary
release profile and preserves the same tests, assertions and deadlines.

Run the ordinary cases with:

```sh
cargo test --locked -p cellar-recovery-tests --test native_raft_large_images -- --test-threads=1
```

See [the recovery runner](../perf/recovery-README.md) for physical peer and
RustFS campaigns, retained evidence and qualification requirements.
