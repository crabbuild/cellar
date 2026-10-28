<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Native recovery qualification

This private test package runs the public SDK's large-image recovery tests and
the native root-image archive/installation tests with the shared fleet fixtures
in `crates/cellar/tests/support`. Keeping its entry
point outside the host package avoids building unrelated example and CLI
executables for each focused recovery campaign. It uses the workspace's ordinary
release profile and preserves the same tests, assertions and deadlines.

Run the ordinary cases with:

```sh
cargo test --locked -p cellar-recovery-tests --test native_raft_large_images -- --test-threads=1
cargo test --locked -p cellar-recovery-tests --test native_raft_rooted -- --test-threads=1
cargo test --locked -p cellar-recovery-tests --test native_raft_daemon
```

The daemon target reuses the production CLI fixture as a custom process runner.
It starts three separate native processes and two groups over TLS, kills the
elected worker with SIGKILL on Unix, reconciles saved receipts, writes through the
surviving quorum and resumes the killed voter from its retained disk. Version-bound
local reads verify catch-up and new writes after restart. The configured node
label and disk lineage stay fixed; the process generation and advertised endpoints
change through authenticated node-directory refresh.

Set `CELLAR_RAFT_PROCESS_EVIDENCE` to a new absolute file path to retain its
controller-clock JSONL ledger outside the disposable fixture. Request identities
are synced before sends; ambiguous replies keep the same immutable request.
Duplicate results must match their saved value and history, while their covering
version may advance. All surviving/restarted processes must still return complete
joined shutdown reports. This small dev-store fixture does not qualify object-store
recovery, erased-disk replacement, 10 GB performance or the complete fault matrix.

See [the recovery runner](../perf/recovery-README.md) for physical peer and
RustFS campaigns, retained evidence and qualification requirements.
