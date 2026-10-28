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

## Empty-process recovery after SIGKILL

The large-image target also contains explicit `process_recovery` fixtures for
272 MiB and page-rounded decimal 10 GB. Three separate native worker processes
start from the same independently verified baseline. After an acknowledged
command, the controller kills the command-ready owner with SIGKILL, waits for
its exit status, and erases its entire worker directory. Only then does it start
the replacement process, with an absent data directory and `JoiningFromS3`.

The replacement must read at least the image size through the object-store
adapter and receive zero peer-recovery bytes. It then serves a local read,
becomes leader, acknowledges a quorum-durable write, serves a version-bound
read, reconciles both old and new receipts, and passes the payload oracle.
Surviving workers independently serve the new committed value. Every surviving
process must join its owners and exit successfully; failed workers and raw
evidence remain available. Real TLS sockets carry Raft traffic; controller files
carry only test commands, addresses and results, outside each worker's data.

Use the source-pinned runner after providing AWS credentials and
`CELLAR_LARGE_IMAGE_ENDPOINT` / `CELLAR_LARGE_IMAGE_BUCKET` in the environment:

```sh
python3 tools/perf/process_recovery.py --profile 272mib \
  --work-directory /absolute/disposable-workers \
  --store-data-directory /absolute/rustfs-data --output /absolute/new-evidence
```

For the full-size case use `--profile 10gb --fixture /absolute/readonly/source.sqlite`.
The runner checks disk admission, exact test selection, source/executable
identity, the process/phase ledger and data-source counters. A fixture must have
9,999,998,976 bytes; build and source preparation are outside the recovery timer.
Run the small case first on the same source. Keep builds separate from physical
measurements. This is single-owner loss with a surviving Raft quorum, not recovery
after losing all replicas, a cold OS-cache benchmark or a production percentile.

The harness explicitly selects the existing full-capacity fixture's one-hour
`snapshot_timeout_ms` transfer budget and records the effective configuration.
This is a capacity/correctness profile, not a one-hour recovery objective. The
image-limit builder alone selects a five-minute minimum; slower storage can
exhaust that budget before the image is installed. Read deadlines remain 60
seconds, and all image, authorization, receipt and durability checks still apply.
