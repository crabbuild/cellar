<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Dense filesystem capacity and cold recovery

The opt-in `filesystem_dense_capacity_cold_recovery` case in the
[native node harness](../../crates/cellar/tests/support/node_filesystem_capacity.rs)
uses the public `context.fs()` and `tx.fs()` primitive. Each profile has its own
private development bucket, nodes, cell and temporary data. It requires no mount,
container or external service.

Run with the checkout-specific target directory from [AGENTS.md](../../AGENTS.md):

```sh
CELLAR_FS_CAPACITY_MIB=10,100,1024 \
CELLAR_FS_CAPACITY_REPORT="$PWD/.cellar/filesystem-dense-capacity.json" \
cargo test --locked -p cellar --test native_node \
  filesystem_dense_capacity_cold_recovery -- --ignored --nocapture
```

Select one profile per process when comparing memory between sizes. The accepted
sizes are 10, 100 and 1024 MiB. Run the smaller cases before the GiB case. The
default Cargo test profile gives functional capacity evidence; production latency
comparisons require an explicitly recorded optimized profile, identical machine
and storage, and an otherwise idle host. A passed run records the executable
SHA-256, compiled fixture/backend source hashes and `debug_assertions` alongside
its measurements. Preserve the command, toolchain and raw log with that report.
The SDK workflow is configured to run the 10 MiB case in each Python ABI job
and upload only its structured JSON report. That configuration is separate from
evidence of a completed remote CI run.
For a failing warm/cold phase, set `CELLAR_FS_CAPACITY_KEEP_FAILED=1` to retain
its private temporary directory after shutdown; the error names that directory.
It contains generated payloads and a development bucket, and is never uploaded
by CI. Successful profiles always remove their temporary data.

## Workload and correctness checks

1. Write dense, deterministic, nonzero payloads in one-MiB transactions. Each
   transaction commits a block digest receipt and business progress with the
   file effect. No transaction buffers the whole file.
2. Check exact logical/allocated size and replay the last write receipt without
   changing file identity or version. Whole-file reads must refuse these files
   before allocating a whole-file buffer.
3. Apply three 4 KiB edits, including a 64 KiB chunk boundary and the file tail.
   Verify every one-MiB block against an independent expected-content digest.
4. Require a complete joined handoff shutdown, delete the predecessor's local
   directory, and start a differently named node with empty local storage.
   Verify every block, inode/version, progress and effect receipt from the bucket.
5. Append another one-MiB block under the new owner, verify it, then truncate it.
   Wait for automatic maintenance to return allocation to the original size.
   Require the successor's complete joined shutdown too.

Mutation failures fail the case; the client never blindly repeats a write.
Cold read admission can retry the documented 503 response within a bounded
deadline. Every successful read still verifies its contents. The block-digest
commitment is SHA-256 over the ordered binary block digests, rather than a
whole-file SHA-256.

The profile captures fixed 64 KiB chunks, one-MiB transfers/helpers, a 2 GiB
file/allocation ceiling and the normal transaction-work limits. Recovery attempts
and handlers have explicit 120-second budgets. It retains the native fixture's
three-second ordinary operation/adoption window; extending that window beyond
the drain-stall deadline would make a single-node handoff wait for an absent
successor. Shutdown has a 60-second total and 30-second stall budget.

## What the measurements mean

- Write/read/edit p50/p95/p99/max are end-to-end HTTP request times, including
  routing, native hashing and the configured output-proof gate. Reads fetch a
  one-MiB range inside the node and return its digest, so this is not a measurement
  of streaming one MiB through HTTP or a guest-language bridge.
- Cold admission time starts before successor startup. The first proven new
  write is measured separately after all recovered blocks have been verified.
- RSS samples run every 10 ms and include the test driver, runtimes, SQLite,
  replication and development object store in one process. Unsupported RSS
  observations are null. These are sampled peaks, not hard memory bounds.
- Directory file lengths, WAL lengths and local `.ltx` lengths are sampled every
  16 writes and at the last write. They are logical OS file lengths, not physical
  allocated disk blocks or exact transient peaks. Current bucket-object and
  `.ltx` object sizes are retained-state measurements, not total network traffic.

This harness does not qualify an external object store, peer proof/failures,
disk stalls, guest overhead, 4/16 KiB chunk alternatives, small-file density or
many-cell fairness. Those remain gates in the
[filesystem implementation plan](../../plans/per-cell-filesystem-implementation.md).

## Native fault qualification on S3-compatible storage

The separate [native fault profile](../../plans/per-cell-filesystem-conformance.md#s3-backed-native-recovery-profile)
uses the same file/state/receipt, fencing and failed-recovery assertions as the
development-store case. Provision a disposable bucket whose name starts with
`cellar-filesystem-`, supply its static test credentials through `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY`, and select an explicit endpoint. The profile uses
region `us-east-1`; it never creates or deletes the bucket itself.

```sh
CELLAR_FS_S3_BUCKET=cellar-filesystem-YOUR-UNIQUE-TEST-ID \
CELLAR_FS_S3_ENDPOINT=http://127.0.0.1:YOUR-PRIVATE-STORE-PORT \
CELLAR_FS_FAULT_REPORT=/absolute/qualification/filesystem-s3-proof-fault.json \
cargo test --locked -p cellar --test native_node \
  node_filesystem_faults::filesystem_s3_unproven_write_is_fenced_and_failed_recovery_refuses_admission \
  -- --ignored --exact --nocapture
```

The SDK workflow owns an isolated pinned RustFS container and its generated test
credentials, runs this case serially and uploads the report. The report contains
hashes, receipts, refusal counts and recovery outcomes, with no credentials.
Supplied-client fault gates wrap actual S3 I/O; physical network/disk failures
and deployment performance retain their own qualification profiles.
