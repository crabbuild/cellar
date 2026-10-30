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
and upload its structured JSON report and raw log, including lease-store phases
on failure. That configuration is separate from
evidence of a completed remote CI run.
For a failing warm/cold phase, set `CELLAR_FS_CAPACITY_KEEP_FAILED=1` to retain
its private temporary directory after shutdown; the error names that directory.
It contains generated payloads and a development bucket, and is never uploaded
by CI. Successful profiles always remove their temporary data.

For a lease-expiry investigation, retain native queue, SQLite and result-delivery
phases without changing the fixture's lease or attempt budgets:

```sh
RUST_LOG='warn,cellar::actor::effects=info,store[node_lease_store]=debug' \
CELLAR_FS_CAPACITY_MIB=100 CELLAR_FS_CAPACITY_KEEP_FAILED=1 \
cargo test --locked -p cellar --test native_node \
  filesystem_dense_capacity_cold_recovery -- --ignored --nocapture
```

The machine-local development bucket stages ordinary object bodies larger than
128 KiB as immutable SQLite chunks. Each chunk uses a separate FULL transaction;
the ETag and complete-body reference publish atomically after staging. This lets
lease CAS operations enter between chunks when a filesystem recovery snapshot is
large. Replacement and deletion retire the previous body in the publication
transaction, then reclaim it through bounded transactions. Unpublished bodies
expire after 60 seconds without staging progress. Subsequent writes/deletes
reclaim at most four abandoned chunks per maintenance pass. A process crash can
therefore leave reclaimable staging data; a read never treats it as an object.
These are development-bucket mechanics, separate from the per-cell filesystem's
64 KiB content chunks and its existing replication protocol. Cloud object-store
formats and cell SQLite formats are unchanged. Older development object-table
layouts are refused without migration under the repository's format-1 hard-cutover
policy; use a fresh development bucket for this source revision.

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
[filesystem implementation plan](../../docs/design/per-cell-filesystem-implementation.md).

## Guest read concurrency

The ordinary JavaScript and Python filesystem fixtures include bounded
concurrent reads and sequential offset/append controls, followed by exact local
restart checks. See the
[case profile](../../docs/design/per-cell-filesystem-conformance.md#concurrent-guest-descriptor-profile).
Use a fresh report directory for each runtime/ABI invocation:

```sh
CELLAR_FS_CONCURRENCY_REPORT_DIR=/absolute/qualification/javascript-concurrency \
cargo test --locked -p cellar-javascript --no-default-features \
  --features embed,node --test filesystem -- --test-threads=1

cargo test --locked -p cellar-javascript --no-default-features \
  --features embed,node --lib \
  filesystem_reads_overlap_a_held_mutation_without_releasing_its_slot \
  -- --test-threads=1

CELLAR_FS_CONCURRENCY_REPORT_DIR=/absolute/qualification/python-concurrency \
cargo test --locked -p cellar-python --features embed --test filesystem
```

For Python, set `PYO3_PYTHON` and `CELLAR_PYTHON_SITE_PACKAGES` to the intended
supported interpreter and its dependencies, following the
[adapter guide](../../crates/cellar-python/README.md). Run both GIL and
free-threaded builds; preserve each executable before changing interpreter
configuration. The Python executable uses `harness=false`. Its fixture checks
the actual runtime GIL state as well as filesystem behavior. Successful Node,
V8 and each Python ABI emit six JSON observations apiece. The controlled runtime
test separately holds the mutation slot until all refusal results settle.

These cases permit overlapping known reads, not concurrent guest mutations or
a multi-call snapshot. They use local development storage and graceful restart;
external-store failure and optimized deployment evidence are separate profiles.

## Native fault qualification on S3-compatible storage

The separate [native fault profile](../../docs/design/per-cell-filesystem-conformance.md#s3-backed-native-recovery-profile)
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

### S3 connection cuts

The transport variant runs the same client-ledger scenario through three
supervisor-owned TCP proxies, separately covering replica publication, the
owner's lease store and a failed recovery reader. Use a fresh private bucket;
this variant requires a numeric loopback HTTP endpoint. It closes existing
pooled connections and rejects new connections without replacing object-store
methods or generating HTTP responses.

```sh
CELLAR_FS_S3_BUCKET=cellar-filesystem-YOUR-OTHER-UNIQUE-TEST-ID \
CELLAR_FS_S3_ENDPOINT=http://127.0.0.1:YOUR-PRIVATE-STORE-PORT \
CELLAR_FS_FAULT_REPORT=/absolute/qualification/filesystem-s3-socket-fault.json \
cargo test --locked -p cellar --test native_node \
  node_filesystem_faults::filesystem_s3_socket_cuts_fence_unproven_writes_and_refuse_failed_recovery \
  -- --ignored --exact --nocapture
```

The JavaScript SDK workflow runs this case serially in a separate bucket on its
private RustFS server and uploads its structured report. Each proxy admits at
most 128 active connection tasks; a cut drains and joins them before proceeding.
The report distinguishes forwarded, cancelled and denied connections and requires
joined cleanup. In this mode, the three refusal counters count TCP connections,
rather than object-store method calls. Node lease, recovery and shutdown budgets
are unchanged from the supplied-store scenario.

The [captured transport report](results/2026-10-28-filesystem-socket-faults.md)
records local macOS evidence. This profile covers connection closure during
bucket proof and recovery. It does not qualify packet blackholes/delay, peer
transport cuts, credential expiry, disk stalls, or deployment SLO/RPO/RTO.

### S3 forwarding stalls

The stalled-connection variant leaves established sockets open while stopping
both forwarding directions. New connections are accepted and held without
upstream I/O. Acknowledgement waits for admitted forwarding to stop; supervisor
shutdown aborts and joins the parked tasks. The S3 client remains unmodified.
Use another fresh private bucket:

```sh
CELLAR_FS_S3_BUCKET=cellar-filesystem-YOUR-STALL-TEST-ID \
CELLAR_FS_S3_ENDPOINT=http://127.0.0.1:YOUR-PRIVATE-STORE-PORT \
CELLAR_FS_FAULT_REPORT=/absolute/qualification/filesystem-s3-stall-fault.json \
cargo test --locked -p cellar --test native_node \
  node_filesystem_faults::filesystem_s3_socket_stalls_fence_unproven_writes_and_refuse_failed_recovery \
  -- --ignored --exact --nocapture
```

The JavaScript SDK workflow runs this scenario serially and uploads its report.
The native loopback control also requires reads to remain pending on existing
and newly accepted sockets, and verifies the actual active-task counter reaches
zero. Each proxy retains at most 128 connection tasks.

The failed-recovery observer uses 60 seconds to observe the existing provider
timeouts/retries before requiring the same HTTP 500, no application admission
and complete joined shutdown. This does not alter runtime/store budgets or the
15-second observers in the other profiles. Node startup recovery budgets do not
bound cell replica restoration. The report records the observer bound and actual
failed-read time, alongside file/state/receipt commitments and connection counts.
The [captured stall report](results/2026-10-28-filesystem-socket-stalls.md) retains
the initial observer-budget failure and the final artifact's actual results.
Kernel packet-loss/delay, peer transport, credential/disk and deployment-load
qualification remain separate.
