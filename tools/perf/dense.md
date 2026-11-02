<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Dense cell state durability comparison

`dense.py` runs ten native cells with one GiB of dense payload in each cell.
Pass `--cell-mib 0` to start ten fresh cells without bulk payload instead.
Only the workload schema and initial business row are created before warmup;
timed one-KiB writes replace that row and add normal durability/receipt metadata.
This does not mean the physical SQLite database stays zero bytes. The empty
case audits every acknowledged business state before and after traffic, on all
three replicas for Quorum, and does not run dense-block hash checks.
Three separate containers each have one pinned vCPU, three GiB of memory and
no swap allowance. A pinned RustFS image runs separately with two vCPUs and
four GiB. Use an otherwise idle Linux Docker VM with at least six CPUs and
14 GiB available; eight CPUs and 16 GiB provide additional host headroom.

Build the current checkout with `raft.Dockerfile`, using a checkout-specific
target cache. Supply the immutable image ID. The runner compares its source
manifest and executable digest against this checkout before creating a cluster.
Finish edits and rebuild before running; the manifest includes this runner.

```sh
docker --context CONTEXT build -f tools/perf/raft.Dockerfile \
  --build-arg TARGET_CACHE_ID=cellar-YOUR-CHECKOUT-dense-target \
  --build-arg SOURCE_REVISION="$(git rev-parse HEAD)" \
  -t cellar-dense:YOUR-CHECKOUT .
image=$(docker --context CONTEXT image inspect cellar-dense:YOUR-CHECKOUT --format '{{.Id}}')
python3 tools/perf/dense.py --context CONTEXT --image "$image" \
  --output /absolute/writable-colima-mount/new-run
```

The output must be on an existing writable Colima mount with substantial free
space. Each mode retains its own data; allow at least 150 GiB for the three
modes plus growth and archive history. The runner creates unique containers and
networks, stops its exact containers after each mode, and retains data, logs,
raw samples, source/executable identities, operation ledgers and cgroup outcomes.
Private run configuration contains credentials; do not commit or print it.

For native guest filesystem I/O, pass `--guest-data-root /mnt/cellar-dense-NAME`
pointing at an existing, exclusively owned ext4 mount inside that VM. The runner
creates unique data subdirectories through a private helper container and keeps
reports/configuration on the host mount. Record the loop device, backing file,
filesystem UUID and sparse/prewritten policy alongside the results. A sparse
backing file does not establish reserved physical capacity. Retain its mapping
for inspection; a VM restart requires explicit loop/mount recovery before use.
An owned bind mount of the VM's native data filesystem can also provide this
path; record the VM disk image and native filesystem identity in that case.

`--preload-quorum` measures an already populated Quorum dataset without replaying
thousands of large setup commands. It uses the initial-image procedure from
`crates/cellar/tests/support/raft_full_capacity.rs`: real permanent initial-member
claims, identical initial committed positions on all three disks, and complete
template/extent digest checks before any runtime starts or command is acknowledged.
Each cell has distinct deterministic payloads. A bounded helper creates the
images; the normal SDK then supplies live linearizable admission, version tokens,
replica reads, timed commands and verified archive coverage. This is lab initial
state construction, not a production bulk-import API or ingestion qualification.
Before timing, archive coverage is verified and one ordinary command per cell
initializes its private writer with a separate 120-second setup budget. These
latencies remain in the ledger as `preloaded_warmup`; timed writes keep the
normal five-second budget. This separates cold writer creation from steady traffic.
Use `--modes quorum --guest-data-root ... --preload-quorum`. The helper's source,
container, full-image digests and logs are retained separately. The normal
non-preloaded setup remains available and its failures must not be hidden by
the preloaded experiment.

An initial functional check uses `--cells 3 --cell-mib 4 --seconds 2 --repeats 1
--concurrency 1 3`. This is a separate smoke experiment and cannot substitute for
the one-GiB measurements.

The workload creates one-MiB payload blocks from independently reproducible
SHA-256 output, four blocks per bounded transaction. Payloads are dense and
incompressible. It then measures one-KiB hot-row writes and small state reads
with persistent HTTP connections at concurrency 1, 3 and 10, twice in reversed
order. The default window is twenty seconds; the recorded experiment explicitly
used `--seconds 10`. Workers use disjoint cell sets, so commands never overlap within a cell.
Complete successful responses determine throughput and latency. The client
never retries writes and retains failures and ambiguous outcomes. A short
write warmup is recorded with repeat -1 and excluded from measured comparisons.
The preloaded Quorum case warms at concurrency one before the timed sweep.

For separate diagnostic runs, the Raft executable accepts `RAFT_HOST_POLL_MS`
(default 25) and `RAFT_ARCHIVE_CHECKPOINT_MS` (default 1000; zero disables the
time-based archived-cut checkpoint trigger). The latter retains the entry-count
checkpoint trigger, ordinary archive publication and foreground durability
proofs. `/resources` reports the selected intervals. Change one setting per
experiment and record it; these options do not establish production tuning.

Each cell's logical payload size and first/middle/last block hashes are checked
before and after traffic. Every returned business state is checked against an
independent model. Quorum checks all three replicas using the latest version
token and requires the archive to cover every last acknowledged index. Sampled
hashes do not establish a full payload scrub, and this fixture does not test
crashes, disk loss, recovery or independent host availability.

Quorum uses the public extent-backed image configuration with a 1,152-MiB image
quota, leaving room for SQLite and receipt overhead. It has three voters and
waits for Raft majority commitment. Its measured reads execute on a follower
with the last acknowledged version token. Fleet uses native ownership and
acknowledges either the required follower persistence or bucket publication,
whichever completes first. Bucket waits for object-store durability. Fleet
and bucket reads route to the authoritative owner. These are different
guarantees and read paths; throughput ratios do not establish equivalent
durability or consistency.

Quorum population commands have a separate 120-second setup timeout. Timed
commands retain the SDK's five-second timeout. Setup timeouts and ambiguous
outcomes remain failed attempts; the runner does not retry those mutations.

All containers share one VM, physical host and storage device. The default uses
direct virtiofs data binds; the optional guest mount adds native ext4 over its
recorded backing file. Memory samples include Docker's working-set view; final cgroup counters
retain current/peak charge and OOM events. CPU quotas and memory/swap ceilings
are inspected before traffic. Latencies exclude the client's ledger syncs;
throughput includes durable mutation-evidence waits and client bookkeeping.
Read-only evidence batches are synced after timing by default, and each window
reports its sync count, retained bytes and excluded flush duration. The runner is closed-loop
and does not measure open-loop overload or schedule-to-completion latency.
`completed` covers the timed-traffic and data-audit gates. Inspect
`post_stop_outcomes` and `post_stop_oom_nodes` separately: the embedding fixture's
default SIGTERM shutdown hands cells off, and that lifecycle operation can fail
even after successful traffic. A process exit is not a joined SDK drain proof.

The executable also accepts `RAFT_EXTENT_SEGMENT_MIB` (default zero, meaning the
ordinary file inventory). Values 1–64 opt into append segments in fresh extent
inventories. `/resources` reports the selected value. With segments selected,
`RAFT_EXTENT_BATCH_BARRIERS` controls compatible commit-watermark sharing; image
records in a single segment still share their image's final barrier. Compare
fresh inventories with identical worker counts and durability policy, and retain
both throughput and leader/follower phase timings. This experimental backend
uses conservative whole-segment GC; mixed live/dead records can retain capacity.


## Client evidence and read throughput

`dense.py` defaults to `--ledger-mode group --read-ledger-policy window`.
Concurrent durable appends may share an fsync that started after their complete
records were written. Invocation append still returns only after persistence,
before sending a mutation. Completion append still waits before reusing that
cell. Later records need a subsequent barrier; failures poison admission and
wake all waiting callers. No background writer or deliberate batching delay is
added. Other users of the shared `Ledger` retain its serial `sync` default.

Read-only windows buffer immutable encoded evidence in client memory and sync it
after the traffic timer. The result is published only after this final barrier
succeeds. Records remain bounded by both `--read-ledger-max-records` (200,000) and
`--read-ledger-max-mib` (128). Exhaustion or flush failure fails the history;
no records are silently dropped and mutations cannot enter a read batch. These
are encoded-byte limits; record objects and active caller inputs add bounded
host-language overhead. Reduce long window duration or choose an explicit larger
budget when needed. `--operations read` selects a read-only sweep after normal
initialization. Node storage/durability barriers are unaffected by this client
measurement policy.

Read-only sweeps support up to 256 concurrent readers, including more readers
than cells: `--operations read --concurrency 10 32 64 --cells 10 --cell-mib 0`.
Workers map across the ten cells and readers may share a cell. Write sweeps
still require concurrency at most the cell count, preserving disjoint writers.
Each window reports `successful_by_worker` to expose readers that received no
responses and reconcile their counts with the total successful operations.

For a serial evidence control, select `--ledger-mode sync
--read-ledger-policy durable`. Both policies are recorded in the report. Do not
attribute a client-policy throughput difference to a service optimization.
The window's `ledger` object reports successful evidence syncs and excluded
flush milliseconds; the final ledger snapshot includes durable/total records,
failed state, byte limits, wait/sync times and maximum cohort size. Shutdown
closes connections, joins active ledger callers and retains failed histories.
