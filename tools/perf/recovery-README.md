<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Native cold-recovery measurement

This runner exercises a focused immediate-repair fixture through the native SDK.
It measures from original-owner loss to an empty replacement's first local
indexed read and its first durable small write, including version-bound read-back.
Archive waiting, seed preparation/transfer/verification, promotion, receipts and
joined cleanup remain visible. The replacement SDK runtime is erased and recreated
inside the measured seed interval; it is not a prestarted runtime join. The existing full-capacity quota/restart drills
are separate regression gates.

Current failure injection cancels and joins the owner execution domain, then
erases its entire fixture-owned data directory. It is **not** a hard OS-process
kill. A successful pilot never qualifies a hard-crash recovery-time guarantee.
Public extent recovery and demand loading remain implementation work in the
[recovery-time strategy](../../plans/multi-raft/010-large-cell-recovery-time.md).

## Run a pilot

Choose a Cargo target directory on the workspace volume, unique to this checkout.
The runner builds an optimized test executable before starting measurements.
It requires a new private output directory and lists the exact selected test
before launching any trial. Supply `--toolchain VERSION` when choosing an
installed Rust toolchain explicitly.

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-recovery-my-checkout"
python3 tools/perf/recovery.py --backend peer --profile small-peer-regression \
  --repetitions 3 --output /tmp/cellar-recovery-small-001
python3 tools/perf/analyze_recovery.py /tmp/cellar-recovery-small-001 \
  --mode pilot --target-seconds 120 --minimum-samples 3
```

For physical storage, configure `CELLAR_LARGE_IMAGE_WORKDIR`,
`CELLAR_LARGE_IMAGE_ENDPOINT`, `CELLAR_LARGE_IMAGE_BUCKET` and the AWS region and
credentials in the process environment. Do not place credentials in command-line
arguments or reports. `CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR` optionally places
alternating workers on a second volume; record the layout with your run.

```sh
python3 tools/perf/recovery.py --backend rustfs --profile buffered-archive-272mib \
  --repetitions 3 --output /tmp/cellar-recovery-272mib-001
python3 tools/perf/recovery.py --backend rustfs --profile gigabyte-archive-1280mib \
  --repetitions 1 --output /tmp/cellar-recovery-1280mib-001
python3 tools/perf/recovery.py --backend peer --profile reference-10gb \
  --repetitions 3 --output /tmp/cellar-recovery-peer-10gb-001
python3 tools/perf/recovery.py --backend rustfs --profile reference-10gb \
  --repetitions 3 --output /tmp/cellar-recovery-rustfs-10gb-001
```

The 10 GB profile uses 9,999,998,976 physical SQLite bytes. An optional
`CELLAR_LARGE_IMAGE_FIXTURE` supplies the same verified template contract as the
full-capacity drill. The 272 MiB preflight creates its own template and ignores
that reusable fixture. Physical cases use a fresh random object-store prefix;
they retain their synthetic objects and never clean unrelated bucket keys.
Budget backend storage before repeating large campaigns.

A previously built executable can be reused with `--binary PATH
--binary-manifest PATH`. Its bytes and the complete current source fingerprint
must match the runner's recorded manifest. Stale or unbound executables refuse
execution. Avoid changing relevant sources during a campaign.

## Evidence and interpretation

`campaign.json` binds source/input/executable hashes, the exact profile and all
requested trial outcomes. Each `trial-NNNN/` contains raw native output and a
phase ledger. A failed or interrupted trial cannot silently become a smaller
successful population. The runner owns and joins its child process group on a
timeout or cancellation, then removes only the private worker directories it
allocated for that trial. Use `--keep-node-state` to retain those synthetic
directories for diagnosis. Supplied source fixtures and unrelated paths remain
untouched; test operation deadlines remain unchanged. The CLI handles SIGTERM
and Ctrl-C by unwinding those owners: it stops and joins the test child before
removing its worker data. Further termination signals are ignored during this
bounded cleanup so they cannot interrupt the join.

Retention reaches the native TempDir owners too, preserving surviving and
replacement node files. The fault injection still erases the failed owner's
directory and recreates the replacement. The default removes every allocated
worker directory after the test process has joined.

The raw log names the synthetic object prefix and includes archive encode,
upload, material-read, replay and head-confirmation phases. Publication spans
identify their publisher and requested cut; host debug events preserve typed
publication failures, including expected advisory contention. Failed native drills
also save the latest observed archive index, failure message, node performance
observations and cleanup outcome when the process can unwind. A hard process
kill may leave only the last incomplete ledger; neither outcome counts as a
successful trial. The 1,280 MiB diagnostic profile crosses SQLite's reserved-lock
page boundary and creates its own incompressible template.

The native ledger records monotonic timestamps for owner loss, joined stop,
local erasure, repair admission, archive waiting, seed installation, complete
hydration, first read, promotion, first verified write, receipts, payload checks
and joined cleanup. Timed first-use queries read a single indexed 1 MiB row and
the counter. The broader payload oracle runs afterward and can fail the entire
trial. A materialized first use requires complete hydration.

The analyzer reports nearest-rank p50/p95/p99 over the requested population.
Failed/missing completions count as infinity, rendered as `null` when a percentile
falls in that portion of the population. They remain target misses and prevent
acceptance. `analysis.json` separates an observed target result from qualification.

The native performance observations use their own domain clocks. Do not subtract
owner histogram timestamps from the fixture/controller clock. Histograms provide
bounded aggregate phase costs, not exact individual recovery quantiles. They now
resolve durations through 60, 120, 300, 900, 1,800 and 3,600 seconds.

`recovery_authorize` includes live grant acquisition and source-side waiting.
`recovery_peer_fetch` includes authenticated transfer and disk appends;
`recovery_peer_receive`, `recovery_append_queue` and `recovery_append` distinguish
its main components. `recovery_archive_fetch` observes the recovery-set download;
`recovery_archive_replay` includes physical verification and replay. Peer physical
verification has its own `recovery_peer_verify` phase. `recovery_install` covers
durable engine installation. `recovery_archive_wait` spans source-side retries
until an archive covers the fresh ReadIndex. Existing SQL queue and execution
metrics remain independent.

Snapshot encoding retains the aggregate `snapshot_encode` timer, with separate
`*_seed_encode`, `*_checkpoint_encode` and `*_transfer_encode` operations and
completed encoded-byte counters. This identifies competing encoder purposes.
`snapshot_encode_queue` records blocking-worker queue time. These counters do
not measure physical device I/O or prove publication/delivery. A cache hit still
runs the purpose's preparation timer, but adds no bytes to that purpose's encoded
counter. `snapshot_prepared_hits`, `snapshot_prepared_misses` and
`snapshot_prepared_reused_bytes` distinguish byte reuse from new encoding. These
are node-wide fixed counters; they do not assert that a particular cell received
or installed a body.

The host retains at most one prepared peer/checkpoint body. Its key includes the
engine owner, complete replica identity, birth/schema and immutable image marker;
each consumer must satisfy its current limits. Every capture rebuilds its own
cut, configuration, membership, grant and transfer lease. Archive publication
and standalone capture retain their own physical proof paths.

The cache keeps the anonymous file's ordinary byte/file scratch reservation until
a registered blocking owner physically closes it. Async handles retain metadata
and a mailbox only. One operation slot, one key slot and two handle slots reserve
at most 256 KiB of additional node metadata; source codec/wire reservations still
cover encoding and reads. A new key replaces only an idle body, and incoming
recovery may trim an idle body before scratch allocation. A live different-key
source refuses replacement. Archive scratch exhaustion trims idle cached bytes
before the ordinary publication retry; unrelated errors preserve the cache.
Transport shutdown wakes and joins the physical
owner even if a consumer or an unpolled stop waiter is retained. The source hashes
the outgoing stream and checks its complete encoded digest before its final
chunk; corruption invalidates reuse and refuses that attempt. Destination physical
verification remains required. Reuse saves preparation only for matching bytes;
it does not provide a faster cold restore for arbitrary new generations.

`--mode qualify` requires at least 100 trials, the full-size fixture, verified
reference hardware/workload evidence and a matching hard-failure/contention
suite. Current embedded pilots leave those qualification fields false. Empty
local state does not imply a cold source/store OS cache; the profiles label that
cache state uncontrolled. The 120-second and 30-second targets remain unqualified.

## Verification

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/perf -p 'test_recovery.py' -v
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/perf -p 'test_raft_metrics.py' -v
cargo test --locked -p cellar --test native_raft_large_images -- --test-threads=1
```
