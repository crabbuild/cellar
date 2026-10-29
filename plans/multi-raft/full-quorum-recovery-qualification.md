<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Full-quorum RustFS recovery at 10 GB

The 2026-10-29 UTC trial passed at **9,999,998,976 SQLite bytes** on frozen
commit `cd7712733e5201719b46bbe9f2d9954af5f3a777`. All three original worker
processes were killed with SIGKILL, joined, and their entire data directories
erased. Three fresh processes recovered the cell exclusively from RustFS,
served verified reads and acknowledged a new quorum write. Payload checks,
receipt reconciliation, asynchronous archive catch-up and retained-disk restart
all passed. The source, executable and immutable input remained unchanged.

This extends the earlier [single-owner-loss qualification](process-recovery-qualification.md)
to complete quorum loss. The [machine-readable record](full-quorum-recovery-qualification.json)
contains source identities, evidence hashes, process IDs and every phase time.
It qualifies this local scenario, not a production deployment or RTO percentile.

## Durability and admission

The original and restored commands used `Durability::Quorum`. The test waits for
asynchronous archive coverage before destroying the original quorum; object-store
publication does not participate in those command acknowledgements. Separate
fault-store regressions acknowledge quorum writes during archive failure and
verify lag/readiness transitions when publication resumes. A disaster test also
acknowledges a write beyond the selected archive and verifies restoration stops
at the confirmed cut, preserving the documented recovery-point boundary.

This trial uses an unmanaged Raft namespace. Managed fleet namespaces refuse
this disaster API; its admission claim also excludes concurrent fleet import.
The public disaster API verifies the pinned remote closure before fencing or
publishing the replacement birth. It uses bounded extent reads and needs no
10 GB operator-local database. The fixture's infrastructure adapter retains all
three joined child handles and verifies signal-9 exits and erased directories.
Production must supply its own trusted deployment inventory and launch-revocation
authority; a caller boolean or catalog object is insufficient.

Every destination has a new node/disk identity, group and incarnation, and an
absent local directory. Each independently downloads and seals all extents.
Recovery changes the root manifest's history binding while retaining the selected
SQLite geometry, position, payload objects, receipts and client floors. No
application callback, old membership, old term or old vote is replayed.

## Verified results

| Check | Result |
| --- | --- |
| Original processes | 8625, 8626, 8627; all observed exiting with signal 9 |
| Original disks | All three worker directories erased before operator recovery |
| Fresh processes | 9085, 9086, 9087; all initially empty |
| Object-store body bytes before admission | 10,000,180,326; 10,000,180,330; 10,000,180,330 |
| Peer-image recovery bytes | Zero on all three destinations |
| Restored local reads | All three returned counter 8 and the expected payload hash |
| Old receipt/token | Original receipt returned without a callback; old history token refused |
| New write | Counter 9 with quorum durability and new history identity |
| New receipt and payloads | Retry did not execute another callback; all three verified version-bound payload reads |
| Payload oracle | 9,953,083,392 bytes; SHA-256 samples at rows 1, 4747, 9492 |
| Asynchronous publication | New history reached locally confirmed current coverage |
| Restart | Three new processes resumed the new disks; counter 9 and receipt retry survived with zero callbacks |
| Closure | Three killed children joined; six fresh/resumed processes closed their owners and exited successfully |

Every fetched extent is authenticated in full; the application oracle additionally
checks total payload length and three tagged samples. The immutable source uses
repeated pseudorandom payloads with tagged samples and a quota tail, not independently
random contents in every row. Its SHA-256 is
`24a5f2ff65789e9bbc66b4fa69b470b2f98fe45cd7a3262f1434526d25778764`.

## Observed durations

Times use the controller's monotonic ledger. Setup includes preparing and
independently verifying three baseline replicas and publishing their archive.

| Interval | Seconds |
| --- | ---: |
| Setup through confirmed archive | 302.302318 |
| SIGKILL through erasure of all three disks | 4.916212 |
| Operator preflight and fresh-history publication | 6.622002 |
| Fresh quorum spawn through all three verified seeds | 87.673218 |
| Total replica loss through verified new quorum write | **110.926819** |
| Total replica loss through all three payload/receipt checks | 120.131072 |
| Total replica loss through current archive of the new history | 252.255302 |
| Native trial through final joined closure, including setup | 572.21 |

The new write was acknowledged before the new history's archive became current;
this is the intended asynchronous policy. A second complete replica loss during
that gap is bounded by the confirmed archive, not the latest quorum receipt.

The same frozen revision passed all 97 archive integration tests, including
namespace/import exclusion, archive outages, exact older cuts and corrupt or
missing objects. All 49 Python recovery-runner tests also passed.

It passed the 272 MiB full-quorum drill in 5.759080 seconds
from fault to verified write. The existing 272 MiB single-owner-loss process drill
also passed, in 7.141730 seconds. These are individual observations on one Apple
M2 Max/32 GiB host with worker data and RustFS on its internal SSD. OS caches were
not cleared. They are not directly comparable with the earlier external-USB-SSD
run, nor a cold-OS or distributed-network benchmark.

## Reproduction and evidence

Follow [the process-harness instructions](../../tools/cellar-recovery-tests/README.md#full-quorum-loss-with-asynchronous-archiving).
Use `tools/perf/process_recovery.py --scenario quorum-loss`, first with
`--profile 272mib`, then `--profile 10gb --fixture /absolute/readonly/source.sqlite`.
Provide private RustFS credentials and fresh admitted worker/evidence directories.
The controller uses real TLS Raft lanes between separate processes. Data and logs
remain outside build-cleanup directories.

The validator requires every phase in order, distinct process identities, all
three erased disks, object-store-only full-image reads, restored reads, new
quorum writes, receipts, payloads, restart and joined shutdown. It checks exact
test selection, source/executable identity and the unchanged fixture. The
one-hour capacity transfer budget is recorded; ordinary reads retain their
60-second deadlines. No compilation ran alongside the full-size physical trial.

The complete raw result is retained as `full-quorum-merged-10gb-1/result.json`, SHA-256
`643d5d033b0c2627fc9624a10ea04d78840536c27744b57e1d1daca04801d36c`.
The executable SHA-256 is
`9e3a30ce0385c98d66c0ea90b0308a29f8bf00392b9638744f1881c141a7ebe6`.
The JSON record binds all build inputs separately from the Git commit. Private
credentials, raw worker directories and RustFS object bodies stay outside Git.

An earlier small native trial passed its behavioral assertions but its runner
refused qualification because formatting overlapped source capture. It remains
recorded as unsuccessful. The subsequent small trial and the frozen-revision
repeat both passed the unchanged-source guard before the full-size run.

The earlier full-size trial on `9cdbf2b` passed in 103.155392 seconds from fault
to write. After integrating the managed-fleet changes from `main`, all three
process scenarios were repeated on the revision above. The JSON retains both
sets of evidence; neither pair of runs establishes a latency distribution.

## Remaining production boundaries

This does not qualify independent failure domains, Amazon S3 behavior, production
infrastructure fencing, every startup crash cut, pending-outbox reconciliation,
managed-fleet disaster orchestration, fleet-wide atomic restore, sustained archive
lag under load, or a latency SLA.
Pending old-origin outbox rows continue to refuse admission. Default quorum
acknowledgements do not promise recovery of an unarchived suffix after every
replica disk is lost. See the [disaster-recovery contract](disaster-recovery.md).
