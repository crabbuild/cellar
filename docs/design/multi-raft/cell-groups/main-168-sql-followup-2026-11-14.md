<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Main-168 SQL recovery and persistence follow-up, 2026-11-14 UTC

Purpose: Record SQL recovery reruns and constrained sync profiling with retained failures and unchanged durability proofs.

Machine-readable qualification and benchmark outputs are retained separately.
Keep generated run outputs outside this documentation tree; the prose summaries
preserve their source, scope and unresolved failures.

A fresh fetch still selects origin/main `50b927d9` (#168), already integrated
by `a16bbdc6`. PR 167 head before this addendum is `63feacd6`. Its compiler
inputs remain manifest `b417c200`; the immutable Linux image is `630bbfa5`.
The [complete updated-main verification](three-node-primitive-main-168-2026-11-14.md)
remains failed: eight native primitive controls and ten of twelve comparison
cases fail. These follow-ups neither replace that campaign nor establish a fix.

## Recovery diagnosis

The earlier successful-S3/extent recovery case timed out after retained restart.
The diagnostic preserves the five-second readiness helper and adds separate
source-group/SQL-group phase context, warning visibility and bounded failure-only
status snapshots. It adds one exact test entry for the previously failing case.
Production recovery logic, retries, admission and budgets are unchanged.

Four ranked hypotheses distinguish source reconciliation, SQL apply readiness,
node/control fencing, and state retained across fixtures. Their predictions and
the exact temporary patch are retained in the raw evidence.

The isolated case passes in **23.58 seconds** of test execution. The original
eight-configuration recovery matrix then passes in **198.90 seconds**, using the
same copied executable (SHA-256 `1edf499b683234c7f5cf8e1ec405fd5885dbb227a01eb2f144ec7149e9cc5eb2`).
No failure-only status probe fires. Driver elapsed times, including compilation
where applicable, are recorded separately from test times.

These runs use diagnostic source manifest `138c1216`, including the test-only
patch and logging change. They do not reproduce the earlier failure and cannot
identify its cause. The earlier broad setup failures and targeted post-restart
failure remain distinct, retained failures. Both diagnostic owners join with
exit zero; the temporary patch is removed and compiler inputs return to
`b417c200`. No production recovery change is made from an unconfirmed hypothesis.

## Constrained SQL sync profile

The original SQL quorum workload is repeated diagnostically with three separate
1-vCPU/2-GiB/no-extra-swap nodes, three RF3 groups, 24 logical cells and balanced
leaders. RustFS and the client retain separate 1-vCPU/2-GiB ceilings. Concurrency
1/4/16, five-second warmups, 30-second operations, two reversed repetitions and
five-second operation/lease budgets are unchanged.

Ubuntu `strace` package `6.8-0ubuntu2` is installed in the owned Colima VM.
Only `fsync`/`fdatasync` calls are traced, with file paths and syscall durations;
request bodies and credentials are not traced. Each attachment validates the
exact container ID, owner label, immutable image, process and resource limits.
Tracer processes run on CPU 5; nodes, store and client remain on CPUs 0–4.
This diagnostic changes scheduling and timing and supplies no comparable
capacity or speedup result.

The run verifies both c1 timed operations and the c4 write. Its c4 read fails
with four HTTP 503 responses; later windows remain unmeasured. Nodes 1 and 2
exit with `NodeLeaseExpired`; node 3 records an ambiguous renewal and exits
zero during controlled shutdown. The failed read includes closed transport,
closed runtime and deadline errors. Complete final Raft observations are
unavailable. This differs from the earlier untraced updated-main SQL quorum
case, which fails at c16 write and retains all three end observations without
Raft node lease-expiry exits. A common lease cause is not established.

All three tracer processes exit zero and their complete traces parse without
unmatched calls. The interval starts after topology readiness and includes
load, failure observation and shutdown, excluding initialization. Counts and
summed call durations across all three nodes are:

| Storage path | Completed sync calls | Call share | Summed syscall seconds |
| --- | ---: | ---: | ---: |
| Raft engine logs | 10,627 | 15.31% | 36.07 |
| Serving extent inventory | 23,376 | 33.69% | 69.61 |
| Group manifest pages | 6,858 | 9.88% | 22.47 |
| Archive journals | 9,738 | 14.03% | 30.70 |
| Independent archive verifier | 18,792 | 27.08% | 57.80 |
| Total | 69,391 | 100% | 216.65 |

Every observed sync returns success. Summed durations overlap across threads
and nodes: they are neither elapsed wall time nor additive request latency.
The Raft engine accounts for 16.65% of summed syscall duration, which does not
identify its critical-path contribution. Image publication and independent
archive work are substantial additional physical operations even in quorum
mode. Engine-only sync counters omit these paths. CPU work, queueing, network
and object-store latency remain outside this syscall profile.

The existing runtime already overlaps bounded persistence with control work;
one engine worker and cross-group batches already exist. The earlier untraced
SQL case records 15,247 groups across 15,087 persistence operations, or 1.01
groups per operation. That observation motivates measuring cohort formation;
it does not justify adding another worker or delaying sparse requests.

The next performance priority is to isolate critical-path image preparation
from background archive verification and measure bounded publication cohorts
and shared directory barriers. Preserve full LTX/image verification, exact
root/history binding, all required syncs, original deadlines, fair control
progress and physical cleanup ownership. These traces do not authorize skipping
proofs, increasing limits or attributing the cross-mode gap solely to consensus.
Original-deadline progress and Queue bucket drain backlog remain functional
priorities alongside this profiling work.

All five diagnostic containers stop with zero observed OOM/restarts; failed
volumes and logs remain retained. The controller and every tracer join. The
original RustFS is restored under its exact prior identity and limits and
returns readiness HTTP 200. No production source is changed. The missing
historical CG-01 record and broader OPT/CG qualification gaps remain open.

The machine-readable summary binds source,
images, executable hashes, timings, limits and trace counts. The
compressed raw evidence retains all
six emitted windows, samples/errors, available owner observations, cgroups,
complete traces, native diagnostic logs/patches and exact profiler scripts.
Actual generated credential values are checked absent before publication.
