<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Three-node primitive refusal diagnosis

The instrumented SQL/KV/Queue probe fails all three cases. Three of 20 planned
30-second operation windows verify, one fails and 16 remain unmeasured. Two of
12 warmups verify, two fail and eight remain unmeasured. Eight emitted windows
retain 18 request errors. This narrows the refusal boundary; it does not qualify
throughput, repair the failures or replace the
[complete comparison history](three-node-primitive-capacity-2026-11-14.md).

| API | Verified windows | First failed window | New overload-phase observations |
| --- | --- | --- | --- |
| SQL | Four-client warmup, write and read | Sixteen-client warmup: one overload and 12 deadlines | Node 3: one group-budget refusal and one staging refusal before enqueue; other new counters stay zero |
| KV | Four-client warmup and write | Four-client read: two deadlines | All eight counters stay zero on all three nodes |
| Queue | None | Four-client warmup: three deadlines | All eight counters stay zero on all three nodes |

The SQL counters overlap: they describe the same refusal at group reservation
and at the staging boundary. Node/group counters combine item and byte pressure.
The default stage charge exceeds 64 MiB; a 256-MiB group bank fits at most three
such simultaneous reservations, before other retained work. The observation
identifies group admission, without independently distinguishing its item and
byte ceilings. It does not attribute the other 17 deadline errors to that bank.
Zero overload counts do not exclude writer contention, readiness, routing,
execution, proof waits or lost responses. Counters are node-wide observations,
not per-command outcome or retry proofs.

Queue reports 40 acknowledged IDs and 64 delivery-observed IDs. All 24 extra IDs
match the three unconfirmed enqueue attempts; no acknowledged ID is missing.
Reported backlog and DLQ counts are zero. The full observed body-hash map is not
retained in this failed window, so these ID counts do not independently verify
bodies or qualify delivery/settlement. Deadline expiry cannot establish that an
enqueue did not execute. The original attempt identities remain retained without
fresh-ID retries; no corruption or lost-acknowledgement finding follows.

The topology remains three separate one-vCPU/2-GiB nodes, three RF3 groups,
shared local RustFS and a separate one-vCPU/2-GiB client, with no extra swap.
SQL/KV enroll 24 cells; Queue enrolls 24 queues and 24 DLQs. Serving leaders start
balanced across nodes. Each case uses fresh storage. This diagnostic selects
Raft quorum, concurrency 4/16, five-second warmups and two reverse-order
repetitions of 30-second operation windows. Cases stop their workload at the
first failed window. Five-second operation, lease and callback windows, one SQL
worker, eight private workspaces and the existing image/manifest/admission banks
remain unchanged. No admission or retry behavior changes in this revision.

Run `d1786c729cf3` uses immutable image `16541b6a`, compiler inputs `0df275de`,
base `fd3a401d` plus the captured instrumentation/controller patch, and locked
Rust 1.98.1. The `lab` profile uses size optimization and thin LTO. The production
library and both binaries rebuild; load-client bytes remain unchanged from the
bounded-writer comparison. Full source hashes match before and after the run.
Only the Python evidence reader and its tests change afterward to recognize all
eight counters and preserve complete historical evidence schemas.

All nine node processes and three case stores exit zero; each failed load exits
one. No measured container restarts or is OOM-killed; sampled kernel OOM counts
are zero. Failed containers, logs and volumes remain retained. The original
RustFS service is restored under its unchanged identity and resource limits,
with readiness HTTP 200. Host-load observations are retained; this shared Colima
VM does not establish isolated performance or independent failure domains.

[Machine-readable observations](three-node-primitive-admission-2026-11-14.json)
include the planned matrix, raw-file hashes, per-node counter/operation deltas,
window percentiles and kernel resource observations.
[Compressed raw evidence](three-node-primitive-admission-2026-11-14.raw.json.gz)
retains complete owner snapshots, attempted/successful latency samples, original
errors, source inputs/patch and host activity. All eight emitted windows are
checked against retained logs and recomputed percentiles. Measurements stay
failed; missing windows are not zero-rate observations.

Four native integration tests, two admission unit tests and five performance
registry tests pass, alongside strict affected-target Clippy, Rust formatting
and layout. The Python performance suite runs 332 tests successfully with one
existing optional skip. Documentation and native dependency checks also pass.
The new controller fallback retains live observations if sampling detects load
failure before ordinary end collection; its partial-node behavior is unit tested.
This probe reaches normal end collection for all three cases.

The next priorities are bounded progress for received refusals before SQL enqueue,
independent deadline/writer/proof-wait diagnosis, and reconciliation of original
uncertain identities. Admission waiting must retain its bounded owner, original
identity, inputs and deadline, and wake after physical credits return. A worker
reply, continuation reopen or lost response cannot grant a blanket replay rule.
Fleet lease-renewal progress, sustainable manifest reclamation and the remaining
OPT/CG qualification gates stay open.
