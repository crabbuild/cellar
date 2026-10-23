# TiKV comparison and Cellar performance investigation

Status: PR #98's conflicts are resolved through main `1455fb0`. Direct-image
candidate `71074d6` reduces physical preparation cost and raises observed
successful throughput, but **fails end-to-end performance qualification**:
window two has 5,697 known overload refusals at node one's retained-entry gate.
All 4,500 replica values and 1,500 archive positions verify without reconciliation.
The candidate remains unqualified; no repeatable, error-free TPS gain is claimed.
The archive budget remains 256 MiB. Earlier comparisons and failures remain
preserved, including the rejected capture candidate `8a34732`.

## Seek before evaluating archive eligibility

The next candidate addresses a concrete cost in `pump_archive`: its filtered
map iterator evaluated eligibility before checking the round-robin cursor.
Selecting the next cell after cursor 1,200 in a dense 1,500-cell map therefore
evaluated 1,202 cells. An unsuccessful search evaluated every cell twice.
Eligibility can read the group cache, so these are not merely key comparisons.

Selection now seeks into the ordered map after the cursor, then wraps through
the disjoint range ending at the cursor. Each cell is inspected at most once;
the dense next-cell case evaluates one cell. The production predicate, in-flight
exclusion, cyclic fairness, capture/publication bounds, retry timing, admission
limits and persistence proofs are unchanged. This applies the scheduling lesson
from TiKV's bounded active-FSM batches without claiming to implement TiKV's
mailboxes or replacing Cellar's archive scheduler with a ready queue.

Both regression tests fail on the original selection and pass with ordered
ranges. The selection test covers all 64 eligibility subsets of six cells,
initial/no cursor, wraparound, a cursor outside the map and an empty map.
[Final local validation](evidence/tikv-archive-cursor-local.json) passes 518
library tests, 52 archive tests and nine
multi-group tests; the ignored multi-group case is a harness child entry point.
Strict all-target/all-feature Clippy passes. This reduces verified scheduling
work, but does not yet prove a sustained TPS improvement or explain the brief
retained-entry breach. A fresh 1,500-group observation keeps the image change,
256-MiB byte budget and 4,096-entry cap and requires zero timed errors plus
exact replica/archive verification before a paired confirmation.

[Preservation evidence](evidence/tikv-archive-cursor-preservation.json) records
lossless compression of 428 closed log files and the verified return of nine
historical node directories to internal storage. Original aliases, volume
options and Docker inventories remain intact; read-only probes through all
nine original named volumes verify the preserved bytes. Fresh benchmark data
still uses its original internal backing and external RustFS VM disk.

## Direct exclusive image creation experiment

The next candidate removes redundant per-image namespace operations while
preserving the applied-image contract. Previously each preparation exclusively
created a `.stage` file, verified and synced its contents, exclusively created a
final-name placeholder, renamed over it and synced the directory. Preparation
now uses the existing exclusive readable/writable creation contract directly on
the final name. Full replay/digest checks, file sync, directory sync, conservative
two-image capacity charge and engine-marker ordering are unchanged. An entry
stays busy until both barriers finish; its filename cannot select a recovery
head or expose a query pin. An uncertain creation or failed cleanup still
quarantines admission, and an unexpected existing name is never overwritten.

This addresses the per-cell physical work that TiKV amortizes through shared KV
apply batches; it does not implement a shared SQLite engine or defer image
persistence. The prior profile identified costly image preparation and namespace
work, and the admission-budget experiment showed that moving waits downstream
was insufficient. The ranked alternatives are private-capture directory scans
and full-image copy/hash work after the namespace costs are reduced.

A public-boundary regression first observes 18 file creations for nine verified
images and fails the one-file bound. The candidate passes with nine creations,
zero renames and nine image syncs, while every previous image remains readable
and unchanged. During the directory barrier, attempted adoption returns `Busy`.
Process-kill coverage includes the first partial write, before file sync, after
file sync, after directory sync and after the durable marker. Collision,
ambiguous creation, corrupt payload and shared reclamation tests retain their
assertions. No public filesystem API or file contents format changes.

[Local validation](evidence/tikv-direct-image-local.json) passes 516 library tests,
52 native archive tests, 19 execution,
29 host, 27 image/LTX and nine multi-group tests. The snapshot run passes 42
cases and exposes two seed-fixture hooks still armed by rename; those cuts are
moved to exclusive image creation. All three peer-seed, RustFS-seed and snapshot
crash matrices then pass, including the new partial-write cut and unchanged
recovery assertions. The LTX crate has no standalone library tests; its physical
behavior is exercised by the native suites. Strict all-target/all-feature Clippy
for both affected crates, layout, native dependency policy and docs checks pass.
The first incomplete integration attempt selected the write-only creation API;
its failed read-back results are retained. The final code uses the existing
exclusive `create_read_write` API, which already supplied the stage handle.

The [completed comparison](evidence/tikv-direct-image.json) uses frozen control
`6f57b79` and candidate `71074d6`, the same 256-MiB
archive budget, four SQL workers / 64 reservations, four archive-cut slots,
three pinned 1-vCPU / 3-GiB nodes and 1,500 three-voter groups. Each fresh cohort
uses two 180-second windows at 24 writers and requires exact 4,500 replica and
1,500 RustFS archive checks. The only manifested production source difference
is `crates/cellar-ltx/src/generation.rs`.

| Build | TPS, windows 1 / 2 | Successful-request p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- |
| Control `6f57b79` | 249.64 / 207.66 | 190.78 / 230.12 | 0 / 0 |
| Direct image `71074d6` | 291.97 / 258.20 | 159.84 / 207.93 | 0 / 5,697 |

Mean successful TPS rises 20.31%, but this is descriptive and **not a qualified
gain**. Every failure is a known `Overloaded` HTTP 503 from node one, within a
4.9-second issuance span. Its retained-entry refusal counter increases by exactly
5,697; the configured node-wide limit is 4,096 data-bearing entries. There are no
ambiguous outcomes and no timed retries. Both cohorts verify exact data, stop
cleanly without OOM and retain all images, volumes and raw evidence. Peak node
memory is 1.58 GB for control and 1.92 GB for candidate. Final archive catch-up
takes 0.41 versus 5.76 seconds; neither proves continuous backlog behavior.

In error-free window one, committed-image preparation drops from 6.07 to 4.63 ms,
staging from 16.70 to 14.63 ms and SQL queue residence from 8.07 to 6.30 ms. Shared
Raft persistence stays near 0.97 ms. In window two, preparation is 7.13 versus
4.94 ms, but candidate staging includes 5,650 refused attempts and admission wait
includes 12 failed outcomes. Its all-outcome staging mean cannot be called
successful-write service time; success-weighted admission formulas are omitted.
The first analyzer correctly rejected its all-success assumptions; the retained
second analysis reconciles successful/failed counts and keeps the failure gate.

Other builds occur in 6/37 and 0/36 control samples, versus 3/36 and 29/36 candidate
samples. One host sample overlaps the refusal burst and observes another build;
this is insufficient causal evidence. Candidate node CPU usage is 72–83% of its
allocation, with one throttled period across both windows. Completed control
queue observations never exceed one second. The next diagnostic must distinguish
archive publication/coverage lag from delayed retained-entry release or local
scheduling stalls. Current boundary group lag is not the admission entry count,
and cannot identify which explanation caused the brief cap breach. A reversed
performance confirmation is deferred until the zero-error gate passes.

Between cohorts, [closed host evidence was returned to internal storage](evidence/tikv-host-artifact-return.json)
to restore external physical headroom. All 24,124 files match content, mode,
owner and modification time; symlink targets and original access paths remain.
Docker images/volumes and fresh node/RustFS backing remain unchanged. The first
copy check caught seven reset symlink timestamps and refused release; after
repair, complete inventories match before the redundant original is released.
This intervening preservation and the shared host limit causal attribution.

[First](evidence/tikv-closed-log-compression.json) and
[second](evidence/tikv-closed-log-compression-r2.json) preservation records retain
1,212 Raft log files across ten stopped verified cohorts using transparent APFS
compression. Exact content, lengths, modes, owners and modification times are
verified before releasing redundant physical copies. Original named volumes
pass read-only hash probes; volume options and image/volume inventories remain
unchanged. This saves 13.82 GB of physical allocation without pruning logical
data. Fresh benchmark node directories remain on their original filesystem and
are not compressed during the workload.
A further [source-archive compression record](evidence/tikv-closed-context-compression.json)
retains all 34 frozen archives while recovering 0.86 GB externally. An attempted
compression of closed build libraries saved no space and changed no library files.

## Larger admission budget shifts waiting into SQLite work

The [same-image comparison](evidence/tikv-admission-budget.json) uses executable
`6f57b79` for both configurations: three pinned 1-vCPU / 3-GiB nodes, 500 leaders
and 1,000 followers each, four SQL workers / 64 reservations, four archive-cut
slots, 500-ms ticks and 24 writers. Each cohort has fresh persistent volumes,
a cold benchmark VM and two 180-second windows. Only the node-wide archive byte
budget changes; the entry cap, durability proofs and production default do not.
Every node reports the expected configured budget at both measurement boundaries.

| Archive budget | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- |
| 256 MiB control | 248.01 / 225.93 | 180.69 / 206.32 | 0 / 0 |
| 512 MiB candidate | 251.70 / 223.99 | 176.49 / 233.50 | 0 / 0 |

Both independently verify all 4,500 exact replica values and 1,500 RustFS archive
positions without reconciliation, then stop cleanly with no OOM kill. Peak node
memory remains below 1.66 GB. Final archive catch-up takes 0.38 / 5.74 seconds
for control/candidate; these final waits do not establish continuous archive lag.
All images, volumes and raw measurements remain retained.

The reservation hypothesis is only partially supported. Admission wait per
successful write falls from **32.81 / 38.86 ms** to **0.30 / 0.59 ms**, and the
fraction waiting falls from 95–97% to 2–4%. However, SQL queue means rise from
**8.21 / 8.83 ms** to **14.46 / 15.95 ms**, staging from **17.22 / 18.12 ms**
to **21.84 / 24.03 ms**, and replica preparation from **6.10 / 6.69 ms** to
**7.26 / 8.03 ms**. Shared-log persistence also increases from 1.01 / 1.13 ms
to 1.21 / 1.45 ms. These operations overlap and have different counts; they
cannot be summed into request latency. Reducing admission wait alone does not
remove the downstream image-processing cost.

Mean TPS changes by only **+0.37%**, with lower second-window TPS and higher
worst-window p99. This does not justify increasing the default or claiming an
optimization. A reversed-order confirmation is not pursued for this negligible
observed benefit. Other compiler activity appears in 36/36 and 22/36 control
samples versus 3/36 and 3/37 candidate samples; host swap remains about 9,232 MiB.
The shared host prevents a causal non-regression guarantee despite the identical
executable and workload. Node CPU usage stays around 72–78%; completed control
queue observations over one second are zero in all four windows.

The next priorities are reducing actual private capture/preview and replicated
image work, distinguishing I/O service time from SQL queue residence, and
eliminating repeated archive materialization while retaining dependency checks.
A bounded SQL-owner comparison can test overlap, but must retain the same total
reservation capacity and memory limit. Simply increasing queues or archive
credits has not improved this workload. TiKV-style separation of applied and
checkpointed progress remains a separate recovery protocol design, requiring
log retention, snapshot/GC and crash qualification before any image sync is
removed from Cellar's acknowledgement path.

At pinned TiKV `b3995dab`, transaction scheduler `handle_flow_control` charges
`to_be_write.size()` to the flow controller; deadline failure returns consumed
quota. Cellar must reserve before invoking an arbitrary SQLite callback, then
reduce to verified physical size after preparation. These are different safety
contracts. TiKV's calculated-size flow control is useful design context, not
justification for bypassing Cellar's pre-callback bound. Its shared KV apply
batch also cannot be reproduced by changing Cellar's shared Raft-log writer.

After preserving the pair, internal/external free space is approximately
3.1 / 2.9 GiB. Another full cohort needs verified preservation/compression or
relocation before the unchanged capacity guards can pass. No data is pruned.

## Admission timing result and budget-experiment motivation

The [observation cohort](evidence/tikv-admission-timing.json) uses frozen
`7bbf71e` with the same production policies and resource limits. Two 180-second
windows at 24 writers achieve **254.75 / 232.31 TPS**, **179.19 / 211.87 ms p99**
and zero errors. All 4,500 replica values and 1,500 RustFS archive positions
verify without reconciliation. Shutdown is clean, no OOM occurs, peak node
memory is 1.69 GB and final archive catch-up takes 5.78 seconds. The observation
passes the accepted sub-500-ms gate, but not sustained sub-200-ms p99. Its
historical reference is not a new paired control or proof of a throughput gain.

Admission wait affects **93.31% / 94.96%** of successful commands, averaging
**32.85 / 37.11 ms among waiters** and **30.65 / 35.24 ms per successful write**.
Started/completed wait counts reconcile with successful stage/write counts,
with no failed, abandoned or unavailable timing. Client means are 94.16 / 103.26
ms. This supports reservation waiting as a significant cost; aggregate timings
do not attribute client p99. Admission evaluations average only 0.082 / 0.087
ms at about 4.45 checks per write. Replacing the scans alone has a much smaller
measured opportunity than shortening reservation residence.

Each node consumes 72–79% of its pinned CPU across the boundary intervals.
There are only two throttled periods in the first window and one in the second,
so container CPU quota throttling does not explain these results. Shared-host
scheduling and I/O pressure remain possible: other compilers appear in 0/37
and 13/37 timed host samples, with 9,240 MiB of host swap in use. Control queue
means rise from 32.56 to 54.19 ms, with 6,979 completed queue observations over
one second in window two. These observations overlap across protocol frames
and cannot be counted as distinct slow commands.

The next single-variable experiment exposes the existing archive byte budget in
the fixture, retaining its **256-MiB default**, and compares 256 versus 512 MiB
on the same executable. The default reserves each callback's maximum 64-MiB
image plus envelope before the callback runs; only completed preparation reduces
that reservation to verified physical size. A larger bounded budget may admit
more preparation concurrently without changing durability, but could increase
SQL queueing, memory or retained backlog. Success therefore requires higher
sustained TPS, zero errors, p99 below 500 ms, exact replica/archive verification
and no memory-limit failure. The 3-GiB limit, SQL workers, archive publishers,
entry limit and request concurrency stay fixed. No production default changes.
The runner verifies the actual byte budget on every node before and after load;
the independent analyzer rejects inconsistent node observations.
The verifier regression first rejects the old analyzer's acceptance of a
mismatched 256-MiB node in a 512-MiB report, then passes for both boundaries.
All 78 Raft Python tests and six fixture Rust tests pass; strict fixture Clippy,
format, layout, native dependency policy and docs checks pass. The production
runtime is unchanged from the measured admission-timing build.

[Lossless evidence compression](evidence/tikv-closed-evidence-compression.json)
retains 268 closed host-only JSON files at their original paths, verifying their
SHA-256, size, mode, owner and modification time. Native APFS compression saves
1.39 GB of physical allocation without removing logical evidence, images or
volumes. This restores external-disk headroom for the bounded comparison.

## Archive-admission timing and restored benchmark headroom

The next observation separates time in admission policy evaluation from time a
command spends awaiting a reservation. Fixed `archive_admission_check` and
`archive_admission_wait` histograms add no group/request labels or new admission
budget. Check completion includes grants, deferrals and refusals; decision
counters still distinguish the outcome. Wait starts once per deferred command,
ends on grant/refusal and records abandonment on owner release after cancellation
or retirement. It excludes subsequent authority/SQL work. The native gated
reservation regression first fails because the wait timing is missing, then
passes for both a granted waiter and a cancelled waiter that never executes.

Prior diagnostics measured admission scans around 0.10 ms and archive inventory
scans around 0.16–0.17 ms on an older build. Current decision counts alone cannot
show whether reservation wait or scan CPU is significant. The ranked hypotheses
are reservation wait, repeated scan work, then post-admission image/archive work;
measure the current runtime before choosing a throughput change. The inventory
selector also checks eligibility before rejecting keys preceding its cursor,
but its impact needs qualification rather than assuming it dominates.

[Artifact preservation evidence](evidence/tikv-node-artifact-relocation.json)
records relocation of three stopped, verified cohorts: nine node volumes,
63,351 files and 7,208,229,865 logical bytes. The scoped external archive mount
is added only to the idle benchmark VM. Its existing home mount, CPU/memory/disk
settings, other profiles and selected Docker context are unchanged. Every
existing image and volume is retained. Original node paths remain symlinks;
content, names, permissions, ownership, modification times, absent extended
attributes and internal hardlink topology are verified before releasing the
redundant source copies. Read-only Docker probes through every original named
volume return the expected hashes. Historical benchmark records remain immutable.
Fresh node data still uses real directories on the original internal filesystem.
Host free space rises from 3.61 to 10.46 GiB, restoring the unchanged 5-GiB gate.
The first relocation attempt stops before copying because Python lacks xattr
APIs; the separately recorded retry uses the native xattr command to verify the
same metadata condition. No logical data or evidence is pruned.

## Bounded control-window refill experiment

The failed confirmation motivates three ranked hypotheses: the fixed control
batch barrier amplifies a slow reply; synchronized group ticks exceed lane
service; or shared-host pressure dominates both effects. A real TLS regression
isolates the first: queue nine frames behind a primer, read eight, withhold seven
replies, acknowledge the first and require the ninth frame within 300 ms. The
old sender fails with `one verified reply did not refill the bounded control
window`. The initial fixture attempt timed out before reaching that assertion;
its corrected primer synchronization produces the targeted failure.

The new sender refills each completed slot while retaining the eight-frame
bound. It holds node/peer permits outside cancellable read/write futures,
authenticates replies in FIFO order, rechecks route identity and retains each
frame's original deadline. Partial I/O futures survive scheduling events; errors
close the connection before resolving pending deliveries. No message is blindly
replayed. The data lane's batch policy is unchanged. Control exchange timing now
counts frames rather than batches; queue timing remains comparable.

At pinned TiKV `b3995dab`, `src/server/raft_client.rs::AsyncRaftSender::poll`
repeatedly drains bounded `BatchMessageBuffer` work subject to gRPC sink readiness.
Cellar's change borrows continuous bounded draining while preserving its own
per-frame authenticated admission protocol. TiKV's streaming RPC completion is
not a substitute for Cellar's Raft append responses or durability gates.

The refill regression passes. The complete 27-test transport selection passes,
including lost/reordered replies, revocation, cancellation/physical joining and
one-byte duplex buffers through the new production path. A further regression
holds an older frame through a successful reply and refill and proves its
original deadline still expires. These results establish transport behavior,
not a throughput or p99 improvement. Native validation passes 516 library tests,
52 archive, 19 execution, 29 host, nine multi-group and 44 snapshot tests; child
entry points remain harness-driven. Both new refill tests pass ten repeated runs.
Strict all-target/all-feature Cellar Clippy passes after replacing a manual
conditional dequeue with `pop_front_if`; the route check still precedes removal.
The full candidate cohort uses the frozen admission-reuse control as a
historical reference, not a fresh adjacent pair. Both use the same load AST,
three 1-vCPU / 3-GiB nodes, 500 leaders and 1,000 followers per node, four SQL
workers / 64 reservations, four archive-cut slots, and two 180-second windows
at 24 writers. Each starts fresh persistent volumes after a cold benchmark VM.

| Build | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- |
| Sliding control `a10215e` | 249.58 / 220.35 | 190.00 / 249.74 | 0 / 0 |
| Historical control `230cd4f` | 248.85 / 226.54 | 209.29 / 253.99 | 0 / 0 |

Mean TPS is 234.97 versus 237.70, an observed 1.15% decrease. P99 is lower,
but the preferred sustained sub-200-ms target remains unmet. Both candidate
windows satisfy the accepted sub-500-ms / zero-error gate. All 4,500 replica
values and 1,500 RustFS archive positions verify without reconciliation; the
cohort stops cleanly, with no OOM kill and all volumes retained. Peak node
memory is 1.63 GB. Final archive catch-up takes 5.77 seconds; boundary per-group
lag is at most eight entries. These are not continuous steady-state lag bounds.

Candidate control-queue means are 32.93 / 39.90 ms versus 34.73 / 44.89 ms for
the historical control. Neither has completed control-queue observations over
one second. The deterministic held-reply stall is fixed, but this run does not
establish that removing it raises sustained throughput. Other builds occur in
6/37 and 33/36 candidate samples inside the measurement brackets, versus zero
for control. No builds from this task overlap the timed workload. The cohorts
are nonadjacent, and shared-host pressure limits relative attribution.

Candidate staging averages 16.99 / 18.29 ms, SQL queueing 8.19 / 8.81 ms,
committed-image preparation 6.08 / 6.75 ms and log persistence 1.00 / 1.18 ms.
These overlapping operations have different counts and cannot be added as a
request-latency breakdown. Temporary byte-reservation deferrals remain frequent
at 159,198 / 147,293 decisions; all observation and retained-history refusals
remain zero. Measuring reservation wait duration and archive read purpose is a
stronger next attribution step than increasing queue limits again.

The transport candidate remains reviewable with its demonstrated local progress
benefit; no TPS gain or relative non-regression guarantee is claimed. Fresh
paired confirmation remains outstanding. After preserving this cohort, host free
space is about 3.6 GiB, below the unchanged 5-GiB next-cohort guard. Additional
measurement requires safe preservation/relocation of closed artifacts; no data,
images or volumes have been pruned.

## Reversed-order capture confirmation

The [confirmation evidence](evidence/tikv-capture-confirmation.json) records
control first, then candidate, with the same frozen images and load AST as the
[first pair](evidence/tikv-capture-reuse.json). Each cohort starts fresh persistent
volumes and a cold benchmark VM: three 1-vCPU / 3-GiB nodes, 500 leaders and
1,000 followers each, 24 writers, two 180-second windows, four SQL workers /
64 reservations and four archive-cut slots. No timed mutation retries occur.

| Build | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- |
| Control `230cd4f` | 248.85 / 226.54 | 209.29 / 253.99 | 0 / 0 |
| Rejected capture `8a34732` | 281.40 / 197.25 | 213.73 / 941.58 | 0 / 7 |

All seven errors are HTTP 503 Ambiguous, returned after about five seconds.
Reconciliation uses only the original uncertain identities, inputs and deadlines,
outside timing. Both cohorts ultimately verify all 4,500 replica values and
1,500 RustFS archive positions and stop cleanly with their volumes retained.
Successful reconciliation does not convert the failed timed window into a pass.
The failed report has no peak-memory measurement. No OOM kill is reported.

The strongest new lead is control-message queue residence. In the failed window,
1,223,767 completed queue observations average 384.08 ms; 487,535 exceed 100 ms
and 147,639 exceed one second. The preceding candidate window averages 38.44 ms
with none over one second. The fresh control windows average 34.73 / 44.89 ms,
also with none over one second. Only 26 SQL queue observations exceed one second;
no other measured operation has a completed observation over one second.
These overlapping operation counts are not distinct client requests or additive
request latency. The sender currently processes fixed windows of eight control
frames and waits for the entire window before selecting more work. A bounded
sliding window is the next falsifiable transport experiment, preserving FIFO
reply authentication, permits, deadlines, revocation and physical cancellation.

Neither confirmation build records retained-history or observation refusals.
Candidate byte-reservation deferral decisions are 161,423 / 102,510, including
rechecks; these do not measure distinct requests or wait duration. Other builds
appear in 0/36 and 16/36 candidate samples within the metric brackets, versus
0/36 and 0/36 for control. Shared-host pressure, tick bursts and transport
service remain alternative explanations; the queue association alone does not
prove the candidate caused the failure.

A proposed issue-time phase analysis was quarantined as invalid: the harness
sorts successful latency samples independently from receipts, so those arrays
cannot be joined by position. Its phase findings are not used. Error records
contain their own durations; aggregate histograms and benchmark percentiles
remain valid. Aligned successful-request timing is an attribution gap.

The candidate's first attempt stopped before load at the unchanged 5-GiB host
free-space guard. A separately recorded retry followed verified relocation of
107 closed host-only artifact directories (24,123 files, 4,709,456,257 logical
bytes) to the workspace volume. Content, metadata and inventories were checked,
and original paths remain symlinks. No Docker image, volume, node backing data
or VM mount was removed or changed. The confirmation evidence hashes both the
failed preflight and the relocation record. The restored runtime matches
`230cd4f` exactly; its archive-refusal/workspace-reuse regression passes freshly.

The original ephemeral candidate `68b82e1` also failed at 24 and 16 writers and
was reverted by `69192d2`; [that evidence](evidence/tikv-ephemeral-capture.json)
remains available. Admission reuse alone was effectively neutral at 246.43 versus
244.52 mean TPS in its [own comparison](evidence/tikv-admission-reuse.json).

## Rejected combined capture and admission-reuse candidate (first pair)

Candidate `8a34732` reintroduces bounded ephemeral capture on top of `230cd4f`.
Its fresh control is that admission-reuse build, isolating capture savings from
the already measured refusal fix. The native image/receipt regression first
reports eight disposable LTX file writes for eight commands, then passes with
zero after the capture change. Restart still recovers receipts and accepts the
next command. The archive-outage regression additionally requires no disposable
capture writes while preserving a clean owner through refusals; an actual
callback mutation failure still forces reconstruction.

The first hypothesis is that refusal-induced reconstruction amplified the
earlier capture candidate's overload bursts. The alternatives are that faster
staging outpaces archive service independently, or that shared-host variation
dominates the end-to-end difference. Both candidate and control expose the same
admission reason counters. Qualification still requires two complete zero-error
windows below 500-ms p99, exact 4,500 replica values and 1,500 archive positions;
local capture savings alone do not qualify this experiment.

Only disposable private capture changes. Encoded cuts remain bounded, every
checkpoint cut is returned in order, partial capture failure poisons its owner,
and volatile position cannot fall back to older local history. The independently
verified immutable image and shared Raft log retain their acknowledgement
barriers. TiKV's separation of replayable work from recovery authority motivates
avoiding redundant private output, but this does not implement its shared KV
apply batch or delayed tablet persistence.

The benchmark data disk grows from 224 to 288 GiB to supply inodes without
deleting prior evidence. All 326 volumes and 46 images are retained, other
profiles and the selected Docker context are unchanged, and each node still has
one vCPU and 3 GiB.

The fresh pair uses 1,500 groups, 500 leaders and 1,000 followers per node,
four SQL workers / 64 reservations, four archive-cut slots and 24 writers.
Both builds use the same load-function AST and two 180-second windows, with
fresh persistent volumes and a cold benchmark VM for each cohort. There are
no timed mutation retries and no builds or profiling from this task during load.

| Build | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- |
| Combined capture `8a34732` | 277.52 / 229.89 | 260.35 / 296.57 | 0 / 0 |
| Admission-reuse control `230cd4f` | 245.48 / 224.09 | 199.22 / 226.49 | 0 / 0 |

Both cohorts verify all 4,500 replica values and 1,500 RustFS archive positions
and stop cleanly without OOM kills. Peak node memory is 1.80 GB for candidate
and 1.65 GB for control, within the 3-GiB limits. Every image, volume and raw
outcome is retained. Candidate staging averages 12.08/15.05 ms versus
16.79/17.74 ms for control, reductions of 28.0%/15.2%. The first-window native
file-write regression and these stage measurements support the intended local
cost reduction; overlapping operations are not additive request latency.

Candidate throughput falls 17.2% between windows. Staging, committed preparation,
log persistence and object PUT time all increase. Other builds appear in 3/37
and 21/36 candidate samples inside the metric brackets, versus 3/36 and 0/36
for control. This association does not prove the slowdown's cause. The observed
8.06% mean TPS gain comes with higher p99 and cannot be treated as a repeatable
causal effect from one sequential pair. The reversed-order confirmation above
subsequently failed; the preferred sustained sub-200-ms p99 target remains unmet.

Neither build records observation failures, retained-byte/entry refusals or
reservation-entry deferrals. Temporary byte-reservation deferral decisions are
155,853/132,158 for candidate and 150,039/144,953 for control. These include
waiter rechecks, not distinct requests or wait durations. Candidate per-group
archive lag is at most nine entries at the measured window ends. Final archive
catch-up takes 5.80 seconds for candidate and 0.35 for control; different
outstanding work makes these waits unsuitable as an archive service-rate
comparison. Continuous lag, admission wait duration and repeated material reads
remain the next attribution gaps.

Validation passes 7 disposable-capture and 6 checkpoint tests; 514 Cellar
library tests; native archive 52, execution 19, host 29, multi-group 9 and
snapshot 44; strict all-target/all-feature Clippy for cellar and cellar-ltx;
formatting, layout, dependency policy and documentation checks. The LTX library
target itself contains no enabled tests; its two integration targets provide
the capture/checkpoint coverage above.

## Admission-reuse candidate

The retained admission-reuse change addresses refusal amplification without removing private
LTX files or relaxing archive limits. Registered commands check a known archive
refusal after duplicate receipt lookup and validation, before mutation begins.
The serial SQL worker retains an owner only when staging has joined, no proposal
is active and the executor reports `Ready`; both staging-error cleanup and the
subsequent host discard observe that condition. Authority and published-image
checks still precede the next callback. Dirty, prepared and submitted owners
retain their existing cleanup and uncertainty rules.

The real Node/SDK regression first failed on an unnecessary private-image copy.
Moving only the executor check still failed because worker cleanup discarded
every error. With both changes, three refused commands and a duplicate receipt
produce no private-image copy; a subsequent S3 command succeeds. A deliberately
overflowing SQL update then fails during its callback result read, rolls back,
and forces reconstruction before the next successful command. The committed
counter remains exact. Existing execution, cancellation, publication and
multi-group tests pass alongside this regression.

Fixed admission counters distinguish observation failures, retained bytes,
retained entries and outstanding reservation bytes/entries. They report the first
encountered decision reason, including duplicate attempts and waiter rechecks;
they are not HTTP refusal counts. Native outage tests distinguish retained-entry,
retained-byte and temporary reservation pressure. No budget or decision changes.
This should identify whether the prior overload bursts came from archive
admission; a staging-failure count alone cannot establish that cause.

TiKV's pinned v1 `apply.rs::process_raft_cmd` uses write-batch savepoints and
rollback on command failure while preserving the shared engine. Cellar's change
is narrower: it preserves only a private owner that has not begun mutation;
it does not assume arbitrary SQLite/capture failures can reuse the workspace.
Its paired throughput qualification appears below. The deterministic refusal
regression establishes avoided copies; the timed workload does not exercise
retained-limit refusal and cannot measure that benefit.

A repeated archive-suite run also reproduced the prior producer-shutdown
fixture failure. Its `committed > 1` observation included the startup leader
barrier, so it did not prove the command had committed before requiring an
ambiguous shutdown outcome. The fixture now records each pre-command committed
position and requires a later committed/applied cut. Producer bounds, physical
joins and the ambiguous-result assertions remain unchanged. Repetition also
exposed a second fixture assumption: bundled cuts can share one object read, so
an exact read count cannot measure occupied group slots. This producer-limit
case now explicitly uses one group per batch; separate bundle regressions retain
their normal batching settings. The full 52-case archive suite and 30 consecutive producer-shutdown repetitions pass. Original failed runs remain in diagnostic evidence.

### Admission-reuse paired qualification

Frozen candidate `230cd4f` and control `86297e7` each run two 180-second windows
with 24 writers, three 1-vCPU / 3-GiB nodes, 500 leaders and 1,000 followers per
node, four SQL workers with 64 total reservations, and four archive-cut slots.
The load-function AST matches; each cohort starts with fresh persistent node
volumes and a cold benchmark VM. There are no timed mutation retries.

| Build | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors |
| --- | --- | --- | --- |
| Admission reuse | 251.67 / 241.18 | 214.73 / 220.32 | 0 / 0 |
| Integrated control | 257.43 / 231.62 | 202.72 / 247.56 | 0 / 0 |

Both verify all 4,500 replica values and 1,500 RustFS archive positions and stop
cleanly. All images, volumes and raw outcomes remain retained. Peak node memory
stays below 1.75 GB, with no OOM kills. The candidate's mean TPS is 0.78% higher;
that difference is not evidence of a causal gain. Other builds appear in 52/88
candidate host samples and 0/91 control samples. Swap is approximately
9.4–10.4 GiB for the candidate, versus about 9.4 GiB for control; raw MiB values
are in the evidence. Sequential order
and shared-host variation prevent a non-regression guarantee from one pair.

The initial control attempt stops before loading data at the two-million-free-
inode guard, with 1,800,324 free. A separately named retry preserves that record
and requires 1,486,856 free inodes: four times the candidate's observed 321,714
consumption plus 200,000 spare. No runtime capacity or workload changes; no
data is pruned. The retry completes and verifies normally.

The candidate reports 151,897 and 143,901 temporary byte-reservation deferral
decisions, including waiter rechecks. Retained-byte, retained-entry, observation
failure and reservation-entry refusal/defer counters remain zero. These are
decision counts, not distinct requests or wait durations. The control lacks
these counters; its missing values mean unavailable, not zero. Boundary archive
lag stays below ten entries per group in the candidate, but boundary samples
and final catch-up do not establish a continuous steady-state lag bound.

| Candidate operation | Mean ms, windows 1 / 2 | Interpretation |
| --- | --- | --- |
| Command staging | 15.65 / 15.68 | Private SQL, capture and preview construction remain substantial. |
| Committed image preparation | 5.72 / 5.80 | Each replica still prepares its own image. |
| SQL worker queue | 7.93 / 7.21 | Includes different SQL work types; not a command-only queue measure. |
| Shared-log persistence | 0.90 / 0.98 | Lower per-operation cost than image work; different counts prevent adding or directly attributing request latency. |

After the rejected capture and completed transport experiments above, the
remaining priorities are:

1. Confirm relative transport performance with a fresh matched pair after safely
   preserving closed artifacts to restore benchmark disk headroom. A deterministic
   progress fix and one passing cohort do not establish a repeatable TPS gain.
2. Measure time spent awaiting archive admission and distinguish archive read
   purposes before changing budgets. Worst-case image/envelope reservations
   defer callbacks despite small actual commands; current counters cannot
   attribute their latency. `object_get` combines GET, HEAD and range reads,
   so its count alone cannot establish payload amplification or RustFS saturation.
3. Reduce repeated archive material loading and replay without skipping stored
   dependency verification, missing-object refusal, or publisher failover.
4. Evaluate applied-versus-checkpointed progress as a separate architectural
   change. TiKV's shared write batch amortizes state-machine work across regions;
   Cellar's shared Raft log does not amortize independent SQLite image copies.
   Log replay retention, snapshot/GC boundaries and crash recovery must be
   designed and tested before delaying authoritative image persistence.

## Current performance priorities

The [internal-storage profile](evidence/tikv-current-io-diagnosis.json)
identifies command staging, image construction, SQL worker queueing and archive
publication as useful optimization targets. It instruments the frozen
`12d6e87` runtime without changing durability or retry behavior. Three nodes
retain one vCPU, 3 GiB, 500 leaders and 1,000 followers each; each node has two
SQL workers and two archive publishers. The subsequent capture-handoff
comparison below still fails sustained throughput, latency and refusal gates.
The bounded-concurrency comparison below varies SQL workers at fixed total
queue capacity, then archive publishers. Its tuned configuration passes the
500-ms gate but slows from 245.20 to 168.56 TPS between windows. A subsequent
nine-minute profile finds growing archive-tail costs and SQL waits, without
reproducing refusals. Size-based archive rotation reduces repeated payload
reads in a deterministic regression, but its uninstrumented cluster run again
returns overload refusals. The subsequent bounded SQL priority comparison passes
the 500-ms gate, but still slows between windows. Attribution of earlier refusals
and longer-run slowdown remains open; neither host contention nor an archive
limit is yet a proven explanation for those refusals.

| Priority | Observed gap | Next improvement to evaluate |
| --- | --- | --- |
| 1 | Archive publication grows from 25 to 50 ms as mean tail count grows from 8 to 18 in the tuned diagnostic. Size-based rotation reduces payload reads by 70.4% in a deterministic test, but earlier overload bursts remain unexplained and the latest candidate still slows 14.2%. | Measure steady-state archive lag and retained history on every replica. Evaluate coordinated publication and fewer repeated object reads/replays, preserving follower recovery proofs and publisher failover. Retain refusal counters to distinguish retained bytes/entries, reservations and deadline expiry; the diagnostic has not reproduced those refusals. |
| 2 | With four SQL owners, committed preparation waits 6.0–6.9 ms, publication 5.5–6.0 ms and archive capture 8.6–10.2 ms in the tuned diagnostic. Bounded priority now passes the measured 500-ms gate at 229.64 mean TPS, but misses 200 ms. | Retain the fair scheduling candidate and validate it after integration with main. Measure queue and execution tails separately under sustained offered load; keep per-cell order, 64 total reservations, byte budgets and physical joins. A deeper queue alone is not a latency improvement. |
| 3 | Tuned staging averages 16.5–20.4 ms. The earlier deeper profile attributes 5.1–5.4 ms to preview construction and about 1.5–1.6 ms each to private-file cleanup and private LTX writes. | Reduce disposable file creation/cleanup and image copying. The bounded encoded-capture handoff already removes read-back; the ephemeral-output experiment below removes these writes but fails sustained admission qualification and is reverted. Diagnose refusal/rebuild amplification before retrying it. Keep authoritative image/log barriers intact. |
| 4 | Shared Raft persistence averages 0.71–0.72 ms but handles only about 1.24 groups per batch. | Improve bounded coalescing and investigate combined progress writes. Measure the benefit before adding delay; avoid busy spinning on a one-vCPU node. |
| 5 | Every command still constructs immutable images across replicas. | Explore separate applied and checkpointed progress with Raft-log replay, following TiKV's recovery model. This needs explicit replay retention, snapshot/GC boundaries and power-loss recovery tests before changing acknowledgement semantics. |

Image file sync averages 0.20–0.22 ms and directory sync 0.41–0.43 ms at six
writers on this backing. Removing those barriers is not the first experiment.
Admission scans average about 0.10 ms and archive inventory scans 0.16–0.17 ms;
they are lower priorities than the measured execution and queueing costs.
These are nested operation means, not additive portions of request latency.

Main's `0903e80` (PR #94), observed at `64ebaf7`, independently adds archive
batching, transport changes, resource accounting and a richer performance runner.
Integration resolves the overlapping source changes; the combined runtime
passes the bounded integrated qualification below; an isolated causal comparison
remains outstanding. The combined
archive scheduler bounds total retained cuts by `max_producers`, independently
of the two-cell object batch limit. A completed capture has a fixed 5-ms
collection deadline and its own host wakeup, so another cell's held SQL capture
cannot extend that deadline. Compatible namespaces, schemas, roots and stores
are required for batching. Archive seed reads additionally reserve up to 32 MiB
for four containing-bundle buffers, retaining the reservation through cancelled
physical extraction.

The integration preserves main's eight-frame control window, per-frame
authenticated outcomes, connection accounting and bidirectional exchange. A
new real-TLS regression exposes the merged serial data lane: a second group's
write-bearing append cannot reach its peer before the first admission reply.
The restored data window drains at most sixteen already-admitted frames and
1 MiB of conservatively charged bytes. A larger frame travels alone; a deferred
frame keeps its FIFO position, queue timer and original permits. There is no
wait to fill a window. Invalid replies cannot acknowledge their matching frame;
earlier authenticated outcomes remain observed admission. Prior throughput
results cannot be assigned to the merged runtime; its own results appear below.

The native bundle failure exposed two integration boundaries. Its fixture
assumed one old publication; concurrent producers need their old head
publications synchronized before testing the next shared object. Even with
both publications completing in the same millisecond, a receipt lookup kept
one cell SQL-busy during the archive inventory scan. The cached empty scan
then delayed that newly idle cell until maintenance, missing the collection
window. SQL completion now invalidates that scan only when unarchived work
exists outside an in-flight publication. The previously failing native case
passes with the exact shared-object, lost-PUT confirmation, four-callback,
independent restore and corruption-refusal assertions intact. This is an
archive scheduling fix, not evidence of a measured TPS increase.

The integrated library passes all 514 cases. Native archive (51), execution
(19), multi-group (9) and operations (23) suites pass; the shared-bundle case
also passes ten consecutive runs. Strict all-target/all-feature Clippy for
`cellar` and `cellar-ltx` passes after consolidating a duplicate fixture module.
A startup fixture's one-slot runtime reproduced a read-only status admission
race on the third isolated run; a backtrace identifies the post-startup status
query. That query now retries only `Overloaded` inside one fixed deadline,
retaining the pre-start peer refusal and successful attached-group assertions.
Thirty consecutive isolated runs pass after this fixture correction.
Earlier archive shutdown and concurrent restart-lock failures remain unproven
despite passing reruns. Preserve those failures; these correctness checks do
not establish an integrated TPS increase.

### Storage-engine comparison and the next performance boundary

The pinned TiKV source is `b3995dab28308f09cbb7c125bf32f789dbc203fe`.
Its `raftstore/src/store/fsm/apply.rs` shares `ApplyContext::kv_wb` across region
preparation and writes it through `write_to_db`; callbacks and apply-state
progress follow the engine write. Cellar shares the Raft log, but each cell
still has an independent SQLite image, LTX chain and committed-image marker.
Transport batching and shared Raft fsync therefore do not amortize all apply
work. Replacing per-command images with periodic checkpoints is a separate
recovery design, requiring exact log retention, applied/checkpointed indices,
receipt replay and crash tests before acknowledgement semantics can change.

TiKV's `src/server/raft_client.rs::BatchMessageBuffer` bounds batches by bytes
and message count, retains an overflowing message, and streams queued work.
Cellar can use that scheduling principle while retaining its authenticated
per-frame admission replies and separate control/data budgets. The new TLS
regressions exercise this boundary directly. TiKV's
`raftstore/src/store/async_io/write.rs` also batches shared log work, but the
measured Cellar log persistence cost is smaller than staging and archive costs;
adding an arbitrary coalescing sleep there is not the first optimization.

TiKV v2's `raftstore-v2/src/operation/ready/apply_trace.rs` separately tracks
flushed data and persisted applied progress so recovery knows what to replay.
This is distinct from v1's shared engine batch: neither mechanism permits
Cellar to discard its only acknowledged image without a retained, verifiable
replay path. Avoiding disposable LTX output also needs an explicit bounded
in-memory capture continuation: the current `Db` reads its last L0 cut while
checking capture/checkpoint progress, so simply skipping file writes is unsafe.

### Rejected ephemeral-capture experiment

Candidate `68b82e1` changes only disposable private capture output: bounded
encoded cuts transfer directly to validation without creating local LTX files.
An exclusive owner retains continuation metadata, refuses file-history fallback
and fails closed after partial capture. Authoritative log/image and
acknowledgement barriers remain unchanged. The native regression observes eight
private LTX writes before the change and zero afterward, then verifies duplicate
receipts and a fresh command after restart. LTX tests cover forced multi-cut
checkpoints, empty captures, seeded continuation, bounds failures and restoration.

The [paired frozen-runtime runs](evidence/tikv-ephemeral-capture.json) use the same
load AST, three 1-vCPU / 3-GiB nodes, 500 leaders and 1,000 followers each, four
SQL workers / 64 reservations and four archive-cut slots. Each cohort starts
from a cold benchmark VM and fresh persistent node volumes, drains seed archives,
and runs two 180-second windows. Candidate runs precede their matching controls.
The only setting changed between pairs is client concurrency.

| Writers | Build | TPS, windows 1 / 2 | p99 ms, windows 1 / 2 | Errors, windows 1 / 2 |
| --- | --- | --- | --- | --- |
| 24 | Candidate `68b82e1` | 276.86 / 228.35 | 249.79 / 425.70 | 0 / 448 |
| 24 | Control `86297e7` | 253.43 / 236.85 | 211.26 / 239.37 | 0 / 0 |
| 16 | Candidate `68b82e1` | 184.92 / 239.82 | 770.32 / 220.45 | 1,939 / 0 |
| 16 | Control `86297e7` | 238.90 / 204.46 | 189.13 / 237.96 | 0 / 0 |

TPS counts successful writes; latency percentiles describe successful responses,
with failures reported separately. The 24-writer candidate's failed window has
437 overload refusals, 10 unavailable outcomes and one ambiguous outcome. Only
the uncertain original command is reconciled outside timing, using its original
identity, input and deadline. Exact 4,500 replica values and 1,500 RustFS positions
then verify, including 92,484 acknowledged mutations with seed and reconciliation.
That timed run remains failed. The 16-writer candidate's 1,939 failures are all
overload refusals. Both controls and the other candidate verify the same complete
replica/archive sets. All cohorts stop cleanly without OOM kills, retaining
volumes, images and raw evidence.

Existing metrics sampled outside each timed process show a local cost reduction:
the first 24-writer window averages 12.02 ms staging versus 15.72 ms for control
(23.5% lower), while committed preparation is 5.61 versus 5.74 ms and shared-log
persistence 0.90 versus 0.91 ms. This does not establish a sustained gain: failed
candidate windows have worse staging and queue costs, and one faster clean
window cannot replace the zero-error cohort gate. The 24-writer candidate has
other builds in 33/88 host samples, its control 0/88; the 16-writer candidate has
0/105 and its control 30/87. Host swap remains about 10.6 GiB. Sequential order
and host contention limit causal attribution; neither is used to excuse errors.

The experiment passes 13 LTX tests, 514 library tests and native host (29),
archive (51), LTX (26), multi-group (9) and snapshot (44) cases. Execution passes
19 on rerun, after reproducing the earlier directory-lock failure; the affected
restart case then passes 30 isolated repetitions. Strict all-target/all-feature
Clippy, formatting, layout, dependency policy and documentation checks pass.
A local link initially failed when the workspace disk filled; only this
checkout's rebuildable incremental cache was removed, preserving evidence,
images, volumes and other checkouts. Correctness tests do not qualify throughput.
The candidate is reverted pending admission diagnosis and a sustained passing run.

The next investigation should distinguish retained-byte, retained-entry,
reservation and execution failures with bounded counters at their decision
sites. The failed 24-writer window contains 432 failed SQL staging operations;
engine persistence and committed-image preparation have no failed operations.
Boundary archive snapshots cannot identify transient admission peaks.

A concrete code opportunity is visible in
[`stage_registered_kind`](../../crates/cellar/durability/raft/execution.rs):
`allow_new == false` is checked inside the transaction callback, after `stage`
marks the private owner as requiring rebuild. Consequently a known admission
refusal can force a later private-image reconstruction without executing business
logic. A regression should prove duplicate receipt lookup still succeeds while
new work is refused, the private owner stays reusable, and actual callback or
capture failure still requires rebuild. Move only the known refusal check after
receipt lookup and before mutation; do not change archive bounds or durability.
This inefficiency is established by code inspection, but is not yet proven to
cause the measured overload bursts.

### Integrated runtime qualification

The [frozen run](evidence/tikv-main-integration.json) uses three 1-vCPU / 3-GiB
nodes, 500 leaders and 1,000 followers per node, four SQL workers with 64 total
reservations and four retained archive-cut slots. Each node's disposable
verification workspace is capped at 512 MiB; acknowledged logs and images stay
on persistent storage. Timed mutations never retry.

| Window | Successful TPS | p99 (ms) | Write errors |
| --- | --- | --- | --- |
| 1 | 240.21 | 226.93 | 0 |
| 2 | 213.90 | 252.26 | 0 |

Mean throughput is 227.06 TPS. The earlier frozen `3aeb3ad` mean was 229.64 TPS
with a worst-window p99 of 220.11 ms. These sequential runs change both runtime
and runner and share a busy host; they do not isolate a regression or establish
an additional speedup. The observed 10.95% within-run slowdown remains an
optimization target. Other builds appeared in 84/89 host samples, with about
9.36–9.40 GiB of swap. No builds or profilers from this task ran during timing.

Independent receipt analysis verifies 83,296 acknowledged writes including
1,500 seed writes, all 4,500 final replica values and all 1,500 archive positions.
The final archive wait is 5.89 seconds after the replica scan, not a measure of
steady-state lag. Cgroup memory peaks are 1.40/1.49/1.48 GiB, within the 3-GiB
limits; sampled Docker memory during timed writes is lower and should not be
substituted for the full-run cgroup peak. All nodes and RustFS stop cleanly,
without OOM kills; all four persistent volumes and raw evidence remain retained.

The next isolated comparison should record archive lag and SQL queue tails
throughout the run. The primary remaining opportunity is reducing repeated
image construction and archive replay/publication work. Bounded transport
pipelining and SQL-idle archive wakeups have direct regression coverage, but
this integrated experiment alone does not establish their individual TPS gains.

Remaining measurement gaps are longer steady-state runs, per-replica archive
lag, hot-cell skew, larger SQLite images, mixed reads/writes and failure under
load. Initial/final leader archive coverage is not a steady-state lag measure.
The current node volumes use host storage through Colima virtiofs; compare
native Linux block storage as a separate environment experiment. Preserve
shared-host measurements and record contention instead of attributing all
slowdowns to it. Check both available bytes and inodes before setup.

The diagnostic ran four 60-second windows at 3/6/6/3 writers: respectively
134.84/198.12/200.17/108.76 successful writes/sec, with p99
64.60/86.38/94.29/94.72 ms and zero write errors. It verified all 4,500 replica
values and 1,500 archive positions, then stopped cleanly with volumes retained.
The first three windows recorded no concurrent builds in the host samples;
two samples in the final window did. Host swap remained about 12.6 GiB.
Archive publication rose to 24.35 ms in the final window, but workload, history
and host activity also changed, so its slowdown is not causally attributed.

This instrumented run selects the next experiments. It does not replace the
failed longer runs or establish a control/candidate speedup. Acceptance remains
sustained successful TPS with p99 below 500 ms, preferably below 200 ms, zero
write errors, bounded archive lag and exact replica/archive verification.

### Capture handoff experiment

The [deeper staging profile](evidence/tikv-stage-io-diagnosis.json) separates
the costs inside command preparation. At six writers, staging averages
12.4–13.4 ms: preview construction takes 5.1–5.4 ms, SQLite/receipt execution
2.1–2.3 ms, private-file cleanup 1.5–1.6 ms, and capture 3.7–4.0 ms. Capture
includes 1.3–1.4 ms listing and rereading newly encoded LTX files and
1.5–1.6 ms writing those files. These nested means are not additive exclusive
request latency. The diagnostic verified all 4,500 replica values and 1,500
archive positions with zero write errors; concurrent builds appeared in 39
of 66 host samples. Its timings select an experiment, not a causal speedup.

`Db::capture_with_files` now transfers the encoder's cuts directly to command
preparation, bounded by file count and total encoded bytes. It includes all
cuts emitted by a checkpoint and releases the batch owner on failure. The
existing configured local-write durability, bounded decoding, base/result
checks, image sealing, durable Raft log and applied-marker proofs remain.
This removes read-back of disposable capture output; private LTX writes and
cleanup still occur. Encoder output is not described as verification of the
local file.

The real SDK regression observed eight private LTX reads for eight commands
before the change and zero afterward, while retaining exact results, eight
image seals, eight directory barriers, and receipt recovery after restart.
New LTX cases cover multi-cut checkpoints, restoring after private workspace
loss, count/byte refusal and durable sync failure. The native archive,
execution, host and snapshot binaries pass 134 cases; four child-process
fixtures are invoked by parent scenarios. Ordinary all-feature LTX tests pass
11 cases; these do not include the external `cellar_internal_tests` harness.
Strict all-target/all-feature Clippy for both affected crates, the LTX build
without default features, formatting, module layout, native dependencies and
documentation checks also pass. The [validation record](evidence/tikv-capture-handoff-validation.json)
preserves the failing regression and passing checks.
### Uninstrumented capture comparison

The [120-second comparison](evidence/tikv-capture-handoff-comparison.json)
compares frozen `12d6e87` control with `63069af` candidate. Their runtime
manifests differ only in `db.rs` and `working.rs`; load generation is identical.
Both use six/twelve/twelve/six writers after a cold benchmark-VM restart, with
fresh node volumes and the same 160-GiB VM data disk. Expanding the disk retained
all old images/volumes and did not change node CPU or memory limits.

| Build | Writers | Mean window TPS | Worst window p99 | Unsuccessful writes |
| --- | --- | --- | --- | --- |
| Control | 6 | 154.21 | 245.66 ms | 1,020 overload refusals |
| Candidate | 6 | 130.69 | 638.98 ms | 377 overload refusals, 1 ambiguous outcome |
| Control | 12 | 140.87 | 791.28 ms | 0 |
| Candidate | 12 | 138.99 | 408.33 ms | 4,345 overload refusals |

Neither setting passes both repeats' zero-error and p99-under-500-ms gates.
Candidate six-writer throughput is 15.3% lower; twelve-writer throughput is
1.3% lower. These are observed regressions, not attribution to the handoff:
unrelated builds appeared in 124/125 control and 105/116 candidate host samples,
with substantial, changing swap use. A lower twelve-writer candidate p99 does
not offset its refusals. Successful-request latency excludes refused requests;
the table always reports those failures separately.

The control initially returned `Unavailable` while seeding. It resumed with
the same pending command identities and original deadlines, then verified all
1,500 seed values and initial archive coverage before timing. Its original
setup failure remains recorded. It completed final verification of all 4,500
replicas and 1,500 archives. Candidate setup succeeded directly, but its final
window's ambiguity aborted normal verification. The uncertain original write
was resolved within its original deadline, followed by a separate exact
4,500-replica/1,500-archive verification. Both cohorts stopped cleanly, retaining
all volumes. The candidate measurement remains failed.

The legacy postfailure helper also retried definitive overload refusals outside
measurement. This added unnecessary reconciliation work and one transport
timeout. The expired request was never reissued: every retained immutable image
on all three replicas lacked its receipt, exact counter values matched the
known successful-command set, and absence was confirmed again after shutdown
joined all work. Future reconciliation should prioritize uncertain outcomes
and leave definitive refusals unexecuted. These post-run operations never
contribute to measured TPS or turn a failed window into a passing one.

### Sustained pressure profile and SQL worker experiment

The [sustained pressure profile](evidence/tikv-pressure-diagnosis.json)
instruments `d8b9109` with explicit refusal reasons, retained-history counters
and archive timing. Two 180-second twelve-writer windows complete at
122.77/133.49 TPS and 444.58/226.90 ms p99, with zero write errors and all
4,500-replica/1,500-archive checks passing. No instrumented refusal path fires.
Retained history peaks at 2,436 entries and about 35.4 MiB, below the refusal
limits. This run does not reproduce or establish the cause of earlier overload
bursts; diagnostic overhead and changing shared-host pressure remain limits.

SQL queueing is a directly observed cost. Archive capture waits 22.17/18.85 ms
while its work takes 0.018/0.027 ms. Committed preparation waits 23.81/17.27 ms,
and publication waits 21.17/14.58 ms for only 0.58/0.55 ms of work. Staging
takes 21.54/20.32 ms. Average node CPU is 57–58% in the first window and 65–68%
in the second. Archive publication grows from 17.80 to 25.95 ms as its retained
tail grows; payload loading takes 2.41/5.22 ms and physical replay 0.84/2.06 ms.
These nested means must not be added as exclusive request latency.

The fixture exposes SQL owner count and reservations per owner, checking actual
physical owner/capacity observations before and after traffic. This follows
TiKV's use of bounded execution pools to overlap independent work. Its analyzer
rejects mismatched counts/capacities, missing observers and closed queues.

### Bounded SQL and archive concurrency comparison

The [comparison record](evidence/tikv-bounded-concurrency-comparison.json) uses
one uninstrumented executable from `b08cb6c` for all four cohorts. Each has fresh
persistent node volumes, a cold benchmark-VM restart and two 180-second windows
at twelve writers. The sequence is two SQL owners/two publishers, four/two,
two/two again, then four/four. Two owners have 32 work slots each; four have 16,
retaining 64 total slots. Node limits stay at one vCPU and 3 GiB.

| SQL owners / archive publishers | Mean window TPS | Worst window p99 | Refusals | Every window passes zero-error / 500-ms gate |
| --- | --- | --- | --- | --- |
| 2 / 2, initial control | 162.27 | 209.87 ms | 5,359 | No |
| 4 / 2 | 215.72 | 145.64 ms | 4,790 | No |
| 2 / 2, repeated control | 172.35 | 400.54 ms | 0 | Yes |
| 4 / 4 | 206.88 | 355.05 ms | 0 | Yes |

Four SQL owners improve observed mean throughput by 25–33% over the controls,
but two publishers still fail the zero-error gate. Four publishers pass it:
their windows achieve 245.20/168.56 TPS and 114.82/355.05 ms p99. Mean successful
TPS is 20.0% above the repeated error-free control, with an 11.4% lower worst
p99. No configuration passes both windows with zero errors and p99 below 200 ms.
The tuned second window is 31.3% slower than its first; it does not establish
stable capacity or explain the refusal mechanism.

Increasing publishers also increases aggregate capture allowance from 8 to
16 MiB and the number of bounded recovery sets in flight. Each capture remains
limited to 4 MiB and each recovery set to 256 MiB. Retained-history limits remain
256 MiB/4,096 entries; business budgets, total SQL reservations and node cgroup
limits are unchanged. Actual capture/SQL capacities and startup settings are
recorded. The four/four cohort averages 74–78% node CPU and peaks near 406 MiB
per node over traffic and final verification.

All four cohorts verify exactly 4,500 replica values and 1,500 archive positions,
then stop cleanly with volumes retained. Timed writes never retry, and there are
no uncertain outcomes or postfailure reconciliation in these cohorts. Final
archive coverage waits 5.81/0.38/11.22/5.85 seconds respectively after replica
verification; those waits are not measurements of steady-state archive lag.

Shared-host builds appear in 77/102, 32/89, 72/94 and 41/90 samples respectively;
swap declines from roughly 10.6 to 9.6 GiB. These unequal conditions limit causal
attribution. Treat four/four as a promising measured configuration, retain the
failed windows, and repeat it longer with targeted refusal/history counters.
Production defaults remain two SQL owners and two publishers.

The diagnostic also counts 8,706 and 3,773 publication attempts that find an
already-advanced archive during its two windows. That path still loads and
physically verifies the retained recovery set. Coordinating publication across
replicas could reduce duplicate work, but followers must retain a verified
recovery proof before releasing local Raft history.

Fixture validation passes nine Python harness/analyzer cases, strict all-feature
Clippy for the performance binary, Rust formatting/layout, native dependency and
documentation checks, plus the frozen Linux image build and all four real-cluster
runs. These checks do not replace longer soak, power-loss, failover or broader
application/payload qualification.

### Tuned sustained diagnostic and size-based archive rotation

The [nine-minute diagnostic](evidence/tikv-tuned-pressure-diagnosis.json) freezes
`206d126` with four SQL owners, 64 total work reservations and four archive
publishers. Three 180-second windows verify every replica value and archive
position with zero write errors, then stop cleanly while retaining volumes.
The instrumented image selects an experiment; it does not qualify throughput.

| Window | Diagnostic TPS | p99 | SQL staging mean | Archive publication mean | Material loading mean | Mean tail objects |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 253.95 | 130.67 ms | 16.48 ms | 24.78 ms | 6.09 ms | 8.12 |
| 2 | 219.18 | 151.95 ms | 17.70 ms | 50.25 ms | 15.11 ms | 17.79 |
| 3 | 200.96 | 151.15 ms | 20.36 ms | 41.94 ms | 8.60 ms | 9.74 |

Retained entries peak at 3,736, below the 4,096-entry admission limit; no
instrumented refusal fires. The diagnostic therefore does not explain earlier
overload bursts. Archive material averages about 29/92/72 kB versus current
images of 25/32/36 kB, respectively (decimal units). Existing count-based
rotation occurs 1,501 times in the second window and 3,365 times in the third.
Material loading and replay fall as tails reset, while SQL staging and head
publication remain more expensive. Stage means are nested, not additive.

Unrelated builds appear in 1/36, 21/35 and 19/35 host samples. Swap stays near
9.56 GiB. This variation and the continuing staging slowdown prevent attributing
the full TPS decline to archive history. Nevertheless, each extra tail adds an
object read to complete verification, providing a direct operation-count target.

TiKV's pinned
[`on_raft_gc_log_tick`](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/fsm/peer.rs)
uses count and size thresholds for leader-coordinated history compaction and
caps it at persisted progress. Cellar's candidate applies a related size-based
retention policy to archive tails: a projected encoded tail set larger than
twice the current uncompressed image selects a new snapshot. Absolute tail,
entry and set bounds remain. Prior payload validation, physical replay, exact
head CAS, membership checks and local history-release proofs remain unchanged.
Before a size-triggered rotation, the complete eligible extension is physically
verified so an intermediate update overwritten later in the same capture cannot
escape its digest check. That private replay input never becomes an archive
reference or advances the selected head.

The new small-image regression fails before the change because fifteen tails
accumulate without size-based rotation. A complementary case retains short,
efficient tails for a large image. A twenty-update differential case uses 68
payload reads at ratio two versus 230 at ratio 128, with exact physical recovery
after every publication. This 70.4% operation-count reduction is not a TPS claim.

The [validation record](evidence/tikv-tail-cost-validation.json) preserves the
failing regression and the first candidate's intermediate-digest failure. The
corrected candidate passes all 48 archive, 18 execution, 29 host and 43 snapshot
cases (138 total); four child-process fixtures run through parent scenarios.
Tests that specifically require long tails select a high ratio while retaining
their existing download/replay assertions. Default-policy intermediate-digest
refusal stays active, and prior-payload corruption cases now include size-based
rotation. Strict all-target/all-feature Clippy, formatting/layout, dependency,
documentation and nine performance-harness checks pass. An uninstrumented
control/candidate comparison is recorded below; it does not qualify this policy
for sustained throughput.

### Uninstrumented size-based archive comparison

The [comparison record](evidence/tikv-tail-cost-comparison.json) preserves a
frozen `b08cb6c` control and `972264b` candidate. Runtime manifests differ only
in `archive/mod.rs`, and load-generation code is identical. Both use four SQL
owners, 64 total reservations and four archive publishers per node, with fresh
persistent volumes and a cold benchmark-VM restart. The planned workload is
three 180-second windows at twelve writers, retaining the same 1-vCPU / 3-GiB
nodes, 1,500 three-voter groups and durability contract.

| Build | Window | Successful TPS | Successful-request p99 | Write errors |
| --- | --- | --- | --- | --- |
| Control | 1 | 88.32 | 779.35 ms | 2 ambiguous outcomes |
| Size-based rotation | 1 | 212.82 | 158.56 ms | 375 overload refusals |
| Size-based rotation | 2 | 198.91 | 162.24 ms | 0 |
| Size-based rotation | 3 | 185.47 | 167.80 ms | 0 |

The control stops after its first window; it has no later-window measurements.
Its two uncertain original requests reconcile within their original deadlines,
then a separate read-only scan verifies all 4,500 replica values and 1,500 archive
positions. The original timed run remains failed. The candidate completes normal
verification of all 4,500 replica values and 1,500 archive positions; its overload
refusals are not retried. Both cohorts stop cleanly, retaining every volume.

Candidate mean throughput is 199.06 successful TPS, with p99 below 200 ms in all
three windows. Nevertheless, first-window refusals fail both the 200- and 500-ms
zero-error gates, and throughput declines 12.9% from first to last window. The
independent analyzer therefore selects no qualified configuration. Final archive
coverage takes 5.78 seconds after replica verification; this is not a steady-state
archive-lag measurement. Successful-request latency excludes refused requests.

Other builds appear in 66/66 control and 59/126 candidate host samples, with about
9.5 GiB of host swap in use. The failed, shorter control and changing shared-host
conditions prevent a causal overall speedup claim. The 70.4% deterministic read
reduction remains established, while the default ratio-two policy remains a
candidate under evaluation. The next useful step is to capture refusal causes
in the failing workload, then compare sustained successful TPS without relaxing
latency, admission, durability or data-verification gates.

### Refusal counters and bounded SQL progress scheduling

The [fixed-counter diagnostic](evidence/tikv-refusal-diagnosis.json) freezes
`4e9b825` and adds process-local atomic counters with five-second resource
snapshots. It has no per-operation logging or timing probes. Three 180-second
windows use 12/24/12 writers with the same four SQL owners, 64 reservations,
four archive publishers and three 1-vCPU / 3-GiB nodes. No instrumented refusal
fires. Retained entries peak at 2,433–2,436 per node, below the 4,096-entry cap.
Admission checks defer 58,418/87,508/87,457 times across the nodes, with at most
five observed waiters each. These are repeated decisions, not distinct commands
or failures. The earlier overload bursts remain unexplained.

The runner reports 194.84/206.98/184.04 TPS and 184.79/336.16/368.05-ms p99 with
zero write errors. However, the duplicate twelve-writer setting exposed an
evidence-recording bug: a filename based only on repeat/concurrency/rate lets
the third window overwrite the first raw receipt and latency ledger. The
independent analyzer rejects this run. The first summary remains available,
and the runner checked all 4,500 replica values and 1,500 archive positions
before clean shutdown, but the complete receipt proof cannot be independently
reconstructed. This diagnostic is not a performance qualification. All volumes,
remaining raw evidence and the failed independent-analysis result are retained.

The harness now includes the window ordinal in every evidence filename and
creates it exclusively. Its independent analyzer rejects reused evidence paths,
including old filenames that otherwise have matching counts and percentiles.
Both regressions fail before the fix; all eleven Python harness cases pass.

TiKV's pinned `batch-system/src/batch.rs` handles control work before normal
work and bounds rescheduling to retain fairness. Its flow controller throttles
new writes at the scheduler while tracking storage pressure separately. The next
Cellar experiment applies bounded priority at the physical SQL owner: queued
publication, submission, retirement, discard and archive capture can pass
unrelated ordinary work. An earlier request for the same cell always stays
first. After three priority selections, the oldest request is served. Every
queued and executing item retains its original semaphore permit; no additional
capacity, collection delay or durability shortcut is introduced.

The worker-selection regression first failed because archive capture waited
behind unrelated queued SQL work. It now passes, alongside cases for publication
reservations, per-cell order, sustained priority arrivals without starvation and
draining work after sender disconnect. The [validation record](evidence/tikv-sql-priority-validation.json)
confirms 458 library and 138 native archive/execution/host/snapshot cases pass
(596 total), with four process helpers invoked by parent cases. Strict
all-target/all-feature Clippy, formatting/layout, native dependency policy and
documentation checks also pass. The uninstrumented comparison against `972264b`
with the same archive policy is recorded below.

### Uninstrumented SQL priority comparison

The [comparison record](evidence/tikv-sql-priority-comparison.json) freezes
`972264b` control and `3aeb3ad` candidate. Only `host_sql.rs` differs in production
runtime; load generation is identical. Both use three 1-vCPU / 3-GiB nodes,
500 leaders and 1,000 followers each, four SQL owners with 64 total reservations,
four archive publishers, fresh persistent volumes and a cold VM restart. Each
cohort runs two 180-second windows at 24 writers, without timed mutation retries.

| Build | Window | Successful TPS | Successful-request p99 | Write errors |
| --- | --- | --- | --- | --- |
| FIFO control | 1 | 202.05 | 584.41 ms | 0 |
| FIFO control | 2 | 92.12 | 1,191.32 ms | 0 |
| Bounded priority | 1 | 247.25 | 201.85 ms | 0 |
| Bounded priority | 2 | 212.02 | 220.11 ms | 0 |

Both cohorts independently verify all 4,500 replica values and 1,500 archive
positions, then stop cleanly with volumes retained. Candidate mean throughput
is 229.64 TPS versus 147.08 for control, an observed 56.1% increase; worst-window
p99 is 81.5% lower. The analyzer selects the candidate at the 500-ms limit and
selects neither build at 200 ms. Candidate throughput declines 14.2% between
windows versus 54.4% for control. This is a useful bounded comparison, not stable
capacity or proof of a causal overall branch speedup.

Other builds appear in 62/91 control and 91/98 candidate host samples. Swap is
about 9.47–9.52 GiB for control and 8.27–9.47 GiB for candidate. Node CPU averages
74–81% and peak node memory stays below 417 MiB across traffic and verification.
Final archive waits are 0.95/5.81 seconds after replica checks; they do not measure
archive lag during traffic. The fixed counter workload and sequential shared-host
cohorts limit the conclusions.

The first control setup failed before any timed window: all 1,500 seed writes
were acknowledged, but RustFS archive coverage stalled when the VM filesystem
exhausted its inodes despite 37.1 GiB of available bytes. That attempt remains
failed and has no complete data-verification claim. Its containers stopped
cleanly and all volumes remained. Growing only the benchmark data disk from
160 to 224 GiB added 4.19 million free inodes while retaining 290 volumes,
43 images, CPU/memory limits, other profiles and the selected Docker context.
Both replacement cohorts use that same expanded disk and require at least
two million free inodes and 16 GiB free bytes before setup. This environment
failure does not establish the cause of earlier overload refusals.

## Scope and reproducible baseline

Compare three native Cellar nodes, each limited to one vCPU and 3 GiB, with
500 leaders per node and 1,500 three-voter groups. Quorum acknowledgements must
retain their existing durable-log and committed-image publication proofs.
RustFS archive coverage is checked separately for every acknowledged cell.
Prefer p99 below 200 ms; the accepted upper budget is 500 ms.

The [previous measurements](../../tools/perf/raft-results.md) record an
error-free two-window confirmation averaging 45.53 writes/sec, with a worst
window p99 of 450.58 ms. A shorter window reached 96.81 writes/sec at 144.42 ms
p99, but that result did not reproduce in the longer confirmation. During the
confirmation node CPU averaged approximately 39–42%, memory peaked below
470 MiB per node, and CPU throttling totaled under 0.34 seconds per node.
These observations favor investigating storage waits and concurrency limits;
they do not attribute the entire tail to a measured individual stage.

Cellar base: `fb660b164b3cb0c8f88afcb659473445928a92a2`.
TiKV source: clean local checkout at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`.
Both projects use raft-rs and raft-engine, but their exact versions differ:
Cellar vendors raft 0.7.0 and raft-engine 0.4.2 with execution-domain changes;
the inspected TiKV lockfile pins raft-rs `ad13f3d90780f53aea2488c6a4b76c0d334bf136`
and raft-engine `55bccdea42718ab2b4f90cdd9068da53b6bf4c39`.

## Mechanisms in the inspected TiKV source

| Mechanism | What TiKV does | Cellar implication |
| --- | --- | --- |
| Batched FSM scheduling | Mailboxes schedule an idle FSM once; pollers collect several active FSMs before a batch ends. | Cellar already parks idle groups. Request draining can let multiple queued groups reach its existing shared log writer together. |
| Shared asynchronous log writer | Drains queued write tasks up to a byte limit, then synchronously persists their combined Raft batch. Adaptive bounded waiting may enlarge small batches. | Keep disk work outside the consensus event loop and amortize sync across groups. First test draining work already available, without adding an artificial delay. |
| Ordered outstanding Ready work | Tracks unpersisted Ready numbers and releases the corresponding messages only when persistence completes. | Cellar currently permits one outstanding Ready per group. Multiple outstanding Readies would require additional ticket, failure, and message-order qualification. |
| Continuous bounded transport | Drains size/count-limited message batches into a readiness-controlled gRPC stream. | Cellar now refills its eight-frame control window per completed authenticated reply; it retains its own admission protocol and Raft persistence proofs. This fixes a local stall but has not established a TPS gain. |
| Batched apply | Accumulates state-machine changes in a shared engine write batch; flushes before reporting apply results. | Cellar's separate SQLite images do not share TiKV's KV batch. Its per-image copy, validation, sync, and marker work can dominate despite a shared Raft log. |
| Explicit recovery progress | Raftstore v2 disables tablet WAL while tracking flushed apply indices and retaining the history needed to replay missing state. | Cellar's durable applied marker currently names a durable immutable image. Delaying that image's persistence requires separate visible and durable progress, replay retention, and crash/snapshot/GC tests. |
| Write-size flow control | Transaction flow control charges calculated mutation bytes and returns quota on deadline failure. | Cellar executes arbitrary callbacks and reserves maximum output before they run. Raising its reservation budget nearly eliminates that wait but did not improve sustained TPS in the same-image experiment. |
| Coordinated hibernation | Peer agreement and quorum checks govern leader/group hibernation. | Increasing Cellar's fixture tick interval is a workload tradeoff, not an implementation of TiKV hibernation. |
| Bounded backup concurrency | Backup workers acquire semaphore permits for scanning batches; optional CPU tuning adjusts the permit count. | Overlap independent archive I/O within a fixed window while retaining the recovery-set byte budget. The proposed four-read window does not implement CPU auto-tuning. |

Pinned primary sources:

- [Batch mailbox ownership](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/batch-system/src/mailbox.rs)
  and [poller batching](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/batch-system/src/batch.rs).
- [Async write task draining and durable engine writes](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/async_io/write.rs).
- [Ready sequencing](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore-v2/src/operation/ready/async_writer.rs)
  and [Ready advancement](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore-v2/src/operation/ready/mod.rs).
- [Bounded message batches and continuous sender polling](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/server/raft_client.rs).
- [Apply batching](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/fsm/apply.rs)
  and [flushed apply progress and recovery](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore-v2/src/operation/ready/apply_trace.rs).
- [Transaction flow control and deadline quota return](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/storage/txn/scheduler.rs)
  and [engine flow controller](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/storage/txn/flow_controller/singleton_flow_controller.rs).
- [Hibernation state](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/hibernate_state.rs).
- [Backup concurrency permits](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/backup/src/softlimit.rs)
  and [worker admission and optional CPU tuning](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/backup/src/endpoint.rs).

## Ranked hypotheses and experiments

1. **Archive reservations prevent useful concurrency.** Each pending callback
   reserves the maximum image plus maximum envelope (about 80 MiB), even after
   its much smaller output is known. With a 256-MiB backlog limit, three such
   reservations can prevent the fourth command. Predict that replacing the
   reservation with a verified physical bound after preparation will admit
   more small commands while preserving refusal for unknown or excessive work.
2. **Private execution adds disk syncs ahead of Raft.** Working-copy creation,
   SQLite transactions, LTX capture, preview generation, committed publication,
   and durable markers have distinct I/O requirements. Instrument these stages
   before changing them. Disposable private state may have weaker persistence
   needs; committed images cannot be relaxed under the current marker contract.
3. **Mailbox dispatch fragments log batches.** The worker starts persistence
   after dispatching one request, even when other groups' requests are queued.
   Predict that bounded draining will share the first persistence batch across
   an already queued burst, without requiring a deliberately stalled disk.
4. **Archive verification limits sustained catch-up.** One producer and replay
   workspace serve each node. Even asynchronous archive work competes for CPU,
   storage and admission space. Measure lag and verifier occupancy before
   considering a bounded worker pool and its memory/shutdown obligations.

## Reservation change and safety boundary

The native SDK regression
`prepared_small_commands_release_worst_case_archive_reservations` opens eight
real groups and holds archive storage unavailable. Each small command must
apply while its S3 waiter remains pending. The unchanged implementation refused
the fourth command on two runs. After storage recovers, all admitted commands
must obtain S3 receipts with their exact values.

The proposed change keeps the maximum reservation until the private executor
has encoded the entry and physically verified its preview. It then retains
image length plus encoded-entry length plus framing allowance through proposal,
apply, and proof completion. Retained log entries and applied images continue
to count independently, so overlap is conservative. Unprepared callbacks,
including cancelled callbacks still running, retain the original bound.
Entry limits, duplicate receipt checks and ambiguity semantics remain in force.

The new native tests pass with this change: eight small S3 waiters fit the
default budget, and a deliberately smaller byte budget still refuses a second
callback. The complete archive binary passes all 31 cases.

## Queued request batching

The production runtime now drains at most 64 already queued requests in a
dispatch turn before collecting Ready work. It adds no waiting period and
retains the existing log byte/group bounds, one in-flight writer, per-group
ordering, and persistence receipts. Each bounded turn returns to event, timer,
I/O and shutdown polling.

`queued_groups_share_the_first_persistence_batch` queues eight independent
groups on a current-thread executor, with real raft-engine persistence and no
artificially blocked sync. Before the change, one group used sequence 33 and
the other seven used sequence 34. Afterward they share their initial sync.
All 16 runtime cases pass, including the held-sync message barrier, stalled
group isolation and shutdown with a full queue. The execution binary passes
all 16 cases serially; an initial parallel run encountered a directory-lock
refusal in its restart test, whose isolated repeat passed. The original failure
is retained in the diagnostic log and is not erased by the passing repeats.

## First comparison and follow-up

The first candidate (`sha256:f01a9a747600c29040a6eb3b5c01f75c27d6b9b6a635a13cf44afb79250177bb`)
contained physical-size reservation accounting and request batch draining.
The fresh control (`sha256:f7d8781239d87d2876ee8c8e542cc94ee87dbe311377e1746ec0e3330c6bb9df`)
used the unchanged baseline binary. Both used 60-second windows, two reversed
repeats at concurrency 3, 6 and 9, a 500-ms tick, 4,096 queue items, and 512-MiB
archive scratch tmpfs. Builds and profilers were absent during timed windows.

| Image | Concurrency | Mean successful writes/sec | Worst p99 (ms) | Admission refusals across both windows |
| --- | --- | --- | --- | --- |
| Control | 3 | 84.07 | 192.16 | 0 |
| Control | 6 | 92.56 | 403.11 | 0 |
| Control | 9 | 41.05 | 936.70 | 7,724 |
| First candidate | 3 | 69.56 | 440.75 | 1 |
| First candidate | 6 | 72.10 | 1,102.30 | 0 |
| First candidate | 9 | 79.82 | 458.88 | 328 |

All 4,500 replica values and 1,500 archive positions reconciled in each run.
However, no candidate setting passed both the zero-error and p99-under-500-ms
gates in every repeat. The candidate substantially reduced nine-writer
refusals but **did not establish an overall performance improvement**.
Raw evidence is in `.cellar/raft-perf/1500-tikv-batching` and
`.cellar/raft-perf/1500-tikv-control`, with independent `analysis-500.json` files.
These ignored local artifacts contain full samples; this table is not a soak
or crash qualification. A later `strace` experiment perturbed leadership and
produced ambiguous outcomes, so its timings cannot attribute production costs.

Follow-up regressions reproduced two remaining pre-proposal refusals: another
callback holding an unknown-size archive reservation, and a reader holding the
cell's SQL owner. The host now retains bounded pending requests for temporary
reservation pressure, retries them in FIFO order, and keeps the maximum charge
until preparation completes. A cell can retain one command behind non-command
SQL work and rechecks authority when SQL becomes available. It still refuses
a second pending command. Cancellation must remove waiting work without running
the callback; full retained archive backlog still permits duplicate lookup and
refuses new mutation. These changes require a fresh measured comparison.

The follow-up implementation passes 615 selected cases: 442 library, 33 archive,
16 execution, 27 membership, 20 reads, 16 runtime, 42 snapshot and 19 operations.
Six child-process entry points are intentionally ignored by their standalone
binaries and invoked by their parent scenarios. The integration suites were
run serially after an earlier parallel restart test encountered a directory
lock. Formatting (792 files), module layout, native dependency checks, docs,
and Clippy for the library, performance binary and changed integration targets
also pass. These checks retain the existing durable-log, image publication,
recovery and physical-shutdown assertions; they do not prove a performance gain.

The follow-up image (`sha256:200d020287cc61731206917afcf128c83fd31470c9b316780a4abc96c5f42be6`)
completed eight 60-second write windows at concurrency 3, 6, 9 and 12 in reversed
repeats, with 40,581 acknowledged writes including seed and zero write errors.
However, final replica verification failed: node 1, cell 193 returned
`Unavailable`. A separate read-only scan found 4,499 correct replica values and
the same unavailable replica. Its leader and other follower both returned the
expected value 35, with applied/archive index 38. The failed run is preserved at
`.cellar/raft-perf/1500-tikv-admission2`; it is **not qualified**, and no result
from it establishes a throughput gain. Independent host-process samples also
record concurrent builds/tests from other checkouts during later windows.
Failure-origin instrumentation and a reproduction are required before another
performance conclusion. The original report remains incomplete.

## Follower publication scheduling failure

A diagnostic-only repetition with failure logging passed all 4,500 replica and
1,500 archive checks after 49,129 acknowledgements. This does not explain the
earlier permanent refusal. Subsequent concentrated traffic across 30 of the
same 1,500 groups acknowledged another 14,704 writes without write errors, but
three of 90 active replica reads could not reach their acknowledged versions.
Their committed/applied indices were 449/230, 679/389 and 396/317. The other
replicas continued serving. These runs intentionally overlapped diagnostic
tests and are not performance comparisons.

The smaller real three-node SDK scenario
`follower_drains_committed_batch_after_held_reader` holds one follower's reader,
commits 32 writes through the other voters, then releases that SQL owner. It
reproduced a follower committed through index 35 but stuck applied/published
at index 6. There was no runtime or storage failure event in this case.

The host can receive the next committed apply while publishing the previous
image. Dispatching publication consumes the group's active scheduling slot.
Completion previously cleared `apply_inflight` without restoring the slot for
the queued entry, leaving that follower permanently idle. The proposed fix
restores scheduling after publication physically completes and keeps
`apply_inflight` true when an entry is queued. The reproducer now passes 40
consecutive fresh three-node fixtures. The final implementation passes 643
selected native/library cases, formatting, and affected-target Clippy. Full-cluster
revalidation of this fix remains incomplete. The earlier node-1/cell-193
`Unavailable` failure remains distinct
and unresolved until its origin is captured.

## Latest measurement and diagnostic limits

The publication-scheduling candidate
(`sha256:0a2380ae65b77c6f86547771307217df267e8dc8bf2fc3e5b8504a075c70876f`)
completed its first 60-second windows at concurrency 3, 6, 9 and 12 with zero
write errors: respectively 88.11, 82.82, 82.88 and 93.05 writes/sec, with p99
125.68, 227.52, 339.21 and 357.68 ms. The second 12-writer window fell to
48.72 writes/sec and 3,088.92-ms p99, with two ambiguous outcomes. The harness
stopped before final replica/archive verification. Both ambiguous commands
subsequently reconciled successfully using their original identities and
original retry deadlines, outside measurement. That reconciliation does not
turn the failed measurement into a passing one. The preserved run is
`.cellar/raft-perf/1500-tikv-publication`.

A separate bounded `perf trace` sample on node 1 recorded 5,406 completed
Raft-log syncs (median 1.202 ms, p99 5.509 ms) in approximately 24 seconds.
Their aggregate syscall duration was 7.79 seconds; this is summed thread time,
not a fraction of end-to-end latency. Another 28,877 syncs lacked reliable path
attribution, so the trace cannot rank private capture versus image publication.
The diagnostic load also encountered eight `NotLeader` responses. Its throughput
is not a qualifying performance result. This evidence supports further
storage-stage instrumentation, not a claim that a particular fsync stage has
already been proven dominant. RustFS archive verification remains asynchronous
for the measured quorum-acknowledged writes, but retained backlog can limit
admission.

## Disposable command workspace

Private command execution now explicitly opens disposable WAL capture. Its
SQLite connections omit local syncs, as do temporary LTX cuts, copied bases and
capture cleanup. Every reopen discards the entire workspace before using an
engine-selected committed image. The ordinary LTX constructors retain durable
capture, and disposable open refuses existing sidecars or metadata before
opening SQLite. Captured cuts still undergo bounded decoding and physical
preview verification. Raft logs, previews, committed images and applied markers
retain their existing durability barriers.

This applies the same distinction seen in TiKV between replayable local state
and authoritative recovery progress, but does not adopt TiKV's delayed tablet
flush contract. Cellar still publishes a durably sealed immutable image before
its applied marker can acknowledge a write.

A temporary real SQLite/raft-engine single-voter fixture measured five fresh
instances with 24 sequential counter increments each, excluding initial setup.
Private staging averaged 16.301 ms before and 9.536 ms after; its median changed
from 16.160 to 9.363 ms. Commit/apply averaged 17.133 and 15.770 ms respectively.
These are debug-build macOS stage measurements, not the container workload or
an end-to-end capacity claim. The same probe checked every committed counter;
its source and raw samples remain in the ignored diagnosis directory.

The capture regression first failed at the injected cut-file sync. Explicit
disposable capture now produces an independently restorable chain, including
checkpoint control writes, even when that sync operation is unavailable; normal
durable capture continues to fail. A second test refuses dirty-workspace reuse
before touching the database. The native execution suite passes 17 cases,
including process-kill cuts, committed-image marker barriers, replay/deduplication,
and a new corrupt-private-database/sidecar recovery scenario. Broader checks and
full-container measurement are still in progress.

Temporary timing probes have been removed from source. Permanent group failures
now use structured error events instead of the temporary diagnostic prints,
including their group, operation error where available, and admission-close
location. The measurements retain failed runs; no past timeout or replica
failure is reclassified by this change.

## Verified container runs for disposable execution

The candidate image
`sha256:6fdc0369dc06c3b5a1052d6cd2403d12c61706f71c97f7515d401009859c3c74`
contains the production sources committed at `47615ea`. All 96 recorded inputs
match its frozen build snapshot. Final validation passes 652 selected Rust
cases (644 Cellar library/native cases and eight LTX cases), affected-target
Clippy, formatting, layout, dependency, documentation and eight Python harness
checks. Six standalone crash helpers run through their parent scenarios.

The eight-window 1,500-group run reconciled 56,460 acknowledgements, including
seed, against all 4,500 replica values and all 1,500 verified RustFS archive
positions. The independent analyzer selected three concurrent writers:
**113.25 writes/sec mean, 106.84 minimum, 141.23-ms worst-window p99**, with
zero errors in both 60-second windows. Six writers also had zero errors,
but lower mean throughput and a worst p99 of 255.90 ms. Both 12-writer windows
and the second nine-writer window had admission refusals and are excluded.
The observed 179.37 writes/sec at 12 writers is not a qualifying operating point.
Final archive catch-up took 21.82 seconds after replica checks. No node exceeded
its 3-GiB limit or recorded an OOM event.

A subsequent 90-second, 12-writer workload concentrated traffic on 30 cells of
the same 1,500-group cluster. It acknowledged 19,054 additional writes with zero
errors, then again verified every one of the 4,500 replicas and 1,500 archive
positions. The previously observed follower scheduling stall did not recur.
No permanent group-failure event appeared in the sweep's node logs. This does
not retrospectively explain the distinct earlier permanent `Unavailable` case.

The [sanitized measurement record](evidence/tikv-disposable-progress.json)
includes image/binary identities, all window results, qualification exclusions
and hashes of ignored raw artifacts. Unrelated host builds overlapped 58 of
118 activity samples, limiting timing attribution. A fresh unchanged control
subsequently passed all replica/archive checks but had one six-writer admission
refusal; its three-writer windows varied from 98.81 to 37.79 writes/sec, with
worst p99 357.12 ms. Host builds overlapped 44 of 69 control samples. That control
used only three/six-writer windows. The matching candidate confirmation then
failed with one ambiguous response in its final three-writer window. Its first
three-writer window reached 100.76 writes/sec at 266.79-ms p99; the final one
reached 68.88 writes/sec at 211.01-ms p99 plus that timeout. Both six-writer
windows exceeded 500-ms p99 (569.10 and 1,117.60 ms). The original command
subsequently reconciled using its unchanged identity and original retry deadline;
all three replicas had already applied its index. The failed measurement remains
incomplete, and the timeout stage is not yet known.
These observations establish verified operating points, not a controlled
speedup or a soak/failure-zone qualification. All stopped clusters retain their
named durable volumes.

## Phase diagnostic and remaining tail

A temporary instrumented build repeated the same 1,500-group fixture. Its first
three/six/nine-writer windows had zero errors, but the second nine-writer window
failed with five ambiguous responses and leader changes. This is a diagnostic
run, not a qualifying throughput comparison. Instrumentation records every
completion at least 250 ms long plus one of every 64 completions; these records
are not an unbiased latency distribution.

All five ambiguous producers spent 5,028–5,539 ms between submission preflight
and delivery of their committed apply. Image preparation, applied-marker
completion and publication then took 7–24 ms combined. Their original command
identities reconciled within their original retry deadlines, and a separate
read-only audit verified 33,274 writes including reconciliation against all
4,500 replicas and 1,500 archive positions. The original timed report remains
failed. By that audit, 16 groups had changed from the requested leader placement.

Other slow completions appeared in different stages across nodes at similar
times. A VM pressure sampler remained scheduled throughout the five-second
failure interval; its longest observed sample gap was 342 ms. I/O pressure was
low during much of that particular interval, so it does not support attributing
those timeouts to a whole-VM pause or continuous global I/O stall. This narrows
the next probe to shared-log persistence and transport queue/RPC time. Each peer
lane currently waits for a frame admission response before sending its next
frame; whether that serialization causes these tails remains to be measured.
The failed measurement and tagged probe sources are retained locally under
`.cellar/raft-perf/1500-tikv-phases` and `tikv-phases-source`.

The standard Linux SDK CI job passed at `47615ea`. Its free-threaded Python
matrix job failed the native `management_stop_seals_raft_before_joined_receipt`
case: admission closure was not observed within its 500-ms bound. The original
assertion remains unchanged; this failure is unresolved and is an additional
qualification gate.

## Bounded transport pipelining

The second diagnostic build added shared-log persistence timing, sender queue
and RPC timing, and receiver admission timing. All six 60-second windows at
three/six/nine writers had zero errors; every replica and archive verified.
The five-second timeout did not recur. Shared-log persistence reached
761/1,514/1,505 ms on nodes 1/2/3. Control-frame queue delays reached
424/459/404 ms during measured write windows; the corresponding peer RPCs
completed in 1–3 ms. No receiver admission call reached 100 ms. These bounded
slow-event probes identify queueing separately from persistence stalls, without
qualifying the diagnostic image as a faster implementation.

TiKV's [message buffer and stream sender](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/server/raft_client.rs)
collect messages by count/bytes and flush them onto a client stream. Its
[stream receiver](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/src/server/service/kv.rs)
dispatches batch members without a separate response round trip for each one.
Cellar preserves its authenticated per-frame admission responses, but can avoid
waiting for each response before writing the next queued frame.

The proposed sender drains up to 16 already queued frames with a 1-MiB window;
one larger permitted frame occupies a window by itself. An overflow frame keeps
its FIFO position and original byte/item permits. Concurrent bounded reads and
writes avoid a full-duplex socket buffer deadlock. Every reply still binds the
channel, sequence, route and request tag. Error or cancellation discards the
connection; accepted transport frames remain distinct from durable Raft proofs.
No waiting interval or additional worker is introduced.

The real TLS regression queues two different groups and withholds the first
reply until the second authenticated frame arrives. The original sender times
out; the proposed sender passes. Additional tests reject a mismatched reply and
release all admission, and exchange 16 frames over 64-byte duplex buffers.
The final sender passes 590 Rust cases: 445 library, 27 host, 19 operations,
16 runtime, 14 transport, 27 membership and 42 snapshot cases. Five standalone
process helpers run through their parent scenarios. Affected-target strict
Clippy, module layout, dependency policy, documentation and eight Python harness
cases pass. The previously failing management-stop case passes locally; the
Linux CI failure remains unexplained. A fresh end-to-end comparison is pending.

## Completion admission failure under load

The production pipeline image at `05d4c37`
(`sha256:376db854807e3514e395cb2a6b16467b89c5e818eb3a48c63ce9604bc91e8678`)
completed eight 60-second windows, but failed final replica verification.
It acknowledged 53,577 writes including seed. The second twelve/nine/six-writer
windows also returned 7,002/3,113/2,673 admission refusals. The independent
postfailure scan verified 4,499 replica values; node 1, cell 988 remained
`Unavailable`. All 1,500 RustFS archive positions covered acknowledged writes.
The leader and other follower of cell 988 both applied index 46. This run
qualifies no operating point or speedup, regardless of its faster windows.

The permanent failure log now identifies the origin: `complete_apply` returned
`Overloaded`, and the host failed the group. The runtime reserves eight slots
for apply/snapshot completions independently of ordinary work, but its immediate
admission still refuses a ninth simultaneous completion. This is already-issued
physical apply work, not an optional new command. Losing its completion leaves
the host unable to publish its committed image.

`completed_apply_burst_waits_for_reserved_capacity` prepares seventeen actual
committed apply tickets and fsynced fixture images, then polls their completions
in one current-thread turn before the runtime worker runs. Before the fix, the
first eight succeed and the remaining nine return `Overloaded`. The proposed
change retains excess tickets/images in their existing caller futures and waits
for reserved capacity. The queue remains eight slots; no retry of an admitted
completion or durability relaxation is introduced. Shutdown wakes unsubmitted
waiters, while every accepted ticket still crosses validation and the marker
persistence barrier before publication. The burst reproducer passes after the
change, as do cancellation and shutdown regressions. Final validation passes
593 Rust cases: 445 library, 27 host, 19 operations, 19 runtime, 14 transport,
27 membership and 42 snapshot. Five standalone process helpers run through
their parent scenarios. Strict Clippy, formatting, layout, dependency policy,
documentation and eight Python harness cases pass. Full-cluster revalidation
and a controlled throughput comparison remain pending.

## Aggregate log-cache exhaustion

The completion-capacity candidate at `9e18e03` completed the full eight-window
run and verified all 4,500 replica values and 1,500 archive positions. It still
had admission refusals and no operating point passed every repeat. A subsequent
three-writer, 180-second window acknowledged 10,041 writes (55.20 TPS), with
976 admission refusals and 63 five-second ambiguous outcomes on distinct cells.
Its successful-response p99 was 79.86 ms, which excludes those failed requests
and therefore does not qualify the run. The original command deadlines were
preserved; one reconciliation attempt returned overload for all 63 commands.

The first ambiguous requests on all three nodes started within four milliseconds
of one another. A quiescent copy of node 1's durable log, opened separately for
inspection, contained 268,431,038 cache bytes against the 268,435,456-byte limit:
only 4,418 bytes remained. Across 1,500 groups it retained 89,861 entries, but
the largest group had only 126 entries. None could reach the default 256-entry
automatic checkpoint threshold. New append batches exceeded the aggregate
limit, and the runtime retried their `Overloaded` result without reclaiming
history. This explains the sustained-write stall; it does not attribute every
earlier latency spike to this cause.

The SDK regression `many_small_groups_checkpoint_under_shared_cache_pressure`
uses eight real SQLite groups and a 256-KiB shared cache. Before the change it
returned an ambiguous result at group 1, sequence 20. The proposed maintenance
trigger observes durable aggregate cache usage and begins checkpointing at half
capacity, selecting eligible groups above an average share of that capacity.
Existing entry-count scheduling and explicit compaction disabling remain in
effect. The physical snapshot, image retention and durable checkpoint barriers
are unchanged. With pressure scheduling, all 256 commands and exact reads pass.
The scheduler regression
`checkpoint_reclaims_cache_ahead_of_capacity_blocked_append` independently
reproduces a reclaiming checkpoint rejected in the same batch as an append that
cannot fit. Runtime batches now persist queued checkpoints separately before
appends; the same regression then persists both the checkpoint and append.

This follows TiKV's use of memory-pressure-aware entry-cache reclamation, but
Cellar's synchronous Raft storage view currently retains every uncompacted
entry in memory. Cellar therefore uses its existing verified physical checkpoint
path to reclaim entries. A disk-backed cache-miss path would be a separate
architectural change.

The `9e18e03` SDK CI run failed two snapshot fault scenarios: the regular Python
job timed out in `native_snapshot_shutdown_joins_partial_transfer`, and the
free-threaded job timed out in
`native_snapshot_interrupted_transfer_retries_without_blocking_other_groups`.
The causes remain unresolved. Python application examples passed. The new cache
fix still needs full-cluster sustained measurement and recovery qualification.

Local validation of the cache fix passed 630 Rust cases across the library,
archive, host, membership, operations, runtime, snapshot and storage targets.
One operations case, `node_shutdown_hands_off_to_published_voter`, failed with
zero handoff attempts and one unavailable group. Ten isolated repetitions then
passed, which does not explain or erase the original failure. All 42 snapshot
cases passed locally, including the two scenarios that failed the preceding
Linux CI run. Five process helpers are invoked by their parent tests. Strict
Clippy, formatting, layout, dependency policy, documentation and eight Python
harness tests passed. The PR remains a draft with these qualification limits.

## Cache-fix verification and archive admission diagnosis

The production cache-fix image at `1ac16d1` completed eight 60-second windows,
verifying 91,714 acknowledgements including seed against all 4,500 replicas and
1,500 archive positions. Both three-writer windows had zero errors: 187.49 and
153.19 writes/sec, with p99 46.70 and 59.46 ms. Higher concurrency had admission
refusals. Two subsequent 180-second, three-writer windows acknowledged another
37,126 writes with zero errors: 71.74 writes/sec at 413.84-ms p99, then 134.02 at
74.63-ms p99. All replicas and archive positions verified 128,840 total writes.
The cache stall did not recur, but the longer windows do not both meet 200 ms.
Host compilers overlapped 18/115 main-run and 60/86 sustained-run activity
samples. These observations do not establish a controlled speedup.

A separate frozen diagnostic build added cumulative timing counters to archive
downloads, physical replay, publication, admission scanning and shared-log
persistence. It used the same 1,500 groups and resource limits; production
sources contain none of its probes. Its three 90-second windows at concurrency
3/9/12 returned 0/3,384/19,651 overloads. All 4,500 replica values and 1,500 archive
positions verified. No host compiler appeared in its 71 activity samples.

The refusal counters identify the 4,096 unarchived-entry bound: observed
refusals crossed 4,097–4,099 entries while the charged byte count was about
122–127 MB, below the 256-MiB byte bound. As histories grew, download time rose
from roughly 1 ms per set in the three-writer window to 4.2–5.1 ms in the
twelve-writer window. Downloads accounted for approximately 33–38% of aggregate
archive-publication wall time in that last window. Replay, object writes and
confirmation remained material costs. Cumulative samples bracket window edges
approximately; their nested timings must not be added as independent costs.
The [sanitized evidence](evidence/tikv-archive-diagnosis.json) preserves identities,
window results, stage counters and hashes of the ignored raw artifacts.

The measured throughput ceiling is archive-retention admission. One archive
producer per node must download, validate, replay and publish recovery material
for its resident replicas. Quorum acknowledgements need not wait for RustFS on
every write, but new work is refused when this background work leaves too much
unarchived history. Raising the backlog limit would postpone that refusal;
it would not establish a higher sustained archive service rate. These counters
identify the binding admission limit, not RustFS server saturation or the Raft
algorithm as the cause.

Admission also scans resident groups and their unarchived entries. In the last
window it averaged 0.64–0.90 ms per invocation, accumulating about 10–12 seconds
per node over 90 seconds. This is a secondary cost on a single-vCPU node;
incremental accounting remains a candidate requiring its own correctness and
performance verification. Latency spikes are not fully attributed: other runs
overlapped host compilers, and per-cell image publication and shared-log
persistence retain their durability barriers.

The proposed download window admits at most four object reads per recovery set
and preserves manifest order. The manifest already bounds aggregate payload
bytes; reads now compare store metadata with each exact declared object length
before allocating or polling its body. All hash, coverage and physical replay
checks remain required. No archive admission limit or producer count increases.

Two real recovery-set regressions hold payload reads at the object-store
boundary. Serial download cannot fill the window; bounded download starts four
reads, restores all six ordered tails and two dependencies, and releases every
pending read on cancellation. A third regression reproduces reading a body
whose advertised length disagrees with its manifest; the proposed change rejects
it before the first payload chunk while preserving the object-integrity error.
All three regressions failed before their respective changes and pass afterward.
Local validation passes 651 cases across the library, archive, directory, effects,
host, membership, operations, reads and snapshot targets. Three process helpers
run through their parent cases. Strict affected-target and performance-binary
Clippy, formatting, layout, dependency policy and documentation checks pass.
The earlier shutdown-handoff case passes in this matrix; that does not explain
its previous failure.

The regular `1ac16d1` Linux CI job reached Clippy and rejected two transport-test
calls to `tokio::time::timeout`. Both now use the execution-domain timeout API;
the standalone duplex test also owns and shuts down an explicit domain. Its
two-second bound and frame assertions are unchanged. All three pipeline tests
and expanded `cargo +1.98.1 clippy --locked -p cellar --all-targets -- -D warnings`
pass locally. This includes unit-test code missed by the earlier explicit-target
Clippy command. The first timeout-only edit reproduced a missing-domain panic
in the duplex test; the final domain-owned version passes.

The `1ac16d1` free-threaded Linux CI job failed
`public_node_shutdown_drains_alarm_background_before_the_durability_cut` in
`native_node`: a lease renewal became ambiguous and the watchdog reported
`NodeLeaseExpired`. This is outside the multi-Raft path and remains recorded as
an unresolved CI failure. Production-image comparison of the download window
is recorded below; no overall speedup is claimed.

## Archive-download production comparison

Fresh control (`1ac16d1`) and candidate (`2ee7fd6`) clusters used identical
three-node resources, 1,500 groups, workload source, 500-ms ticks, 4,096 queue
items and 512-MiB archive scratch mounts. Source files, image revision labels
and executable hashes matched each build. Six 60-second windows ran at
concurrency 3/6/9 and then 9/6/3. No builds, tests or profilers from this checkout
ran during measurement.

| Build | Repeat | Writers | Accepted writes/sec | Successful-response p99 | Overloads |
| --- | --- | --- | --- | --- | --- |
| Control | 1 | 3 | 148.58 | 108.07 ms | 0 |
| Control | 1 | 6 | 48.99 | 442.67 ms | 0 |
| Control | 1 | 9 | 151.42 | 207.72 ms | 1,062 |
| Control | 2 | 9 | 155.02 | 205.63 ms | 1,846 |
| Control | 2 | 6 | 154.47 | 119.19 ms | 1,065 |
| Control | 2 | 3 | 169.92 | 42.53 ms | 0 |
| Candidate | 1 | 3 | 155.62 | 63.51 ms | 0 |
| Candidate | 1 | 6 | 201.22 | 91.77 ms | 0 |
| Candidate | 1 | 9 | 152.78 | 201.71 ms | 224 |
| Candidate | 2 | 9 | 173.85 | 210.39 ms | 0 |
| Candidate | 2 | 6 | 112.46 | 176.87 ms | 0 |
| Candidate | 2 | 3 | 72.09 | 164.20 ms | 0 |

Independent receipt analysis verified 51,254 control and 53,618 candidate
acknowledgements including seed, each against all 4,500 exact replica values
and all 1,500 archive positions. Both latency budgets select three writers for
the control (mean 159.25 writes/sec) and six for the candidate (mean 156.84).
The candidate's six-writer setting has no overloads in either repeat, but its
throughput varies substantially. Its three-writer average is lower than the
control's. Total overloads fell from 3,973 to 224; that does not establish an
increase in qualifying throughput.

Other checkouts' build processes appeared in 64/93 control and 68/89 candidate
activity samples, including timed traffic. Only the control's final three-writer
window had no such sampled overlap. This comparison therefore does not prove
a controlled or repeatable speedup. Final archive catch-up after replica reads
took 22.17 seconds for the control and 11.24 for the candidate; different pending
work means these waits are not direct archive service-rate measurements. Peak
node memory stayed below 1.10 GB and cgroup counters recorded no OOM events.

The [sanitized comparison](evidence/tikv-archive-download-comparison.json) records
per-window overlap, hashes and exact verification counts. Raw evidence remains
in `.cellar/raft-perf/1500-tikv-download-control` and
`.cellar/raft-perf/1500-tikv-download-candidate`. Both clusters are stopped;
their voter and RustFS volumes, configs and closing logs are retained.

The next measured cost to address is repeated admission accounting. TiKV's
[entry storage](https://github.com/tikv/tikv/blob/b3995dab28308f09cbb7c125bf32f789dbc203fe/components/raftstore/src/store/entry_storage.rs)
updates memory deltas on append, truncation and compaction. Cellar can apply the
same principle to avoid recounting unchanged histories. The following change
addresses that cost separately from the download window.

## Reuse of unchanged admission accounting

Admission previously reacquired each resident group's log cache and walked its
unarchived entries on every check. Per-group observations now reuse the byte and
entry totals while cache identity, persisted revision and archive position match.
This is revision-based reuse, not TiKV's exact memory-delta implementation.
The node still visits its resident groups and recalculates pending reservation
charges; the optimization removes repeated cache locking and history walks for
unchanged groups.

The storage worker publishes the successful sync sequence with each changed
cache while holding that cache's write lock. A failed sync cannot advance this
revision. Observations retain the cache identity to prevent an address reused
by another group/open from matching, and check closure and poisoning before
using cached totals. If publication races a recount, retaining the earlier
observed revision forces another recount on the next call. Append replacement,
applied-image changes, compaction and snapshot installation all use the same
publication boundary. Archive progress is checked independently.

Five regressions exercise real raft-engine groups. A second lookup of unchanged
history must complete while a blocking worker holds the cache write lock; before
reuse it waits on that lock and fails the test. The other cases compare exact
byte and entry totals with a full recount through suffix replacement, rejected
committed-history replacement, image changes, checkpoint/install, archive
advancement/regression, reopening and two groups in one persisted batch. A
filesystem gate verifies old accounting during stalled sync and refusal after
failed sync; poisoning and closure also refuse cached observations. All five
pass after the change. Existing SDK reservation and cancellation scenarios are
included in the broader validation being run.

A temporary diagnostic used 1,500 real engine groups with 32 retained entries
each, applied index 33 and archive index 31: initially 3,000 unarchived entries,
below the admission cap. Across 100 paired rounds, alternating order, a full
recount averaged 912.23 µs and reuse averaged 49.11 µs. With one different group
persisted before each round, the corresponding means were 1,039.43 and 65.26 µs.
Every pair had identical byte and entry totals. Persistence was outside the timed
region. This local macOS debug-build result measures accounting cost, not SDK
throughput or container latency. The [stage evidence](evidence/tikv-admission-accounting-stage.json)
records fixture constraints, source hashes and raw artifact hashes. The corrected
probe asserts that archive position does not exceed applied progress; an earlier
probe without coherent applied progress is excluded. Instrumentation is removed
from production and test sources; its source and output remain in the ignored
diagnosis directory.

The affected suite passes 692 cases: 451 library, 36 archive, 16 directory,
16 effects, 28 host, 27 membership, 19 operations, 21 reads, 19 runtime,
42 snapshot and 17 storage. Five ignored process helpers run through their
parent cases. Clippy first detected two unit-test modules independently loading
the same filesystem fault fixture. Moving that fixture to their shared parent
resolved the duplicate module; all 12 host unit tests and strict all-target
Clippy pass afterward. Formatting, module layout, dependency policy and
documentation checks pass. The production-image comparison below is complete;
the end-to-end performance objective remains open.

The regular Linux SDK job at `2ee7fd6` failed both
`native_snapshot_interrupted_transfer_retries_without_blocking_other_groups`
and `native_snapshot_shutdown_joins_partial_transfer` with generic elapsed
deadlines (40 passed, two failed). This repeats earlier Linux snapshot failures;
the latest local 42-case snapshot pass does not explain them. The free-threaded
job advanced beyond its test/lint step and is still running at this observation.

## Accounting reuse production comparison

Control `2ee7fd6` and candidate `5080836` each completed six 60-second windows,
first at three/six/nine concurrent writers, then in reverse order. Both used
three Colima nodes with one vCPU and 3 GiB each, 500 leaders and 1,000 followers
per node, quorum acknowledgements, 500-ms ticks, a 4,096-item request queue and
512-MiB archive scratch mounts. Image, executable and frozen source identities
matched; neither run overlapped builds, tests or profilers from this checkout.

| Build | Repeat | Writers | Accepted writes/sec | Successful-response p99 (ms) | Overloads |
| --- | --- | --- | --- | --- | --- |
| Control | 1 | 3 | 104.96 | 92.39 | 0 |
| Control | 1 | 6 | 182.81 | 89.94 | 0 |
| Control | 1 | 9 | 206.20 | 124.03 | 568 |
| Control | 2 | 9 | 206.87 | 117.26 | 888 |
| Control | 2 | 6 | 239.37 | 52.65 | 0 |
| Control | 2 | 3 | 161.69 | 43.72 | 0 |
| Candidate | 1 | 3 | 181.60 | 44.74 | 0 |
| Candidate | 1 | 6 | 255.41 | 53.21 | 0 |
| Candidate | 1 | 9 | 247.30 | 106.03 | 892 |
| Candidate | 2 | 9 | 246.90 | 79.39 | 2,098 |
| Candidate | 2 | 6 | 232.56 | 69.86 | 0 |
| Candidate | 2 | 3 | 161.62 | 49.72 | 0 |

Independent analysis verified 67,639 control and 81,052 candidate acknowledged
writes including seed against all 4,500 exact replica values and all 1,500
RustFS archive positions in each run. At either the 200-ms or 500-ms p99 budget,
six writers passed both repeats without errors: control averaged 211.09
writes/sec with worst-window p99 89.94 ms, and candidate averaged 243.98 with
worst-window p99 69.86 ms. Nine writers still produced overloads. The candidate's
additional concurrency did not materially increase accepted throughput; its
successful-response latency excludes refused requests.

Other checkouts' build processes appeared in 19/93 control activity samples and
0/92 candidate samples. Reconstructed issue times place all 19 overlapping
samples in the control's nine-writer windows; neither six-writer window had a
sampled build process. Sampling does not prove host isolation, the control's
six-writer repeats varied substantially, and the candidate's second six-writer
repeat was slightly slower than the control's. These short measurements show a
promising operating point, not a controlled, repeatable improvement or sustained
service-rate qualification. A longer confirmation remains necessary.

Peak node memory was 1.21 GB for control and 1.35 GB for candidate, below the
3-GiB limits, with no recorded OOM events or CPU-quota throttling. Shared VM disk
use rose from 89% to 93%; this is another uncontrolled environmental change.
Archive catch-up after replica checks took approximately 16.48 seconds in each
run; different pending work prevents comparing these waits as archive
service rates. Both clusters stopped cleanly, with voter and RustFS volumes,
receipts and closing logs retained. The [sanitized comparison](evidence/tikv-admission-accounting-comparison.json)
records per-window host overlap, resource counters and raw evidence hashes.

Archive-retention admission remains the measured throughput limit. The earlier
stage counters identify the 4,096-entry bound and material download, replay and
publication costs; this production comparison does not further attribute those
costs or establish RustFS server saturation. Raising the backlog cap would add
buffering without demonstrating faster archive catch-up.

## Bounded publications across cells

TiKV's pinned backup endpoint acquires a `SoftLimit` permit before scanning a
batch and releases its progress lock before doing the work. Its worker limit is
independent of the number of regions. Cellar's archive producer instead held
one node-wide slot through capture, all object-store waits, replay and head
confirmation. The first hypothesis is that overlapping independent I/O improves
catch-up. Repeated history download/replay and contention for the single CPU
remain alternative limits; a larger window can increase memory and contention.

The real Node/SDK regression
`independent_cell_archives_while_another_publication_is_stalled` holds one
cell's archive payload read and requests S3 durability for a second cell. With
one producer the independent acknowledgement hits its two-second observation
deadline; after the scheduler change it succeeds while the original read is
still held. The test releases and joins the held publication on either outcome.

`RaftArchiveOptions::max_producers` now bounds concurrent publications from one
to four, with default two. Each slot covers SQL capture through final archive
completion. The in-flight scope set excludes a cell until its captured cut
finishes; round-robin selection continues across other eligible cells. The
capture bank reserves the configured count times the per-cut byte limit. Each
producer keeps its bounded recovery sets, so aggregate memory scales with the
window. The existing verifier serializes physical replay through the same
exclusive workspace. Cancellation applies to every producer and shutdown joins
all publication work before releasing that workspace. Head checks, physical
replay, quorum/image acknowledgement and backlog limits are unchanged.

Additional regressions exercise same-cell exclusion while newer quorum writes
commit, exact restoration after catch-up, limits of one/two/four with an extra
waiting cell, and cancellation releasing every held read and capture permit.
Invalid zero or oversized windows are refused. The cancellation fixture first
encountered `Overloaded` from a read-only status observation colliding with the
host's existing status poll. That observation now retries only this transient
failure under its original deadline; write identities and all progress,
resource-bound and shutdown assertions remain unchanged.

Validation passes 696 Rust cases: 452 library, 39 archive, 16 directory,
16 effects, 28 host, 27 membership, 19 operations, 21 reads, 19 runtime,
42 snapshot and 17 storage. Five process helpers are invoked by their parent
cases. Strict all-target Clippy, formatting, layout, dependency policy and
documentation checks pass. The performance Python suite passes 94 cases; its
opt-in Docker freshness case was not enabled. The frozen-image comparison below
independently checks source, executable and image identities. The comparisons
below are complete; sustained throughput qualification remains pending.

The performance runner exposes `--archive-producers`, records the setting and
refuses to change it during setup continuation. A same-image comparison at one
and two producers used six/nine/twelve writers in both orders. The
idle performance VM's disk was expanded from 80 to 120 GiB before either run;
CPU, memory and node limits remain unchanged. All previous volumes are retained,
including an explicit check of the preceding accounting comparison's eight
volumes. The new filesystem has about 44 GiB available. This expansion avoids
running the comparison near disk exhaustion; it changes the environment from
earlier measurements and applies to both comparisons below.

## Archive publication concurrency measurements

The frozen production image at `22d7689` completed the one-versus-two-producer
comparison with all source/image/executable identities verified. Each fresh
cluster ran six 60-second windows at six/nine/twelve writers and then reverse
order. One producer verified 83,777 acknowledged writes including seed; two
verified 81,075. Every run passed all 4,500 exact replica-value and 1,500 archive
coverage checks. Neither run overlapped builds, tests or profilers from this
checkout.

| Producers | Best error-free setting across both repeats | Mean writes/sec | Worst-window p99 (ms) | Total overloads across the sweep |
| --- | --- | --- | --- | --- |
| 1 | 6 writers | 232.31 | 84.65 | 19,723 |
| 2 | 12 writers | 243.53 | 149.06 | 0 |

Both the 200-ms and 500-ms analyses select these settings. Two producers removed
refusals in this sweep, but did not establish a general throughput gain. At six
writers, two producers averaged about 206 writes/sec versus 232 for one. Other
checkouts' builds appeared in 26/94 serial and 88/88 parallel activity samples;
all sampled parallel windows overlapped builds. The serial twelve-writer
windows had no sampled build process and still returned 7,471/9,272 overloads.
These results are useful for admission behavior but are not a controlled or
sustained speedup comparison. The [sanitized comparison](evidence/tikv-archive-producers-comparison.json)
contains every window, approximate host overlap, sampled CPU use and raw hashes.

Peak node memory was 1.70 GB with one producer and 1.60 GB with two, below the
3-GiB limits, with no recorded OOM events. Final archive catch-up after replica
checks took 16.54/11.18 seconds; differing pending work prevents treating those
waits as comparable archive service rates. Both clusters stopped cleanly and
all voter/RustFS volumes, receipts and closing logs remain preserved.

A fresh higher-concurrency run used the same image and two producers, with
twelve/eighteen/twenty-four writers and then reverse order. It verified another
86,502 acknowledgements including seed against every replica and archive.

| Repeat | Writers | Accepted writes/sec | Successful-response p99 (ms) | Overloads |
| --- | --- | --- | --- | --- |
| 1 | 12 | 250.31 | 149.35 | 0 |
| 1 | 18 | 274.82 | 172.70 | 0 |
| 1 | 24 | 263.65 | 215.60 | 1,141 |
| 2 | 24 | 220.74 | 274.05 | 2,852 |
| 2 | 18 | 198.15 | 165.79 | 6,835 |
| 2 | 12 | 207.19 | 175.12 | 0 |

Twelve writers remained the only setting passing both repeats, at 228.75 mean
writes/sec and 175.12-ms worst-window p99 under either latency budget. The first
eighteen-writer window cannot qualify that setting when its second repeat has
errors. Other builds appeared in 22/90 host samples, although none appeared in
the failing second eighteen-writer window's twelve samples. Peak memory was
1.66 GB without recorded OOM events. This cluster also stopped cleanly with all
volumes retained. The [higher-load evidence](evidence/tikv-archive-producers-high.json)
preserves complete measurements and provenance.

The new scheduler resolves cross-cell archive blocking, but higher load still
reaches admission limits. The measured publisher materializes the previous
recovery set and then downloads its retained payloads again while verifying the
extended set. The next experiment targets eliminating this duplicate
materialization while preserving the complete final digest/coverage/replay
proof, including covered-head and snapshot-rotation behavior. History growth,
physical replay and shared-log/image persistence remain separate candidate
costs; the new measurements do not fully attribute the remaining ceiling.

The `22d7689` Linux SDK jobs were still running their test/lint step at the last
observation. The earlier snapshot/management-stop/lease failures remain
unexplained; local passes do not establish their resolution.

## Avoid duplicate archive materialization

The native archive fixture reproduces redundant reads at the object-store
boundary: extending a snapshot, three tails and one retained dependency makes
12 payload reads. One confirmation of the new tail plus a complete read of the
resulting set requires seven. This exercises actual durable engine cuts, LTX
entries and physical restoration; it measures object requests rather than
end-to-end throughput.

The publisher now plans an extension from validated head/manifest metadata.
The complete resulting set still has every payload downloaded, hashed, decoded
and physically replayed before conditional head publication. Covered-head
success continues to require a full material load and replay. Snapshot rotation
validates the prior material before creating the replacement base, since final
verification would otherwise omit those prior objects.

The corruption regression covers missing and equal-length corrupt snapshots,
tails and dependencies across covered heads, extensions and rotations. Review
also exposed a dependency-reuse case: if a cut supplied a missing prior object's
bytes, re-uploading it could silently repair the selected history before final
verification. The added case reproduced this failure in the first candidate.
The final implementation computes the dependency reference, retains an exact
existing reference without PUT, and requires final verification to read it.
New dependencies still use conditional creation and read-back confirmation.
Failed publications must leave the selected head unchanged; successful recovery
must restore the exact committed SQL image.

The final read-count regression passes at seven payload reads; a subsequent
extension that explicitly references the retained dependency uses eight reads
for the larger set, without re-uploading that dependency. All 601 selected Rust
cases pass: 452 library, 41 archive, 28 host, 19 operations, 19 runtime and
42 snapshot cases. Three ignored process helpers run through their parent
scenarios. This includes the covered/extended/rotated head integrity cases and
the native SDK quorum, S3-outage, failover and shutdown scenarios.
Strict all-target Clippy, formatting, module layout, native dependency policy
and documentation checks also pass on Rust 1.98.1.

The frozen `b2e4850` candidate and `22d7689` control each completed a fresh
1,500-group run with two archive producers, using sixty-second windows at
concurrency 12, 18, 24, 24, 18 and 12. All twelve windows had zero write errors.
Both runs verified all 4,500 exact replica values and 1,500 archive positions:
52,131 acknowledged writes including seed for the candidate and 53,547 for the
control. Both clusters stopped cleanly, retaining their volumes and logs.

| Implementation | Repeated qualifying concurrency | Mean writes/sec | Worst-window p99 | Peak node memory |
| --- | --- | --- | --- | --- |
| Candidate: avoid duplicate materialization | 12 | 145.71 | 325.81 ms | 1.06 GB |
| Control: prior materialization | 12 | 193.00 | 224.17 ms | 1.09 GB |

Neither run qualified a setting under 200 ms across both repeats; only
concurrency 12 qualified under 500 ms. No OOM events were recorded. Final
archive catch-up took 17.33 and 17.47 seconds respectively. The
[complete comparison evidence](evidence/tikv-archive-materialization-comparison.json)
records every window, resource counters, source/image/executable identities and
hashes of the retained raw results.

These measurements do not establish an end-to-end speedup. The candidate was
slower at concurrency 12, while the control varied from 56.99 to 220.29 writes/sec
at concurrency 18 and from 150.68 to 52.06 at concurrency 24. Other builds
appeared in 73/92 candidate and 60/94 control host samples. A snapshot during the
control run recorded 12,568.19 MiB of swap in use on the 32-GiB host; active VMs
had 40 GiB of configured memory. That allocation is not a measurement of
resident use, and a single snapshot cannot attribute individual windows. The
user explicitly chose continued shared-host measurements. Our own builds,
tests and profilers were absent throughout both timed runs; other workloads
were left untouched. The object-read regression establishes reduced archive
work, but this sequential comparison establishes neither a causal performance
improvement nor a causal regression.

The `22d7689` Linux SDK matrix finished with the 3.14t job passing and the 3.14
job failing `node_shutdown_hands_off_to_published_voter`: one group examined,
zero handoff attempts and one unavailable group, without an elapsed overall
deadline. This is the previously observed intermittent handoff symptom;
the earlier local passes did not resolve it.

## Preserve the drain turn through temporary archive capture

An instrumented repetition of the original shutdown case reproduced the failure
on its fourth iteration. The drain skipped a replica with `sql_busy=true`, while
failure, retirement, pending command, configuration, incoming snapshot, apply
and publication flags were all clear. Application calls had completed, but
background archive capture could still own the SQL worker. The scan advanced
its cursor before that check and never revisited the group.

The regression holds an actual immutable-image read during archive dependency
capture after all three replicas return the exact committed value. The original
implementation fails because drain closes the SQL queue instead of waiting
for that capture. The implementation now retains the original group deadline
while local work finishes. Waiting and authenticated candidate inspection share
the same one-second budget, capped by the original Node deadline; each group
is counted once. Failure and retirement remain immediate refusals. Transfer
fencing, target publication checks and physical shutdown joining are unchanged.

The held-capture case now completes handoff after the read is released. A second
case keeps the physical reader held until the group budget is exhausted: drain
reports one unavailable group without a transfer, and Node shutdown still waits
for release before returning. The six focused Node shutdown cases pass. All
temporary diagnostic probes have been removed. This explains and repairs the
locally reproduced handoff path; fresh Linux CI must still validate the change.
It does not explain the separate earlier snapshot or management-stop failures.

The broader local suite passes 630 cases on Rust 1.98.1: 452 library, 41 archive,
28 host, 27 membership, 21 operations, 19 runtime and 42 snapshot cases. Four
ignored process helpers are invoked through their parent scenarios. This run
includes the previously failing Linux snapshot cases and the management-stop
cases; local passes alone do not explain those earlier failures. Strict
all-target Clippy, formatting, module layout, dependency policy and
documentation checks pass.
An additional 40 isolated repetitions also pass: twenty of the original
handoff case, ten releasing the archive reader within budget and ten holding it
through budget exhaustion. The deterministic pre-fix failure and these
post-fix repetitions exercise the actual Node, quorum and physical SQL paths.

## Batch physical archive replay without dropping intermediate proofs

TiKV's `ApplyContext::prepare_for`, `commit` and `write_to_db` accumulate apply
work before the durable engine write and completion notification (local TiKV
revision `b3995dab28308f09cbb7c125bf32f789dbc203fe`,
`components/raftstore/src/store/fsm/apply.rs:542–635`). Cellar can use that
bounded batching principle in disposable archive replay while retaining its
SQLite image and LTX proofs. No TiKV source was copied.

The native archive regression reproduces one image sync and rename for the
snapshot plus each of twenty tail entries: 21 seals. Bounded replay now needs
one seal for the same history and produces identical SQLite bytes. Restricting
the destination to three files per batch requires seven seals; one file per
batch retains the original 21. This is an operation-count result, not a measured
end-to-end throughput improvement.

`GenerationStore::prepare_chain` copies a pinned base once and checks each
intermediate image's complete SHA-256 and checksum before applying the next
update. Sealing happens only after the complete batch succeeds. The tests include
a false middle digest with a metadata-contiguous following entry that overwrites
it; both checksum modes refuse the chain. The archive fixture separately proves
that this failure leaves the previous published head unchanged. Sync failures
preserve the original pin and refund successfully cleaned temporary capacity.

The archive adapter retains at most 64 envelopes or 4 MiB of entry bytes per
batch, with a larger valid envelope handled alone. Destination file/page/byte
budgets can end the batch sooner. The snapshot is borrowed from the recovery
set. Peak image size controls disk reservation, and a shrink ends the batch so
later growth uses the smaller pinned base. A real shrink/growth restore under
a two-generation disk budget covers this capacity boundary. Ordinary committed
image publication still needs its existing image sync and durable Raft marker;
archive batching changes neither quorum nor S3 acknowledgement requirements.

Before the next comparison, the dedicated benchmark VM's 120-GiB logical
data disk was relocated to the Workspace volume. A complete independent SHA-256
read matched the source before switching paths. The VM booted with the same
8-vCPU/16-GiB configuration, all 164 retained Docker volumes and all 28 image
entries matched, and the original redundant file was then removed. This freed
about 100 GiB on the host filesystem without pruning Docker data. Both sides of
the next comparison must use the new backing location; earlier internal-disk
timings are not a controlled baseline for that comparison. Shared-host load
remains part of the measurement conditions.

## Select the healthy group's actual leader in snapshot drills

The fresh Linux SDK job for `b68c086` and the local archive-batching regression
suite both hit the interrupted-snapshot-transfer deadline. The snapshot drill
looked for a command-ready leader of `Counter:healthy` while excluding the node
receiving `Counter:snapshot`. Leadership is per cell, so that node can be the
healthy group's only legitimate leader after the partition heals.

A deterministic regression transfers the healthy group's leadership onto the
snapshot receiver while its partial snapshot response is held. Before the fix,
the fixture expires after ten seconds with that excluded node command-ready and
both remaining nodes following it. The corrected fixture selects the observed
healthy leader and still requires a committed command while transfer is held,
a retry after the snapshot deadline, exact follower data after recovery, and
zero callback replays for the snapshot cell. Per-cell callback accounting keeps
that last assertion exact when the same node executes a legitimate command for
the healthy cell. The total callback assertion remains twelve snapshot commands
plus one healthy command. No deadlines or production Raft behavior change.

All 210 affected native integration cases pass after the fixture correction,
including 43 snapshot cases. The 452 library cases also passed against the same
production code. Six process helpers run through their parent cases. Strict
all-target Clippy for `cellar` and `cellar-ltx`, the LTX build without default
features, formatting (799 Rust files), module layout, dependency policy (452
packages) and documentation checks pass. Fresh Linux qualification is still
required; the old CI failure is retained rather than represented as a passing
run. Thirty isolated final repetitions also pass: ten original interrupted
transfers, ten with the healthy leader forced onto the snapshot receiver, and
ten shutdowns with a partial transfer held.

## Shared-host measurements after disk relocation

[Recorded failure evidence](evidence/tikv-replay-shared-host-failures.json)
retains both incomplete experiments on the external VM disk. The control's first
12-writer window reached 42.07 writes/sec with p99 1,481 ms and sixteen ambiguous
outcomes. All sixteen original requests were reconciled within their original
deadlines; all 4,500 replicas and 1,500 archive positions then matched. This does
not turn the failed timed window into a successful result.

A separate candidate-first experiment lowered concurrency to 3, 6 and 9. Its
first windows reached 15.28, 34.44 and 45.10 writes/sec with p99 974, 854 and 737 ms.
The reverse-order nine-writer repeat reached 28.55 writes/sec and p99 1,629 ms,
with 21 ambiguous outcomes and one unavailable response. The control half was
not run after this failure. All original retry deadlines had expired before
reconciliation, so no mutations were replayed. A read-only scan found all
4,500 replicas available: the 21 ambiguous cells each had one extra increment
on all three replicas, while the unavailable request had none. All 1,500
archives covered the observed applied positions. These observations are not
recovered receipts for the expired identities, and the original run remains
failed. Both failed cohorts stopped cleanly; their volumes remain preserved.

Candidate node CPU averaged 22–23%, with final memory around 220 MiB per node.
The host monitor observed other builds in 74 of 89 samples and 26,481 additional
16-KiB swap-in pages. These are shared-host results, not an isolated capacity
limit. Separate interleaved filesystem probes measured a complete 4-KiB image
seal at mean 2.50 ms / p99 6.87 ms on the external ext4 VM disk, compared with
0.57 / 1.01 ms on an internal-host virtiofs path. A write-plus-fsync probe also
favored the internal path. Neither probe attributes the entire multi-second
write tail, and no filesystem or durability settings were changed afterward.
Earlier internal-disk throughput numbers cannot serve as a controlled baseline
for these runs. Archive replay batching has a proven operation-count reduction,
but no end-to-end performance gain is established by these experiments.

## Reuse an exact sealed leader preview for committed apply

A real Node/SDK regression issues eight counter commands, waits for S3 proof,
and counts physical seals only in that cell's image directory. Before the change,
the eight commands require sixteen seals: one before proposal and another during
committed apply. With preview transfer, they require eight. Restart recovers all
eight original receipts and the exact counter without executing callbacks again.
This proves less physical work; container throughput and latency still need a
fresh comparison.

`PrivateExecutor` records the complete encoded update digest and exact base/result
states only after physical preview verification. `ImageApplier` validates the
existing durable ticket, committed entry, applied head, schema and base before
accepting the preview. Reuse also requires submission and the same replica and
open generation-store owner. A mismatch leaves the private pin intact and follows
ordinary physical replay. The transfer removes the pin from private retirement
ownership; publication and acknowledgement still wait for the durable apply
marker. Followers and recovery keep their physical verifier. The binding test
covers changed payload bytes, base/result states and a foreign store containing
the same image ID and bytes, and confirms transferred retention survives private
owner teardown.

The original archive-pressure drill assumed that a third leader write must
allocate a fourth generation after commitment. That assumption no longer holds
for an exact preview: the updated Node test requires quorum publication while
the old archive pin is held, then S3 catch-up without callback replay after its
release. A separate real RawNode/raft-engine/ImageApplier test fills a two-image
store with a pinned old generation and the current head. A committed replay with
no preview must return capacity pressure, preserve its original ticket and public
image, refuse publication, and finish only after the old pin is released. This
retains the committed-replay backpressure boundary instead of requiring the
optimized leader to allocate duplicate bytes.

[Local validation](evidence/tikv-preview-reuse-validation.json) passes 713 distinct
Rust cases: 453 library and 260 native integration cases across eleven binaries.
Seven process helpers execute through their parent scenarios. Strict all-target,
all-feature Clippy for `cellar` and `cellar-ltx`, the LTX library build without
default features, formatting (801 Rust files), module layout, dependency policy
and documentation checks pass. The earlier archive-pressure failure is preserved
with the successful checks. The container comparison below is complete; fresh
Linux qualification of this change is still required.

## Sealed-preview container results

[Failed sweeps](evidence/tikv-preview-reuse-failures.json) preserve the first
preview-reuse candidate (`c6aed34`) and a subsequent control (`1ba6a3a`) sweep.
The candidate reached 48.10, 47.16 and 47.48 writes/sec at one, three and six
writers, with p99 62, 360 and 830 ms. Its nine-writer window failed at 25.16
writes/sec and 2,908-ms p99 with thirteen ambiguous and four unavailable
outcomes. The control later reached 39.98 and 47.74 writes/sec at one and two
writers, then failed at three writers with four ambiguous and two unavailable
outcomes. All 23 requests reconciled outside their timed windows using only
original identities, inputs and unexpired deadlines. Each cohort then passed
4,500 exact replica values and 1,500 archive positions. These timed runs remain
failed. Both cohorts stopped cleanly and retained their volumes.

The candidate's failed sweep averaged 36–39% node CPU, with less than 0.09 seconds
of CPU throttling per node and no OOM events. Closing cgroup counters recorded
83–88 seconds of some-task I/O pressure per node over the entire cohort,
including setup and verification. These are evidence of storage waits, not
stage-specific attribution of the timed latency distribution.

A [fresh lower-load comparison](evidence/tikv-preview-reuse-comparison.json)
then ran control first and candidate second, each with four 60-second windows
at concurrency 1, 2, 2, 1. Both used the same external Colima disk, three 1-vCPU /
3-GiB nodes, 500 leaders per node, two archive producers, 512-MiB archive scratch
tmpfs, and the existing durable quorum plus image-publication acknowledgement.
There were no timed mutation retries. This task ran no builds, tests or
profilers during timed windows; other host workloads continued. Source revisions, image/binary hashes, limits and raw evidence hashes
are retained in the comparison record.

| Image | Writers | TPS in each repeat | Mean TPS | Worst p99 (ms) | Both repeats below 500 ms |
| --- | --- | --- | --- | --- | --- |
| Control `1ba6a3a` | 1 | 43.09 / 22.07 | 32.58 | 276.67 | Yes |
| Control `1ba6a3a` | 2 | 51.06 / 41.33 | 46.19 | 183.08 | Yes |
| Preview reuse `c6aed34` | 1 | 40.78 / 7.33 | 24.06 | 900.25 | No |
| Preview reuse `c6aed34` | 2 | 23.58 / 8.26 | 15.92 | 1,046.89 | No |

All eight windows had zero write errors. The control acknowledged 10,976 writes
including seed and the candidate 6,301; both passed all 4,500 exact replica values
and 1,500 RustFS archive positions. Both cohorts stopped with exit code zero and
no OOM kills; all eight volumes remain preserved. Independent analysis selects
two writers on the control at either the 200- or 500-ms ceiling. It selects no
candidate setting at either ceiling.

Control node CPU averaged 27–28%, with final memory around 228 MiB; candidate
CPU averaged 23–25%, with final memory around 212 MiB. The host monitor recorded
other builds in all 89 control samples and 58 of 70 candidate samples, alongside
roughly 10.2 GiB of swap usage and active paging. This sequential shared-host
comparison does not isolate the cause of the candidate's worse results. The
operation-count reduction is proven, but **this comparison shows no end-to-end
speedup and the candidate does not meet the requested repeated latency gate**.
Do not attribute all of the regression to host interference without further
evidence. The PR remains a draft.

Both Linux SDK variants and the Python examples passed on `1ba6a3a`, including
the corrected per-cell snapshot drill. Preview-reuse Linux CI failed as detailed
below; those preceding-revision passes do not qualify the new implementation.

## Image publication and reclamation barriers

An isolated, instrumented image of `34f9493` ran four 60-second windows at two
writers with the same 1,500-group, three-node resources and durability. The
[diagnostic record](evidence/tikv-preview-io-diagnosis.json) includes source,
binary and raw-evidence hashes. Instrumented throughput is diagnostic only.
All windows were error-free; all 4,500 replica values and 1,500 RustFS archive
positions verified. Containers stopped cleanly and their volumes are retained.

During sampled intervals the VM data disk was 80–85% busy. Image file syncs
averaged 6.3–7.9 ms, image directory syncs 4.1–6.4 ms and reclamation directory
syncs 4.3–6.8 ms. Physical materialization averaged 0.7–1.3 ms, and committed
prepare queue waits averaged 0.18–0.37 ms. These nested timings cannot be added
as exclusive request latency. They prioritize reducing storage barriers over
raising SQL worker counts; they do not establish the cause of the earlier
candidate/control regression.

Raft persistence averaged roughly nine batches per acknowledged write across
the three nodes, with about 1.02 groups per batch at this load. Archive progress
does not itself persist another Raft watermark: verified archive completion
updates volatile host/core state. Append, commit and applied-image persistence
remain separate proof boundaries. No barrier has been removed from those paths.

The next implementation applies the shared-barrier principle illustrated by
TiKV's bounded write batching to obsolete image deletion. Normal image
preparation removes eligible retired files before the directory sync already
required for the new image's rename. Capacity stays charged and the selected
entries stay busy until that sync completes. Tight admission first reclaims
and syncs capacity separately. Reader pins, retained markers and monotonic
references still protect their images; any failed shared deletion/barrier
quarantines the store. The host defers ordinary post-publication cleanup to the
next preparation rather than issuing another directory sync on the write path.
Idle retired files remain within the existing store limits and are reconciled
from authoritative references on recovery.

The real SDK regression initially observed 14 image-directory syncs for eight
commands, despite only eight image seals. The added assertion requires eight
directory syncs while also requiring actual deletion, exact results and receipt
recovery after restart. Physical tests cover capacity before sync, pinned-image
pressure, bounded two-name recovery cleanup, I/O failures and process loss during
shared deletion/publication. End-to-end performance qualification is pending.

[Local validation](evidence/tikv-shared-image-sync-validation.json) passes all
718 selected Rust cases: 453 library and 265 native cases across eleven test
binaries. Eight process helpers execute through their parent cases. Strict
all-target/all-feature Clippy, the LTX build without default features, formatting,
layout, dependency policy and documentation checks pass. The SDK assertion now
passes with eight directory syncs; the original 14-sync failure is preserved.

Both `34f9493` Linux SDK jobs failed. The 3.14t job failed in the Python bridge
fixture with response JSON decoding and incomplete host shutdown. The 3.14 job
passed 87 native Node cases and failed two: lost append/ack response qualification
exceeded its deadline, and named binding recreation ended with the owner fenced
for an expired lease. These causes remain unresolved; preceding revision passes
do not qualify this revision or the new shared-barrier change.

## Shared image-barrier container results

The [external-disk attempts](evidence/tikv-shared-image-sync-failures.json) retain
the shared-barrier candidate `12d6e87` and preview-reuse control `c6aed34` failures.
At one writer, the candidate completed 619 writes (10.31 TPS, 871-ms p99) and
returned one unavailable outcome. The control completed one error-free window at
22.11 TPS and 435-ms p99; its next two-writer window completed 2,118 writes
(35.28 TPS, 269-ms p99) and returned two ambiguous outcomes. All three outcomes
reconciled outside timed windows with original identities, inputs and unexpired
deadlines. Each cohort then passed all 4,500 exact replica and 1,500 archive checks.
Both original timed runs remain failed. All containers stopped cleanly and all
volumes remain preserved. This evidence establishes neither a speedup nor the
cause of the different rates.

A [fresh internal-storage comparison](evidence/tikv-shared-image-internal-comparison.json)
uses persistent node bind volumes on the host internal filesystem through Colima
virtiofs, with RustFS still on the external VM disk. Both images use this same
backing and the same three 1-vCPU / 3-GiB nodes, 500 leaders per node, quorum plus
committed-image publication acknowledgement, two archive producers and 512-MiB
archive scratch tmpfs. A preflight checks locking, file sync, rename, directory
sync and readback; it does not qualify host power loss. These absolute results
must not be compared against external-node-disk runs as a code-only speedup.

The control ran first, then the candidate. Each ran four 60-second windows at
2, 4, 4 and 2 writers, without timed mutation retries. Source/image/binary hashes,
resource limits, raw evidence hashes and storage placement are recorded.

| Image | Writers | TPS in each repeat | Mean TPS | Worst p99 (ms) |
| --- | --- | --- | --- | --- |
| Control `c6aed34` | 2 | 71.29 / 79.33 | 75.31 | 164.95 |
| Control `c6aed34` | 4 | 112.52 / 79.48 | 96.00 | 136.03 |
| Shared barrier `12d6e87` | 2 | 137.74 / 101.92 | 119.83 | 54.76 |
| Shared barrier `12d6e87` | 4 | 134.16 / 149.96 | 142.06 | 73.91 |

All eight windows were error-free and met both the 200- and 500-ms p99 ceilings.
Both cohorts passed all 4,500 exact replica values and 1,500 archive positions;
acknowledged counts including seed were 22,067 for control and 32,934 for candidate.
Both stopped with zero exit codes and no OOM kills, retaining all eight volumes.
The four-writer candidate mean is 48.0% higher, with 45.7% lower worst-window p99.
This is an observed improvement under these conditions, not an isolated causal
estimate or a sustained-capacity qualification.

Other builds appeared in every host sample. Recorded swap use ranged from 11.1
to 12.4 GiB during control and 10.1 to 11.1 GiB during candidate, so host conditions
were not identical. Control setup initially failed two seed requests, which were
continued using their original identities, inputs and unexpired deadlines before
measurement. Its host monitor has a 55.64-second gap overlapping the first window;
that period cannot support continuous host-pressure attribution. No timed mutation
was retried. Higher concurrency and reversed image order remain to be measured.

## Higher-concurrency shared-barrier comparison

The [fresh higher-load comparison](evidence/tikv-shared-image-internal-high.json)
reverses image order: candidate first, then control. Each image ran six 60-second
windows at 6, 12, 24, 24, 12 and 6 writers, with the same internal node volumes,
external RustFS storage, limits and durability contract as the preceding pair.
All twelve windows had zero write errors and no timed mutation retries.

| Image | Writers | TPS in each repeat | Mean TPS | Worst p99 (ms) |
| --- | --- | --- | --- | --- |
| Control `c6aed34` | 6 | 203.54 / 97.30 | 150.42 | 601.11 |
| Control `c6aed34` | 12 | 211.91 / 133.75 | 172.83 | 413.41 |
| Control `c6aed34` | 24 | 205.35 / 180.96 | 193.16 | 395.11 |
| Shared barrier `12d6e87` | 6 | 140.90 / 151.77 | 146.33 | 148.61 |
| Shared barrier `12d6e87` | 12 | 190.65 / 183.26 | 186.96 | 147.75 |
| Shared barrier `12d6e87` | 24 | 188.25 / 197.67 | 192.96 | 274.38 |

The candidate's best observed setting under 200 ms is twelve writers; no control
setting passes that ceiling in both repeats. Under 500 ms, both select 24 writers
at essentially the same 193 TPS, with lower worst-window p99 for the candidate.
Control has faster individual windows and a slightly higher six-writer mean.
These mixed results do not establish a general throughput increase. Twelve
candidate writers retain about 97% of its 24-writer throughput with substantially
lower tail latency, so that setting is selected for longer confirmation.

The candidate acknowledged 64,717 writes including seed; control acknowledged
63,960. Each cohort passed all 4,500 replica values and 1,500 RustFS archive
positions, stopped with zero exit codes and no OOM kills, and retained all four
volumes. Candidate archive catch-up after measurement took 5.79 seconds.
Other builds appeared in 65 of 99 candidate samples and 52 of 94 control samples;
recorded host swap usage and paging also differed. Shared-host conditions still
prevent an isolated causal estimate. The longer comparison uses two 180-second
windows at twelve writers per image, control first. Both subsequently failed as
recorded below.

The older `accelerated` baseline image carries revision label `a849cc2`, while
its later run report names `fb660b1` with a nonempty working diff. A subsequent
[clean current-main build](evidence/tikv-main-baseline-build.json), exported from
unmodified `fb660b1` and compiled with locked dependencies, produced the exact
same executable SHA-256 as that older image. This resolves the executable's
provenance gap. The fresh image is used for the final comparison; executable
equality does not make historical measurements a controlled baseline for the
current storage layout and host conditions.

## Longer shared-barrier confirmation failures

The [retained longer runs](evidence/tikv-shared-image-long-failures.json) use two
180-second windows at twelve writers on each image, with the same internal node
storage and external RustFS placement. Both cohorts failed the repeated latency
and zero-error gate; the shorter sweep's result did not reproduce here.

| Image | Repeat | TPS | p99 (ms) | Write errors |
| --- | --- | --- | --- | --- |
| Control `c6aed34` | 1 | 122.69 | 512.22 | 0 |
| Control `c6aed34` | 2 | 56.03 | 1,534.67 | 47 ambiguous, 1 overloaded |
| Shared barrier `12d6e87` | 1 | 131.08 | 234.94 | 0 |
| Shared barrier `12d6e87` | 2 | 52.97 | 2,942.36 | 1 ambiguous |

All 49 unsuccessful requests reconciled outside measurement using only their
original identities, inputs and unexpired deadlines. The control subsequently
verified all 4,500 replica values and 1,500 archive positions. The candidate's
first read-only scan matched 4,499 replica values and received one 503 from node
one for cell 952. A separate full read-only scan, with the original scan retained,
verified all 4,500 values and 1,500 archive positions. Neither reconciliation nor
read-only repetition converts a timed failure into a performance pass. Both
cohorts stopped cleanly with no OOM kills and retained every volume.

Control errors span all three nodes across a 31-second interval. All six host
samples in that interval observed other builds and about 13.6–14.1 GiB of swap
usage; sampled node CPU was also high. This correlation does not establish the
cause. After these experiments the benchmark VM reported about 13.5 GiB of page
cache, with 2.3 GiB used outside that cache. The next comparison will cold-start
only this benchmark VM before each cohort, preserving its 8-CPU / 16-GiB settings,
all images and volumes, external disk backing, and the active Docker context.
Other profiles and workloads stay untouched. This tests carryover from the long
benchmark sequence; it does not assume that page cache explains the regression.

## Fresh main comparison after benchmark VM restarts

The [cold-start failure record](evidence/tikv-main-coldstart-failures.json)
compares the clean `fb660b1` main executable with candidate `12d6e87` using fresh
internal node volumes and the same external RustFS backing. The benchmark VM
was stopped and started before each cohort, preserving its 8 CPUs, 16 GiB,
images, volumes and active Docker context. Main uses one archive publisher;
the candidate uses two within the same three 1-vCPU / 3-GiB node limits.

| Build | Writers | TPS | p99 (ms) | Write errors |
| --- | --- | --- | --- | --- |
| Main `fb660b1` | 3 | 123.27 | 80.81 | 1 overloaded |
| Main `fb660b1` | 6 | 152.77 | 98.34 | 1 overloaded |
| Main `fb660b1` | 12 | 23.17 | 1,520.73 | 1,791 overloaded, 31 unavailable, 4 ambiguous |
| Candidate `12d6e87` | 3 | 53.32 | 307.50 | 0 |
| Candidate `12d6e87` | 6 | 132.38 | 251.35 | 1 ambiguous |

Both runs stopped at their first window containing a non-admission error, so
neither completed the planned 3/6/12/12/6/3 sweep. Main's 1,828 unsuccessful
requests reconciled outside measurement; four needed a second attempt after
another overload refusal. The candidate's one ambiguous request also
reconciled. Every attempt retained the original identity, input and unexpired
deadline. Both cohorts then verified all 4,500 exact replica values and 1,500
archive positions and stopped with exit code zero, no OOM kills and all eight
volumes retained. All failed windows and first reconciliation outcomes remain
in the evidence.

Other builds appeared in every host sample, with about 12.6–13.0 GiB of swap in
use. The VM restarts did not remove the observed failures. The candidate was
slower in the common three- and six-writer windows; the incomplete sequential
comparison does not isolate the cause. No sustained performance qualification
or general throughput gain is established.

## Remaining qualification

- Repeat the Linux management-stop admission-seal qualification; the latest
  `b68c086` 3.14t job passes this case, but the earlier failure lacks a direct
  reproducer.
- Complete Linux SDK qualification of the preview-reuse revision; the corrected
  per-cell snapshot drill passes both SDK variants on `1ba6a3a`.
- Distinguish a preview-reuse regression from storage/host stalls with stage-level
  I/O and SQL queue evidence, then repeat with the image order reversed.
- Compare fresh baseline and candidate clusters with identical resources,
  durability, cell counts, client partitioning and archive scratch placement.
  Avoid builds or profilers during timed windows; retain repeated raw samples.
- Require exact values on all 4,500 replicas, archive coverage for all 1,500
  cells, and zero ambiguous outcomes or admission errors in selected windows.
- Publish measured results separately from architectural expectations. TiKV
  mechanism reuse alone is not a performance or durability qualification.
