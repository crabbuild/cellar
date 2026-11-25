<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# TiKV-informed optimization implementation

Purpose: Track implementation and validation against the
[optimization design](tikv-informed-optimization.md), independently of historical
CG acceptance and release qualification.

Machine-readable qualification and benchmark outputs are retained separately.
Keep generated run outputs outside this documentation tree; the prose summaries
preserve their source, scope and unresolved failures.

Earlier local qualification bundles have been cleaned up. Their source-pinned
prose records observations made at those checkpoints; it does not assert that
the old raw media or privacy caches are still available. The current campaign
starts a fresh private evidence baseline and keeps generated outputs outside
this tree.

## Updated-main workspace qualification

Main has since advanced to `b23ec563f` with merged #186. Its integration with
the workspace repair requires a new qualification; the results in this section
remain pinned to the preceding combined source.

[PR #189](https://github.com/crabbuild/cellar/pull/189) applies the bounded
workspace wait to main `7810ead24`, after merged #187 and #188. Combined
compiler inputs `24674839` pass **15 workspace**, **38 checkpoint/commit**,
**16 prefetch** and **39 query controls**, **13 public RF3 selections** and
**13 native service selections**. The complete six-thread library suite passes
**1,438 tests / zero failures / two existing ignores**. Strict locked
all-feature/all-target Clippy, formatting, layout, native dependency,
documentation and performance-tooling checks pass. The five workspace Rust
files match the previously qualified implementation at `3d75f9d8`.

An earlier combined-source public campaign fails its last interrupted-SQL
configuration during managed startup, before business SQL enters. The captured
failure is a group-startup transport timeout. Six unchanged baseline/candidate
controls pass, including both complete eight-configuration replays. The fresh
complete native campaign also passes. These controls do not establish a startup
repair; the original failure remains recorded.

A later retention audit finds older temporary fixture originals unavailable.
All **61 surviving exported native corpora** match their sealed inventories and
closed stream hashes; three independently moved originals remain available.
The seven new native corpora also retain matching originals on a persistent
internal filesystem. Export verification does not establish that a missing
temporary original is still available. Cargo artifacts remain on Workspace.

The combined-source Linux image matches all compiler inputs, both executable
hashes and the pinned compiler/runtime environment. The first build fails on
guest registry DNS; a build using the exact previously captured OCI digests and
offline Cargo succeeds without changing host or guest DNS. Compilation finishes
and the owned VM restarts before workloads begin.

In the requested shared-host topology, SQL Quorum passes all nine C1/4/16 smoke
windows. SQL Quorum+S3 verifies every write and earlier read window, then records
**one overloaded read among 8,498 C16 attempts**. No mutation is unconfirmed;
runtimes remain healthy and extent stores remain unquarantined. That read window
and the case fail. The current-source KV/Queue cases and the full 30-second,
two-repeat matrix remain unqualified. The next gate is to identify the exact
read-admission refusal and repair it with an owning regression, preserving the
original deadline, authority, full capacity charges and callback rules.

Two fixed error-only diagnostic replays then pass: the original short SQL
Quorum+S3 case and the 30-second, two-repeat case. The probes record **14 SQL
reader-bank refusals** and **zero control-metadata refusals**, while every window
verifies. They do not reproduce the failed response or establish its cause.
All children join and the three temporary probe files are restored to the exact
production inputs. These diagnostic passes do not waive the failed production
case or qualify the full performance matrix.

The earlier all-12-case smoke and failed longer comparison below belong to
pre-integration inputs `3d75f9d8`; they do not qualify this combined source.
All measurements remain scoped to the shared host. Generated JSON and private
media stay outside this documentation tree and the PR.

## Fresh main and constrained-runtime verification

Main `86cfe5d1c` includes #184 and #185. The bounded-cache follow-up
`6a0690913`, compiler inputs `fe495002`, fits eight idle successor hints within
the existing 512-byte node bound. A compact lookup tag remains discardable:
the full 256-bit history/schema scope must still match, and every command keeps
its original authority, fingerprint, deadline and durability checks. The owning
eight-target rotation fails before the change and passes afterward; forced
lookup collisions cannot select another scope's sequence.

The full library passes **1,430 tests / zero failures / two existing ignores**.
All **13 selected public RF3 controls** pass, including both image forms of
successor reuse and all eight SQL interrupted-parent recovery configurations.
Strict all-feature/all-target Clippy and repository checks pass.

The unchanged native service campaign passes **8/13 complete functions**:
SQL, KV and Queue callbacks each **2/2**, combined Queue C2 **1/2**, combined
C4 **0/1**, separate materialized C4 **1/2**, and extent-backed C4 **0/2**.
Main's separate clean C4 control passes materialized **1/1** and fails both
extent-backed repeats. The earlier three controls alongside compilation fail
all three and remain diagnostic observations. These are not a matched full
service performance comparison. Original request, callback and drain windows,
identities, unknown outcomes and admission limits remain unchanged.

A fresh Colima VM provides five CPUs and 11.65 GiB usable memory. Each case
uses three independent 1-vCPU/2-GiB nodes, three RF3 groups, shared RustFS and
separate capped store/client containers, with no container swap. The original
6-GiB filesystem reserve passes before every case. Adjacent baseline/candidate
pairs alternate source order across SQL, KV and Queue and all four durability
modes. Both images use identical Rust, Python and runtime package versions.

The **five-second, one-repeat smoke** at C1/4/16 passes **16/24 source cases**.
All Fleet/Bucket cases pass; **8/12 Raft cases fail**, including every Queue
Raft case. Failed windows retain incomplete settlement probes and unconfirmed
mutations. Runtime/connection closure and subsequent `NotFound` observations
require diagnosis; they are not proof that an unconfirmed mutation did not
commit. A targeted failed Queue quorum trace retains healthy node-lease
renewals. Separate SQL trace, C16-only and untraced controls pass, so those
passes do not erase the original failures or establish a repair.

The full 30-second, two-repeat comparison remains unqualified. This checkpoint
closes the bounded working-set regression, not full service, low-latency or
OPT/CG/recovery/release acceptance. Follow [the remaining priorities](remaining-work.md#current-optimization-follow-up).

## Preserve committed prefetch under item pressure

Compiler inputs `a53855fb` address a second bounded admission problem. When
original apply work cannot acquire another item, a uniquely owned cached
successor can carry its existing active item into that apply. Its entire cached
byte charge remains held, and the full ordinary workspace is charged in
addition, against the same unchanged bank. Shared ownership, byte pressure,
another bank, an inactive item or overflow retain the original refusal or
optional-prefetch eviction path. Physical join custody prevents promotion while
the earlier preparation is still running.

The full entry, base, group, engine and page-owner checks and actual page-read
recheck remain required before publication. Reusing preparation does not admit
another operation or establish a durable receipt. The original command and
required durability proof remain individual.

Four owning regressions fail on the test-only baseline `0d579b07`: both image
forms lose their expected cache hit, and both injected read failures miss the
physical cached-page recheck. All four pass on `a53855fb`, including unchanged
selected roots after failure and reservation release after physical join.
Twelve existing controls also pass, covering byte-pressure eviction, real read
and sync faults, close, late cancellation and uncommitted successors in both
image forms. The full library passes **1,431 tests / zero failures / two existing
ignores**. Strict all-feature/all-target Clippy, formatting, layout and native
dependency checks pass. Public and
service requalification keeps the original identities and time windows. All
**13 public RF3 selections** pass, including all eight interrupted-SQL recovery
configurations.

The unchanged native service campaign passes **6/13 complete selections**:
SQL, KV and Queue callbacks each **2/2**, combined Queue C2 **0/2**, combined C4
**0/1**, materialized C4 **0/2**, and extent-backed C4 **0/2**. Failed producer
responses retain their ambiguous deadline outcomes. All captured processes join;
nine structured Queue cleanup records report joined domains and zero errors.
Original and retained fixture copies match. These captures and the earlier
`fe495002` campaign are separate observations, not a matched performance result.

The first failed rooted C2 snapshot has no optional-prefetch eviction or
admission refusal.
Its available, unsaturated aggregate observations report log file sync means of
about **45–48 ms**, manifest preparation **138–157 ms**, and follower image
preparation **162–170 ms**. They include setup and failed work, are not request
percentiles, and must not be added as latency components. Native fixtures occupy
an external USB APFS SSD.

Six unchanged rooted C4 controls bracket internal-filesystem baseline/candidate
ABBA runs with candidate USB runs. Both sources pass both internal runs, each
delivering all **528 messages uniquely**. The candidate passes **1/2** USB runs.
Its internal enqueue-batch p50 is **693–730 ms** and p99 **1,095–1,239 ms**;
the baseline ranges overlap. These controls establish neither a native code
speedup nor a repair of all USB failures. The harness originally placed runtime
fixtures on USB alongside build artifacts. Full service requalification on the
normal internal persistent temporary filesystem passes **13/13 complete
selections**, including every original C2/C4 selection and reverse-order repeat.
Cargo artifacts remain on Workspace. The original failed storage profile, time
windows and media remain retained; no identity, durability minimum or admission
assertion changes. Both complete captures have unchanged source and verified
closed fixture copies. These native results do not qualify the pending capped
Colima performance comparison.

Separate temporary error-only probes at `e263e8e2` retain Queue and SQL deadline
failures with all 15 observed node outcomes recorded as `Stopped` and no captured
runtime/controller/quarantine failure. Those probes have been removed. This is
a distinct failure pattern; it neither reproduces nor repairs the original
runtime closures. Both the service and constrained comparison gates remain
required. All measurements are scoped to the shared host, as requested.

## Keep deferred commit metadata out of local checkpoints

The paired `a53855fb` smoke completes all 24 source cases. All 12 Fleet/Bucket
cases pass; all 12 Quorum/Quorum+S3 cases fail on both main and candidate.
A private error-only reproduction captures the first runtime failure:
`Storage(Invalid("mixed local checkpoint write"))`. Later controller closure and
`NotFound` observations are consequences of that failure in this reproduction.

Compiler inputs `61ed9d55` keep a local checkpoint's write independent of the
later deferred commit metadata. The checkpoint compacts an already durable
applied cut; the deferred HardState remains available for the next ordinary
persistence write. Storage validation, applied-root publication, execution
authority and exact receipt/durability requirements remain unchanged.

Both image forms fail before the fix with the captured mixed-checkpoint error.
They pass afterward, preserving the entire retained suffix and HardState while
refusing successor authority until the original applied-root sync completes.
All **38 checkpoint/commit controls**, **16 prefetch/admission controls**,
**1,431 library tests** (two existing ignores), **13 public RF3 selections** and
the unchanged **13 native service selections** pass. Strict all-feature/all-target
Clippy, formatting, layout, native dependency and documentation checks pass.
The Linux image's compiler inputs and executable hashes match this candidate.

In the requested capped Colima topology, SQL and KV pass all four durability
modes at C1/4/16. Each SQL Quorum voter publishes three actual local checkpoints;
the run crosses the repaired boundary. The smoke then stops at Queue Quorum C16:
C1/C4 delivery and settlement pass, but C16 retains ambiguous deadline responses.
All runtimes remain healthy. A fresh C16-only diagnostic verifies warmup and
then receives two immediate ambiguous `Overloaded` responses. These are separate
admission/tail-latency failures, not recurrence of the mixed-checkpoint error.
Unconfirmed mutations and failed settlement probes remain retained privately.

The smoke uses five-second windows, three-second warmup and one repeat on the
shared host. Queue's other three modes and the full 30-second, two-repeat matrix
remain unqualified on this candidate. No performance ratio is inferred from
main's failed Raft cases. No deadline, quota, durability minimum or validation
assertion has been relaxed, and generated results remain outside the PR.

## Wait for bounded workspace capacity before SQL entry

A fresh C16 Queue diagnostic at compiler inputs `809e95e0` reproduces one
ambiguous overload among 181 warmup attempts. The first physical refusal is
rooted workspace allocation: all **eight** node workspaces are charged, with
**4,966,055,936 bytes** retained and no quarantine or runtime closure. The
per-workspace reservation is **620,756,992 bytes**. The temporary numeric-only
probes have been removed. This identifies the immediate-overload reproduction;
it does not explain every earlier multi-window deadline tail.

Compiler inputs `3d75f9d8` let finite invocations wait for transient workspace
capacity before SQL entry. The original deadline, complete image/WAL/capture
reservation and existing creation lane remain held. Each refused physical
attempt joins before another can start. An inventory observation is only a
scheduling hint; the unchanged store must admit the full reservation. A request
that cannot fit after live workspaces release still refuses. Storage failures,
quarantine, requests without a finite invocation deadline and SQL-entry failures
retain their existing paths. No callback or mutation is replayed.

The owning byte-only and item-only exhaustion controls both fail with the
captured overload before the fix. They pass afterward, holding actual directory
cleanup before its sync and requiring unchanged full charges, no early SQL entry
and exactly one entry after release. All **15 workspace controls** pass, including
deadline, shutdown, permanent-capacity and existing physical-failure controls.
Their tests and deadline wiring are byte-identical across the one-file repair.
All **38 checkpoint/commit controls**, **16 prefetch controls**, **13 public RF3
selections** and **13 native service selections** pass. Strict all-feature/all-target
Clippy and repository checks pass. The completed executable matches the early
owning preview; that preview overlapped compilation and is not a performance run.

The initial six-thread library capture passes **1,436 tests**, fails **one** and
retains **two existing ignores**. An unchanged peer-tunnel fixture loses its
receiving tunnel after a 500-ms sleep against an 800-ms idle limit, before its
intended submission cut. Production and fixture code match the baseline.
All four fixed-plan focused ABBA controls pass; the sealed baseline full suite
passes **1,431/0/two ignores**, and both unchanged candidate full repeats pass
**1,437/0/two ignores**. Timers, assertions and six-thread concurrency remain
unchanged. The failed capture remains failed and retained; tunnel-fixture timing
repeatability is a separate observation.

The exact-source Linux image passes all **12 capped smoke cases**: SQL, KV and
Queue across Quorum, Quorum+S3, Fleet and Bucket at C1/4/16. SQL/KV each complete
nine windows per case; Queue completes six, including C16 with zero errors and
verified delivery and settlement. The original primary validators, eight
workspaces, deadlines, 1-vCPU/2-GiB node caps, three RF3 groups and shared RustFS
remain unchanged. Passed data moves only after independent restore verification;
failed media remain retained. This closes the reproduced C16 smoke refusal,
not the longer comparison or full optimization/release acceptance.

The 30-second, two-repeat comparison stops in its first SQL Quorum case. All
write windows and earlier read windows verify; the second C16 read records one
`Overloaded` response among **47,262 attempts**, so that window and the complete
matrix remain unqualified. The runtimes stay open and extent stores remain
unquarantined. This read-side refusal is separate from the repaired pre-SQL
workspace failure. All generated results remain outside the PR, and measurements
remain scoped to the shared host. No numerical speedup is inferred from the
earlier failed Raft baseline. The next integration starts from advanced main,
which includes #188's forwarded-read authentication change and merged #187.

## Verified ingress successors across requests

Starting from merged main `49828ff6` (#183), compiler inputs `99a34233` retain
four single-use successor hints per node after the last active caller leaves.
Fixed hashes and scalars bind each hint to cell history, schema and the
successful call's original deadline. The table owns no cell, task, payload,
storage pin or admission permit. Active FIFO entries still disappear with their
last caller. Shutdown clears the table and prevents late repopulation.

Fetch and RPC publish hints while retaining their FIFO turn through the original
cancellation/deadline success selection, exact receipt/minimum validation and
response decoding. Selection consumes the active, invocation and node candidates;
only a successful receipt replenishes them. Every command retains fresh dispatch
authority, its immutable input fingerprint and required durability proof. A stale
candidate still requires the ordinary fresh-quorum consumed-other proof before a
different command sequence can be selected.

The new public RF3 Queue regression completes separate pause/resume HTTP requests,
checks their two individual commits and all-voter catch-up, and joins cleanup
before evaluating reuse. Both image modes fail only the reuse oracle on the
test-only baseline `b9080ee7`: zero hints instead of one. The byte-identical
fixture passes in both image modes twice on `99a34233`. This proves healthy-path
sequence-query avoidance; it does not establish a throughput or latency gain.

The expanded owning campaign passes **93/94** selections, including all 48
preceding controls, eight new cancellation/scope/expiry/capacity/shutdown controls,
and four executions of the public regression. The Axum stateless Fetch selection
fails its two materialized configurations on deadlines; both rooted configurations
pass. A separate pre-change control and standalone candidate repeat each pass all
four configurations. The initial failure remains retained and repeatability is
open. Locked all-feature builds, Clippy, formatting, layout and native dependency
checks pass.

**Native service qualification remains FAILED: 3/13 complete selections pass.**
The unchanged campaign uses the original identities, admission banks and request,
callback and drain windows, with reverse-order repeats.

| Complete native selection | `99a34233` |
| --- | ---: |
| SQL bindings | 1/2 |
| KV bindings | 1/2 |
| Queue callbacks | 1/2 |
| Queue C2, both image modes | 0/2 |
| Separate extent-backed load | 0/1 |
| Materialized Queue C4 | 0/2 |
| Extent-backed Queue C4 | 0/2 |

Deadline and timeout errors remain in the failed selections. Seven structured
Queue cleanup reports each show three node shutdown attempts, three host close
attempts, joined
domains, zero cleanup errors and successful cleanup. Unexecuted phases earn no
cleanup or load credit. No complete Queue load selection supplies qualified
throughput/tail measurements.

Eighteen available, unsaturated failure snapshots show aggregate manifest
preparation means of about **152–329 ms**, with zero SQL and optional-prefetch
admission-refusal counters. These include setup and failed work; they are not
request percentiles, additive latency components or a controlled comparison.
Separate packed-file sync, directory sync, selector publication and pending apply
work before choosing the next persistence change. Increasing quotas is not an
established repair.

All failed media, immutable executables and verified retained copies remain
outside this documentation tree. The requested three-node, 1-vCPU/2-GiB
Colima/shared-RustFS comparison has no new admitted run or passing reserve/resource
witness. Full service, low-latency, OPT/CG/SDK/recovery and release qualification
remain open. See the [current priorities](remaining-work.md#current-optimization-follow-up).

## Committed preparation with main #182

Compiler inputs `bb1b2cac`, including main `ff5cdec33` (#182), prepare at most
two contiguous, already committed edits to independent cells in one bounded
manifest batch. Each entry retains its own exact-base root publication and
acknowledgement. Optional successor inputs and image pins use the existing
apply bank through physical join. Cached pages and images are rechecked before
reuse; locally durable but uncommitted successors refuse prefetch. Live hints
are sampled after the current image is prepared, and canonical entry hashing
streams without an extra whole-entry allocation. No limit, deadline, command
identity or durability requirement changes.

**All 48 selected correctness controls pass.** They include the unchanged-body
backlog and late-commit regressions in both image modes, actual paired sync
failure, corruption refusal, admission pressure, cancellation/close joins,
uncommitted-suffix refusal, digest compatibility and park/wake behavior. The
merged SDK reply-fencing and independent archive-publication controls also pass.
Both regression bodies fail their new barrier-sharing oracle on the retained
earlier runtime after completing their original correctness and cleanup checks.
Locked all-feature builds, strict Clippy, formatting, layout and native dependency
checks pass.

**Merged service qualification remains FAILED: 5/13 complete selections pass.**
The table preserves the earlier pair (`08472ab3`), late-hint (`492ba983`) and
merged (`bb1b2cac`) campaigns. These are separate native RF3 conformance captures,
not a matched throughput or latency comparison. C2 and C4 retain two and four
in-flight Queue requests respectively.

| Complete native selection | Early pair | Late hint | Merged #182 |
| --- | ---: | ---: | ---: |
| SQL bindings | 2/2 | 1/2 | 2/2 |
| KV bindings | 2/2 | 2/2 | 1/2 |
| Queue callbacks | 2/2 | 1/2 | 1/2 |
| Queue C2, both image modes | 1/2 | 1/2 | 0/2 |
| Separate extent-backed load | 0/1 | 1/1 | 1/1 |
| Materialized Queue C4 | 0/2 | 0/2 | 0/2 |
| Extent-backed Queue C4 | 0/2 | 0/2 | 0/2 |

Combined C2 counts require both image modes to finish. A passing materialized
phase cannot qualify a case whose later extent phase fails or never runs. All
eight executed load-phase cleanup reports show three node/host close attempts,
joined domains and zero cleanup errors; an unexecuted mode earns no cleanup
credit. The merged failures report request deadlines. Before #182, one callback
restart/close returned `Unobserved`, withholding producer-quiescence proof;
that observation does not establish that producers remained active and remains
retained separately.

Eighteen available, unsaturated failure snapshots record paired preparations
and verified cache reuse, with zero SQL and optional-prefetch admission-refusal
counters. Manifest preparation averages about **100–167 ms** across those
aggregate snapshots. They include setup and failed load, and are neither request
percentiles nor additive latency components. The manifest store already packs
changed pages into immutable files. Measure packed-file sync, directory sync,
root publication and successor availability separately before choosing the next
change. This checkpoint qualifies no end-to-end speedup.

The original requested three-node, 1-vCPU/2-GiB Colima/RustFS comparison has no new
admitted case or passing reserve/resource witness. All failed captures, immutable
executables and exact retained copies remain outside this documentation tree.
Full service, OPT/CG/SDK/recovery/release and constrained performance acceptance
remain open. See the [current priorities](remaining-work.md#current-optimization-follow-up).

## Live apply phase observations

Compiler inputs `be721db4` add three bounded live-apply observations to separate
image replay/rechecking, immutable SQLite inspection and manifest preparation.
The same domain clock, outcome accounting and fixed histograms serve these
phases; recovery replay does not contribute. No admission, deadline, command
identity, image verification, publication or acknowledgement ordering changes.

Seven unchanged apply control selections pass with additional phase assertions:
materialized/rooted preparation, actual image/manifest directory-sync failure,
cancelled cleanup and materialized preview reuse. The failure checks distinguish
the failed physical phase from later work that must never start. Locked
all-feature native builds, strict Clippy for the library and both native targets,
formatting, module layout, native dependency and documentation checks pass.

Both fixed, original rooted Queue C4 repetitions still fail, with three and four
producer failures. Both report joined cleanup and zero cleanup errors. The
strict delivery/load gate remains failed. Six available, unsaturated node
snapshots show:

| Live apply phase | Range of aggregate mean durations |
| --- | ---: |
| Image replay or exact local image recheck | About 19–118 ms |
| Immutable SQLite inspection and bounded effect summaries | About 0.9–1.2 ms |
| Changed manifest path and page persistence | About 91–109 ms |

These observations include setup and failed load; they are not request
percentiles, additive latency components or constrained performance results.
All SQL refusal counters are zero in these snapshots. Manifest preparation is
therefore the stronger measured target for the next bounded batching work;
SQLite inspection is not the dominant phase in this capture. Actual barrier
accounting, repeatable service and throughput/latency acceptance remain open.
The failed controls and exact retained media copies remain outside this tree.

## Merged main service reverification

Main `0f3b18e7` includes merged PR #167. Compiler inputs `31dfbc55` pass one
unchanged full SQL binding selection and one unchanged Queue callback selection.
The unchanged KV selection fails rooted `PublicLarge`: its 25 MiB put returns
HTTP 503 with `Runtime(Overloaded)` and `ambiguous=false`; its first target
attempt starts with 1,430 ms remaining. This is not a verified repair or an RPC
deadline failure.

Separate diagnostic inputs `70fa62b3` leave the production library unchanged
and add only test-side resource snapshots around `PublicLarge`. One full case
passes materialized conformance but fails rooted startup/cleanup with lease
fencing. Two fixed repetitions also fail: one after rooted service with lease
fencing, the other with a materialized public-write deadline and a separate
rooted failure. Around the materialized deadline, payload operation and byte
usage are zero before and after the request; SQL work remains retained. These
samples do not identify the bank that refused the earlier overload or establish
usage at the instant of that refusal.

The existing cold-preparation control passes. A separate serial diagnostic
runs the existing KV revision control first to enable its existing tracing,
then passes full KV conformance in both image modes. That traced pass does not
close the unchanged KV failure or establish service repeatability. No deadline,
resource limit, command identity or source-proof assertion changes.

Two fixed, untraced rooted C4 controls on clean inputs `31dfbc55` both reach the
original four-request envelope. They fail three and four producers respectively
with deadline errors, while reporting three node shutdown attempts, three host
close attempts, joined domains and zero cleanup errors. Original producer
identities, offered batches, errors and terminal synthetic media remain retained.

The first C4 failure includes these aggregate operation observations:

| Observation | Mean duration |
| --- | ---: |
| Engine file synchronization, across the three node snapshots | About 38 ms |
| SQL image preparation, node 0 | About 129 ms |
| SQL image preparation, nodes 1 and 2 | About 244 and 247 ms |
| SQL settlement (`sql_publish`), node 0 | About 163 ms |

The samples include setup/load work and different operation lifetimes; they are
not request percentiles, additive latency components or a controlled throughput
comparison. All six SQL admission-refusal counters are zero on every node.
These observations prioritize preparation/publication accounting over quota
changes, without proving which individual filesystem operation caused a failed
request.

A bounded SQL continuation-pass experiment, compiler inputs `28b5d89c`, removes
early returns after completed retirements/preparations so ready RPCs can receive
a turn before queued SQL. Its only runtime input change is that scheduling
function. The correct-profile build, formatting, layout, native dependency and
documentation checks pass. Both fixed, untraced original rooted C4 repetitions
still fail four producers; both report joined cleanup with zero errors. The
strict 528-delivery/528-unique-ID qualification gate remains failed. The candidate
is rejected and its runtime source restored to main; no performance improvement
is claimed.

All captures above have observed terminal process exits, immutable artifact
bindings and independently checked retained copies. A compiler/formatter overlap
is rejected before native execution. An initial reused-artifact audit also
rejects Cargo's mutable executable path after a later diagnostic rebuild;
the successor audit verifies the original immutable parent executable and
identifies that replacement explicitly. Both rejections remain retained.

The original RustFS reserve gate still has no passing witness, and no constrained
Colima/RustFS case is admitted here. Current Quorum service, complete OPT/CG/SDK/
recovery/release and throughput/latency acceptance remain open. See
[current priorities](remaining-work.md#current-main-service-deadlines-remain-open).

## Integration with main #180

Main `a85ab6f6c` adds shared Sandbox execution and autonomous cleanup. The
integration retains recorded archive dependencies, bounded reservation renewal
and accepted-apply waits together with the optimization branch's admitted-batch
archive eligibility, canonical configuration checks and joined preview retirement.
Joined physical task panics close the SQL worker before its reply is dropped;
opening, resume and retirement retain that fatal closure behavior. Both parents'
added native test wrappers remain unchanged.

Compiler inputs `4bcd59fd` pass **1,410 library tests / two existing ignores** and
**29 of 30 selected native controls**. The complete S3 read-source reuse case
fails in materialized mode 2 while waiting for a target read to enter; its parent
command reports an ambiguous deadline. That failure prevents the case from
completing both image modes. Original deadlines, identities and proof assertions
remain. The failed media, process waits and exact retained copies are audited.

Strict all-feature/all-target Clippy for Cellar and cellar-container,
**60 Python tests**, formatting, layout, dependency and documentation checks pass.
Overall runtime acceptance remains failed. Earlier Queue C2/C4, service
repeatability, recovery/release and the original constrained Colima/RustFS
comparison remain open. No end-to-end throughput or latency gain is qualified.

## Integration with main #179

The merge of main `09cdcf423` preserves its Container lifecycle/recovery,
pending ReadIndex heartbeat retry and selected-cell archive coverage. Conflict
resolution keeps the first-submission read cohort boundary and both branches'
tests and qualification suites. Reverification of an existing archive head uses
the original admitted-batch eligibility, including when a newer cut is pending.

The merged sources pass **1,378 library tests / two existing ignores**, all
**13 selected native controls**, strict all-feature library/test Clippy for
Cellar and cellar-container, and formatting, layout, dependency and documentation
checks. The native selections cover packed checkpoint corruption refusal,
bounded checkpoint/registry reads, covered-prefix receipts, archive admission,
ReadIndex heartbeat recovery and cohorts, public routing, and all three storage
API controls. The qualification runner and primitive checker also pass **83
Python tests**. Original and closed-copy inventories match for all available
retained media; the driver, domain owners and independent verifier have finished.
Two initial compile/lint failures remain retained with their source bindings.

These passes qualify the selected merged boundaries. The earlier C4, service
repeatability and full Python 3.14t RPC failures below remain open, alongside
complete-prefix recovery, physical reclamation and combined OPT/CG/SDK/release
acceptance. The original constrained Colima/RustFS comparison has no admitted
cases; its last reserve witness was below the unchanged 6 GiB minimum. No
end-to-end throughput or latency gain is qualified.

## Intermediate RPC checkpoint retirement

Compiler inputs `8240ea6c` move applied intermediate RPC preview retirement onto
the existing joined cleanup owner. SQL first checks the exact checkpoint receipt;
physical work owns the pending preview and log/image reservations. Only its
returned token can be adopted by the same SQL executor after a fresh authority
check. SQL retains the handler, continuation, writer, admission and workspace.
Newer-term reconciliation retires through the same owner and refuses continuation.
Close joins outstanding retirement before aborting the handler or releasing custody.
No worker, bank, FIFO order, deadline, durability proof or execution grant changes.

The byte-identical corrected native RF3 fixture holds a real serialized inventory
read with one SQL worker. Both prior-runtime repetitions fail unrelated SQL
progress; both candidate repetitions pass. The original query is observed without
cancellation/reissue. Retained admission/workspace, withheld continuation, exact
receipt/replay, one handler invocation, all three voter values and joined fixture
close remain checked.

An earlier fixture compared combined command/read bytes after the unrelated read
legitimately shrank its own charge; that failed attempt and verifier rejection
remain retained. The corrected matched fixture captures the command-only reference
before either unrelated read is admitted. Its original admission lower bound and
physical custody, continuation, receipt/replay and close assertions remain.

The full library passes **1,387 tests / two existing ignores**. An initial run
has thirteen Unix-socket path-length errors from its fresh fixture prefix. The
same immutable binary passes all thirteen exact controls and the full suite in a
fresh shorter directory on the same volume; the original failure stays retained.
Strict all-feature/all-target Clippy for Cellar and cellar-logic, formatting,
layout, native dependencies, documentation and **44 plan-validator tests** pass.
The validator tests qualify their evidence gate, not the runtime algorithm.

**Broader qualification remains failed: 53 of 59 current-source selections pass.**
All 47 selected positive controls pass; the new owning repetitions are separate.
Twelve prior-runtime controls pass three selections and are counted separately.
A selected existing ignored diagnostic stops the initial driver; its zero-test
result and verifier rejection remain retained. Its scenario is included in the
passing complete S3 read-source case. The 25 previously unstarted selections run
in fresh fixtures, completing 71 nonignored selections across both captures.
No started command or failed history is reset or reissued. The paired results are:

| Original service case | Prior runtime (`98bf1829`) | Candidate (`8240ea6c`) |
| --- | --- | --- |
| SQL bindings | 0/2 | 1/2 |
| KV bindings | 1/2 | 2/2 |
| Queue callbacks | 2/2 | 2/2 |
| Queue C2 load/drain | 0/2 | 1/2 |
| Rooted C4 selections | 0/2 | 0/2 |
| Materialized C4 selections | 0/2 | 0/2 |

The successful candidate C2 case has two exact **528 deliveries / 528 unique IDs**
drains and joined cleanup without errors. All four candidate C4 cases fail during
load. Two prior C4 controls fail before load; all six load cases report joined
cleanup with zero errors. Startup logical joins are not inferred from missing
markers. All available originals, exports and exact closed copies are audited.
SQL/C2/C4 failures, checkpoint repeatability and complete recovery remain open.
These results do not establish an end-to-end gain, the original constrained
Colima/RustFS comparison or full OPT/CG/SDK/release acceptance. See
[current priorities](remaining-work.md).

## Final preview retirement

Compiler inputs `98bf1829` move final command preview retirement from the SQL
worker to its existing joined private-cleanup task. SQL still validates the exact
committed or reconciled receipt and closes its SQLite writer. The cleanup owner
retains the pending preview and log/image reservations; SQL retains the reply,
writer lease, bytes, activation and completion lane until physical cleanup joins.
Any retirement/cleanup failure withholds the acknowledgement. Direct private
executors retain synchronous retirement. The later intermediate RPC checkpoint
handoff is described above.

Byte-identical native RF3 regressions hold a real serialized inventory read with
one SQL worker. Finish and reconciliation each fail the unrelated-query progress
window twice on the unchanged runtime and pass twice on the candidate. The
original query and settlement are observed without cancellation/reissue. Exact
values, proposal/position/receipt binding, withheld reply, retained logical and
physical reservations, release and joined close are checked. Original failed
media, exports and closed copies remain retained and independently verified.

All **1,387 library tests / two existing ignores** and 25 additional selected
publication/receipt/recovery controls pass, alongside strict affected all-feature
Clippy. Fifteen of nineteen primitive controls pass. The broad current-candidate
selection is **46 passed / six failed across 52 selections**; four preserved
runtime C4 controls are counted separately. Ordinary SQL/KV/Queue and C2 fail;
both rooted candidate C4 runs fail and both materialized candidate C4 runs pass.
The prior C4 runtime fails both rooted runs and one of two materialized runs.
All eight C4 cleanup joins and retained copies verify. Earlier current-source
failures and checkpoint repeatability remain open despite individual passes.

A fresh 16-case service comparison uses the same public fixtures, two repetitions
and reversed case/runtime order. Pass counts are:

| Original service case | Prior runtime (`a6f9c594`) | Candidate (`98bf1829`) |
| --- | --- | --- |
| SQL bindings | 1/2 | 1/2 |
| KV bindings | 1/2 | 0/2 |
| Queue callbacks | 0/2 | 0/2 |
| Queue C2 load/drain | 0/2 | 0/2 |

The candidate's second KV failure is a typed overload at rooted `PublicLarge`;
earlier KV failures include ambiguous deadlines. The prior runtime's variable
KV result does not establish candidate non-regression. One failed prior C2
control records **536 delivery attempts / 528 unique IDs**. The initial capture
verifier rejects this against its strict 528/528 check. That rejection and the
count mismatch remain retained as failures; a separate audit verifies complete
capture, joined processes and exact retained/closed-copy media, with service
qualification still failed. The original fixture separately checks completed
messages and unique IDs; its printed delivery count measures attempts.

Retained whole-process stacks also show intermediate RPC checkpoint retirement
waiting on inventory serialization. They identify a remaining owning boundary,
not request timing or a causal explanation of the service failures. The next P0
is measured target/checkpoint service and bounded-worker interference, followed
by the routing, reclamation, recovery and combined OPT/CG/SDK/release gates in
[remaining work](remaining-work.md). The original deadlines, admission banks,
command identities, FIFO order and durability proofs remain unchanged. No
end-to-end throughput or latency gain is qualified.

## Final image geometry verification

Compiler inputs `a6f9c594` remove a duplicate final geometry-validation pass
from immutable image preparation. The nonempty LTX chain already independently
checks every committed cut's physical SQLite header and lock-page geometry,
including the final cut. Exact result binding/commitment checks, destination
verification, file sync, the shared directory barrier and serialized inventory
ownership remain required.

In the byte-identical owning fixture, preparation of a 1,060,864-byte SQLite
image reads **4,243,600 filesystem bytes on the unchanged runtime and 3,182,736
on the candidate**, about 25% fewer. Both baseline repeats fail the read-work
bound; both candidate repeats pass with and without LTX checksums, preserving
old/new SQLite values and integrity. These are filesystem bytes, including
reads served by the OS cache, not device traffic or request latency. Empty-chain
refusal and invalid intermediate geometry remain checked. An independent audit
matches the registered/executed tests; two earlier verifier-construction
rejections remain retained.

All **18 extent-apply tests**, **39 extent-store tests / one existing ignore**,
**1,387 host library tests / two existing ignores**, five native
publication/recovery controls and thirteen RF3 execution/receipt/apply/cleanup
controls pass. Strict affected LTX/host Clippy and LTX no-default/all-feature
checks pass.

**Service qualification remains failed.** All five fresh C4 selections fail
during load: two rooted candidate runs, two preserved rooted controls and one
materialized candidate run. They report joined cleanup with zero errors, and
available media, synthetic exports and exact closed copies are independently
verified. The original Queue fixture, deadlines and admission are unchanged.
This qualifies reduced image-read work; it establishes no end-to-end throughput
or latency gain and does not close service repeatability, checkpoint progress,
full recovery/SDK/release acceptance or the constrained Colima/RustFS matrix.

## Applied observations during persistence

Compiler inputs `77f944ab` serve reconciliation and enrollment-state observations
from the current applied root while unrelated Ready persistence is pending.
Existing term, membership and exact-leaf checks remain in the proof constructors.
These observations grant neither private execution nor initialization authority.
Failed/retiring groups retain their refusals; park preparation, parked groups and
wake barriers keep their existing ordered path. Every request retains ordinary
node item/byte admission. No worker, bank, deadline or durability proof changes.

A byte-identical native RF3 regression holds actual leader-log sync for another
cell. The unchanged runtime fails the one-second observation-progress window
twice; the candidate passes twice. The fixture checks the known cell's exact
version and enrollment, absence of the unpublished cell, lack of early
initialization authority, unchanged publication and successful settlement after
release. A timeout observes the original requests; it never reissues them. All
workers and the domain close before the final result, and failed media is kept.
Eighteen existing receipt, leadership-change, ambiguous-initialization,
cancellation, capacity and quarantine controls pass, as do managed park
admission/restart and source sealing. All **1,387 library tests / two existing
ignores** and strict affected all-feature Clippy pass.

The uninstrumented original C4 Queue comparison still fails both rooted
candidate and both rooted control runs during load. One materialized candidate
run delivers **528 messages / 528 unique IDs**. All five Queue cases report
joined domains, three node/host close attempts and zero cleanup errors; original
media, synthetic exports and exact closed copies are independently verified.
The unchanged workload retains its three-second RPC, five-second callback and
30-second drain windows and original admission. This qualifies the removed
observation dependency, not service repeatability or an end-to-end speedup.
Target SQL/image/persistence interference, checkpoint repeatability and full
OPT/CG/recovery/SDK/release acceptance remain open, followed by the constrained
Colima/RustFS comparison. See [remaining priorities](remaining-work.md).

## Rooted workspace preparation

Compiler inputs `44ea1133` move rooted workspace creation onto the existing
execution domain, with one FIFO lane per existing SQL worker. The SQL owner
retains command/enrollment authority, original writer/item/byte admission and
the returned workspace pin until physical completion and SQL adoption or joined
cleanup. Initial entry and parked RPC resumption share this owner. SQLite and
application execution remain on their SQL thread; the serialized immutable-store
gate, durability proofs, budgets and deadlines remain unchanged.

The byte-identical managed RF3, one-SQL-worker regressions fail on the earlier
runtime in both Quorum and S3 modes: holding actual workspace creation blocks an
unrelated cell query. Both pass with the candidate, including values, exact replay,
one callback, admission custody and joined close. Nine real-storage ownership
controls and **1,387 library tests / two existing ignores** pass. Strict affected
all-feature Clippy and repository checks pass.

**Broader native qualification remains failed: 14 of 21 selections pass.** The
checkpoint submission control misses rooted Quorum follower progress once;
ordinary rooted Queue and rooted C2/C4 producers fail. Two materialized C4 cases
fail during startup before load, so they establish neither load behavior nor
logical cleanup joins. Their available memory exports and disk copies remain
retained. A separate paired checkpoint control passes all eight baseline modes
and seven of eight candidate modes; its candidate failure is an ambiguous rooted
S3 enrollment deadline before the checkpoint workload. It does not reproduce the
earlier follower-progress failure or establish a scheduler cause.

A subsequent four-case C4 campaign brackets current-candidate rooted/materialized
traces with uninstrumented rooted controls. All four reach load and fail; all
original three-node/three-host cleanup markers report joined domains without
cleanup errors. Independent verification checks **194 captured input digests**,
all observed trace records, retained media and exact closed copies. Temporary
tracing is removed byte-exact. No failed mutation is reissued or history reset.

Observed rooted writer-admission waits reach 1,524 ms, image preparation 1,076 ms
and settlement 1,191 ms. Fresh entry-authority refresh reaches 15 ms. One rooted
checkpoint-proof timer remains pending for about 25 seconds until shutdown.
These overlapping observations locate further investigations; they are not
exclusive costs, a complete business outcome attribution or a performance gain.

A follow-up exact-proposal observer binds **72 checkpoints** to their original
command, input digest, step, prepared bytes and expected image. **71 complete**
proof observation and SQL adoption; one rooted proposal remains unresolved until
joined cleanup. Its accepted log position is in term 4, while every matching
initial load-failure snapshot is in term 2. It belongs to later work than the
original producer failures. Reconciliation repeatedly selects an older cell
sequence; no continuation-fence refusal is recorded. Calls return within 264 ms
across the three traces. This excludes delayed reconciliation or SQL handoff as
the explanation for that long proof; it does not prove the uncommitted outcome
of an accepted proposal. The pending producer correctly retains custody.

Four of those five fresh C4 cases reach load and fail; the final uninstrumented
control fails at startup before load. Its logical cleanup joins are not established
by a load-cleanup marker. All available exports, disks and closed copies are
independently verified, and temporary source instrumentation is removed.

The earlier request trace shows five of 15 rooted Queue dispatcher captures and
48 of 132 materialized captures have under two seconds left in the original
three-second budget. These are per-invocation remaining budgets, not elapsed-time
comparisons across node clocks.

A subsequent five-case ingress campaign reaches load and fails in every case;
all five cleanup joins and retained copies verify. Its three traced selections
validate **85 normalized inputs**. Every call enters source ingress with the full
three-second budget, with at most 1 ms to first poll. Completed FIFO waits reach
2,272 ms and target-command service reaches 2,320 ms. Enrollment and name work
are rare and take at most 2 ms; history and sequence observations are smaller
than the largest FIFO and target waits. These overlapping phase observations
are not request percentiles and cannot be added across calls or node clocks.
Temporary tracing is removed byte-exact. Preserve FIFO ordering and focus the
next optimization on target SQL writer/image/settlement interference. The
separate checkpoint follower-progress repeatability gap remains open. Close the
unchanged C2/C4 delivery/uniqueness oracle and complete OPT/CG,
recovery/SDK/release qualification and the constrained Colima/RustFS comparison
before claiming completion or a performance gain.

## Earlier Queue diagnostic boundary

Merged source `06fe9101f` retains the original Queue load body and budgets. Fresh
unsampled C4 trials pass twice in materialized mode and fail twice in rooted mode;
the C2 control passes materialized delivery/uniqueness before failing rooted
producers. A later five-selection filesystem-observed campaign fails every
selection, including materialized mode. These observations leave service
repeatability and the cause open; they do not establish a rooted-only regression.
All selected domain, node and host cleanup joins finish without cleanup errors.

The observer forwards every filesystem operation, including optional native
windows, copy-on-write and streaming operations. One rooted steady-load window
spans 26,923 ms and contains, per node, 124–186 engine file syncs, 108–112 extent
syncs, 108–112 root syncs and 99–102 manifest syncs. The longest observed file or
directory fence in that window is under 300 ms. These are overlapping provider
intervals and counts, not exclusive request costs or an established cause.
Unclassified file and directory work remains material. The macOS sampler produced
no call graph, including on an owned sleep-process pilot; no stack attribution is
claimed from it.

Every probe process has terminated, and independent verification matches the
retained original media to closed copies. Temporary instrumentation is removed
byte-exact; production/compiler inputs return to the published source. No
production optimization or performance improvement is qualified by these probes.
The next P0 is request-level attribution of admission, image preparation,
committed apply and settlement waits, followed by an owning regression and the
unchanged full load oracle. The [remaining priorities](remaining-work.md) also
retain recovery, shared-worker fairness, physical reclamation and release gates.

## Queued persistence and encoded bounds

The node's existing storage worker now combines an already-queued prefix of
disjoint cell-group writes into one physical fence. Overlapping groups and
control operations retain FIFO order. Each caller keeps its original operation
and exact ordered group identities; payloads, retention roots and the original
item/byte admission stay owned through physical completion, including abandoned
waiters. Uncertain I/O closes admission without replaying the cohort.

The complete encoded persistence batch, including cached metadata and the root
record, is checked before publication or I/O. Durable metadata size also feeds
the existing Ready estimate: a guard alone reproduced permanent retry without
durable progress. The unchanged 3,328-byte Ready control now makes progress with
two syncs and a largest write of 2,099 bytes. Three queued disjoint requests share
one sync instead of three; the metadata-heavy 6,400-byte control splits into
three fences, with a largest write of 2,628 bytes. These are storage controls,
not end-to-end throughput or latency measurements.

Before the main #179 merge, all three public storage controls, seven owning controls, strict Clippy and
repository checks pass. The complete library passes **1,372 tests / two existing
ignores**. A first native campaign passes **four of 17 selections**; all eight
C4 Queue trials fail across both candidate and baseline. A separate fresh paired
campaign passes **eight of eight selections**: SQL, KV and Queue in both image
modes, plus cross-group apply, on both executables. Original deadlines and
workloads remain unchanged. Every campaign is closed and its available media
independently verified; later passes leave earlier failures and repeatability
gaps open. Whole-test elapsed times do not establish request performance.

The earlier published parent `34c3913f9` has a failed full Python 3.14t RPC
matrix during proof warmup/fencing; its root cause remains unqualified. Remaining
priorities are
repeatable service/C4 progress, complete-prefix recovery and lifecycle coverage,
fair persistence/read/archive scheduling and physical segment reclamation, then
the original constrained three-node Colima/RustFS comparison. Its last RustFS
witness failed the unchanged reserve check; no overall performance gain or
complete OPT/CG/SDK/release qualification is claimed.

## Current private workspace cleanup

Code `1840466d4` includes main `5ae2b21f` and moves settled private-workspace
removal and parent sync onto the existing execution domain. SQLite and capture
close on their original SQL thread. The SQL owner retains the frozen settlement,
activation, writer lease, bytes and progress lane through physical join; retiring
work still consumes the original admission capacity. Receipt/fence and S3/output
checks, cache timestamps, deadlines and worker limits remain unchanged. See the
source-bound qualification.

The original four-mode cleanup control improves from **zero to four passes**:
unrelated fresh linearizable queries advance on the single SQL worker while
actual cleanup is held. Values, exact replay, one callback and held reply/resource
custody remain checked. Eight additional native fault observations pass across
both image modes and Quorum/S3: original five-second waiter expiry retains the
physical owner, and real filesystem errors retain typed ambiguity, quarantine
and complete rooted workspace charges. Receipt-only lookup and a fresh peer
value read verify the original mutation without resubmitting it. Shutdown joins
the same close future; failed bytes remain intact after every node/host join.

The full library passes **1,365 tests / two existing ignores**, including five
actual-workspace custody controls. All three native selections pass, alongside
strict all-feature/all-target Clippy and repository checks. Independent
verification retains **36 node disks, eight synced client ledgers, 2,574 files /
17,692,291 bytes and 12 synthetic exports / 1,455 objects / 5,684,975 bytes**.
Fresh public frozen-receipt controls and ordinary KV in both image modes also
pass with concrete retention. Earlier missing public-control media remains
recorded; the temporary Unicode trace was removed before final KV validation.

**Combined qualification remains FAILED.** The unchanged runtime's broader
candidate passes **14 of 18 native selections**. C2 passes both original
528-message/528-unique-ID assertions, while all three C4 selections retain
ambiguous producer deadlines. Rooted KV also exceeded its original 20-second
Unicode phase budget; subsequent passes leave that intermittent failure open.
The latest read-only original RustFS witness at
**2026-11-21T20:12:18.276453+00:00** reports **19,156,992 available bytes**, below
the unchanged **6 GiB reserve**, with original identity/configuration/resources/
start unchanged, zero restarts and zero admitted cases. A fresh three-node
resource witness, the constrained comparison and full OPT/CG/recovery/SDK/release
gates remain open. No throughput or latency gain is qualified.

## C4 window and lifecycle diagnosis

The current diagnostic record
restores production inputs `040479d8` byte-exact after all temporary lifecycle
observations close and are independently verified. The unsampled materialized
C4 control passes the original **528-message / 528-unique-ID** assertions.
Unsampled and sampled rooted controls fail with **seven explicitly ambiguous
producer deadlines**. All observed controller, SQL and domain closures follow
the workload boundary and join successfully. Premature shutdown and sampling
are therefore unnecessary for this reproduced deadline failure; no runtime
change or performance gain follows from the materialized pass.

The preceding short-window cohort separately retains four materialized ambiguous
deadlines and four rooted producer failures without explicit outcome fields.
Its rooted snapshot shows closing controllers, and one host close returns a
SQL-pool-join storage failure. The strict cleanup gate remains failed. A separate
closed-media audit verifies **six disks and two exports**, retaining both the
premature and terminal strict-verifier failures. The lifecycle follow-up verifies
**nine disks and three exports**; its clean closure does not repair the earlier
failure or prove complete-prefix recovery. Persistence sync work remains the
next measured target. Full repeatability, OPT/CG/SDK/recovery/release and the
original constrained comparison remain open.

## Queue diagnostic closure

The Queue domain-retention follow-up
joins the shared fixture domain before exporting concrete synthetic memory,
including known startup/setup error paths. The original load body and all
production/library inputs remain unchanged. Long-callback and operator
archive/auth/fresh-quorum controls pass both image modes. All six scenarios
report joined domains, three node/host close attempts and zero cleanup errors;
**18 node disks, six exports / 2,127 objects / 8,124,016 bytes** and their closed
copies are independently verified.

Both fresh C4 tests fail with **eight explicitly ambiguous producer deadlines**.
Profiles triggered after the original outage drains identify engine, manifest
page-store and archive-journal sync callers. Their windows can truncate at process
exit and overlap errors/cleanup; sampler overhead and concurrent compiler jobs
prevent a throughput/latency or exclusive-cause claim. These controls close a
fixture export boundary, while repeatable service, complete-prefix recovery,
general constructor-panic coverage and the original constrained comparison stay
open.

## Checkpoint proof progress

Code `27b811cb8` includes main `5ae2b21f` and observes the unchanged exact applied
or newer-term reconciliation proof on the existing admitted checkpoint task.
The original SQL owner still adopts producer custody, rechecks the frozen receipt
and activation fence, and performs required S3/freshness/output checks before
continuation. No worker, admission bank, timeout or retry policy changes. Handoff
now includes proof waiting; the observations overlap and cannot be added as one
request cost. See the source-bound qualification.

The byte-identical owning native control improves from four failing progress
predicates to **four passes**, covering both image modes and Quorum/S3. It holds
the actual single SQL worker after physical apply preparation, then releases the
engine fence. Proof observation and follower replication advance before SQL is
released. No early continuation/acknowledgement, exact final values, once-only
replay and joined cleanup remain asserted. Earlier fixture setup failures remain
retained and are excluded from the reproduced-defect evidence.

The library passes **1,360 tests / two existing ignores**, the waiter/task and
submission controls pass, and **10 of 14 native selections pass**, including
ordinary SQL/KV/Queue in both image modes, SDK source-proof/fencing, long callbacks,
cross-deployment faults, operator archive/fresh-quorum and KV archive/quorum reads.
Strict all-feature/all-target Clippy for Cellar/LTX/logic and repository checks pass.

Combined qualification remains **FAILED**. All four Queue load selections fail:
materialized C2 and combined materialized C4 each complete the original
**528-message drain / 528 unique IDs**. Rooted C2 reaches its original **30-second
final primary drain timeout**, with **504 consumer-reported completions** and no
recorded producer error. The three C4 failures retain **ten explicitly ambiguous
producer deadlines**. All six entered load cohorts report successful three-node
and three-host cleanup with zero errors. Independently verified original/copy
inventories retain **38,712 files / 841,344,275 bytes** and **38 synthetic exports /
22,755 objects / 218,358,241 bytes**. Consumer observations and independent last
info/peek reads are neither durable settlement nor complete-prefix recovery proof.

The next **P0** is repeatable command/callback service and final drain within the
original 3,000-ms RPC, 5,000-ms callback and 30-second drain windows. The four-mode
proof control and failure snapshots do not establish an exclusive Queue cause,
request percentiles or a throughput/latency gain. Successful-fault and constructor
cleanup/retention gaps must close before broad execution/routing qualification.
The original RustFS read-only probe at **2026-11-21T14:57:31.938706+00:00** reports
**12,001,280 available bytes**, below the unchanged **6 GiB reserve**. Original
identity/configuration/resources/start match, with zero restarts and no admitted
cases; this is not a fresh three-node resource witness. The constrained comparison
and full OPT/CG/recovery/SDK/release acceptance remain open.

## Checkpoint phase observations

Code `c9258a82a` adds four fixed observations to the existing checkpoint path:
captured preparation/producer completion, retained submission, return to the SQL
owner, and exact applied or newer-term reconciliation waiting. Cancellation
records abandonment while entered physical work remains owned through joining.
The handoff observation ends on the owner's first poll, before custody adoption
or acknowledgement. The source-bound record
retains the exact boundaries and limits; this change supplies observations rather
than a Queue progress repair.

The full library passes **1,360 tests / two existing ignores**, alongside the
registry failure/cancellation controls, strict all-feature/all-target Clippy and
repository checks. **10 of 14 native selections pass**, including ordinary
SQL/KV/Queue in both image modes and the checkpoint, SDK, callback and archive
controls. All four C2/C4 load selections fail with **11 explicitly ambiguous
producer deadlines**. Materialized C2 completes the original **528-message drain /
528 unique IDs**. Combined C4 stops at its materialized failure; its extent mode
is not entered. All five entered load cohorts report successful three-node and
three-host cleanup. Source, artifacts, logs, original/closed-copy fixture
inventories and **33 synthetic memory exports** are independently verified.

The failure snapshots distinguish checkpoint phases but do not establish a
per-request breakdown or an exclusive producer bottleneck. Proof polling still
depends on SQL-owner turns after submission returns; a separate owning progress
control is required before changing that dependency. The original RustFS check
at **2026-11-21T13:27:01.813617+00:00** still reports **zero available bytes** below
the unchanged **6 GiB reserve**, with stable original identity/configuration,
resources and start, zero restarts and no admitted cases. It is not a fresh
three-node resource witness. The original constrained comparison and complete
OPT/CG/recovery/SDK/release acceptance remain open. No throughput or latency gain
is qualified.

## Checkpoint submission qualification

Code `5e0b9836c` retains main `5ae2b21f` and moves captured immutable checkpoint
preparation and proposal admission to the existing execution domain. The original
SQL owner retains SQLite, its continuation, writer and byte admission. Entered
I/O returns and joins before custody releases. Lost waiters keep the same
captured submission unless a positive pre-submission seal permits cancellation;
exact applied receipts and required S3 proofs still gate replies and continuation.
See the source-bound qualification.

The unchanged checkpoint fixture now passes all four image/durability modes
while unrelated SQL is held; its baseline fails all four progress predicates.
Both retain checks for final values, exact replay, no leader apply or caller
acknowledgement while SQL is held, and joined cleanup. Four unchanged waiter controls improve
from two failures/two passes to four passes. These demonstrate progress at their
owning boundaries, without qualifying a throughput or latency gain.

Combined qualification remains **FAILED**: the library passes **1,360 tests /
two existing ignores**, and **10 of 14 native selections pass**. Ordinary SQL,
KV and Queue pass both image modes, alongside SDK fencing, reply bridges,
long-callback progress, archive/fresh-quorum controls and KV inline-read guards.
The ordinary Queue result is one fresh current-source run, not repeatability.
All four Queue load selections fail with **ten explicitly ambiguous producer
deadline errors**. Materialized C2 and combined C4 each finish the original
**528-message drain / 528 unique IDs**; standalone materialized C4 also fails.
Every load phase reports successful three-node/three-host cleanup and complete
synthetic exports. Failed/unknown histories remain retained and unreissued.

Strict all-feature/all-target Clippy for Cellar, LTX and the pure logic crate,
format, layout, dependencies, docs and whitespace checks pass. Source, artifacts,
logs, original/copy fixture inventories and **34 memory exports** are independently
verified. Two separate sampled C4 diagnostics fail with declared overhead and
verified terminal media; their startup/service/cleanup stacks identify I/O callers
but do not establish exclusive cause. The record preserves exact hashes and scopes.

The original RustFS check at **2026-11-21T11:00:43.246451+00:00** reports **zero
available bytes**, below the unchanged **6 GiB reserve**. Original identity,
configuration, resources and start match, with zero restarts and no cases
admitted. This is not a fresh three-node resource witness. The original constrained
comparison and full OPT/CG/recovery/SDK/release acceptance remain open. Earlier
source-specific integration results remain below.

## Current main integration

Code `a24eddbe9` includes main `5ae2b21f` and the bounded native Queue transfer
claims. The host-native RF3 campaign passes **21 of 24 primitive selections**,
two ordinary Queue restart/callback repetitions in both image forms, and the
original C2 exact drains. Standalone materialized C4 passes; fresh extent C4 and
combined materialized C4 fail with six explicitly ambiguous producer deadlines.
Checkpoint replication while unrelated SQL is held fails all four image/durability
cases, while final values, exact receipt replay and joined close pass.

The SQL error follow-up `fc0a09a91` preserves both the nested message and the typed
local cause. It passes **1,352 library tests / two existing ignores**, five native
SQL error contracts, both owning error regressions, strict all-feature/all-target
Clippy and repository checks. The earlier native failures remain open. These
host-native fixtures use actual TLS/disks and retained synthetic object stores;
the original constrained Colima/RustFS comparison remains unqualified. Full OPT,
CG, SDK and release acceptance remain open. The qualification record
keeps exact source, profile, result and retention scopes.

## Retained event metadata history

Qualified runtime `0a6fb57b67` selects a present local applied enrollment after
verified namespace routing for internal event metadata. The result is a locator,
never read/serving/durability authority. The actual query still gets fresh quorum
and independent target S3 proof. Missing records and ordinary public name/absence
observations retain full quorum discovery. Existing admission, source checkpoints,
fences, deadlines, quotas and marker 1 remain; no cache/task is introduced.

The byte-identical baseline owning fixture fails the redundant name-cut predicate
in native materialized mode. Final native/real-RustFS tests pass both images,
including fresh destination reads, public absence and quorum-loss refusal. Election
setup is settled before the cost oracle. Current-source qualification is **43
integration selections and 190 units**, plus strict Clippy and repository checks.
The complete original public cohort 735 is **FAILED, 7/16**. Four frozen baseline/
current native/Axum paired trials all pass, including the current failed binary;
they do not replace the cohort or establish stability, exclusive cause, absence
of regression, measured speedup or an accepted OPT/primitive/release package.
See the [current checkpoint](progress.md#current-integration-checkpoint).

## Leader commit/root publication

Qualified runtime `d5af055628` combines commit-only metadata with applied-root
publication for a bounded leader prefix backed entirely by exact live SQL
preparations. The local lease/engine-bound ticket permits image preparation before
that metadata sync; it grants no serving authority. Log entries and term/vote
changes already have their own durability barrier. Real image/page and atomic
root/hard-state sync still gate successful output. Followers, membership/control
entries and missing/expired preparations keep separate commit persistence.
Existing read protocol barriers remain available during apply; write admission
uses the actual committed prefix. No task, cache, budget or format is added.

Three new physical regressions and 38 existing integration controls pass, as do
180 units, locked build, strict Clippy and repository checks. Baseline fails both
independent-barrier predicates; final fixtures also hold actual root sync and
refuse foreign replica tickets. An overly broad candidate failed nine controls;
all pass unchanged with the final eligibility/read-barrier fix. Fixture oracle
corrections and excluded attempts remain recorded.

The complete original public cohort 719 is **FAILED, 13/16**: seven EventStream
selections pass, while native extent-image outbox restart activation and both
Queue loads fail. This establishes the scoped publication behavior, not timing
stability, a complete primitive/OPT package or release. See the
[current checkpoint](progress.md#current-integration-checkpoint).

## Cell-version change waits

Runtime `ed8fd243b`, qualified at `9a43d4712`, adds public SDK
`LinearizableAfter(CellVersionToken)` and uses it for pending catalog activation.
It parks in existing bounded admission until the authenticated exact cell advances,
then obtains a fresh quorum ReadIndex. It does no SQL/image work before advancement
and does not join ordinary read cohorts while waiting. Other-cell progress cannot
satisfy it. Current authority, independent target minima and caller-side strict
advancement checks remain. Activation drops its previous SQL/archive response
before waiting again. No new task/cache or increased deadline/quota is introduced.
Authenticated mode 4 is new; wire markers remain 1.

Both image formats and actual TLS follower dispatch pass; **33 integration checks
and 135 units pass** with locked build, strict library/all-test Clippy and repository
checks. The eager-barrier negative fails both required oracles. Final tests keep
that safety guard and replay held replies afterward; fixtures are not byte-identical.
The complete original public cohort 698 is **FAILED, 6/16**: all eight EventStream
and both Queue loads fail. This qualifies the change-wait capability, not a public
deadline fix, complete primitive family, OPT package or release. See the
[current checkpoint](progress.md#current-integration-checkpoint).

## Materialized preview and apply

Runtime `0eeea3033` delivers a typed handoff from independently verified private
SQL preparation into committed apply, covering initialization, mutations and RPC
checkpoints in materialized and extent images. A private witness binds the full
encoded entry to the same live engine, group history and term. Pending producers
own strong image pins; public command clones and accepted proposals hold weak
references. Only an exact committed apply ticket upgrades custody. No payload
copy, new cache/task or separate unbounded pin lifetime is introduced; metadata
fits existing envelopes and images retain their original admission.

Apply requires matching input/owner and the same physical inventory/result. It
rereads/verifies actual destination bytes and syncs before publishing the manifest.
The witness proves the input/result relation, never commit or serving authority.
Expired or foreign-inventory preparations independently replay, as do followers,
cold restart and archive/recovery. Current commit/route/receipt fences and original
budgets, quotas, minima and wire markers remain. The earlier materialized physical
generation reuse is retained and extended to initialization.

Both old-base read denial oracles fail at baseline `f31f24c41` and pass with the
byte-identical fixture at feature `94217eaf4`. Final additions test producer closure
after ticket custody; the original checks remain. Eight physical cases cover live
reuse, expiry, actual destination corruption and transferred custody in both image
forms. The complete integration controls pass **32/32**, including enrollment,
leadership/cancellation, retained recovery and warm forged-LTX refusal; **104 units
pass**. That historical public cohort is **FAILED, 8/16**, with six EventStream
and two Queue load failures. These results establish a safe local I/O reduction,
not latency stability, an exclusive speedup, full OPT or primitive-family acceptance.
See the [current checkpoint](progress.md#current-integration-checkpoint).

## Recovery PR integration

PR [#166](https://github.com/crabbuild/cellar/pull/166) includes this implementation
through committed source `19982a1b`, merged with managed S3 recovery at
`79f2ccc5`. The integration retains the source-seal routing fence and the narrow
published-child discovery authorization. It passes 63 selected Rust cases and
focused strict Clippy with the CI test profile, including all six public routing
cases and cached-source SQL after child publication in both image formats.
See [combined-source progress](progress.md#combined-recovery-and-tikv-optimization-source)
for exact scope and retained records. At `b85c609c`, three combined-source recovery
suites pass real RustFS in both image formats with host-native RF3 voters, including
closure selection, seeded SDK retries and uncertain outbox recovery. Seven archive
safety cases and full strict all-target Clippy pass. See the
[current-source checkpoint](progress.md#cg-13-checkpoint-held-receipt-replays-and-current-source-rustfs)
for held-proof reuse, failed controls and fencing limits. Full CG-13 and release
profiles remain open. This checkpoint does not accept a complete OPT package,
qualify any CG-17 primitive runtime case or establish a measured speedup.

## Current boundary

Implementation starts from `d0c2873f` (the shared cell-group runtime merged into
`origin/main`). Deliveries now span routing, read preferences/cohorts, bounded
execution, archive/lifecycle ownership and persistence work. None of OPT-01
through OPT-08 is accepted as complete by this record.

An earlier bounded runtime change, `4e779bd1`, reuses an exact captured producer
connection for receipt inspection under fresh authority and quiescence checks.
Both native image modes pass the matched read-dependency tests, the full library
and execution suites, and original SDK proof/fencing controls. Combined primitive
qualification still fails Queue load, rooted large-KV and ordinary Queue callback
controls. Earlier/candidate artifact controls retain their distinct failure
boundaries. See the scoped record
and [current priorities](remaining-work.md#captured-receipt-image-reuse); this
checkpoint closes no complete OPT package or constrained performance gate.

### Authenticated read permission refusal

Published `bf1bbc68` passes all four hosted Typed RPC jobs, all three JavaScript
jobs and the three filesystem release builds. Both completed push-native
framework jobs still fail the initial
unauthorized read in `partial_park_and_stale_certificate`: the receiver refuses
the request, but closing its transport becomes `Query(Unavailable)` at the caller.
The frozen host-native reproduction reaches the same error. Its first signed
refusal candidate still fails because the metadata-only ingress has no regular
query pool. Startup inspection knows its physical process without granting data
access or creating that pool.

The follow-up signs a plain-read refusal under the original request tag, route,
sequence and TLS exporter. It authenticates the bounded request body before
replying, using the existing node and peer byte/item limits and I/O deadline.
Metadata-only work and a later regular pool share one incoming query bank and
retain the physical process fence. No namespace permission or sender worker is
created. Only plain queries accept this response; commands and other purposes
reject it as a protocol error, preserving uncertain mutation outcomes.

The enhanced grant fixture reproduces the missing metadata-only path as a broken
pipe before the final transport change. At compiler input `d81c2da3`, the unchanged
native park case passes in both image formats, and both refusal codec tests pass.
The original drop-only TLS grant/cancellation control still fails its three-second
deadline while the partial receive retains its credits. Isolated TLS probes show
variable peer-read exit after drop, orderly shutdown and reset; periodic polling
does not remove that variation. Explicit TLS shutdown passes the selected actual
transport control and full 1,245-test library at `d7846d50`, with two existing
ignores. All six native park/wake cases pass. The broader routing run reports
22 passes, one failure and thirteen explicit RustFS ignores. The failed
`removed_member_advice_cannot_grant_authority` case returns ambiguous
`Transport(StalePeer)` for original command ID 1 after actual member replacement,
before bad advice is injected. Its snapshots record two successful local S3
command calls and current leader 5; these counters do not resolve the failed
call's receipt. Both the published and candidate binaries pass the unchanged
isolated case, in 63.219 and 62.598 seconds respectively. Those passes do not
establish a causal repair of the full-run failure. The original identity and
failed data remain retained. A complete physical-process cancellation/fairness
oracle and broader qualification remain open.

Six of eight selected primitive controls pass: ordinary/Axum SQL and KV,
ordinary Queue callbacks and interrupted SQL recovery. Queue proof sharing fails
during fast-queue warmup with an ambiguous runtime deadline, before its main
oracle. The four-in-flight Queue case fails a public producer with ambiguous
`Runtime(DeadlineExceeded)`; command sequence 6 enters its first Raft target
attempt with 2,034 ms remaining. Original identities and failed data remain
retained. Strict all-feature/all-target Clippy, the default recovery consumer,
format, layout, dependency, documentation and 44 plan checks pass. The current
native disaster runtime target was not reached after the routing failure.
See the scoped record.
This repair establishes no constrained throughput
or latency-percentile gain and closes no complete OPT or CG gate.

### Bounded peer-query progress

The peer sender previously awaited one complete application response before
dequeueing another request for the same physical peer. A held SQL callback could
therefore prevent an unrelated group's request from reaching a stale leader's
ordinary redirect, even after that group's leadership transfer completed.
The final old-sender fixture records no cold RPC at its 31 ms observation and
ends at the hot command's original deadline. Its possible mutation outcomes and
original identities remain retained; the observation does not establish their
later admission or commitment.

Compiler input `439494ee` changes only the two production transport files relative
to final baseline `37c58f4e`; its production files are identical to the earlier
public checkpoint `7e2e3933`. The existing sender polls independently admitted
exchanges concurrently, retains at most one idle stream per peer, and caps all
outgoing query sockets, including retiring pools, at the configured peer limit.
Each stream still has one exclusive exchange and exporter-bound replay protection.
Requests retain their original byte/item credits, identity, deadline, route checks
and receipt proofs. No sender worker or admission bank is added.

The public native fixture keeps one physical SQL worker on the held node. All six
SDK/native HTTP/Axum cases pass across materialized and extent-backed images:
cold leadership transfer and an S3 receipt complete while the hot callback remains
held, then the original hot command completes, exact S3 receipts replay, all
voter values and serving identities agree, and callbacks remain deduplicated.
An imported cell retains its original write witness; current child membership is
checked separately. The unchanged existing routing controls also pass 22 cases,
with thirteen explicit RustFS ignores. Both initial socket-cap checks then hit
their unchanged three-second fixture timeout. A later phase probe records no
first socket: the fixture installed a physical route for an unclaimed parked
replica, which correctly refuses before query I/O. Its join also masked that
early refusal behind a waiting server. Those TLS attempts are invalid setup
evidence and remain failed; the public native comparison remains valid.
The corrected fixture uses the ingress query-route API, fails promptly on an
endpoint error, and retains an actual negative test for the unclaimed physical
route. Both old-sender concurrency cases now reach the first held TLS exchange
before their original three-second timeout; the role-refusal case passes. The
candidate passes all three cases in 0.239 seconds, including reuse of the second
stream while the first remains held, the two-socket ceiling and cancellation
credit release. This elapsed test time is not workload throughput or a latency
percentile. The full all-features library passes 1,243 tests with two existing
ignores. The current routing executable is byte-identical to the already tested
six-public-case/22-control artifact, permitting exact reuse. Strict affected
Clippy and the recovery facade also pass. The full native disaster target passes
thirteen cases with eight explicit RustFS ignores on this same compiler input.
Failure-path diagnostics now return an explicit error when source hosts have
already been drained, allowing their best-effort caller to preserve its primary
recovery error. This does not establish a repair of the older Linux failure.
The final all-feature/all-target strict Clippy check also passes on `439494ee`.
Source/executable hashes,
invalid earlier fixtures, genuine matched controls and private-retention audits
are recorded in the qualification record.

This is host-native SQL/control/peer-lane evidence. Complete RL-07 still requires
archive outages, bulk transfer, sustained hot/cold traffic and contention across
peers at the physical socket ceiling. All OPT and combined release gates remain
open. The original three-node RustFS campaign remains held at zero available
bytes against its unchanged 6 GiB reserve; no new constrained throughput or
latency percentile improvement is claimed.

### Lease readback continuity

Compiler input `31cc0e00` retains bounded information about unresolved ordinary
lease writes under the held ETag. Reading the old root does not cancel native
work; a later attempt's folded-log stamp must not erase an earlier possible
winner. Historical candidates require the original node, lineage, address,
protocol and generation, a remembered None/Open stamp and bounded expiry.
Recovery and sealing states do not enter that history. Displacing the ETag
invalidates its old guards.

Positive readback and applied completion require continuous prior authority at
both latest observed clock bounds. Adoption cannot revive an expired prior
lease or replace an older possible winner's sample with a newer callback time.
The monotonic anchor retains the earliest sample, expiry-delta and observed
remaining-time bounds. Original deadlines, work banks, worker counts, wire
format, durability proofs and retry identities remain unchanged.

Eight of eleven public lease tests fail against the unchanged core; all eleven
pass afterward within the full 107-test pure-core integration suite. The frozen
native fixture uses the public Node, actual Fleet publisher and conditional
SQLite object store. An original physical CAS commits after its response is
lost; the next actual Open-stamped CAS rejects. The old core closes the owner
before its prior expiry. Two fixed-core invocations retain authority, execute
one unreissued HTTP mutation, verify value 1 across a full TTL and check the
stored generation, Open ensemble and Fleet-ack marker. Both nodes, the physical
task and the explicit observer domain join; the caller executor remains usable.
The full default library also passes 1,233 tests with its two existing ignores.
Its first serial sweep fails thirteen mocked Unix-socket binds because the
harness temporary path exceeds macOS `SUN_LEN`. The identical frozen executable
passes the same serial command after only shortening that unique temporary
prefix on the same workspace volume. Both runs remain recorded; no runtime
deadline, assertion, worker or resource limit changes.
The qualification record pins
final identical fixtures, compiler/executable/profile hashes, earlier fixture
failures, the initial observer-domain failure and private retention audits.

At parent `46bc043a`, all four Typed RPC jobs and all three JavaScript jobs
pass, while both full native jobs fail. Its focused Linux disaster build succeeds
and reports six passes, three failures and eight RustFS ignores. Post-failure
physical probes report `Overloaded` on five peers in each failed six-node case;
those nontransactional observations do not establish cause. This component
isolates a known readback race, not the original RPC COMMIT timing. Genuine
expiry still fences, and the Linux disaster failures remain open. The original
RustFS still has zero available bytes against its unchanged 6 GiB reserve.
No constrained SQL/KV/Queue campaign, measured percentile/throughput gain or
complete CG/OPT qualification follows from these focused checks.

### Managed seal phase observations

The seal-authorization follow-up records only fixed phase/error classes and
optional monotonic elapsed time. A dropped future retains its unknown mutation
outcome. The operator fixture captures the named event per HTTP request, bounds
retention to sixteen allowlisted rows and snapshots after joined server close.
Observation retention cannot replace the primary mutation error. Post-failure
physical probes run sequentially with their original one-second bounds; they
grant no admission, capacity or recovery proof. CI uploads only the two exact
structured metadata filenames, excluding raw errors, objects, manifests,
databases, configurations, credentials and logs.

At compiler input `c3ca152c`, the expanded native target passes eleven cases with
eight explicit RustFS ignores. After moving capture access through joined close,
input `526ae55a` passes its privacy and actual three/six-voter CLI controls, but
the full target reports ten passes, one failure and eight ignores. The original
split-then-merge replay fails at `transition capture-source` with
`unavailable / unknown`; all six post-failure physical probes succeed. This is
an unresolved failure, not a cause established by those later samples. Final
fixture cleanup also joins the server when plan creation fails. The
qualification record pins each
source, executable, failed build/check, final focused checks and retained data.
No original failed attempt is promoted by a later pass.

Published parent `de7cc144` still fails Linux: the focused disaster target has
seven passes, two failures and eight RustFS ignores. Both full native jobs fail;
the free-threaded job also fails source capture. Two Typed RPC jobs pass and two
fail with Python shutdown-incomplete evidence, including `host_failed`; those
failures do not establish a lease-readback cause. The new seal phase capture is
not present in those parent runs. Production admission, accounting, request
identities, proof requirements and deadlines are unchanged by this follow-up.
The original RustFS volume remains below its unchanged 6 GiB reserve, with about
10 MiB available at the read-only recheck. No new constrained comparison or
measured throughput/percentile gain follows. Full CG/OPT qualification remains open.

### Transition progress under bounded admission

The current follow-up addresses two reproduced progress failures. A staged source
acquire that is positively proved superseded can deliver one positively journaled
successor within the original call. Unknown allocations retain their exact intent;
held allocations reconcile it, and repeated supersession stops without a third
intent. The frozen five-case control changes from four passes/one failure to five
passes. Its first full default-library run passes 1,238 tests, but the subsequent
native disaster run still fails one case with an ambiguous command deadline.
That failed attempt remains unqualified; a later combined-source pass does not
establish a causal repair of its separate ambiguous command deadline.

Seal collection also reserves response bytes before responses exist, blocking a
required fresh-trust read under the unchanged 16 MiB bank. The frozen admission
control and actual six-voter CLI scenarios in both image modes reproduce the
refusal. The candidate keeps the full-model fit check, retains only the live
catalog page initially and transfers each delivered inventory's byte charge into
the seal collection. Completed immutable outputs release their active work slots;
their bytes stay charged through validation and selection. Shared placement data,
epochs, inventory completeness, sample age, birth accounting and the final CAS
remain unchanged. At compiler input `c05e1ec0`, both frozen native scenarios pass,
the full all-features library passes 1,240 tests with two existing ignores, and
the full native disaster target passes thirteen tests with eight explicit
real-RustFS ignores. The shared recovery consumer also builds. These local
results establish no constrained throughput/latency gain or hosted qualification.

That run's strict Clippy check then refuses two direct filesystem writes in the
unit fixture's retention helper. Final input `f476d493` changes only that helper
to use the execution-domain filesystem. Setup, fault injection and all five test
bodies remain byte-identical to the original controls. The full 1,240-test
library and both exact native scenarios pass again, and the freshly resolved
native executable is byte-identical to the thirteen-pass full-target artifact.
Strict all-features/all-target Clippy passes for Cellar and the shared recovery
consumer. The qualification record
retains the failed builds/lint check, every source and executable identity,
before/after controls, private data verification and the inherited native-artifact
proof. The earlier ambiguous-command failure remains a separate open observation.

The published `8ee50b18` Linux target reports eight passes, three failures and
eight explicit RustFS ignores. All three failures record physical-inventory
`peer_overloaded`, with successful later sequential probes. Its four Typed RPC
jobs and three JavaScript jobs pass. Both native SDK builds and both Python SDK
jobs fail to compile the shared recovery fixture because its facade lacks the
`tracing` dependency. The local dependency correction passes that consumer's
build and strict Clippy check; combined-source hosted qualification remains open.
The same original RustFS now reports zero free bytes at the read-only recheck,
below its unchanged free-space reserve, so the
constrained SQL/KV/Queue comparison and all complete OPT/CG gates remain open.

## Receive cancellation and current native checks

At compiler input `8adcad97`, the truncated-read fixture retains its TLS read
half after shutdown until actual peer termination, then requires zero original
node/peer receive credit and zero physical query connections. Its original
three-second bound covers the complete operation. Production runtime code,
admission banks and credit ownership are unchanged. The frozen old fixture fails
one of ten fixed repetitions while retaining its receive credit; the corrected
fixture passes ten repetitions and its case in all three initial library sweeps.
Standalone counted TCP/TLS probes support distinguishing local shutdown from
observed peer closure, but their variable controls do not isolate an underlying
OS, Tokio or rustls defect.

Two of three initial library sweeps fail the unchanged partial park-certificate
case with `NotReady`; the other sweep passes. The park fixture now retains its disks,
labels operation/voter errors and joins begun owners after a returned scenario
error, preserving the primary failure and any cleanup errors. Original park/wake
assertions, timing, deadlines and selected certificate lifetimes remain. Ten fixed
partial-delivery cases, the full-delivery control and three subsequent serial
library sweeps pass: **1,248 passed, zero failed, two existing ignores** per sweep.
These passes do not establish a causal repair of the earlier park failure.
Complete retention of its two earlier automatically cleaned failed disks remains
unproven; their logs, sources and executables remain retained.

The ordinary native disaster target passes **13 cases**, with eight explicit
RustFS ignores. Its executable is byte-identical at the final input. The
published parent's original Linux serial disaster job also passes 13 cases;
neither result qualifies constrained voters or the ignored RustFS scenarios.
Strict all-feature/all-target and default recovery-consumer Clippy, formatting,
layout and native dependency checks pass. Closed retained fixtures are verified
twice and their copies verified. See the
source-specific record.

Next priorities remain original-ID positive receipt reconciliation after unknown
remote writes, Queue preparation/publication deadlines and physical fairness,
reclamation, movement, recovery and combined SDK qualification. The original
RustFS recheck at 16:12 UTC reports zero available bytes against its unchanged
6 GiB reserve, with the same backend, image, resource limits and no restart.
The original constrained SQL/KV/Queue matrix remains held. No throughput,
latency-percentile, full OPT or remaining CG acceptance gain is established.

### Queued prepared submissions

Compiler input `816bcded` collects adjacent prepared submissions already queued
under ordinary/group request admission before polling Raft Ready. The existing
32-item group envelope and byte limits remain. Every producer still enters
`owner.submit` independently; the collector stops at a different request, a
refusal/error, or insufficient conservative event/Ready headroom. The estimate
includes buffered entries/messages and a possible append or snapshot per current
peer. There is no fill timer, new worker, larger bank or deadline change. Protocol
acceptance remains distinct from committed application and required S3 coverage.

The actual SQL producer/runtime/apply regression uses three persisted voters in
both image forms. It holds the first real engine sync, queues the original
later futures, and measures leader proposal persistence before delivering peer
frames. Three entries require three calls before the change and two afterward.
With four event slots, four entries require four calls before and three after;
an intervening ReadIndex retains three calls in both versions. All correctness
checks and joined shutdown precede the counter assertion. Eight final before
invocations fail only that assertion; the read-order control passes. Identical
fixture bytes pass all ten after invocations, including two read controls that
each cover both image forms. Every voter retains the original creation and SQL
value/version; exact accepted positions/digests replay without another initializer.
The fixture also verifies zero retained producer operation/byte credits after
output release.

The full default core (1,233 passes), execution (213), storage (23), archive
controller (46) and archive (40) suites pass, with their existing ignores. Eight
selected native/Axum SQL/KV/Queue cases also pass, including Queue claim/lease
proof, callbacks, cross-deployment faults and four in-flight deliveries. The full
measuring tool passes 26 tests. Two subsequent exact rooted-128 repeats (129
cells across two RF3 groups) use the same executable and original budgets:
enrollment takes 22.138 and 53.210 seconds
under the unchanged 60-second limit. Their leader work counts match despite the
wide timing variation. These passes do not attribute a sequential enrollment
latency repair to proposal batching or erase earlier source-specific failures.
Strict affected-target Clippy, formatting/layout, dependency, documentation and
all 44 design checks pass. The
qualification record
pins source inputs, unchanged fixtures, executable/profile hashes, earlier build
failures and copied, verified data from 43 retained fixture directories.

This is an avoided-work result for leader proposal persistence; later
commit/apply writes remain. It establishes no hardware flush count or matched
SQL/KV/Queue throughput/percentile gain. Ordered apply/image publication,
producer cleanup, transport batching and sustained fairness remain work.
The original RustFS `/data` filesystem still has zero available bytes against
the unchanged 6 GiB reserve, so no current-source constrained campaign ran.
On the published parent `f979d7a66`, all four Typed RPC jobs pass, but three SDK
disaster cases fail in both Python modes, the recovery-barrier case also fails
in 3.14, and Node package delivery exceeds its original 250 ms handler budget.
Those hosted observations belong to that parent source; they are not candidate
SDK qualification. Full SDK/feature/daemon, primitive, fault, movement/wake,
independent-domain and CG/OPT release gates remain open.

### Managed transition failure evidence

Test-only source `c19203ee` has compiler input `02983021`; production inputs
remain at the batching checkpoint. The all-features native disaster target
passes nine tests with eight explicit RustFS ignores. Two exact controls on
that source, plus its full-target control, use three real managed voters to
check metadata-only output, private synthetic object retention and unchanged
canonical fleet/directory state. An earlier expanded fixture also passes nine
tests. Before those controls, two exact retry invocations and the original
eight-test serial target pass locally. These runs do not reproduce the hosted
Linux failure and do not establish a repair.

On published runtime head `f845e8a3`, the standard-Python native job fails the
three public disaster cases; two authorization refusals retain
`unavailable / outcome unknown`. Its full free-threaded Typed RPC job also
fails while the other three Typed RPC jobs pass. These source-specific hosted
gates remain open. The original business assertions, request identities,
five-second budgets and fault policy are unchanged.

Failed managed transitions now sample bounded post-failure catalog, enrollment,
trust, control and authenticated physical-inventory metadata before joined close.
Only that fixed report joins CI's structured-observation upload allowlist.
Synthetic memory objects are sampled directly into private numbered files;
external stores are untouched. Those nontransactional samples grant no admission,
capacity or recovery proof. Raw objects, manifests, databases, configuration and
credentials are excluded from upload. Successful all-disk-loss oracles still
erase their original voter/cache roots, with no pre-erasure backup.

Affected-target Clippy, formatting/layout, native dependencies and all 44 design
checks pass. The reviewed baseline changes only the CI anchor among 24 anchors;
runtime baseline and task/profile acceptance remain unchanged. The
qualification record
retains exact source/executable/profile hashes, earlier preflight failure,
separate intentional erasures and private retention audits. A fresh read-only
backend check still finds zero available bytes against the unchanged 6 GiB
reserve, with the same original identity, resources, start and restart count.
No constrained performance comparison, hosted repair or CG/OPT acceptance follows.

### Node delivery source checkpoint and focused Linux feedback

All three JavaScript CI jobs pass on published runtime source `f845e8a3`,
including the full 100-case packaged-Node delivery loop. Its captured Node
version is 24.22.2. Compiler input `a5246111` changes only local test retention
and includes the emitted example; supplemental SDK input is `c5e2ee12`.
All 100 local invocations of the same compatibility/counter/restart test pass
using Node 24.13.1 and the original 250 ms handler, two-second Node turn,
five-second actor operation and one-second lease budgets. No failed request is
retried. These platform/patch-specific passes do not explain the earlier Node
failure or prove a causal runtime repair.

The test now retains its original directory on every exit. All 100 stores are
kept in place and copied privately with twice-verified byte manifests; all 2,136
packaged files are independently verified unchanged after execution. The
Node qualification pins source,
SDK/npm/generated inputs, executable/profile hashes, every invocation, retained
data and the three successful hosted jobs. Affected-target Clippy, Rust format
and layout, dependencies, workflow YAML and all 44 design checks pass.

The focused `cell-group-disaster.yml` workflow runs the existing serial disaster
target independently on Linux with the SDK Rust/O1/assertion/overflow settings.
It retains original test budgets and assertions, and uploads only the fixed
structured transition report. It is unexecuted at this source checkpoint and
supplements the full workspace/feature/Python matrix. Native/RPC failures and
all constrained primitive, authority, recovery, movement and complete CG/OPT
release gates remain open. This wave changes no production runtime code or
original RustFS/Colima state and qualifies no throughput or percentile gain.

The first focused-workflow publication, `24894dd8`, fails GitHub validation
before any job or test starts. Actionlint 1.7.12 independently reproduces both
invalid job-level `runner.temp` expressions. The correction creates fixture
paths during setup and exports target/temp paths through `GITHUB_ENV`; Rust,
profile, business budgets, test selection and artifact allowlist remain unchanged.
The workflow validation follow-up
retains that failed run and source hashes plus the passing corrected lint.
Runtime execution remains unqualified at this checkpoint.

### Earlier component checkpoints

The explicit SQL read path uses `Database::prepare_read_only` and the existing
linearizable pinned-image reader. It preserves source-write proof admission,
refuses cold targets without enrollment, and keeps writable prepared statements
on their original path. Read-only `first()` and result refusals can stop early;
writable `INSERT ... RETURNING` still drains completely. The primitive benchmark
selects the new API for its SQL point reads. See the
SQL read qualification for compiler
inputs, native/SDK/Axum/backend coverage, failed controls and test limits.
This does not establish an end-to-end speedup or close an OPT/CG release gate.
The original constrained SQL/KV/Queue matrix and rooted-128 enrollment gap remain
separate priorities.

Rooted SQL now creates its private directory through an explicit disposable
workspace API. The actual SQL factory regression retains the full reservation,
refuses a competing allocation, and holds capacity through the last physical
pin. It measures two creation syncs before the change and zero afterward; final
deletion still requires its original parent sync before refund. The durable
workspace API retains its child and parent creation barriers. Private scratch
supplies no recovery authority: independently persisted images, committed Raft
state and required S3 proof still gate acknowledgement. See the
workspace qualification for
source-specific coverage, failed controls and the original-budget enrollment
observations. This does not close the constrained performance or OPT/CG gates.

The same real-RustFS run exposed a SQL SDK error conversion that hid a nested
quorum refusal behind `pinned resource query`. Alternate error formatting now
retains the cause chain and direct SQL error families. The original refusal
assertion remains unchanged. The qualification separates the workspace source,
final error-reporting source, retained failures and independent test namespaces.
That checkpoint's full measuring-tool suite retains two rooted-128 enrollment
timeouts under its original budget, despite passing narrower invocations.

Archive retirement now folds a confirmed predecessor release into the next
durable journal intent. Successful early exits and final cleanup still persist
cleared debt; successor remote mutations still wait for the exact intent's file
and directory barriers. The real-controller regression observes one standalone
journal replacement before the change and zero afterward, twice per image form.
The same four checks pass against the original shared RustFS. This avoids one
complete journal store on an eligible advance; it does not establish a hardware
flush count or an end-to-end throughput/latency gain. See the
archive retirement qualification
for frozen inputs, unchanged fixture bytes, failed controls and retained data.

At compiler input `2ac6b776`, the core library (1,232 passes), execution (208),
archive controller (37), selected SQL/KV/Queue and recovery cases, strict
all-target Clippy, formatting and layout pass. The full measuring-tool target
still fails rooted-128 enrollment at its original 60-second deadline (25 passes,
one failure), despite two exact passes. RustFS passes 14 of 15 selected
invocations: materialized Axum KV fails to reach the injected archive PUT outage
within its deadline; its extent iteration passes. Both failed owners remain
failed. The original backend identity, resources and data remain unchanged.
Hosted CI on published `65f888af` also retains native disaster authorization,
primitive-catalogue and named-container capacity failures. These remain release
gaps; the component optimization accepts no complete OPT or CG gate.

The catalogue failure reproduces locally and is repaired by following the
reviewed `workflow_read.rs` to `resource_read.rs` rename in `12be0e235`.
Catalogue admission, all 13 coverage-guard tests and all 44 design-plan tests
pass after that pointer correction. This changes no task/case acceptance and
does not repair the separate native disaster or container capacity failures.

The follow-up at test-only input `abc75976` retains bounded snapshots before
shutdown. Frozen pre-change `ac4a18e9` and component `2ac6b776` KV binaries each
pass once and fail once at the same materialized archive-outage oracle. The
final snapshot records a denial counter unchanged at 136: a source archive
owner and a target archive owner are stopped on `Archive(Payload(Storage(...)))`
from the earlier injected payload-store error, while their peers report `Busy`.
This identifies a concrete archive-progress failure path, not a repaired retry
contract. Exact payload-pin/adoption reconciliation and terminal corruption,
missing-data and journal errors must remain protected in a repair. Native-only
named-container capacity passes three local repetitions and leaves its hosted
JS/Node failure unqualified. See the qualification's `kv_outage_followup` for
source scopes, retained failures, snapshots and checks. No production behavior
changes in this diagnostic follow-up, and no isolated latency gain is claimed.

At compiler input `b32cb4bc`, generic payload-store I/O no longer permanently
stops the archive owner. It retries through the original exact journal intent;
byte verification and pin/adoption reconciliation still precede coverage.
Missing/corrupt payloads and failed journal barriers retain terminal handling.
The real-controller regression fails twice per image form before the change
and passes with identical fixture bytes afterward. Two actual denied reads
preserve the same serialized Reserve intent and grant no coverage; restoring
the original bytes then produces independently verified exact-cut coverage.

The payload archive retry qualification
records the seven-line production change, initial fixture capability refusal,
valid red controls and retained data. Native qualification passes and joins:
core 1,232, controller 41, archive 40, execution 208, ordinary SQL and immutable
payload tests, selected native/Axum SQL/KV/Queue and interrupted-binding checks,
strict all-target Clippy and repository gates. All 22 selected original-RustFS
invocations pass, including the formerly failing Axum KV fixture twice in
independent prefixes. The backend identity, resources, restart count and start
time remain unchanged. The evidence scans 46 actual private representations
with zero matches. This closes the reproduced generic-I/O archive progress
path within the recorded scope; other typed payload errors retain their prior
policy at that source cut. The constrained performance, rooted-128, remaining
SDK/recovery and complete OPT/CG gates remain open.

At compiler input `8fb09d7f`, payload operation-admission pressure also leaves
the archive owner retryable. A real foreground GET fills the focused fixture's
single operation slot and waits at the actual store read. The identical fixture
fails twice per image form before the classifier repair and passes afterward.
Two completed `Overloaded` failures preserve the same serialized Reserve intent
without coverage. Joining the original GET then permits fresh byte verification
and independently verified exact-cut coverage. Only the focused test reduces
operation capacity from two to one; production limits and deadlines are unchanged.

The pressure qualification
retains exact inputs, executable identities, failed controls and fixture disks.
Final-source controller qualification passes 43 tests, strict library/controller
Clippy and repository checks. The preceding `c7fd8ee6` source has identical
production bytes and passes the broader native archive/API/recovery checks; its
failed fixture-select lint is retained separately and corrected through Cellar's
execution API before the final exact before/after comparison.

RustFS qualification remains **FAILED: 8/18 selected invocations pass**. All
eight pressure/generic-I/O/data-refusal selections pass; all ten broader
API/payload/recovery invocations fail, including actual PUT HTTP 500 responses.
A read-only observation finds the original RustFS `/data` on its named Docker
volume at 100% usage with zero available bytes. Readiness still returns HTTP 200;
it does not establish write availability. The original identity, image, limits,
start time and restart count remain unchanged. No resize, restart, pruning,
relocation or reserve relaxation is performed. The record scans 46 actual private
representations with zero matches. Payload deadlines, uncertain cleanup under
pressure, fitting-capacity wakes, fairness and all broader performance/release
gates remain open.

Performance evidence tooling at input `0ebbf597` now recognizes all 53 existing
native operation phases, including the five per-command phases. Its native-name
contract extractor accepts digits in names such as `s3`; complete historical
evidence inventories remain explicit. Current phase samples still require the
complete counter inventory, and mixed windows, missing/unknown metrics and
impossible samples still refuse. The exact hosted `CellCommandS3Coverage` guard
error reproduces locally before the repair. All 19 owning tests pass afterward;
the full CI performance-guard command runs 343 tests with one skip and no failures.
See the metric vocabulary qualification.
Only Python evidence tooling/tests change from runtime input `8fb09d7f`. This
does not qualify new throughput/latency results or repair the separate native
disaster authorization, container capacity or full release gates.

At compiler input `1ac2cced`, fitting payload credit can wake the original live
S3 request through the retained archive context. Each publication installs its
job-local hint on the actual registry's pool wrapper; the original node-wide
budget and namespace bindings remain shared. Ordinary callers inherit no capture,
and background publication receives the default no-capture state. The hint grants
no reservation or proof. Payload byte and ownership admission preserve the same
ceilings, deadlines, request identities, callback and durability contracts.

The payload capacity wake qualification
retains a final identical-fixture comparison: four original S3 requests expire
before the change, twice per image form, while all four invocations pass after it.
Each passing invocation completes two successive intended S3 commands, checks
callback counts and receipts, and independently verifies the exact selected hold
and real bytes. A deliberately long diagnostic maintenance interval distinguishes
a credit wake from timer retry; the request/GET budgets stay five seconds.
Actual-payload background cadence and existing tail/checkpoint/expired-request
controls pass. A resolved-payload unit preserves ordinary-caller isolation,
retained byte charges and competition for returned credit.

Final native qualification passes core 1,233, controller 46, archive 40, execution
208, selected native/Axum SQL/KV, Queue callback/proof/acknowledgement and interrupted
SQL recovery checks, strict all-target Clippy and repository gates. The initial
candidate's failed audit and disk remain retained: S3 output had succeeded, but
that audit additionally required deferred predecessor retirement. The final
selected-cut audit verifies the real hold/bytes without that unrelated condition;
all existing settled lifecycle assertions remain and their controller suite
passes. All seven final oracle files are byte-identical across the accepted red
and green comparison. Thirty-one remaining native fixture directories and 1,928
regular files have verified retention manifests.

No new backend or constrained campaign is qualified. A fresh matched read-only
RustFS data-volume check observes 12,066,816 bytes available, below the unchanged
6 GiB reserve; readiness still returns HTTP 200. The earlier zero-space/PUT
failures retain their own source scope. No resize, restart, pruning, relocation
or reserve relaxation is performed. No throughput or percentile-latency gain,
independent physical-domain recovery or complete OPT/CG gate is accepted.

Preceding `edeb5c0d5` hosted CI passes JavaScript/Node's three jobs and Python
examples, but fails Typed RPC full (3.14) in Python-provider proof-wait warmup.
Owner and ingress fence on `NodeLeaseExpired`; the bounded recent trace contains
ambiguous renewals before watchdog fencing. That fixture keeps its original
1,000 ms lease, distinct from the five-second benchmark configuration. The other
three Typed RPC jobs pass; no cause or noncommit inference is established.
SDK qualification also fails four native disaster cases in the 3.14 job and
embedded-Python compilation of `python-fleet` on missing `crate::object_clients`.
Those separate boundaries remain open at this checkpoint.

The Python measuring frontend's compile error is subsequently reproduced and
repaired at input `dbef0707`. Declaring its existing `object_clients` module
restores the same shared application/lease client factory already used by the
native executables. The identical owning example check reports two `E0433`
errors before the three-line wiring repair and passes afterward. Its existing
real HTTP cross-runtime factory test passes from the Python example executable;
all `cellar-python` embed test targets compile, and example Clippy/repository
checks pass. See the frontend qualification.
No runtime-library, factory, dependency, quota or deadline changes follow native
input `1ac2cced`. This local Python 3.14.6 GIL build/control does not qualify full
adapter runtime or free-threaded ABI execution; fresh hosted CI and the separate
disaster/lease/performance gates remain required.

At compiler input `bb325bdb`, the ordinary storage worker skips periodic purge
when the engine's existing policy reports no eligible rewrite/delete work. Such
a pass previously still appended and synced Cellar's durable root fence. Every
later durable write rearms the check. Managed scheduling already uses the same
predicate; eligible purge, explicit purge, shutdown and settlement retain their
barriers. Production intervals, thresholds, banks and deadlines are unchanged.

The periodic purge qualification
records two identical-oracle failures before the change and two passes afterward.
Each invocation previously adds one idle periodic sync and another after a real
durable write; both periodic syncs become zero. The application write and explicit
purge retain their syncs. Eligible physical deletion and live-history restart
also pass. These are filesystem API work counts, not hardware flush counts or
an end-to-end throughput/percentile result.

The existing scheduled-purge-failure test initially times out because its engine
contains no eligible files. Final input `9b684f00` changes only that fixture after
`bb325bdb`: it seeds eligible files before the fault, preserves the original
20-ms interval, five-second observation, exact failed-purge count, I/O close,
joined lock release and recovery assertions, and verifies all sixteen retained
entries. It passes twice; the full storage target passes 23 tests with one
existing ignore. Core 1,233 and controller 46 pass, and their executable hashes,
along with the measuring executable, are unchanged by the fixture correction.
Strict affected-target Clippy and repository checks pass. Thirty-seven retained
fixture directories and 25,049 files have verified manifests. The initial legacy
fault fixture cleaned its temporary disk; its complete source/log/footer remain,
and the corrected fixture retains its disk on every exit.

Broader qualification remains failed. Both focused rooted-128 invocations still
reach the original 60-second enrollment limit with zero periodic purges on all
three nodes; the complete measuring target passes 25 tests and fails one. This
removes reproduced work but does not repair the remaining preparation,
persistence and physical-close latency. Another checkout's compiler was observed
during this wave; native wall times are not an isolated comparison. A matched
read-only backend check again observes zero available bytes on the original
RustFS data volume, below the unchanged 6-GiB reserve. No new backend campaign,
resource change, complete OPT/CG gate or measured speedup is qualified.

The complete failed `edeb5c0d5` Typed RPC artifact also narrows its lease incident.
Owner and ingress reach native COMMIT within one millisecond, then spend about
774 and 668 milliseconds inside that call. The owner starts with 664 milliseconds
of prior authority; its async timeout overruns by only two milliseconds, and it
fences about 110 milliseconds before the late commit completes. This identifies
native COMMIT elapsed time, not an isolated device-sync cause or a repaired RPC
gate. Preserve the original one-second lease and fence; late completion cannot
revive the fenced process or justify a fresh identity for an unknown mutation.

The first change implements the command outcome and authenticated advice
foundation:

- The Raft owner samples history, applied membership revision/configuration
  digest, term and a legal incoming-voter leader in one owner turn. Advice is
  absent during park/wake fencing; command discovery first completes the existing
  fresh-quorum wake. Explicit stale queries do not wake a group for advice.
  Digest scratch consumes the existing 64-KiB membership observation allowance;
  local status-only selection retains its smaller reservation. Advice is withheld
  when the configuration revision differs from the applied manifest.
- Command replies distinguish `Applied`, `RejectedBeforeAdmission` and
  `AdmittedOrUnknown`. Only construction/admission failures before controller
  ownership can report rejection. Once the controller owns the invocation,
  errors retain uncertainty and physical ownership. Exact receipts and the
  requested Quorum/S3 proof still determine success.
- A fresh command sent to a follower is rejected before callback entry. A
  rejection after an earlier uncertain transmission cannot clear that command's
  ambiguity. Command IDs, captured inputs and fingerprints are unchanged.
- Command, probe and query replies carry optional fixed-width advice under the
  existing authenticated request/response binding. Senders reject foreign
  history; probes claiming local leadership cannot advise another member.
  Decoding remains bounded by the existing reply reservation and frame limit.
- Aggregate performance counters expose command RPC attempts, leader probes,
  proven pre-admission refusals and unconfirmed application replies. Attempts
  count entry into the transport exchange, including exchanges that later fail;
  they do not count committed commands or prove physical transmission.

This changes marker-1 response layouts and the query TLS-exporter/MAC domains
(`query-advice-1`) with a hard internal cutover. Rebuild
matching peers and clients. There is no predecessor response decoder. The
test-only value helpers use the same mandatory envelope decoder.

## Remaining optimization work

The [implementation sequence](tikv-informed-optimization.md#13-implementation-sequence-and-owning-boundaries)
and its named RO/RL cases remain the acceptance contract:

| Package | Outstanding delivery |
| --- | --- |
| OPT-01 | Finish smallest-scope negative-path repair and validate membership, public authority isolation and ambiguous outcomes; RO-01/02 runtime cases pass below, while broader RO-03/04/07/08 gates remain open |
| OPT-02 | Selected public RO-05 interval-repair and RO-06 SDK/native HTTP/Axum split oracles pass below; recovery lookup cuts and remaining demand-refresh qualification stay open |
| OPT-03 | Automatic preferences, bounded fallback and public entered-source expiry/pin checks pass below; public partition coverage and full RO-10 acceptance remain open; the public RO-11 cohort oracle passes at its source checkpoints below |
| OPT-04 | Physical shared-file/segment release proof and once-only accounting qualification; RL-01 through RL-05 |
| OPT-05 | Load telemetry, reserve-aware placement and repeated release-scale movement; RL-06 |
| OPT-06 | Measured scheduling fairness, batching and queue isolation; RL-07 |
| OPT-07 | Optimized wake/transition/recovery routing; RL-08 and retained CG-13 gates |
| OPT-08 | Combined-source full primitive, backend-fault, crash and performance qualification; selected local RustFS/daemon checks pass below |

### Priorities for completing the optimization

The latest completed twelve-case comparison is the
[main-170 capacity-wake matrix](three-node-primitive-main-170-2026-11-15.md),
bound to compiler input `362f78db`, public commit `1d663425` and main `16c45b26`.
Four cases pass: KV+S3, Fleet SQL, bucket SQL and bucket Queue. Eight fail;
109 windows verify, eight fail and 75 remain unmeasured, with 51 request errors.
SQL/KV quorum, Fleet KV/Queue and bucket KV record lease-expiry markers. Queue+S3
also fails without such markers. These need separate reproductions under the
original five-second budgets. Only complete passing cases receive descriptive
rates; no complete four-mode comparison or isolated optimization gain is accepted.
The subsequent foreground-only archive-capacity cadence candidate has separate
native regressions and is not in that measured image.

The earlier twelve-case
[main-173 campaign](three-node-primitive-main-173-2026-11-15.md) includes main
`189edd9a` and source cut `69654995`. Nine candidate cases pass; KV+S3,
Queue+S3 and bucket Queue fail. All SQL modes and the complete SQL/KV quorum
pairs pass. Candidate timed windows record 104 verified, two failed and fourteen
unmeasured, with sixteen request errors across emitted windows. Queue quorum
passes on the candidate but fails its control's C16 drain. A broader host audit
finds another checkout's primitive qualification plus a second VM, missed by
the narrow compiler monitor. SQL timings are mixed and KV rates are lower;
neither direction establishes an isolated optimization effect.

The later [checkpoint-window S3 comparison](three-node-primitive-main-173-2026-11-15.md#checkpoint-object-window-comparison)
fails all three candidate APIs, including SQL, with 16 request errors versus
ten for its baseline. No API has a complete passing pair; leaders and terms
remain unchanged. The subsequent logged SQL pass leaves these deadline failures
open. Fresh checkpoint preparation for empty tails passes scoped native and
RustFS qualification below, but its subsequent constrained comparison fails all
six S3 cases. Candidate KV loses node 3 to `NodeLeaseExpired` during load;
baseline Queue loses node 1 to a startup `Runtime(DeadlineExceeded)` before
load. No complete passing pair or end-to-end gain follows.

The Queue validator correction accepts the API's valid floating-point counts
while preserving finite whole-number checks, complete observer rounds, exact
identity/body reconciliation and zero settlement. Its regression fails before
repair; all 341 performance tests run successfully with one existing skip.
Saved-window replay and the fresh three-family smoke pass. The original smoke
stays failed. Both frozen images retain their exact prior Rust/load-client bytes
with verified new manifests. All comparison owners join and the original RustFS
identity, limits and mounts are restored, with HTTP 200 readiness.

The earlier [main-172 campaign](three-node-primitive-main-172-2026-11-14.md)
retains its five passing and seven failing candidate cases. Later native
follow-ups below pass automatic handoff, child selection and the scoped
removed-member oracle with both backends. Current library qualification passes
1,192 tests with two existing ignores after an earlier park/wake `Closed`
failure; 103 isolated attempts do not explain that failure. Current-head Typed
RPC CI also retains a standard-Python proof-wait `NodeLeaseMismatch` failure.
These scoped passes do not close the full primitive, fault or feature-mode gates.

Direct routing, targeted cache repair, consistency-aware read selection and
bounded shared workers already exist. Completion requires the following delivery
and evidence, under the design's original limits and acceptance oracles:

| Priority | Gap | Evidence required to close it |
| --- | --- | --- |
| P0 | Public SQL/KV/Queue reliability | Close the SQL/KV/Queue S3 ambiguous deadlines, bucket Queue durability/drain failure and control Queue drain cut. Complete the source-matched native/SDK/native HTTP/Axum and three-node C1/4/16 matrix. Account for every acknowledged and unconfirmed identity; preserve exact bodies, settlement and original deadlines/admission limits. |
| P0 | Original-deadline progress | Show bounded routing/readiness, writer admission, SQL preparation, ReadIndex and output-proof progress. Diagnose the observed node-lease expiry and startup deadline alongside request deadlines. Slow callbacks and archive work must leave lease/control/election progress available; expired waiters must retain their actual physical owners until completion. |
| P1 | Persistence and archive amplification | Measure image/manifest file and directory syncs, engine batch bytes/groups, independent archive replay and queue waits. Compare matched before/after throughput, p50/p95/p99, errors and resources; accept a tuning change only with its correctness controls. Sparse traffic must not wait to fill a batch. |
| P1 | Safe public read integration | Connect explicit read-only SQL operations to fresh ReadIndex/source-image proof without treating writable `__sqlQuery` as a read. Preserve KV TTL/metrics mutations, exact tokens, read preferences and partition refusal. Recheck gains with a broader host audit and crossover measurements. |
| P1 | Sustainable physical capacity and movement | Complete shared-file/segment release and once-only accounting, followed by repeated split/move/merge under the original bounds. Retained readers, snapshots, archive/transfer work and uncertain completions must protect bytes; credit follows synced deletion and joined owners. Preserve placement reserve and failure-domain checks. |
| P1 | Authority and read-consistency fault coverage | Complete the remaining RO-03/04/07/08/10 cuts for current membership, boot, revocation/expiry, ambiguous commands and partitioned reads. Advice cannot authorize execution; fresh ReadIndex cohorts, exact tokens and image pins remain required. |
| P2 | Wake/recovery and combined qualification | Exercise optimized routes through park/wake, interrupted movement, retained restart and fenced all-disk-loss recovery. Run the complete primitive/backend/crash/feature-mode/daemon suite on one combined source and restore legitimate missing historical evidence. A single Colima VM does not establish independent physical failure domains. |

The current persistence candidate, `97b805f3`, shares a final inventory-directory
barrier across a bounded image update while retaining every file sync and full
destination verification. File-only observations cannot supply a closure seal;
ordinary readers establish their own barriers, and a failed shared barrier
quarantines mutation without refunding charges. At its pre-172 source checkpoint,
103 extent/store/cache/workspace tests, strict owning-crate Clippy, 27 host-image
controls and 28 archive-controller controls pass. Existing ignores remain explicit.
The superseded native remainder and Linux build are deliberately canceled when
the new main is observed. No throughput or latency gain is measured or claimed
at this checkpoint. The final comparison must isolate this change against the
same integrated main, rather than attributing upstream progress fixes to it.

### S3 deadline reproduction and command-phase observations

A fresh namespace rerun uses the exact main-173 candidate image and load client,
with the original three 1-vCPU/2-GiB RF3 nodes, 24 cells, C1/4/16, two reversed
repetitions and five-second operation/callback/lease bounds. It fails at the
first C16 read: 547 attempts, 538 successes and nine request errors. All three
groups retain their initial leader and term. An election is therefore not
necessary for this failure. Final SQL and controller retained-owner counts are
zero, without a quarantined group; these observations do not prove a permanent
owner leak or identify the deadline's critical path. Other-checkout qualification
activity is observed during the run, so the timings remain diagnostic.

Compiler manifest `cc89c945` adds five bounded aggregate command-phase
histograms: local execution, S3 command coverage, proof freshness, individual
output checks and the resumed output-wait loop. Coverage includes native RPC
checkpoint commits at the shared archive-client boundary. The existing timer
records the original future's result or abandonment; it changes no admission,
deadline, retry, authority, acknowledgement or Ready/sync ordering. These timers
overlap and must not be summed. A retryable output-check refusal describes that
attempt, not the command's durability or final result.

The real RF3 held-publication/later-apply test fails before implementation on a
missing phase and passes after instrumentation, across both image modes and
new/duplicate commands. Its original no-early-output, fresh-authority,
callback-once and exact replay assertions remain. Strict all-target Cellar
Clippy, five metric-lifetime tests and three owning S3 controls pass. Compilation
and tests retain the same compiler inputs and joined owners. Format/layout
checks cover 2,321 Rust files; native dependency and documentation checks pass.
The earlier private-field compile failure remains recorded separately. This
checkpoint supplies observations, not a deadline fix or measured speedup.

The matching Linux image `b49af6da`, from source `337abc29`, rebuilds both owning
libraries and binaries with verified complete inputs and unchanged load bytes.
The [command-phase follow-up](three-node-primitive-main-173-2026-11-15.md#command-phase-follow-up)
retains both original-bound S3 runs. KV passes all 18 windows without request
errors; its earlier nine-error reproduction remains unexplained. Queue verifies
five windows, fails the first C16 enqueue and leaves six unmeasured. One of 340
requests has an ambiguous deadline. All 2,712 acknowledged IDs/bodies are
observed, plus eight matching the one uncertain batch; no S3 receipt follows
from delivery observation.

KV coverage averages 220 ms versus 313 ms local command lifetime. Queue records
one S3 coverage failure and no proof-freshness or resumed-output-wait failure;
coverage averages 367 ms, against 696 ms local lifetime. Timers overlap, include
verification/background work and lack per-request causal linkage. Archive
coverage admission/publication/replay is the next profiling target, while the
original correctness and deadline gates remain. Leaders/terms stay unchanged;
all nodes/stores exit normally without observed OOM/restarts. Host qualification
activity occurs during both runs, preventing an isolated speedup claim. Owners
join and the original RustFS identity/limits/mounts return with HTTP 200 readiness.
The earlier bucket Queue's 32 extra IDs/bodies also reconcile to four uncertain
batches; its eight then-unobserved acknowledged IDs and durability/drain failure
remain open.

At published head `ac64ca16`, the JavaScript adapters job separately fails
`removed_class_recovers_an_uncertain_create_and_retires_without_business_code`:
retained provider retirement times out and shutdown reports `NodeLeaseExpired`.
The other two JavaScript jobs and Python examples pass; SDK and Typed RPC runs
are still in progress at this observation. No rerun or suppression closes that
failure or the broader feature-mode gates.

### Bounded checkpoint object I/O

The coverage investigation identifies serial checkpoint chunk upload and
independent download inside the existing archive lane. Compiler manifest
`948c229d` polls two ordered object futures using the already reserved
eight-chunk I/O workspace. Sparse input can progress without filling the window.
Upload byte/chunk limits count admitted inputs before network work; completed
references and downloaded chunks retain source order. Conditional PUT/readback,
full physical recovery, source/receiver admission, typed failures and actual
worker joins remain mandatory. No deadline, operation bank, format or output
authority changes.

Real RF3 regressions hold the first object and require the second to start,
while forbidding a third and charging exactly one archive operation. Both fail
on serial manifest `0bc80b7f`, with the same final test source and original
five-second bounds, then pass with both image formats. Repeated upload retains
the exact checkpoint reference; independent download verifies every recovered
image. Failed fixtures retain their disks after joined cleanup.

Strict all-target Cellar Clippy, formatting/layout (2,322 Rust files), native
dependencies and documentation checks pass. The complete default native archive
target passes 38 cases with 10 explicit ignores; three owning S3 output/result/
deadline controls also pass. Six selected ignored controls pass against real
RustFS, covering physical cancellation, competing publication CAS, exact S3
results, deadlines, sibling dependencies and retained payload recovery/GC. This
executes three of the archive target's declared backend ignores separately;
other ignored cases retain their unrun scope. All owners join, inputs remain
unchanged and the original RustFS identity/limits/readiness are preserved.

The qualification record
retains source/command/log hashes and actual footers, including the prior serial
failures and the initial candidate type-inference failure. Source `8f674406`
builds Linux image `0ccf1de9` with all inputs verified and the same locked
compiler/profile and byte-identical load client as baseline `337abc29` /
`b49af6da`.

The [six-case S3 comparison](three-node-primitive-main-173-2026-11-15.md#checkpoint-object-window-comparison)
still fails under the original constrained topology and C1/4/16 profile.
Baseline SQL passes; both KV cases and both Queue cases fail, as does candidate
SQL. Candidate windows are 15 verified / three failed / 30 unmeasured, with
16 request errors, versus baseline 31 / two / 15 and ten errors. There is no
complete passing API pair, and no accepted throughput/latency gain. Leaders and
terms stay unchanged in every case. SQL candidate's four coverage failures and
zero freshness/output-wait failures narrow the next investigation without
providing per-request causality. Exact Queue IDs/bodies reconcile all extra
deliveries to uncertain attempts; they supply no missing S3 receipt.

All 30 participants stop without observed OOM/restarts, frozen inputs/engine
checks pass, owners join and original RustFS identity/limits/mounts/readiness
are restored. Other-checkout qualification activity and fixed image order
prevent isolated attribution. Existing archive-stage logging is the next
diagnostic, with traced timings excluded from performance qualification. The
window proves bounded object overlap and scoped correctness, not a deadline
fix or complete OPT/CG acceptance. Earlier constrained failures remain open.

### Fresh checkpoint preparation for empty tails

The [archive-stage diagnostic](three-node-primitive-main-173-2026-11-15.md#archive-stage-diagnostic)
repeats SQL/S3 on unchanged window image `0ccf1de9` with existing stage logging.
All 18 windows pass without request errors; logging changes timing and the
preceding failures remain unexplained. Across 3,308 publications, the mean
uploaded-to-verified interval is 52.70 ms, versus 31.22 ms captured-to-uploaded.
These include background work and lack request linkage. Source/engine checks
pass, owners join and the original RustFS identity/limits/readiness are restored.

Code inspection identifies a complete physical image/manifest pass after the
checkpoint receiver even when the tail contains no entries. A real RF3
differential regression uses a checkpoint-plus-payload-inventory control.
Original empty-tail recovery makes 47 filesystem-provider read calls versus 32
for that complete control in materialized mode, and 62 versus 44 with extents.
Checkpoint reception alone accounts for 26/32 calls; the additional 6/12 inspect
required payload metadata and must remain. Both baseline regressions fail on
the intended excess-read assertion with the final test source. They also
exercise a forged empty cut and damaged warm destination, retaining failed
disks after cleanup. These are provider calls in a three-cell fixture, not
device reads or end-to-end throughput/latency measurements.

The candidate retains that invocation's exact freshly received preparation for
an empty tail after the original owner, bound, cut/configuration/membership,
cancellation and liveness checks. The ordinary receiver still fetches every
remote dependency and verifies/syncs existing local bytes. Nonempty tails retain
full replay and final verification; publication/response authority and all
admission banks remain unchanged. The first runtime candidate passes strict
all-target Clippy and formatting, records 32/44 total tail reads and fails both
performance assertions: its checkpoint-only control omitted the mandatory
payload-reference inspection. The other 38 archive cases and three owning S3
controls pass; the command remains failed. A corrected control independently
queries each received fixture image's payload-table inventory through the
ordinary immutable adapter, validates canonical empty cells and retains the
exact read-count equality and all cut/corruption/ownership checks. Its baseline
reproduces the extra pass with source `1b3f9a13`; candidate manifest `a37458ee`
passes both focused regressions, making exactly 32/44 reads. This removes 15/18
duplicate reads while retaining all 6/12 required payload-inventory reads.
On the final source, strict all-target Cellar Clippy and formatting/layout
(2,323 Rust files) pass. The default archive target passes 40 cases with ten
explicit ignores; three owning S3 controls and six selected real RustFS controls
also pass. The latter separately execute three of the archive target's declared
backend ignores; other ignored cases retain their unrun scope. Native dependency
and documentation checks pass. All owners join, inputs remain unchanged and
original RustFS identity/limits/readiness are preserved.

The qualification record
retains exact profiles, source/executable/log hashes, footers and read counters.
The earlier Clippy duplicate-helper rejection also remains recorded; the shared
harness export replaces the duplicate inclusion without a lint suppression.
The failed incomplete-control run and canonical-empty control setup are retained
separately. The matching Linux image `9b98065f` preserves the locked compiler,
lab artifact profile and byte-identical load client. Its
[six-case S3 comparison](three-node-primitive-main-173-2026-11-15.md#empty-tail-constrained-comparison)
against checkpoint-window image `0ccf1de9` fails every case. SQL and KV retain
request deadlines, candidate KV node 3 fences with `NodeLeaseExpired` during
load, and baseline Queue node 1 exits with a startup deadline before load.
Candidate Queue observes all 568 acknowledged messages plus eight matching its
one uncertain attempted batch; this supplies no missing receipt. All created
participants stop without observed OOM/restart, owners join and the original
RustFS identity/limits/readiness are restored. Host qualification activity is
observed. The read-call reduction remains scoped evidence; no deadline fix,
throughput/latency gain or complete OPT/CG acceptance follows.

The subsequent [lease/archive diagnostics](three-node-primitive-main-173-2026-11-15.md#lease-and-archive-diagnostics-after-empty-tail-comparison)
also fail under the same frozen image and original bounds. The lease-only run
fails C1 read; adding archive stages reaches eleven passing windows before eight
second-C16 read errors. All 165/576 lease attempts apply with ample prior
authority and no wait overrun, while all nodes remain alive. This separates these
request failures from the earlier untraced node expiry. The longer run also
records retried output-check failures. Completed archive jobs, zero remaining
waiters at observed settlements and unclassified attempts do not identify the
failed requests' queue/retirement/retry path. Closing that causal observation gap
precedes further scheduler or admission tuning. Both failed diagnostics and
their exact source/event/resource evidence remain retained; no deadline, proof
or capacity bank changes.

### Archive publication and recovery follow-up

The original automatic balance/repair restart scenario reproduces against both
the in-memory object store and real RustFS. Its first live history reaches fresh
survivor verification, then archive acquisition returns `Conflict`. Retained
publisher diagnostics identify a permanent `archive publication exact cut`
refusal on both live groups. Replay verifies the complete canonical configuration
encoding but returns raft-rs's unordered configuration vectors; publication was
comparing those vectors directly with the captured source configuration.

The correction retains the source's validated canonical configuration bytes and
uses the same validating encoder for the replayed configuration. It preserves
exact manifests, membership ledgers, independent physical replay, joint roles,
duplicate/unknown-field rejection, existing reservations and original deadlines.
It changes neither acknowledgement requirements nor the archive format.

At compiler-input manifest `78eb379a` (3,413 inputs), format/layout, strict Clippy
for the transition and archive targets, and compilation pass. Six archive controls
pass in both image formats: source progress during upload, enrollment/control
replay, uncertain publication reconciliation, committed-source refusal, compacted
prefix extension, and reset/expired-intent refusal. The original mixed SQL/receipt
handoff completes its materialized-image profile in both backend executions.
Both two-image commands still **fail overall**: the extent profile stops earlier
in `Repairing`, with one settled movement; RustFS retains `Overloaded` for one
blocked history. The cause of that remaining admission/progress failure is open.
Child-selection discovery also still fails while sampling a child: the first
child in-memory and the second child in the RustFS follow-up.
These results do not close either original test or an OPT/CG gate.

Failure harnesses now retain all node and replay disks after the original owners
join, and report bounded archive status and selection-stage context without
replacing the original failure. Final compiler-input manifest `9a38928c` differs
from `78eb379a` only in making the test's archive diagnostic non-fallible;
production inputs are identical. Records are retained under
`main172-archive-configuration-fix-01`,
`main172-archive-configuration-rustfs-01` and
`main172-transition-diagnostic-final-01` in the checkout-owned qualification
directory. Both runtime executions preserve their failed status, source hashes
and joined shutdown; RustFS identity, 1-vCPU/2-GiB limits and readiness remain
unchanged. These are in-process native RF3 controls, not the constrained
three-container comparison. No new throughput or latency gain is established.


### Repair memory and group inspection follow-up

The extent handoff's remaining refusal is a reproducible memory-admission
cycle, independent of the archive configuration correction. The original
16-MiB control bank retains 11 MiB plus a 4-KiB extent-collection owner before
a source-membership read needs another 5 MiB. The same refusal repeats 307
times in the native probe and 186 times with RustFS. The actual extent-collection
owner retains its charge under the original 16-MiB bank.

Repair now reserves memory for the data actually alive in each phase. It first
checks/adopts the original full-model bound, retains one charged source proof,
regrows that same permit before constructing the full model, then shrinks it
after the model drops and before independent admission. The original intrinsic
misfit refusal, item ownership, root checks, exact request identity, capacity and
failure-domain checks remain. Source age starts at receipt, before the evidence
callback; both planning freshness checks and admission's new source proof remain
required. Three memory regressions cover both nested quorum phases, failed growth
without losing the retained charge, and unchanged oversized-model refusal.
At source `f5496d47`, the full library passes 1,187 tests with zero failures and
two existing ignores. The original two-image native handoff passes.

A separate read-only test watcher now resamples only `Conflict` when the active
controller changes Root between admission/debit status reads. It retains the
original 60-second timeout, actual learner predicate and every mutation identity;
it retries no mutation. At source `b170f4cf`, the complete handoff passes both
natively and with RustFS, while the separate child-selection command still fails.
These failed broader commands and the earlier archive/admission failures remain
retained rather than being relabeled as passes.

The child probe identifies a second progress defect: sampling asks for Fleet
revision 35 while all nodes have installed revision 33. Local group inspection
returns unavailable and remote inspection returns stale-peer before reaching
quorum work. One existing authenticated node inspection installs revision 35,
after which both local child-membership reads succeed. Fleet group inspection
now uses that existing demand-refresh path for local inspection and an eagerly
owned remote request. A complete current node cut is installed before endpoint
selection. The original deadline, cancellation, finite reservations and boot
validation remain; the receiver independently authorizes the exact history and
obtains fresh quorum proof. Neither a cached leader nor refreshed node trust
grants group/application authority.

At production source `f305af1d`, both original automatic handoff and child-selection
commands pass in both image formats, with the in-memory shared store (63.02/25.94 seconds), then
against RustFS (92.41/37.85 seconds). All owners join, inputs remain unchanged,
and the original RustFS identity, limits and readiness remain intact. These are
in-process RF3 controls, not the three-container throughput comparison.
Source `bc1782e5` adds only a managed integration-test module and its registration;
production and original transition-target inputs are identical. The new
regression passes both image formats: hold the actual periodic reader, advance
the real Root, catch up one local request and an unpolled remote request, then
cancel an independently held 30-second request within the existing five-second
oracle. The actual read and exact memory/item charges are released, and no
application callback runs. Its first attempt failed before the tested operation
because the fault adapter was attached to payload storage rather than the Fleet
reader; that setup failure is retained with joined owners and disks.

At `bc1782e5` (3,416 compiler inputs), strict all-target Cellar Clippy and the
full library pass: 1,187 passed, zero failed, two existing ignores. The original
managed single-request trust-catchup and current-endpoint inspection controls
also pass. Final manifest `ef7274e7` differs only in formatter-controlled
module-registration ordering; the exact module path/name declarations, all test
bodies and every production input remain identical. Final formatting/layout,
native dependencies and documentation checks pass. These results use integrated
main `0e925f6b`. Main advanced to `189edd9a` (#173 Blob/payload ownership) after
this checkpoint; that combined source requires its own verification.
Retained records include `main172-fleet-trust-fix-02`,
`main172-fleet-trust-rustfs-01`, `main172-fleet-inspection-regression-02` and
`main172-fleet-trust-qa-01` in the checkout-owned qualification directory.
Temporary sampling/admission probes are removed. The retained candidate SQL
quorum nodes' data volumes are Docker local named volumes on VM-local ext4;
host directory sharing does not explain that case. This read-only mount audit
measures no latency and makes no claim about every campaign volume.

No application throughput or latency gain is established by these functional
passes. The next priorities remain the source-matched constrained SQL/KV/Queue
comparison with complete identity/body/settlement evidence; the removed-member
protocol cut and remaining authority/read-consistency faults; sustainable shared
physical reclamation and once-only capacity settlement; then repeated movement,
fairness, wake/recovery and combined feature/backend/crash qualification.
No complete OPT/CG gate is promoted.


### Main-173 integration and archive cancellation progress

Merge `96b4bcb2` includes main `189edd9a` (#173 Blob providers and retained
external-payload ownership). Conflict resolution keeps the captured canonical
configuration and every new payload adoption/pinning check. The archive target
retains both physical-owner and new payload/disaster tests; checkpoint enrollment
supports the real fault filesystem and optional payload dependencies together.
Named-binding/EventStream and Blob/payload/disaster harness selections are all
retained with the recorded matching compiler settings.

At `1d0c5e0f` (3,425 compiler inputs), 14 native commands pass: original automatic
handoff and child discovery, the two-image group-inspection regression, existing
node-inspection controls, six archive controls, physical archive ownership,
external-payload recovery and fenced filesystem recovery. The five-case RustFS
command remains **FAILED**: child selection, external-payload recovery and
filesystem disaster pass; extent handoff and archive physical ownership fail.
The handoff stops in `Repairing` at scan revision 51 with two blocked histories
and a rejected result; its cause remains open. The original backend is unchanged
and ready, and command owners join. The initial strict all-target Clippy stops
at an execution-fixture constructor missing #173's new filesystem mode; restart
now carries the previous voter's actual mode forward. Broader checks remain due.

The archive-owner failure is a distinct fixture progress error. After its first
50-ms poll, the upload can still await earlier async I/O. Waiting only for the
physical-read notification then parks the upload itself. A diagnostic preserves
the failed five-second result, then polls the same upload for at most 500 ms:
the held file read is immediately reached. A deterministic 100-ms startup case
reproduces the failure with the in-memory backend, before correction. Its real
disk is retained after joined shutdown.

The fixture now continues polling that same original upload alongside the
notification until the actual held read is reached. Both the 50-ms window and
original five-second deadline remain, and every physical pin, exact charge,
cancellation, collection and independent live-cut recovery assertion remains.
The original native case and delayed-start regression pass in both image forms
(1.56 and 1.73 seconds); the original RustFS case passes both forms in 2.16
seconds. This changes no production code and does not repair the separate
handoff rejection. All temporary archive probes are removed. Future archive-owner
failures retain the real disk until shutdown joins instead of deleting it on an
early return. The two earlier failing archive-owner commands retain their logs;
the first did not retain its default temporary disk.

Records are `main173-native-controls-01`, `main173-rustfs-controls-01`,
`main173-qa-01`, `main173-archive-owner-rustfs-probe-01`,
`main173-archive-owner-before-01`, `main173-archive-owner-fix-01` and
`main173-archive-owner-rustfs-fix-01` in the checkout-owned qualification directory.
Python tooling runs 188 tests with one existing skip; native dependencies cover
496 packages without a JavaScript engine, and documentation checks pass. These
are component/fixture results. The combined RustFS command and optimization,
primitive, performance and release acceptance remain open.

### Retained source boot during Seeding

The remaining automatic handoff rejection reproduces natively at diagnostic
source `9f3e9969`, while the same RustFS command passes. Its two blocked histories
form a dependency: the admitted balance lacks terminal settlement, while another
repair cannot admit during topology cancellation. Read-only postmortem of the
earlier failed RustFS Root 51 identifies the elected Seeding leader as an original
source voter on its qualified retained engine under a newer boot. Execution
previously accepted such a source only in Joint or Complete. Other voters cannot
substitute as a fresh seed source while that retained voter is leader.

A regression restarts every original source during committed Seeding, rejects
each source before capacity requalification and preserves the original target,
permit and reservation. Before correction, source `bff90ea4` exhausts the same
eight reconciliation turns with `Unconfirmed` in the first peer-seed profile.
All four failed disks are retained after joined shutdown. The correction permits
the qualified retained source to resume the exact consumed Seeding permit and
promote the original learner. Preserved source-boot lineage, current rooted
capacity, fresh quorum membership, exact permit/sequence, authenticated seed grant,
current source process and original target boot remain required. It changes no
resource limit, deadline, mutation identity, cancellation guard or settlement
credit.

At production source `00958495`, the original two-image native handoff and
selection pass in 60.74 and 25.99 seconds. The other 12 original inspection,
archive, payload and disaster controls also pass. The new source-restart and
existing Joint-source tests instead fail later in their data audit: it scans an
already removed voter before the current voters. A bounded failure-only probe
confirms zero pending rows, completed membership sequence 1, fresh stable RF3 on
the replacement and survivors, and `NotFound` only for the removed runtime.
Stage context identifies the final voter lookup in both failures. These commands
retain their failed status.

The restart tests now select audit hosts from the independently verified current
membership. A missing current voter still fails. Original receipt replay, S3
durability, SQL reads/writes, negative boot checks and eight-turn limits remain;
failed restart disks are kept after owners join. Final source `18319f85` differs
from `00958495` only in this test module and the component README. All production
Rust and the original transition/archive tests are byte-identical. The native
Seeding, Joint-source and Joint-target restart tests pass in 43.70, 16.77 and
12.37 seconds. Eight RustFS commands pass: those three restart controls,
automatic handoff, child selection, physical archive ownership, external-payload
recovery and fenced filesystem recovery. The original two-image handoff and
selection finish in 84.00 and 39.98 seconds. Compiler inputs remain unchanged,
all owners join, and the original 1-vCPU/2-GiB RustFS identity, limits and HTTP 200
readiness remain intact. The voters are host-native fixtures, separate from
the three-container performance comparison. Format/layout, dependency,
documentation and 21 selected benchmark-ledger tests pass. Broader verification
is recorded separately below.

The first follow-up Clippy command stops before runtime tests because the
earlier archive fixture uses forbidden `tokio::select!`. It now uses the required
execution-domain `select_biased!` with an explicit physical-read priority reason;
the branches and original timeout remain. At `4772bc9b`, both native archive-owner
tests pass in both image forms, and the original RustFS case passes in 2.08
seconds. Only this test selector differs from `18319f85`; production is identical.
A second strict command stops on an unused filesystem wrapper in the execution
target. The two redundant checkpoint wrappers are removed: default enrollment
passes `DirectFileSystem`/no payload, archive ownership passes its actual fault
filesystem/no payload, and payload recovery passes `DirectFileSystem`/the exact
reference. The shared enrollment body is byte-identical. At `f4505de5`, all 14
owning native checkpoint/archive commands pass. The three affected RustFS
archive-owner, external-payload and filesystem-disaster commands also pass, with
joined owners and unchanged backend identity, limits and readiness. The two
earlier failed strict commands ran no library or primitive tests and remain failed.

On that same frozen source, strict all-target Clippy passes in 376.06 seconds.
All six selected current-main primitive commands pass: ordinary SQL (61.85 s),
ordinary KV (109.89 s), warm KV source proof (33.74 s), Queue callbacks (140.79 s),
four-in-flight Queue (74.66 s) and Blob providers (40.64 s). They retain their
original profiles, deadlines, assertions and request identities. This is selected
native compatibility, not the whole primitive target or a throughput measurement.

The first complete library run remains **FAILED**: 1,191 passed, one park/wake
test returns `Closed`, and two existing ignores. The exact failed case passes
three isolated reruns and 100 further unchanged attempts. After all checkout-owned
test and compiler owners join, a process snapshot finds no relevant compiler or
test commands; the same executable's full 1,194-case inventory then passes with
**1,192 passed, zero failed and two existing ignores**, in 94.77 seconds. No source,
assertion, timeout or ignore changes separate those runs. The original park/wake
failure is unreproduced, not causally explained or claimed fixed. The failed
broader command is not relabeled. These observations establish no sustained host
isolation or application speedup.

Records include `main173-repair-probe-01`, `main173-repair-rustfs-probe-01`,
`main173-rustfs-catalog-01`, `main173-source-boots-01`,
`main173-seeding-before-01`, `main173-seeding-fix-01` and
`main173-seeding-audit-probe-01`, followed by `main173-seeding-final-01`,
`main173-seeding-rustfs-final-01` and `main173-seeding-checks-01` in the
checkout-owned qualification directory. The later records are
`main173-seeding-qa-01`, `main173-archive-select-final-01`,
`main173-archive-select-rustfs-final-01`, `main173-seeding-qa-02`,
`main173-seeding-qa-03`, `main173-checkpoint-final-01`,
`main173-checkpoint-rustfs-final-01`, `main173-park-repro-01`,
`main173-park-repro-02` and `main173-library-final-01`.
This addresses recovery progress, not a measured SQL/KV/Queue throughput or
latency gain. No full OPT/CG acceptance is promoted.

### Current-source removed-member recheck

At unchanged compiler manifest `f4505de5` and source `e9509a2f`, the original
removed-member authority oracle passes natively in 55.34 seconds and with real
RustFS in 72.75 seconds. Each command emits all six SDK/native HTTP/Axum and
materialized/extent evidence profiles. An actually removed member remains outside
the fresh stable configuration. The intended authenticated faulty-peer command
is reached, its complete pre-admission refusal preserves the callback count
(1 to 1) and SQL-stage count (3 to 3), and retry/replay of the same original
command produces the second callback exactly once with S3 durability. Membership,
term and range-directory assertions remain unchanged.

No production, fixture, assertion, deadline or request-identity changes are made
for this rerun. The earlier pre-current-main failure is unreproduced; no isolated
cause is established. The original failed records remain failed. Both commands
join their owners and retain unchanged compiler inputs; the original RustFS
identity, limits and readiness remain intact. Linux control compilation overlaps
these functional runs, so their durations establish no isolated performance
effect. Records are `main173-removed-member-01` and
`main173-removed-member-rustfs-01` in the checkout-owned qualification directory.
This passes the scoped oracle on the current source, without accepting the
remaining public authority/read-consistency cuts or a complete OPT package.

## Direct-routing candidate

The current candidate adds a leader index beside the transport's existing route
registry. Each entry charges 4 KiB and one item to the outgoing control bank.
Its capacity is the minimum of the route limit and one quarter of the parent's
byte/item capacities. Saturation evicts advisory metadata or falls back to
discovery; no parent limit is enlarged. Entries retain weak route references,
and shutdown clears them independently of physical query/SQL ownership.

Managed entries bind a verified quorum configuration digest/revision and legal
incoming voters. Unmanaged startup can bind the immutable initial configuration
as historical metadata; a response must match it exactly, and it grants no
current membership or startup permission. Metadata changes require new verified
configuration rather than learning a peer/address from advice.

Advice has a configuration-specific term floor, sticky same-term conflicts and
monotonic age measured from the physical attempt's admission. Its lifetime is
bounded by the existing transport I/O timeout. Re-observing unchanged membership
does not renew that age. Every request captures a cache generation; eviction,
replacement, endpoint change and trust fencing prevent late replies from updating
the replacement entry. Enrolled endpoint changes preserve verified membership
while discarding advice. Control-bank snapshots include these charged bytes/items.

Warm ingress-only commands use the cached leader directly. A proven pre-admission
NotLeader response may repair advice and retry the same invocation, original
inputs, command ID, deadline and finite bank. Unknown outcomes remain ambiguous.
Incoming commands use the actual applied local manifest instead of another range
lookup; the existing Raft execution authority still checks readiness, cell/history,
route, seal/activation/recovery state and ownership before SQL and proposal.

The new `native_cell_groups_routing_optimization` target's
`cached_leader_direct_write` passed: ordinary SDK plus native/Axum ingress, both
image modes, exact Quorum/S3 writes and replays, callback counts and all-replica
values. Each measured warm invocation had one command RPC, zero probes, zero
command-directory lookups, zero demand membership refreshes and one advice hit.
Background membership observations have a separate counter and were observed
while the warm path remained direct. This is a routing-work oracle, not a
throughput or latency comparison.

`election_repairs_leader_only` and `metadata_outage_on_cached_path` passed in
the final combined public suite. The former checks authenticated
pre-admission handoff, stopped-leader uncertainty, explicit same-ID reconciliation
and exact original result with the current covering cell version/witness. A prior
receipt is not required to carry its old covering version after later mutations.

The outage case denies real range, Fleet and node-directory reads after warmup.
Quorum requests exercise the direct path. The archive publisher independently
requires a current catalog birth check, so this implementation cannot promise S3
progress during that cut: the case requires no S3 acknowledgement, then restores
metadata and reconciles the same ID with S3 proof and no duplicate callback.
Removing the publisher's revalidation would need its own authority proof.
The outage case does not close RO-07's broader expiry/revocation and unknown-route
qualification. Cache race/quota tests and strict focused Clippy also passed.
OPT-01 and combined release qualification remain open.

## Candidate checks

The table below qualifies the preceding direct-routing checkpoint. The newer
interval-repair candidate described next has separate checks pending.

| Check | Current result and scope |
| --- | --- |
| Cache unit cases | 8 passed, including quota plateau, eviction/endpoint/trust generation races, term/configuration/role checks, original-attempt expiry and unchanged-configuration route reinstallation preserving the term floor/conflict |
| Authenticated query/command wire cases | 24 passed on the cache build; malformed shapes, outcome semantics and Quorum/S3 downgrade refusal |
| Performance-registry unit cases | 5 passed |
| `native_cell_groups_execution`, `shared_remote_` and `shared_nonmember_` | 14 passed on the new cache/direct-destination implementation; cancelled/expired work, revocation, replaced-route replies, exact receipts and previously uncertain rejection remain covered |
| Public routing suite | Final combined run: 3 passed in 80.96 seconds, each covering both image modes. Direct SDK/native/Axum writes and replays; live leader handoff with one pre-admission rejection, two RPCs, zero probes/directory lookups/demand member refreshes; stopped-leader uncertainty and explicit same-ID reconciliation; real metadata outage with direct Quorum progress and correct S3 refusal/reconciliation. No deadline/capacity increase or warning suppression. |
| Source-seal and child-publication boundaries | 3 passed: `seal_authorization_cancel_race`, `source_seal_blocks_delayed_work`, `child_requires_install_and_publication` on the new implementation |
| S3 physical delivery ownership | `s3_remote_slow_delivery_retains_proofs_and_bounds` passed in 44.29 seconds: both image modes, command/receipt replies and drain/disconnect/revocation/timeout cuts, original quotas and actual TLS/filesystem ownership. In-memory conditional object store; no RustFS qualification. |
| Strict focused Clippy | `cargo +1.98.1 clippy --locked -p cellar -p cellar-logic --lib --test native_cell_groups_routing_optimization --test native_cell_groups_execution -- -D warnings` passed. The first candidate run's `field_reassign_with_default` was fixed with a complete initializer. The separate all-target failure recorded below remains an open gate. |
| Module layout and format check | Final checks passed for 2,138 Rust files using the built `cellar-xtask` (`layout` and Rust 1.98.1 `fmt --check`, the Cargo aliases' implementation) |
| Native dependencies, docs and plan checkers | Passed: 496 reachable native-only packages; 117 documents / 2,451 references; 42 Python checker tests. These do not establish runtime or backend qualification. |

## Interval-repair candidate

Ordinary directory publication now retains its exact canonical mutation bytes at
`changes/<request-digest>` before the selecting journal/root CAS. The existing
journal already binds that digest. This adds proof storage without changing a
marker or retaining a predecessor wire decoder. Unavailable proof selects full
cache invalidation rather than assuming an unchanged range.

Repair verifies at most four intervening publications, sixteen parent/child
routes per publication and 128 immutable pages. Namespace/schema, identities,
epochs, complete interval coverage, exact selected parents/children and the
predecessor chain must check. A bounded ordered Merkle comparison also proves
that nothing outside each declared interval changed; claimed parents/children
alone are insufficient. Verified overlaps are removed atomically. Unaffected
entries retain the root and revision where they were actually checked.

Each asynchronous lookup captures a weak cache generation before metadata I/O.
Insertion and proof completion recheck it under the cache lock. Changed recovery
or import fences participate independently of routing revision; the fence digest
is streamed without an extra serialized buffer. A changed fence or unverifiable
chain clears all entries. Stable fence observations can retain newly checked
unblocked routes. Session revision floors survive invalidation and cannot be
satisfied by relabeling an older unaffected observation.

Cancellation review found that the first candidate advanced its generation only
after proof completion. The updated candidate records the verified head and
retires the old generation before target lookup or proof I/O. Earlier rows remain
ineligible while proof is pending. A drop guard preserves that negative boundary
and removes unverified rows on timeout/cancellation; newly verified current rows
survive. A second publication during pending repair selects full invalidation.
The lookup receives the exact generation from the observation's locked commit,
rather than sampling a possibly newer generation after I/O. A new deadline case
holds an old lookup, expires a newer publication's proof read, then requires the
old lookup to refuse and a fresh lookup to select the child. All 30 catalog cases,
including that deadline case, passed on the interval-observation build.

One active repair borrows 512 KiB and an item from the original invocation bank.
Admission temporarily retains one maximum normal invocation reservation before
admitting the proof, then returns that headroom for business progress. A one-slot
or byte-tight parent falls back; it cannot be monopolized by repair. The cache
envelope and parent capacities are unchanged. Busy proof ownership,
bank exhaustion, missing/corrupt records, excessive chain distance or page work
select full invalidation. The original invocation deadline also bounds proof I/O.
No new per-group worker or unbounded invalidation history is introduced.

The interval-observation build passed all 30 catalog cases. The 17 selected
execution regressions passed (`shared_remote_`, `shared_nonmember_`, cached
catalog-outage authority and both controller replica-routing variants), as did
three source-seal/child-publication cases and the S3 slow-delivery ownership case
(49.10 seconds, both image modes; in-memory conditional store). Public election
and outage cases passed together. Direct-write startup timed out in that combined
run before the test exercised its routing oracle; its unchanged isolated rerun
passed in 33.74 seconds. No timeout or capacity was increased. The emitted interval library test executable
also passed all 40 selected private cases: three interval proof/fence/headroom,
eight leader-cache, 24 authenticated wire and five performance-registry cases.
That makes 94 selected interval-build cases, with the startup failure and isolated
rerun recorded separately. Logs are under `interval-observation-*`. Final-source
Clippy remains pending; these runtime results precede the coalesced-discovery
changes described below.

These metadata-boundary cases do not replace RO-05/06's public live split/recovery
and original cell-version checks or the remaining OPT-01 negative-path work.
Recovery qualification must additionally verify propagation of namespace fences
to dependent transport membership/advice owners; the cache proof alone does not
establish that integration.

## Coalesced-discovery candidate

Demand membership refresh now selects an exact history plus weak transport-route
and verified-configuration generation. Concurrent requests for that key share one
operation in the existing joined directory worker. Route/configuration replacement
selects a different key; no allocation identity is reused while a key retains its
weak references. Completed work is removed from the index instead of becoming a
reusable authority or freshness result.

The first request's existing 32-KiB/item reservation covers the physical owner,
first waiter, index slot and result. Every additional waiter charges 4 KiB and an
item to the same original control bank. The shared reservation survives all caller
cancellations and remains charged through undelivered results. Each waiter keeps
its original absolute deadline; joining cannot extend the first physical owner's
deadline. Queued close/drop publishes a typed Closed result. The directory drains
queued reservations before releasing its registered service lease; entered work
still finishes before that lease is released. There is no new task per group or
increase to a parent bank.

Two counters distinguish `cell_membership_discovery_owners` and
`cell_membership_refresh_joins` from the existing demand counter. Six private cases
cover sharing/cancellation, deadlines, actual byte/item saturation, the minimum
managed control envelope, generation/history isolation, completion with delayed
delivery and queue closure. The new public `refresh_storm_is_bounded` candidate
uses a cold ingress and a held real birth-record read, SDK/native and SDK/Axum
pairs, original controller-bank saturation, cancelled SDK waiters, exact same-ID
reconciliation and separate held-discovery close cuts in both image modes.
Source review found that the original controller limit is two commands, tighter
than the dispatcher's eight invocations. The first eight-waiter test candidate
could not reach discovery in that envelope; it was corrected before runtime
qualification without increasing either limit. The case checks both HTTP adapters'
typed overload refusal while the two admitted requests share one discovery owner.

The full discovery build and focused Clippy passed before the public pair
correction; the corrected public target also built successfully. Six
coalesced-owner cases and the existing 40 private proof/cache/wire/performance
cases then passed in the serial driver.

The paired public run proved two demands, one owner and one join, typed SDK and
both HTTP overload refusals, exact reconciliation and cancelled first-caller
retention for both image modes. The close scenario incorrectly requested the
Raft service close while a full HTTP application request was still active, then
expected a complete node-drain proof. Both the original and a narrowed repeated
reproduction correctly returned conservative `Unobserved`. Moving the observation
after the controller join did not change that result; that production change was
reverted. Held application work still makes the proof unavailable.

The candidate now drives public `Node.shutdown(Preserve)` under an independently
owned test task, cancels only an observing waiter, and keeps the actual node drain
alive. It requires admitted work to finish, exact same-ID reconciliation at a live
voter, complete physical control release, and matching first/repeated close reports
with a drained local-producer proof. The narrowed corrected case passed both
image modes in 27.36 seconds. Temporary diagnostic selection and tagged logging
were removed, and the complete SDK/native/Axum matrix was restored.

The final build, focused Clippy, format/layout (2,143 Rust files), native
dependency check (496 packages) and docs check passed. All 46 selected private
cases passed. The first full public attempt completed six refresh scenarios,
then failed enrollment in the next fixture while Clippy was still active.
An unchanged serial rerun after build and Clippy completion failed in the second
fixture's group startup. Original five-second request/fixture bounds were kept;
failed logs and disks remain under `node-drain-final-*` and
`node-drain-static-ready-*`. RO-09 and the combined 101-case run remain failed,
with native startup stability and broader busy-peer/fault/load cuts still open.
This candidate is being checkpointed for the requested constrained Linux tests,
not accepted as a complete optimization package.

The final runtime driver waits for its complete successful build, flushes build
writes, requires exact test selections and executes all selected cases serially.
The earlier interval catalog/public run overlapped independent fixtures,
contrary to the qualification guide; its startup failure and isolated rerun
remain recorded above.

## Transient-refusal candidate

Source `0cef5350` separates a probe set containing only overload or valid
no-leader observations from the existing membership-discovery fallback. Such
observations do not establish a changed placement. Commands, pre-submission
enrollment and receipt reads back off using the controller's configured cadence
and retry the cached route within the original session. The shared destination
selector keeps its existing return contract for all three operations.

An authenticated command rejection for overload may retry only while the
invocation is unambiguous. A previously unknown outcome remains sticky. The
retry retains the command ID, captured inputs, fingerprint, original absolute
deadline, attempt bank and physical owner. Backoff uses the owning execution
domain, clamps to that deadline and refuses after owner closure. No parent bank,
worker or request budget is added. IO/stale-member fallback still needs the
remaining membership/endpoint/range scope separation; this change does not close
OPT-01.

The new public `busy_leader_keeps_placement` candidate retains two real destination
commands waiting for S3 proof, observes an authenticated rejection at an
ingress-only origin, releases archive work, and requires the same invocation and
exact replay with once-only callback entry. It requires zero probes, command
directory lookups and demand membership refreshes for that retry. Three private
cases cover probe-failure scope, the original deadline and closed-owner refusal.
Focused Clippy passed in the isolated source checkout. The source-pinned CI-profile
run at `729b006f` subsequently passed focused Clippy, format/layout, native
dependencies, docs, all 42 plan tests and 49 selected private Rust cases. Its new
public overload case failed and stopped the remaining serial campaign.

Review found that this new fixture submitted its two concurrent destination
commands with one client identity, violating the documented per-client sequence
contract. Setup correction `729b006f` gives each concurrent command its own client
and preserves exact replay of the routed command. All overload, routing-work,
durability and callback-count assertions are unchanged. The first CI-profile
build at `0cef5350` completed, but its validation was invalidated before any
runtime test; the in-flight Clippy process was cancelled. The corrected source was
used for the `729b006f` campaign above. No invalidated run is counted as a pass.

The next diagnostic showed that one of the two held S3 commands completed early
with SQL `Busy`: both targeted the same SQLite cell. The intended node-level
capacity hold therefore did not survive until the incoming request. That request
was admitted and returned an ambiguous `Unavailable`, correctly preserving its
identity and refusing automatic replay. The corrected fixture uses two independent
cells, establishes initial S3 coverage, and explicitly refuses any held call that
completes before archive release. It keeps the same two-slot node bank and five-second
request deadline; callbacks and exact per-cell results remain checked.

With a valid capacity hold, all four original request tickets were consumed by
authenticated overload responses in approximately fifteen milliseconds. S3 work
completed about 890 milliseconds after release. The controller's five-millisecond
poll cadence alone did not give that work time to free capacity. Retry spacing
now divides the original remaining window geometrically across the remaining
tickets and final execution, with the configured cadence as its minimum. Each
retry still consumes an original ticket and every sleep clamps to the original
absolute deadline. No retry count, timeout or admission limit is increased.
This follows the bounded exponential pacing principle in the
[TiKV Rust client](https://github.com/tikv/client-rust/blob/master/src/backoff.rs),
while deriving Cellar's spacing from its own existing invocation bounds.

The paced public case passed both modes in 29.79 seconds before diagnostic cleanup.
Materialized images observed three proven refusals before success; extents observed
two. Both preserved the command identity, one callback, exact replay and zero
probes, directory lookups and membership refreshes. Temporary instrumentation was
removed. A fourth private case checks time reserved for progress and final
execution without deadline renewal.

Final source `0bf9571506fe5207215cb8055cea739c3eeacd06` passed 83 selected
regressions under the SDK CI test profile after all builds and static checks
completed. This includes the complete SDK/native and SDK/Axum refresh-and-close
matrix in both storage modes, the public overload/direct/election/catalog-outage
cases, and selected routing, enrollment, receipt and wire regressions. Focused
Clippy, formatting, layout, dependency isolation, documentation and all 42 plan
tests passed. The source manifest digest is
`10160068de81411681829c06df350994bb023acaaf6768e4513cd6709ea18644`.
Source, test binaries, operator CLI and logs are retained outside the repository
at `qualification/native-paced-routing-0bf95715` under this checkout's workspace
target. Its `record.json` SHA-256 is
`6a912e7262f6aad8cd3b170ec71fbb2d254ed7424726b80ee3d81d1e9a3f0478`;
all 15 recorded artifact hashes were rechecked. These selected regressions do
not close every RO-09 fault/load cut, any complete OPT package, or performance
and release qualification.

Artifact inspection found the preceding failing native startup runs used
optimization level 0 and debug information level 2. The qualification guide and
SDK CI require test optimization level 1, no debug information, and enabled debug
assertions and overflow checks. The new build and serial driver use those exact
settings, verify the emitted Cargo profiles, wait for all static checks and flush
build writes before fixtures. Original timeouts, assertions and resource limits
remain unchanged. This corrects the reproduction setup; it does not yet establish
the cause of the earlier failures or supersede their failed results.

## Fresh ReadIndex cohort candidate

Already-queued linearizable cell reads may share one newly initiated barrier
for the exact group history, term and applied configuration revision.
Initiation seals the cohort; later arrivals need another barrier. Each logical
read retains its original node/group reservation and absolute deadline, selects
its own coherent target-cell image, and receives its own consumed authority and
pin. Cancelling the first waiter leaves the shared context available to remaining
members. Last-member removal discards correlation state. Term or configuration
changes refuse pending selection. No lease or completed-barrier cache is added.

The first candidate gathered up to 32 adjacent reads per group turn. The
current worker takes the existing queue prefix through its last linearizable
cell read and processes intervening work in FIFO order. Later arrivals cannot
extend that prefix. The cohort stays open while the owner waits for readiness
and seals at the first ReadIndex submission. The existing group and node
admission limits bound the queue, and each read keeps its original deadline.
Separate counters report first submissions, all submissions and joins.

Source manifest
`986cbfd6d2318897b09730fae39e8368819480ab7e8f047ed76dfd56bfe91f43`
passed 33 selected regressions under the SDK CI test profile. These include three
private cohort tests, actual consensus tests in both storage modes for fresh
arrival boundaries, cancellation and independent image pins, selected strong,
follower, source-age, membership and query-lifecycle regressions, and all five
public routing optimization cases. Focused Clippy, format/layout, dependency
isolation, documentation and all 42 plan tests passed. Source patches, binaries
and logs are retained at `qualification/read-cohort-candidate-986cbfd6` under
this checkout's workspace target; its `record.json` SHA-256 is
`afe4f8e1aa01585e4c9a5550fb1c1cba0935fc02ce269dadd040e59ac93b5d6d`.
Four additional actual-consensus term/configuration invalidation cases passed
in both storage modes, alongside eight companion barrier, pin-cleanup and
follower-read cases. The corrected fixture uses the stored manifest's public
membership-revision accessor; the preceding compile-only failure ran no tests
and is retained separately. This 12-case run passed all six static checks at
source manifest
`48c54ec3943737d9a9663f466b3f846684424bda3a11e82225229949642ecaee`.
Its source, executable and logs are retained at
`qualification/read-cohort-invalidation-48c54ec3` under this checkout's workspace
target. Comparing all source inputs with the 33-case campaign confirmed changes
only in five test files; production inputs are identical.
The retained 12-case `record.json` SHA-256 is
`8c4d5e42d2b0bcae0e4345f0e18decdeee17ba8411f61c1104c6cee00ec4eeb2`.
At that checkpoint, replica preference ranking and the public SDK/native/Axum
RO-11 oracle remained open; those component checks did not close OPT-03 or
release qualification.

A focused test reproduced premature sealing when readiness prevented ReadIndex
submission. The same test and the three existing cohort unit cases pass after
the fix. The native fixture now inserts a peer heartbeat response between
queued reads, and the public fixture waits for the actor's queue observation
before releasing its storage sync gate. At source `fa64fda8`, four actual crate
unit cases, all six consensus cohort cases and the public read-preference case
passed. The public cohort case failed during its late Axum arrival.

The late-arrival fixture could hold an engine metadata sync while the selected
leader remained ready. An assertion at source `498e582b` reproduced that boundary.
The filesystem gate now selects a sync while the owner reports a pending Raft
phase. At source manifest
`ffc61abd512b2c0e7d1a96e6b85f7eb07362649de29aa94b56ae64c8391687c5`,
the complete public cohort case passed both image modes across SDK, native HTTP
and Axum. Its independent request ledger accepted 406 events. The runtime log
and ledger are retained at `qualification/pending-late-ffc61abd` under this
checkout's workspace target. Formatting and combined qualification follow this
checkpoint. Original deadlines, read quotas and barrier assertions still apply.

The final focused read checkpoint at source manifest
`f72af1e57d4409d19fe69142fd9c0c3cea256194a9412515a125fd90baffe2fc`
uses Cellar's selector in the sync fixture and passed all twelve selected Rust
cases. These comprise four cohort unit cases, six actual consensus cases and
the public cohort and replica-preference cases. Both public ledgers passed
with 406 and 204 events. Focused Clippy, formatting, module layout, native
dependency isolation and documentation validation passed. Records are retained
at `qualification/cohort-queue-f72af1e5` and
`qualification/cohort-static-f72af1e5` under this checkout's workspace target.
Combined-source and broader RO-10 qualification remain open.

## Public read cohort verification candidate

The ordinary SDK, native HTTP and Axum candidate holds actual Raft filesystem
sync and actual application query callbacks in each storage mode. It requires
two pre-initiation reads to share one barrier, later arrivals to initiate another,
and independently retained image pins and original two-slot controller admission
after an entered caller is cancelled. Signed observations must match each cell's
own version under the registered namespace schema. Higher-term handoff cuts
require refusal before query callback entry and exact S3 command replay.

This public drill exposed a production handoff failure: the former leader relayed
another member's ReadIndex request through raft-rs with the original sender.
Cellar's authenticated transport correctly refused that nonlocal identity, and
the controller closed the node. The incoming owner now validates the message,
then refuses peer ReadIndex at a non-leader before RawNode can relay it. Local
follower reads still create and forward their own authenticated context. The
transport identity check is unchanged. The new owner-boundary regression failed
on the preceding source and passed in both storage modes with the guard.

The public sharing/later-arrival/cancellation/term matrix passed its development
smoke in 28.46 seconds with all three ingress paths and both storage modes. The
schema-token check initially used the raw example seed and was corrected to the
actual registered namespace digest. Temporary shutdown diagnostics were removed.
The cleaned source passed 40 selected native cases: read/cohort, membership,
seed, all six public routing cases, and all five park/wake cases. Focused Clippy,
format/layout, native dependency isolation, documentation and all 42 plan tests
passed under the SDK CI test profile. A callback type alias resolved the preceding
Clippy-only failure; no runtime cases ran in that failed campaign and no lint was
suppressed. Source inputs are pinned by
`c2bbcdd1c342d60808910f76fd188f59f1b7ce93550a95bed877c126739406f9`.
Tested executables, operator CLI, source patches and logs are retained at
`qualification/read-relay-c2bbcdd1` under this checkout's workspace target;
its `record.json` SHA-256 is
`f5fa796524142d63894f8e70f77ef26b251c3bc2e28a0a42e477fa169c79e9e6`.
The public configuration change cut and replica preference ranking remain open;
Those partial or component results did not close RO-11 or OPT-03. The first
public configuration candidate held the leader's configuration fsync, but that
did not force the new configuration to overtake follower read selection. Both
reads could validly complete first. The failed development runs are retained at
`qualification/public-config-red-0f7dab94` and
`qualification/public-config-diagnosis-55096f6c`; they qualify no configuration
invalidation behavior. Its proposed production counter and second filesystem
provider were removed.

The replacement candidate holds actual follower persistence/apply, queues the
original two public reads, commits a rooted learner admission through the other
voters, and admits a later S3 mutation's data before releasing apply. It requires
one fresh shared barrier, refusal before callback entry, a changed local
configuration with the same term, independently verified fresh cell versions,
unchanged original quotas, and the exact pending movement debit. Its optimized
Rust 1.98.1 development smoke passed in 46.044 seconds, with both storage modes
and all three public ingress paths. Source inputs are pinned by
`7c741c89be9724f8b95236c36e8f4f2223e865c0cd780c75bac5f9374af5fbae`;
`qualification/public-config-apply-smoke-7c741c89` retains the executable, sources
and logs. This is a development result, not the combined regression campaign.
An initial diagnostic rebuild under the shell's Rust 1.99.1 default was stopped
before runtime and supplies no qualification. The complete request/evidence
ledger and replica preference ranking remain open. The expanded candidate adds
a both-completed shared cohort, explicit HTTP query identities, a later arrival
while a follower barrier remains pending, and a structured
request/result/cancellation/receipt and physical-owner ledger. Malformed,
duplicate and command-side query identity headers refuse before ReadIndex or
application callback entry. Its development smoke passed in 68.791 seconds.
The independent ledger audit passed 394 events: 76 valid queries, 50 verified
results, 20 generation refusals, six joined cancellations, 34 S3 receipt
observations, and 24 rejected malformed or misplaced identity requests.
Source inputs are pinned by
`8c4fb2f62d433c4a4e3d8afc57831eb299ce0a4035928cd619bc35cf69f558f7`;
`qualification/public-read-ledger-smoke-8c4fb2f6` retains that smoke.

The combined campaign passed focused Clippy, format/layout, dependency isolation,
documentation and all 42 plan tests. It passed 31 native cases, then
`election_repairs_leader_only` failed during initial group readiness with
`Runtime(DeadlineExceeded)`, before reaching its election oracle. The remaining
eight cases did not run. Replaying the exact election executable under the same
source passed in 26.816 seconds; the intermittent startup cause remains
unresolved. The failed campaign and replay are retained at
`qualification/public-read-ledger-election-startup-failed-8c4fb2f6`.
This is not a passing 40-case campaign or release qualification. The next
candidate retains startup-failure disks and records voter/group/resource state
before joined shutdown; it preserves the original deadline and assertions.

The diagnostic candidate preserves the original eager startup admission. On
failure it captures every voter's resource and performance counters before any
awaited inspection, then records later group observations, joins shutdown and
retains the disks. Its combined run passed 32 cases, including the election
case, before `metadata_outage_on_cached_path` failed at the next extents-cluster
startup. The materialized outage scenario had completed. Passive snapshots had
zero read barriers and pending persistence; sequential group observations later
showed a ready leader. Another checkout's formatter was observed after the
failure, but that sample does not establish overlap or causality. The failure,
executables and disks remain retained at
`qualification/public-read-ledger-startup-state-failed-64b27b95`.

The unchanged candidate subsequently passed all 40 selected native cases with
Rust 1.98.1 and the CI test profile. Each case began after a five-second sampled
quiet window and `sync`; the runner sampled competing compiler, formatter and
native fixtures every 100 ms. Three passing attempts overlapped with observed
external work and remain explicitly unqualified. Their later attempts passed
without observed overlap. An uninterfered failure would stop the campaign;
request deadlines, admission caps and assertions were unchanged. Sampling cannot
exclude short unobserved activity, and the intermittent startup cause remains
unresolved; this pass does not erase either earlier failure.

Focused Clippy, formatting, layout, native dependency isolation, documentation
and all 42 plan tests passed at those exact source inputs. The independent
public-read audit also passed all 394 events. The SDK, native HTTP and Axum
RO-11 oracle now covers two completed prequeued queries, cancelled waiters,
later arrivals during pending and completed barriers, term changes and a real
learner configuration change in both image modes. It verifies independent
image pins, exact versions and S3 receipts, generation refusals before callback
entry, unchanged two-slot admission, and joined resource drainage. The learner
cut retains its exact pending movement debit; it does not qualify completed
repair or reclaimed capacity.

The source-input digest is
`64b27b9500ee75f666fcd762077cc0e0dc18a5018f9eb7d0ffa14806700452aa`.
The executables, source overlay, all 43 attempts, checker logs, evidence and
auditor are retained at `qualification/public-read-ledger-isolated-64b27b95`
under the checkout's workspace target. Its `record.json` SHA-256 is
`84af9fd5f9afd698a5f679520441c484a6df3058722dfa92d2125dde07cf329b`.
This closes the selected public RO-11 functional oracle. RustFS verification
was pending at that native checkpoint and subsequently passed as recorded below.
Replica preference ranking, RO-10, broader OPT-03 acceptance and release
qualification remain open.

## Consistency-aware read candidate

Automatic linearizable reads prefer the local owner's observed leader, or
authenticated, configuration/boot/age-bound leader advice on an ingress-only
node. Other modes prefer local execution. A bounded list of cached alternatives
handles read unavailability, pressure or a ticket timeout before requiring route
repair. Selection reads one immutable transport view; tracking tried member IDs
prevents changed advice order from skipping an eligible peer. Every destination
still consumes the requested read authority and verifies the original minimum.
Explicit local and replica plans retain their ordering and bounds. Automatic
attempts divide the remaining original timeout among remaining tickets; no
deadline, attempt bank, controller quota, cache or worker is added.

The initial public lag cut denied metadata to every voter. The first local
attempt expired behind actual follower persistence, both remote candidates
returned `Unavailable` at the required destination catalog check, and the origin
then tried range repair. The diagnostic is retained at
`qualification/read-preferences-lag-diagnosis-f7cc3cd5`. A manifest-only
destination experiment was rejected by the regression below. Incoming reads
still resolve their destination against the current catalog and the actual read
owner still consumes the requested proof. The incoming session is clamped to
the receiver's captured RPC deadline, including time spent queued. Temporary
diagnostic probes were removed.

The unchanged production baseline first failed the new public leader-preference
oracle with member 1 serving a strong read that should prefer observed leader 2;
`qualification/read-preferences-leader-red-17f49fcc` retains that red seam. An
initial candidate failed enrollment before the read oracle because its handoff
setup waited for announcement alone. The setup now requires the actual leader's
readiness and current-term apply within the original five-second wait. The
failure remains at `qualification/read-preferences-setup-notready-faad90c7`.
The first healthy preference matrix passed at
`qualification/read-preferences-healthy-b4eb33c2`. The later metadata failure
and an interrupted attempt remain at
`qualification/read-preferences-lag-metadata-failed-7a84c42e` and
`qualification/read-preferences-interfered-4883d1c1`.

The manifest-only experiment's development matrix passed in 39.792 seconds at source inputs
`eae6b9ee05b3f1e81e32631321809875669a9fb3d10b21171129cca501e1a34b`.
Both image modes exercise SDK, native HTTP and Axum from voter and ingress-only
origins, all four read policies, exact S3 receipt minima, real follower fsync lag
under verified metadata denial, foreign-cell token refusal, and expiry followed
by caller cancellation while leader and follower SQL callbacks retain separate
image pins. The 204 events contain 60 verified results, six joined cancellations
and six foreign-minimum refusals. Each mode checks 36 physical query callbacks
and five command callbacks, unchanged two-slot/64-KiB controller caps, and joined
drainage. The in-memory conditional store supplies the archive backend for this
native matrix. That development pass did not exercise the superseded catalog
route and is not accepted evidence for removing the receiver check.

`qualification/read-preferences-fault-smoke-eae6b9ee` retains that development
checkpoint. The formatted candidate adds explicit metadata-denial counts and an
applied-index observation after the lagged query returns. Strict focused Clippy,
format/layout, dependency isolation, documentation and all 42 plan tests passed.
The broader source-bound campaign at input digest
`b59b054bf26ddf13e409ec0c03adfb65fbe3241788b4829f1a8081a5b17522be`
accepted 37 individual cases before the added
`catalog_routed_queries_preserve_invocation_and_authority` case failed at its
five-second SQL-entry observer, without sampled competing native/compiler work.
`qualification/read-preferences-regression-failed-b59b054b` retains the complete
failed campaign, source overlay and exact binaries. An isolated backtrace replay
reproduced that wait on both this candidate and the retained pre-change
`64b27b95` executable; those differential records remain at
`qualification/read-preferences-catalog-differential-b59b-64b2`. A targeted
precondition assertion confirmed that the held query selected replacement
group 203 at epoch 2 instead of the installed group 3 at epoch 1. That result
remains at `qualification/read-preferences-catalog-precondition-red`.
The fixture now checks cancellation and joins the physical callback before
publishing the replacement. It separately verifies that a subsequent query
with the obsolete hint selects the replacement, with no extra SQL callback.
The original deadlines and ownership assertions are unchanged. Both corrected
image modes passed in the next campaign before the subsequent failure.

The campaign at input digest
`62b5ed57cb29be81ff0812d4e3ef2f36571753a99846c7b258a21b270a0e112a`
accepted 41 individual cases before `shared_remote_queries_bind_cell_versions_and_gateway_owners`
rejected the manifest-only destination experiment. The exact catalog publication
replaced the old route while its physical manifest was still active. The gateway
did not return the required typed refusal. The complete failed campaign is
retained at `qualification/read-preferences-regression-ci2-failed-62b5ed57`
(`record.json` SHA-256 `3aecc7595bf55e4de45ba019e4d7572778b35fbd32f2be2a1fb144db910cdb76`).
The receiver's original catalog check is restored without weakening this test.
The new lag cut scopes metadata denial to the origin's adapter over the shared
store, checks actual origin denial and receiver access, and observes a receiver
range head read on the successful fallback. Both the original superseded-route
refusal and the revised public matrix passed without sampled interference at
`f21fdc4930f832c29a5e1b8c42d13848f34927ec82da9d026ebbfedb26ff8c8e`.
The matrix completed in 40.355 seconds and the independent ledger reconciled
all 204 events. `qualification/read-preferences-origin-smoke-f21fdc49` retains
the pre-format source and both binaries. The broader regression of the formatted
candidate, network partition, RecentSource expiry, full RO-10 and release
qualification remain open. The RustFS/Colima result below remains pinned to its
own source.

## Local container verification

The user explicitly requested local RustFS storage and Colima containers for
nodes. The verified dedicated profile is `cellar-multi-raft-6aff` under the
existing workspace-volume target directory
(8 CPUs, 18 GiB, 100-GiB backing ceiling). Actual VZ paths and the backing file
were checked on that volume. Two initial owned profiles used the default home
because the requested custom directory did not exist; they and their initial
backend were stopped and retained. No other profile was modified or stopped,
and the active Docker context remains unchanged.

The fresh digest-pinned RustFS container on the verified workspace profile passed
all seven signed S3 protocol operations, including conditional create and ETag
compare-and-swap. The local harness's one-CPU/2-GiB/no-additional-swap envelope is
unchanged. Preflight positively matched the Docker and Colima SSH Engine IDs,
cgroup v2, the eight-CPU VM and its workspace backing file.
Credentials and generated runtime state stay outside the repository in the
checkout-private qualification directory.

The managed daemon runner previously hardcoded the default profile/context. Its
reviewed change binds Docker calls to the owned backend's exact profile/context,
accepts an explicit existing Colima home, binds the corresponding Lima home for
guest calls and records the Engine identity. Three configuration/routing tests
passed; the local multi-Raft Python suite passed 31 tests with its one opt-in
Docker configuration case skipped. Review found no change to voter/backend
limits, workload assertions, source-image checks, cleanup inventory or request
budgets. The plan checker initially caught the changed recovery anchor (41
passes, one preflight error). Source checkpoint `4a00fd96` was reviewed, and the
baseline's source revision, recovery digest/excerpt and review note were advanced
together. All 42 checker tests and direct source-anchor preflight now pass; no
checks were weakened. The full `plan.py preflight` CLI remains unavailable in this
fresh checkout because prior accepted tasks reference private ignored evidence
files that are not present; no synthetic replacement evidence was created.

The managed Linux daemon campaign passed at source
`a2bbabae57f1386e25370999c11e9b050903b760`, built with Rust 1.98.1 and the locked
dependency graph. The primary checkout remained clean at that revision throughout
the build and campaign. The runner checked the immutable image, source manifest
and deployed daemon/operator binary digests. The image is
`sha256:65e6ca2d92aa57a42294654e076a27fbbab1fefac191ff5ee1ed406f2714980a`;
its source manifest is
`519fc5131bee8e1fea9fb3a83eea896fe07ebf18a05633290cd4001299b02e82`.

| Image mode | Result | Fixture duration |
| --- | --- | ---: |
| Materialized | Passed | 74.977 seconds |
| Extents | Passed | 71.547 seconds |

Both modes used three separate Linux voter containers, each with its own disk,
one CPU, 3 GiB RAM and zero cgroup swap. The record includes observed cgroup-v2
limits and Docker inventory. The actual daemons acquired RustFS-backed node leases
and authenticated peers. Native and Axum ingress exercised Quorum/S3 receipts,
park/fresh-quorum wake, archive-write outage and exact S3 reconciliation,
leader SIGKILL and successor selection before retained-disk restart, maintenance
stop/return/resume, and quorum-loss read/write refusals. The intentionally killed
container exited 137 in each mode; the four other original/replacement containers
exited zero. Both fixture controllers exited zero. These results retain the
original request, resource and workload limits.

Private artifacts remain in the checkout-specific workspace target directory.
`linux-daemon-build.json` records the successful build and verification;
`managed-daemon-a2bbabae57f1/` retains the summary, mode reports and event ledgers.
RustFS objects, logs and voter disks remain available. Credentials stay outside
source. This campaign used one VM and did not select standby or failed-voter
repair. It does not close the failed native RO-09 run, public transition/fencing
cases, performance gates, independent host/zone resilience or release acceptance.

The current runtime/example source inputs were subsequently verified at committed
checkpoint `1ef5385e97d6fccdc544fb067394b993a3b0dcb1`. Rust 1.98.1 for ARM64
Linux built the daemon and operator CLI from a clean source archive with the
locked dependency graph. The selected Colima profile, Engine identity and
container limits remained the same. The runner independently matched the image's
source manifest and both deployed binary digests before launching voters. The
source-input digest is
`64b27b9500ee75f666fcd762077cc0e0dc18a5018f9eb7d0ffa14806700452aa`,
matching the 40-case native checkpoint above. The immutable image is
`sha256:1b4334a7d084b754182188267ef6c940c29813cf6ada15f2303027bddf047eb3`.

| Image mode | Current-source result | Fixture duration |
| --- | --- | ---: |
| Materialized | Passed | 79.509 seconds |
| Extents | Passed | 93.455 seconds |

Each mode passed all seven signed RustFS protocol operations, three live
RustFS-backed node leases, native/Axum Quorum and S3 SQL, fresh-quorum wake,
archive-write refusal and exact reconciliation, leader SIGKILL, retained-disk
restart, maintenance stop/return/resume and quorum-loss refusal. Each mode
retains five live voter/replacement cgroup samples and a separate joined
SIGKILL observation. Every live sample confirms one CPU, 3 GiB memory and zero
additional swap. The killed container exited 137; four other original/replacement
containers exited zero. Both controllers exited zero and all voter containers
are stopped. RustFS remains healthy and running; voter disks and remote objects
remain available.

`qualification/linux-daemon-1ef5385e97d6/` under the workspace target retains
the source archive, Linux compiler identity, tested binaries, immutable image
identity, build logs, mode reports, event ledgers, resource observations and
artifact checksums. Its `managed-daemon/record.json` SHA-256 is
`f8b757ba1610281ff247086e87780b65024e101c1caeb4f40a08742cf6ff53ab`.
The source archive SHA-256 is
`a294f50e957e01ed6327a09e602f06c781b7b59cf74fac8ef0538daaa433ea9e`.
This closes the selected current-source constrained RustFS/daemon campaign.
It preserves the one-VM limitation and does not select standby, failed-voter
repair, independent failure domains, sustained performance or release gates.
The intermittent host-native startup cause above remains unresolved.

## Validation

The table below records the **foundation checkpoint before the cache candidate**;
it does not automatically qualify newer work. The candidate table above records
58 selected Rust cases, separately from the 42 Python plan-checker cases. Local
logs are under `/tmp/cellar-tikv-routing-*`, `/tmp/cellar-tikv-election-*` and
`/tmp/cellar-tikv-cache-*`; final public results are in
`/tmp/cellar-tikv-routing-suite-final.log`.

Failed initial candidate runs remain diagnostic history: the first combined
public run had two 5-second operator-CLI fixture-startup timeouts; an isolated
election run found the test's incorrect historical-covering-version assertion;
an isolated outage run confirmed the publisher's independent catalog gate for
S3. The final suite passed with the original timeouts and the documented
receipt/authority contracts. Those failed attempts are not additional passes.

The SDK CI toolchain is Rust 1.98.1. Builds use a target directory unique to this
checkout on the Workspace volume. Current local results:

| Check | Result and scope |
| --- | --- |
| `cargo +1.98.1 test --locked -p cellar --lib raft::query_wire:: --no-default-features` | 24 passed, including three new outcome/advice cases, malformed frames and Quorum/S3 downgrade refusal. This checks the wire boundary; subsequent runtime deadline/reservation adjustments still require their own checks. |
| `python3 -m unittest discover -s docs/design/multi-raft/cell-groups -p 'test_*.py'` | 42 passed; plan checker coverage, not runtime acceptance |
| `python3 tools/check_native_dependencies.py` | Passed; 496 reachable native-only packages |
| `python3 tools/docs/check.py` | Passed; 117 documents and 2,451 local references |
| `native_cell_groups_execution` executable, `shared_remote_` and `shared_nonmember_`, one test thread | 14 passed, covering TLS grants, exact receipts, fresh versus previously uncertain rejection, cancelled/expired producers, follower read constraints and stale route replies. The fixture runs both image modes; these are selected runtime checks, not the proposed public RO target. |
| `cargo +1.98.1 clippy --locked -p cellar -p cellar-logic --all-targets -- -D warnings` | Failed on five unused fixture items in unchanged `cell_group_disaster_effects.rs` and `cell_group_rustfs.rs`, compiled by the transitions/archive-controller targets. This gate remains failed; no warning suppression was added. |
| `cargo +1.98.1 clippy --locked -p cellar -p cellar-logic --lib -- -D warnings`; `cargo +1.98.1 clippy --locked -p cellar --test native_cell_groups_execution -- -D warnings` | Both passed; this narrower scope does not replace the failed all-target gate |
| `cargo +1.98.1 format --check`; `cargo +1.98.1 check-layout` | Both passed for 2,134 Rust files |
| `native_cell_groups_archive_controller` executable, `s3_remote_slow_delivery_retains_proofs_and_bounds --exact --test-threads=1` | Passed; both image modes, command/receipt replies and drain/disconnect/revocation/timeout cuts retain physical output ownership and original quotas. Actual TLS/filesystem I/O with an in-memory conditional object store; no RustFS qualification. This build includes the final absolute-deadline constraint and membership scratch reservation. |

The initial default-toolchain compilation failed on the new codec's `GroupId`
representation; the codec now uses its actual 16-byte identity, and the CI-toolchain
wire run above passes. Failed/cancelled initial commands do not count as passes.
Local command logs are under `/tmp/cellar-tikv-*`; they are temporary diagnostic
artifacts, not durable release evidence or a source-pinned qualification bundle.

## Leader-discovery owner and combined qualification

The next OPT-01 candidate shares unknown-leader discovery by captured physical
route, verified configuration generation, logical epoch and birth digest. A
transport task owns the first finite candidate plan and deadline. Every caller
retains its own deadline; cancelling callers does not cancel the physical owner.
Metadata borrows the existing control bank with a quarter-bank sublimit, and
owners use the unchanged transport task limit. Full metadata or task admission
falls back to the original bounded inline discovery. Results leave the index
before delivery, retain their charge until consumed, and revalidate the captured
route/configuration before use. Source manifest `19402216` passed ten focused
ownership cases, two mutation retry cases, the existing leader-cache and refresh
cases, command codecs and performance observations: 52 selected Rust cases in
all. Focused strict Clippy, formatting, layout, native dependency and documentation
checks passed. The qualifier resumed its codec checks against the retained binary
after correcting a nonexistent test selector; the earlier completed results and
original harness error remain recorded. Public probe-storm qualification remains
open.

The combined published revision `6521122c`, source manifest `cebed8f9`, passed its
execution, routing, primitives and storage runtime suites, both public read-ledger
verifiers, and the real RustFS physical segment-credit case. Its scheduler suite
reported ten passing cases and a failure in `control_plane_fanout_bounded`:
after 32 cells formed four groups, enrollment of cell 32 returned a nonambiguous
`Execution(Refused(Unavailable))`. An isolated rerun of the identical binary,
with the original quotas and deadlines, reproduced the same failure at cell 32.
Both runs retained their failed disks. Those failed runs do not establish the full scheduler or repeated-split gate;
the later repair and composed rerun are recorded below. Evidence is
retained in the checkout-specific `integrated-runtime-6521122c` and
`fanout-reproduce-6521122c` qualification directories. Combined strict Clippy also found a duplicate
storage-fault test module. The routing fixtures now reuse the test-root module in
the repair checkout; its focused and composed checks are recorded below.

The fresh Linux Colima/RustFS campaign for `6521122c` passed materialized images
and failed extent images at `resume-maintenance` with an unknown management
outcome. The retained voter logged a lease-maintenance timeout and fenced its
session. An isolated extent rerun of the identical image, using a fresh owned
cluster and the original limits and deadlines, failed at acknowledgement of the
original receipt after archive restoration. Both failures and their process
ledgers remain retained; neither extent run is counted as qualified.


## Retired-parent retry repair

The repeated-split failure was a stale cached parent whose retired peers closed
preflight connections before returning a typed group refusal. All four original
attempts retried that unreachable placement. A repeated wholly unreachable probe
plan now checks its exact history through the existing joined discovery owner.
Only the trusted directory's fenced result requests interval repair; readiness,
busy responses and the first unreachable plan retain their ordinary retry scope.
No application mutation is replayed from an unknown response, and the original
tickets, deadline, control banks and engine slots remain in force.

Repair source manifest `82a17791`, component commit `a7bd8b0c`, passed the original
64-cell repeated-split test in both image modes in 335 seconds. Each mode formed
seven groups while preserving all existing receipt, SQL and reserve assertions.
The trace records the formerly stuck parent becoming typed `StalePlacement` on
its second probe plan, followed by successful enrollment. Focused strict Clippy
for the library, scheduler and routing targets, formatting, layout, native
dependency and documentation checks passed. The shared routing fixtures now reuse
the test-root storage-fault module instead of loading that file twice.

The separate coalesced-leader source manifest `e88191c7` passed 52 selected Rust
unit cases, all seven public routing cases, both public read-ledger verifiers and
focused strict Clippy/static checks with the shared fixture module. These are
component results. Public probe-storm and broader OPT-01 through OPT-08 gates
remain open.


## Composed runtime verification and daemon diagnostics

Published revision `a69acc34`, source manifest `9dafe562`, passed 60 selected
library cases and the complete nonignored execution, scheduler, routing,
primitives and storage suites in the selected campaign. Both public read-ledger
verifiers and the physical segment-credit case against local RustFS passed.
Strict Clippy for the library and six native test targets, formatting, layout,
native dependency and documentation checks passed. The qualifier retained all
seven executables and verified unchanged source and HEAD throughout the run.
Evidence is in the checkout-specific `combined-runtime-a69acc34` directory.
This closes the composed scheduler regression above; it does not establish all
feature modes, the Linux process campaign or OPT-01 through OPT-08 acceptance.
The daemon target was compiled in that campaign, without running it on macOS.

Diagnostic source manifest `703438f6`, Linux component revision `bb1d1965`, adds
an authenticated fixture endpoint for group observations, archive publisher
status and existing resource/performance counters. Only after the original
archive or maintenance assertion fails, the process harness records bounded
snapshots from the voters, then propagates the original error. Snapshots do not
retry a mutation, extend its reconciliation window or establish archive proof.
Strict library/daemon Clippy and static checks passed before committing this
source. Its immutable Linux image passed both materialized and extent modes twice in
fresh three-voter Colima/RustFS clusters. The image is
`sha256:51b8a98add4ff1071f2f8ff198f194501644cb31edf24e3267d7c3afe5232901`.
Both runs retained the original one-CPU/3-GiB voter caps, zero swap and request
and maintenance deadlines. The independently checked ledgers cover original S3
receipt acknowledgement after archive restoration, leader loss/restart,
maintenance resume, park/wake, SQL/receipts and both native/Axum ingress paths.
Evidence is in the Linux checkout's `linux-daemon-bb1d196501ed-diagnostics` and
`linux-daemon-bb1d196501ed-repeat` directories. The earlier failed source and its
disks remain retained; these passes do not establish the cause of those failures.
The scope is three Linux voters with separate cgroups and disks in one VM,
without independent failure-domain, performance or complete OPT acceptance.


## RPC base integration and public probe candidate

The optimization branch includes the RPC checkpoint base through `f1ad8b90` in
merge `c51c8475`. Composed source manifest `cdfb3219` passed strict library/six-target
Clippy and all 60 selected library cases. Its new public probe fixture passed
the first cancellation case but failed the shutdown case because two concurrent
commands used sequences one and two from the same client. Correct `SequenceGap`
refusal exposed that invalid fixture ordering. The candidate now uses two
independent clients at sequence one and preserves each exact ID for replay.
The public oracle and complete composed runtime campaign remain under validation.
No RO or OPT acceptance follows from the failed fixture run.


The corrected-ID candidate `e0e57e6e` again passed strict Clippy and the 60
selected library cases. Its first two public probe scenarios passed; the next
scenario returned typed `Execution(Busy)` during exact-ID replay. The fixture now
waits, within its existing five-second bound, for the actual voter SQL/controller
owners to drain before starting replay. Quiescence only synchronizes the fixture;
the subsequent replay must still establish the exact result and current authority.
That correction is under validation. Both failed candidates retain their binaries,
source manifests, logs and disks; neither is counted as full runtime acceptance.


The `339df88c` candidate passed strict Clippy and the 60 library cases. Its first
public scenario completed both exact-ID replays with two callbacks, but its final
counter assertion rejected two sequential discovery plans (one join and four
actual probes) during wake. That assertion confused cumulative starts with live
ownership. The candidate now records terminal discovery owners, including failure
and unpolled-drop paths. The held overlap still requires exactly one live owner,
one join and one probe. Completed plans and joins must fit the two callers' existing
four-attempt banks, and every plan retains its original three-member probe bound.
Final owner termination and joined shutdown remain required. The new counter and
revised oracle are under validation; the original failed records remain retained.

## Composed public probe result and remaining failures

Source manifest `ca1432ed`, based on merge `c51c8475`, passed strict library and
six-target Clippy, all 60 selected library cases and the public
`leader_probe_storm_is_bounded` oracle. All eight combinations of image mode,
native/Axum HTTP and cancellation/joined-close cuts passed. During the held
physical wake sync, SDK and HTTP callers shared exactly one authenticated probe
owner, one join and one actual probe. The original two-slot ingress bank refused
a third caller before admission. Both original command IDs reconciled with two
callbacks and exact results; all entered probe plans terminated within the
original attempt banks. This qualifies that selected oracle, not every RO-09 cut.

The same immutable source and retained binaries passed all 193 nonignored
execution cases, both primitive cases, all 21 storage cases, both public read
ledger verifiers and the real RustFS segment-credit case in both image modes.
Formatting, layout, native dependency and documentation checks passed. Evidence
is retained in `composed-probes-ca1432ed`, including the source manifest, working
patch, changed sources, seven executable hashes and complete logs.

The composed campaign failed two existing cases. Scheduler results were ten
passes and one failure: `periodic_raft_segment_settlement_retries_the_local_receipt`
returned an ambiguous `Runtime(DeadlineExceeded)` for seed command client `c8`,
sequence one, in materialized mode. The seven earlier routing cases had six passes
and one failure: `refresh_storm_is_bounded` returned HTTP 503 with an ambiguous
`Unavailable` outcome for client `a8`, sequence one, in the extent/native HTTP
cold-refresh pair. Neither failure is treated as successful qualification or
attributed to a particular change without further evidence. Limits, deadlines,
receipt assertions and command identities were unchanged. These failures and
matched-source Linux verification are the next integration priorities; live
range-publication and remaining RO/RL, crash and performance gates stay open.

## Failure isolation and live interval-repair candidate

The retained `ca1432ed` binaries passed both originally failed cases when run
individually in fresh clusters. The corresponding `a69acc34` control binaries
also passed both cases. These four serial runs are retained in
`isolated-failures-905fa882`; they do not erase the initial combined failures.

A second full scheduler/earlier-routing run of the same `ca1432ed` binaries is
retained in `broad-repeat-905fa882`. Each suite again had one failure, in different
cases. Scheduler periodic settlement passed, while extent-image physical segment
credit returned an ambiguous deadline for client `d4`, sequence nine. Routing
refresh passed, while election-case startup failed at initial group readiness in
materialized mode. The three startup observations had term one, no leader and no
SQL/controller work admitted. They do not establish the cause of the other
command deadlines. All original limits and deadlines remained unchanged.

The next test-source candidate records compact owner, SQL, transport, performance
and bounded actor observations only after a failure. It retains periodic-case
disks and propagates the original failure. It adds no mutation retries or success
criteria. The live `split_repairs_only_overlap` candidate uses six data nodes and
a zero-replica ingress. Two actual RF3 splits must preserve unrelated cached
ranges, original command receipts, old AtLeast tokens and all current replicas'
SQL values within the existing two-history/512-MiB node profiles. Both image
formats and SDK/native/Axum calls are required. The result follows below;
RO-06 and all complete OPT packages remain open.

## Live interval-repair result

Source manifest `fc2e5e15` passed the new public RO-05
`split_repairs_only_overlap` oracle in both image modes in 85 seconds. Six data
nodes hosted two actual RF3 splits, while ingress retained zero replicas. The
first split created two cached ranges; splitting one again preserved the other's
zero-directory-lookup path before and after overlap repair. Changed cells used
SDK and both HTTP adapters. Original IDs and S3 receipts replayed without extra
callbacks, old AtLeast tokens remained valid, and every current voter returned
the expected SQL value using the latest token. Per-node profiles remained at
two histories and 512 MiB, with 256 MiB per group.

The same source and retained binaries passed all eleven scheduler cases and all
eight earlier routing cases. Separate focused runs of periodic settlement,
physical segment credit, cold refresh and election also passed. Strict library
and six-target Clippy, formatting, layout, dependency and documentation checks
passed. The qualifier retained both executable hashes, changed sources and the
working patch in `live-ranges-fc2e5e15` and checked unchanged source and HEAD
throughout. This closes the selected public RO-05 functional oracle. The earlier
variable failures remain retained; a passing run of this test-only change does
not establish their cause. This campaign does not qualify the new source on
Linux/RustFS, the delayed-lookup RO-06 case, performance or a complete OPT package.

The next RO-06 candidate holds a real tree-changing directory CAS after ordinary
split preparation, then holds an ingress caller's actual old directory-head
response. A second public lookup must install the activated child generation before
the old GET completes. The old command must retain its ID and refuse the
superseded observation without an application RPC; exact-ID reconciliation must
use the already-cached child. Unrelated cached routes, old receipt tokens and
current replicas' SQL values remain required. Fault gates surround the actual
object-store operations and release on every error path. Once the old command
is constructed, all work keeps its original five-second call bound. This candidate
is unqualified until its own source-matched compile and runtime checks pass.

Candidate `3b06f2c1` passed strict Clippy and repeated RO-05 in both modes, but its
RO-06 setup found no physical immutable-page read: the ordinary directory client
already cached those bytes. The fault cut now retains the actual head GET result
after the backend selects its body and version. Cached immutable pages remain
enabled. The corrected physical cut has not yet been qualified.

Candidates `84dd66d8` and `1d301685` reached the controlled publication cut,
but the query assertion incorrectly required a transferred cell's data witness
to name its current group. The captured reply retained the original parent
commit, matching `CellManifestLeaf.witness`'s documented contract. The candidate
now uses a fresh Quorum command receipt to anchor the new child before releasing
the old lookup. Query checks retain the exact latest data witness across transfer;
local reads must report the expected voter identity, independently observed in
the current child. The original delayed command still must refuse a superseded
observation without an application RPC, then reconcile its exact ID. These
fixture corrections remain unqualified pending a new source-matched run.

## Delayed SDK lookup result and current Linux checkpoint

Source manifest `5e71d680` passed the selected public RO-06 SDK split oracle in
both image modes, using the same retained executables, in 75 seconds. The physical
old head response crossed a real split publication. A fresh Quorum command
anchored the activated child; releasing the old response produced the exact
superseded-observation refusal without an application RPC. Reconciliation kept
the original command ID and S3 requirement, used the already-cached child, and
left unrelated cached routes intact. Every current voter returned the expected
SQL value and reported its expected member/node identity. Query observations
retained each logical version's original commit witness across transfer.

Those binaries also passed RO-05 in both modes, periodic settlement, physical
segment credit and election. The cold-refresh case failed during materialized
initial group readiness, before its discovery cut: all three voters had no leader,
with idle SQL/controller admission. Broad suites were not entered. The prior
run's materialized RO-06 passed, but its extent startup failed before the race.
Both failures remain retained. Strict library/six-target Clippy and all static
checks passed for this source. Evidence is in `live-old-lookup-5e71d680`,
`live-old-lookup-repeat-5e71d680` and `live-old-lookup-static-5e71d680`. This
qualifies the selected SDK split oracle, not HTTP/recovery cuts, a complete
composed runtime campaign or an OPT package.

Published revision `dd0b1060`, source manifest `fc2e5e15`, independently passed
both materialized and extent Linux Colima/RustFS campaigns. Its immutable image
is `sha256:a6e80300d34457df9ef6b9f039ebcb5c7791d572c6016e1a76d2ff2f0fb791a1`.
Retained resource observations confirm one CPU/3 GiB and no additional swap for
each voter, with the original request, archive-restoration and maintenance
deadlines. The ledgers cover archive-outage refusal and original S3 receipt
replay, leader hard kill/restart, maintenance resume, park/wake and joined
shutdown in both modes. Evidence is in `linux-daemon-dd0b1060ea3a-ranges`.
Standby repair and failed-voter repair were disabled, so these are not full CG-13
results. The scope is separate Linux cgroups and disks in one VM; no independent
failure-domain or performance claim follows. This Linux image does not qualify
the later `5e71d680` test source.


## Public HTTP delayed-lookup checkpoint

Source manifest `2ed2a24c` passed the RO-06 live split oracle through SDK, native
HTTP and Axum in both image modes: six fresh RF3 scenarios in 186 seconds. Each
old command held the actual backend-selected directory head response across
publication. Before releasing it, a fresh Quorum command installed the activated
child's route. Stale completion refused within the original five-second call
window without another application RPC or callback. HTTP callers received the
exact `503 Unavailable` response; SDK callers retained their original ID and
nonambiguous superseded-observation error.

Reconciliation retained the original S3 command identity and fingerprint, reused
the newer covering child and returned the exact logical result. HTTP replay of
that same request added no callback or directory lookup. An unrelated interval
was warmed while the old response was held and remained usable after publication.
Current child voters retained the expected SQL values, member/node identities,
original receipts and historical cell data witnesses.

The same retained binaries passed RO-05 in both modes, all 11 native scheduler
cases and all eight earlier routing cases, including probe ownership, read
preferences and fresh cohorts. Focused periodic settlement, physical segment
credit, cold refresh and election cases also passed. Strict library/six-target
Clippy, formatting, layout, native dependency and documentation checks passed.
Evidence is in `live-old-lookup-http-2ed2a24c`, with source inputs, working patch,
CI profiles and retained executable hashes. The nine opt-in RustFS scheduler
cases were ignored in this native suite; the separately recorded constrained
Linux/RustFS checkpoint remains source `fc2e5e15`.

This accepts the selected delayed-lookup split surfaces, not recovery cutover,
complete routing authority/ambiguous-outcome qualification or a full OPT package.
Earlier startup/deadline failures remain retained; this passing run does not
identify their cause. PR #166 has since advanced to `02d71c81`; its native
activation/configuration integration and additional startup/tick observations
require their own composed-source qualification.

## Startup observations and current composed qualification

The branch includes PR #166 through `02d71c81` and adds bounded, fixed-vocabulary
observations at the actual shared cell-group owner: timer rounds, requested and
coalesced ticks, executed RawNode ticks, election-timeout work and log-admission
refusals. Managed readiness separately times archive authorization, durable
attachment and fresh quorum. Returned errors and dropped futures retain distinct
outcomes. These observations establish neither serving authority nor capacity
release, and do not establish measured fairness or an election-latency fix.

Source manifest `9ec7d32b` passed strict library/seven-target Clippy, five metric
owner tests, held-sync tick coalescing, both selected park/wake cases, and public
startup/quorum-loss checks. The readiness test explicitly preserves the operator
fixture's intentional missing-registry refusal before successful startup, then
checks exact counter deltas for the later quorum-loss call. The earlier
`65642ab1` assertion failure remains retained; it incorrectly omitted that
negative control, rather than demonstrating a production startup regression.

The same retained `9ec7d32b` executables passed all six public RO-06 split
scenarios, all 11 native scheduler cases and the remaining nine routing cases,
including both RO-05 modes. Native storage and activation-cache cases passed.
The ordinary JSON RPC case failed in extent mode during exact-ID replay of an
unpolled command: client `da` repeated 16 bytes, sequence one, returned ambiguous
`Execution(Busy)`. The expected transaction-panic controls are unrelated to this
failure. Its disks, source inputs, CI profiles and executable hashes remain in
`startup-phase-count-9ec7d32b`. This is an incomplete composed run.

Formatting, module layout, native dependencies, documentation links and all 317
performance-tool tests passed, with one opt-in Docker test skipped. The design
suite reported source drift in the host-options anchor. Review found exactly
15 added lines in `host.rs`: default-off periodic settlement, the required
existing cleanup owner and its matching 2-MiB evidence reservation within parent
control limits. All other anchor bytes, excerpts and lines matched committed
source `9b5d55ca`. The baseline now pins that reviewed source while retaining its
historical runtime revision and task acceptance. All 44 design tests pass after
that source review; the original failed run record is unchanged.

The RPC fixture waits for the fixture's actual RF3 SQL cell leases
inside its existing five-second replay window, then replays the original ID with
only the remaining time. It continues to reject unexpected `Busy` and preserves
every receipt/version/value/factory assertion. A visible receipt is not proof of
physical producer exit. The original failure log alone cannot identify which
Busy check fired. This is not a production retry-policy change or a claimed fix
for older startup failures.

Source manifest `e09efd7a` passed the new selected follow-up recorded in
`rpc-owner-replay-e09efd7a`: all three native primitive cases, strict
library/seven-target Clippy, formatting, layout, dependency and documentation
checks, and all 44 design tests. Two opt-in RustFS primitive cases were ignored.
Both image modes completed exact-ID RPC replay; their SQL-owner barriers observed
zero active leases immediately, so this run does not establish a causal fix for
the earlier intermittent `Busy`. Failure diagnostics now capture bounded owner
observations before joined shutdown.

All five CI-profile executable hashes were retained. The library, managed,
routing and scheduler executables are byte-identical to their `9ec7d32b`
counterparts; their selected passing results above remain applicable to those
executables. The earlier failed record is unchanged. This is selected follow-up
qualification, not a new full composed or Linux/RustFS campaign. Source-matched
release, performance and complete OPT-01 through OPT-08 acceptance remain open.

## Membership repair and RPC interleaving checkpoint

The branch now includes PR #166 through `d3c2621b`, including same-cell native
RPC interleaving between proven roots. Source manifest `20cb5342` passed the
selected native campaign in `membership-routing-20cb5342` at base `2fb0b365`.
Five CI-profile test executables and the exact Cargo operator CLI are retained
with hashes; the source manifest and both retained and executed artifact hashes
were checked after the campaign. The compiler profile retains optimization level
one, debug assertions and overflow checks without debug symbols.

The new `membership_refresh_keeps_range` case passed all six SDK, native HTTP
and Axum scenarios across materialized and extent images in 72.608 seconds.
Each fresh seven-node fixture replaces an RF3 voter with member five, transfers
leadership to that new voter and holds the ingress's actual birth-object read
until public demand refresh is observed. Directory root, cell range and range
epoch remain unchanged. Subsequent cached writes use the repaired membership;
removed local and explicitly selected replicas refuse without entering SQL.
The removed node's independent namespace grant still permits forwarding as an
ingress. Original IDs, stored results and S3 proof survive; replay selects the
current covering image version and witness. Current-voter local reads and old
version tokens remain checked. Eighteen structured warm, repair and fence
observations retain receipts, identities, counters and resource owners.

Two failed candidate records remain unchanged. Source `02e2fb23` failed after
movement settlement because the new fixture immediately requested physical
inventory at a newer Fleet root. Its retained binary reproduced the failure;
the `226d7cf3` probe measured required revision 35 with installed trust revision
34. The fixture now waits for the existing directory's exact node observation
and requests inventory with the remaining time inside one original five-second
window. Source `412efb53` then passed repair and removed-replica checks but
incorrectly expected the replay's image proof to predate later mutations.
The retained `e40e017f` probe confirmed unchanged ID, result and S3 durability
with the exact latest image version/witness. The oracle now checks that proof,
coverage of the original version and no additional callback, matching
`CommandReceipt` and `CellCommittedReceipt`. Both corrections are confined to
the new fixture; no production retry, authority, durability or capacity rule
changed. Temporary probe tags were removed from the qualified source.

The same `20cb5342` campaign passed strict library/seven-target Clippy, all four
native primitive cases in 281.267 seconds, all 11 scheduler cases in 508.478
seconds and the ten remaining routing cases in 631.160 seconds. The latter
includes all six delayed-lookup RO-06 scenarios and both RO-05 modes, making
11 routing cases overall. The primitive suite includes native activation,
ordinary JSON RPC, storage and the newly integrated proven-root interleaving
case. Its original-ID SQL-owner barrier actually waited once in materialized
mode; the extent scenario observed no active lease. This exercises the barrier
without identifying the precise cause of the earlier `9ec7d32b` extent Busy.
Three opt-in RustFS primitive cases and nine scheduler cases were ignored.

All five metric-owner tests, held-sync tick coalescing, both selected park
cases and startup/quorum-loss checks passed. Formatting, layout, native
dependencies, documentation, all 317 performance-tool tests and all 44 design
tests passed; one opt-in performance-tool Docker test was skipped. No request
deadline, command identity, profile capacity or archive assertion was relaxed.

This qualifies the selected combined native checkpoint. The prepared RustFS
routing entry points, constrained daemon source update, remaining authority and
recovery cuts, measured placement/fairness and complete primitive/performance
matrix still need their own evidence. The earlier constrained Linux/RustFS
campaign remains bound to `dd0b1060` / `fc2e5e15`; it does not qualify this newer
source. Full OPT-01 through OPT-08 acceptance remains open.

## Real RustFS routing harness and incomplete campaign

Revision `833c57d7`, source manifest `fb653fe1`, adds three opt-in real RustFS
routing cases. The `routing-range-repairs` runner requires exact test execution
and complete structured evidence for six membership, two overlap and six
delayed-lookup scenarios. Each caller/image combination uses its own object
prefix. The runner captures the CI compiler profile, source inputs, retained
test executable and exact Cargo operator CLI, and rejects missing or duplicate
evidence. Static/type/build preflight passed strict library/seven-target Clippy,
formatting, layout, native dependency and documentation checks, 91 multi-raft
tool tests and all 44 design tests; one opt-in Docker tooling test was skipped.
Evidence is retained in `rustfs-routing-preflight-fb653fe1`.

The actual campaign used the owned Colima RustFS container with one CPU,
2 GiB memory and no additional swap, pinned to image
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
Cellar nodes in this campaign were host-native TLS fixtures. Membership repair
passed all six SDK/native HTTP/Axum image combinations in 97.563 seconds, with
18 validated observations. Overlap repair passed both image modes in 115.439
seconds, with two validated observations.

The delayed-lookup case passed SDK/materialized, SDK/extents and native
HTTP/materialized, then failed in native HTTP/extents. The failing operation
was the fresh SDK Quorum anchor for `Counter:five`, client `cc` repeated
16 bytes and sequence two, returning ambiguous
`Execution(Refused(Unavailable))`. It was not the held HTTP S3 command for
`Counter:two`, client `c9` repeated 16 bytes and sequence two. The available
failure snapshot observed the original, already retired group rather than
the selected child, so it does not establish the cause. The campaign stopped;
the ordinary RustFS primitive suite did not run. The failed record, logs,
objects, fixture disks and executable hashes remain in
`rustfs-range-campaign-fb653fe1`.

An exact retained-binary repetition on a fresh bucket/prefix passed all six
delayed-lookup scenarios in 329.151 seconds. No source, command identity,
deadline or fixture capacity changed. Its diagnostic record is
`NOT_REPRODUCED` in `rustfs-old-lookup-reproduce-fb653fe1`; this does not fix or
supersede the failed campaign. The test executable hash is
`2edd498bd01b4bdaca73a23284d9cd586188877189eb2b540090bc7297151d4b` and the
operator CLI hash is
`76d725c285ab356575c387cd5cc0dcd92cd74be2ea6d6a281a3d2c796a53a0a3`.

Keep PR #167 draft. The immediate priorities are to capture the selected
child's state at the intermittent Quorum refusal, resolve its cause, repeat
the complete routing/primitive campaign and qualify the matching constrained
Linux source. The earlier Linux checkpoint remains `dd0b1060` / `fc2e5e15`.
Authority/recovery and ambiguous-outcome cuts, partition/RecentSource expiry,
held-owner reclamation and restart, repeated movement, measured placement and
scheduling fairness, and the full primitive/performance matrix remain open.

## Failure-boundary follow-up

The retained `fb653fe1` primitive executable subsequently ran against fresh
RustFS objects. Ordinary JSON RPC passed in 23.348 seconds and activation
fencing/cache/restart passed in 153.293 seconds. Interleaving between proven
roots failed in its extent/S3 variant with `Runtime(Overloaded)` after 113.160
seconds. The original error lacks operation context; it does not identify which
admission owner refused. The failed record and three fixture disks remain in
`rustfs-retained-primitives-fb653fe1`. This is another incomplete campaign.

Four fresh native-HTTP/extent delayed-lookup scenarios passed on diagnostic
source `df233673` in 235.806 seconds. A subsequent original six-scenario run
on diagnostic source `7fa80620` passed both SDK scenarios, then failed initial
RF3 readiness before the native-HTTP/materialized split. All three voters
observed term two, no leader and applied/committed index one. They executed
43--45 ticks without admission refusals. This is a separate startup failure;
it did not reproduce or explain the fresh Quorum anchor refusal. Its record
and disks remain in `ro06-authority-diagnostic-7fa80620`.

Diagnostic source `7afcdfcf` passed four fresh extent/S3 interleaving scenarios
in 138.711 seconds, then its exact retained executable passed the original four
Quorum/S3 and materialized/extent variants in 101.629 seconds. Those repetitions
did not reproduce the overload. Their records remain diagnostic; neither
supersedes the failed primitive campaign or establishes a causal fix. Command
identities, fixture capacities, deadlines and assertions were preserved.

Failure reporting now labels interleaving operations and captures bounded
group/resource observations before shutdown. A failed fresh Quorum anchor
records its original ID and request timing, then bounds an observation of the
directory-selected child to two seconds. That observation runs after the
request has failed; it neither retries the command nor extends its deadline.
Temporary diagnostic entry points and production debug probes were removed.
Current-source composed qualification and diagnosis of the retained failures
remain open, alongside the broader optimization and release gates above.

The resulting permanent-diagnostics source `8b529396` completed the selected
native campaign in `failure-boundary-native-8b529396`, with the overall record
retained as **failed**. Strict library/seven-target Clippy, all six membership
replacement scenarios, the five metric-owner cases, held-sync/park/wake and
startup/quorum-loss checks passed. All four primitive cases passed in 305.134
seconds and all 11 scheduler cases in 607.557 seconds. Nine of the ten remaining
routing cases passed, including all six delayed-lookup scenarios and both
overlap modes; eight range observations were retained alongside the 18 membership
observations. Formatting, layout, dependencies, documentation, all 317
performance-tool tests and all 44 design tests passed, with the opt-in Docker
tool test skipped. Five test executables and the operator CLI were retained
and checked against their executed hashes.

`leader_probe_storm_is_bounded` failed initial RF3 readiness before its routing
race. At the bounded failure observation, all three voters knew leader two in
term two. The leader had persisted its no-op at index two, but commitment and
application remained at index one; both followers still had an engine file sync
and persist operation in progress. There were no tick-admission refusals or
quarantined groups. Per-member/node election-seed derivation and the elected
leader do not support identical schedules as this failure's cause. This narrows
the observed boundary to completing the post-election durable quorum; it does
not establish a causal storage fix or explain every older startup failure.
Readiness correctly remained unconfirmed. The original deadline and required
durability proof were preserved. The latest fully passing selected native
checkpoint remains `20cb5342`; current-source backend and constrained Linux
qualification, the retained failures and complete OPT acceptance remain open.

## Cached advice and namespace origin revocation

Source `5231fdd2` passed `cached_advice_respects_origin_revocation` in 26.419
seconds: SDK, native HTTP and Axum in both image modes, using two fresh RF3
fixtures with a replica-free ingress. Each interaction warms a direct route,
revokes the ingress's write grant on every voter, and observes one cached
application RPC attempt with no probe, range or membership repair. Neither SQL staging
nor the application callback enters while permission is absent. An independent
read grant still serves the latest minimum-version token.

Stale grant revisions and an unverified process cannot restore permission.
A newer valid grant reuses the refused command's original ID and inputs with
explicit S3 durability. SDK and HTTP replay retain its exact result and current
covering version/witness, without another callback. Current local voters expose
the final value. Six `AUTHORITY_ROUTING_EVIDENCE` records retain identities,
routing/outcome counters, SQL entry observations, owners and receipts in
`public-origin-native-5231fdd2`. Strict library/seven-target Clippy and the
CI-profile executable/CLI build passed. All 94 multi-raft tooling tests passed,
with one opt-in Docker test skipped.

The earlier `e5387c52` run retained a new fixture expectation failure: it
expected plain HTTP `Unavailable` after transport revocation. The public
counter API instead preserves the ambiguous original command ID and typed
`Unavailable` cause. The corrected case explicitly requires that response and
SDK ambiguity; fixture observations cannot grant the client non-commit proof.
The no-SQL, no-callback, one-attempt and exact S3 replay assertions remain.
No production retry or authority behavior changed.

The `routing-authority` RustFS entry point passed the same six interactions
in 31.321 seconds at revision `52dcf964`, recorded in
`rustfs-origin-campaign-5231fdd2`. Its six-row ledger verifies complete
caller/image coverage, no entry during revocation, preserved ambiguity and
S3 proof. The retained test executable and operator CLI are byte-identical to
the native run. The backend observation confirms the owned pinned RustFS image
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`,
one CPU, 2 GiB memory and no additional swap. Cellar voters were host-native
TLS processes; constrained Linux voter verification still requires its own run.
This case covers current origin permission, rather than forged/lower/conflicting
term advice or restarted endpoint wire cuts. Full RO-03, current-source composed
qualification and the broader optimization/release gates remain open.

The same source passed the format check for 2,194 Rust files, module layout,
native dependencies, documentation and all 44 design tests, recorded in
`public-origin-static-5231fdd2`. These checks and the focused native case do not
qualify a real-backend or constrained-process run.

The subsequent broader `5231fdd2` RustFS campaign remains failed in
`rustfs-range-campaign-5231fdd2`. All six membership interactions passed in
124.665 seconds, with 18 audited observations. Overlap repair then failed in
materialized mode after 80.886 seconds: the native HTTP write for
`Counter:three`, client `ca` repeated 16 bytes and sequence three, returned an
ambiguous `Unavailable` while requesting S3 acknowledgement. That response
does not establish whether the mutation committed. The original log, object
prefix and 14 fixture/control disks remain retained. Its generic snapshot
observes the retired original group rather than the selected serving group,
so the failure cause remains open. Delayed lookup and the ordinary primitive
suite were not reached. No deadline, identity, capacity or assertion was relaxed.

## Retry horizons and protocol future boundary

A fresh RustFS diagnostic on source `d214e53b` captured a destination refusal
for `Counter:five`, client `cc` repeated 16 bytes and sequence three:
`Execution(Invalid("command retry window"))`. The selected child had an active,
ready leader. The fixture had captured one 60-second retry horizon before
initial commands and two split preparations, then reused it for new command
identities. This proves that fixture hazard for this observation. It does not
individually explain the earlier `ca`/sequence-three HTTP or `cc`/sequence-two
Quorum failures. The original records remain retained.

Source `95db97a5`, committed in `c27d04a5` and `d9792f5b`, captures the existing
60-second horizon when each new logical command is created. Original receipt
replays and pending delayed-lookup reconciliation retain their saved IDs,
inputs and horizons. Individual five-second calls, the remaining held-call
deadline, transition bounds, resource capacities and assertions are unchanged.
Failure context identifies the selected serving group after a split.

The composed library-test build exposed a `Send` type-evaluation overflow at
the connection task. One private `IncomingCall::execute` boundary now returns
a boxed future; its dispatch bodies and ownership transfer remain unchanged.
Strict library/seven-target Clippy passes with this boundary. The first
subsequent build retained a Rust 1.98.1 incremental-fingerprint compiler panic.
After retaining only this checkout's affected incremental directories, the
same source and compiler/profile built successfully. This comparison does not
establish a general compiler fix.

The native campaign `query-future-fresh-native-95db97a5` remains **failed**.
Ten of the 12 routing cases passed, including both overlap modes, all six
delayed-lookup interactions and all six cached-origin interactions. All five
primitive cases, all 11 scheduler cases, metric-owner/held-sync/park/wake and
public startup/quorum-loss checks passed. Library/seven-target Clippy,
CI-profile builds, formatting for 2,198 Rust files, layout, dependency and docs
checks passed, as did 317 performance-tool, 94 multi-raft-tool and 44 design
tests; each of the first two tool suites skipped one opt-in case. Five test
executables and the operator CLI were retained and verified against their
executed hashes.

`membership_refresh_keeps_range` failed before scenario evidence at the
five-second read-only `first-group standby` CLI call. Separately,
`leader_probe_storm_is_bounded` failed initial RF3 readiness in a later fresh
materialized fixture with `Transport(Timeout)`, after an earlier overlapping
probe interaction completed. Neither failure is a completed optimization
oracle or evidence of a causal fix. Their original logs and records remain
retained; no deadline, retry budget or assertion was relaxed.

The matching Linux build did not reach voter execution: `rustc` received
`SIGKILL` and BuildKit reported `ResourceExhausted: cannot allocate memory`
on the owned four-CPU/6-GiB parent VM. Its failed record remains
`linux-build-d9792f5b403e-95db97a5`. No current-source Linux image or daemon
qualification is claimed. The older `dd0b1060` / `fc2e5e15` constrained voter
checkpoint above remains separate.

Keep PR #167 draft. Next are diagnosis of the membership setup and initial probe-storm readiness
timeouts, current-source RustFS routing/primitive qualification, and matching constrained
Linux voters with adequate parent-VM memory and unchanged per-voter caps.
Endpoint-only repair remains an isolated, unqualified draft. The authority,
recovery, reclamation, placement, scheduling and final matrix gates listed
above remain open; none of OPT-01 through OPT-08 is accepted as complete.

## Current-source RustFS routing and primitive campaign

The seven-case `rustfs-current95-composed-20261111` campaign at revision
`920380f6`, source `95db97a5`, remains **failed**. It used the exact retained
native routing executable `d528f992`, primitive executable `e09a5d96` and
operator CLI `5717a275`; full hashes and source inputs are retained in its
record. Each independent case used fresh fixtures and separate object
subprefixes. Continuing the remaining cases did not retry failed mutations.

Both split-overlap image modes passed in 127.504 seconds, with two validated
observations. All four ordinary primitive cases passed against real RustFS:
JSON RPC in 63.012 seconds, activation/cache/restart in 160.206 seconds,
interleaving between proven roots in 164.111 seconds, and canonical SDK
source-proof dispatch in 71.749 seconds. These results do not retrospectively
explain the older intermittent Busy or Overloaded records.

Membership completed both SDK image-mode interactions, then failed initial
readiness in the next, native-HTTP/materialized fixture after 55.099 seconds.
All three groups were attached in term one, with no leader and applied,
committed and persisted index one. Their observed file syncs had completed.
Attachment consumed about 2.9 seconds of the original five-second startup
window, leaving about 2.1 seconds for fresh quorum; readiness correctly remained
unconfirmed. This differs from the native source-`95db97a5` probe-storm failure:
its three groups had not attached, with one retained preparation per node and
`NotFound` runtime observations. Neither snapshot establishes a causal fix.

Delayed lookup completed all four SDK/native-HTTP image-mode interactions,
then failed cell enrollment while preparing the Axum/materialized fixture,
before that fixture's lookup race. The case failed after 281.479 seconds with
ambiguous `Runtime(DeadlineExceeded)`. This is distinct from the earlier
native-HTTP/extents fresh Quorum-anchor refusal. Four observations are retained;
the complete six-scenario gate remains unsatisfied.

RustFS retained its pinned image, one CPU, 2 GiB and no additional swap. The
owned parent VM was restored to four CPUs/14 GiB before the campaign, without
changing voter or store caps. Host compilation and a later owned Linux build
ran concurrently; no performance or scheduling-causality claim follows from
these checks. Matching Linux execution is recorded below; the remaining
optimization gates above remain open.

## Current-source constrained Linux voters

Source `95db97a5`, Linux revision `d9792f5b`, passed the constrained daemon
campaign in both image modes using image
`sha256:4dcb170a3509f1a8efc52096041fa94771fd62710e507e9399a2410061ea2edf`.
The implementation bytes match the published source at `087df66a`; that later
revision adds documentation. Source manifests and both executable hashes were
verified from the immutable image before execution. The record is
`linux-daemon-d9792f5b403e-95db97a5`.

Materialized mode passed in 107.233 seconds with 137 ledger events; extents
passed in 105.494 seconds with 131 events. The existing verifier checked S3
outage refusal and exact-ID replay, continuing Quorum/linearizable operations,
retained-disk hard kill/restart, maintenance return/resume, park/wake,
no-quorum refusal and joined shutdown. Separate voter containers retained
separate disks. Captured effective cgroups show one CPU, 3 GiB and zero swap
per voter. RustFS retained its pinned image, one CPU, 2 GiB and no additional
swap. No test deadline or per-voter/store capacity was enlarged.

The failed six-GiB parent build remains retained. The identical source,
compiler, dev/debug-zero profile and one build job succeeded after restoring
the parent to four CPUs/14 GiB. Runtime then used six parent CPUs/14 GiB to
satisfy the daemon runner's existing preflight. The preflight was unchanged.
These are correctness results in one local VM, with host work running
concurrently; they establish no independent failure domains or measured
speedup. Standby and failed-voter replacement were disabled. Full CG-13,
CG-17 and OPT-01 through OPT-08 acceptance remain open.

## Startup phase diagnostic

Temporary source `795d66da` ran fresh materialized startups through the same
unarmed filesystem adapter as the probe-storm fixture. Twenty-two starts
passed, then iteration 22 failed readiness. All three blocking page-recovery
closures, preparation futures, manager handoffs and runtime attachments had
completed. Attachment took about 3.133 seconds, leaving about 1.864 seconds
of the original five-second startup window. Voters remained in term one with
no leader, with 18--19 ticks executed and no election timeout. Admission
correctly remained closed.

This observed failure is after attachment; it does not reproduce or explain
the earlier native `95db97a5` pre-attachment `NotFound` snapshot. It exposes
the budget consumed before fresh quorum, without establishing the cause of
that elapsed time. Original probe-storm and membership cases subsequently
passed on the diagnostic source. These repetitions are not causal fixes and
do not supersede their failed records. Exact source, phase logs, binaries and
failed disks remain in `startup-phase-diagnostic-795d66da`. After the process
finished, all seven temporary files were restored from their exact baseline;
no phase probes or diagnostic entry point are published.

Next are the remaining startup/enrollment causes, endpoint-only repair's
read and unrelated-follower negative paths, full authority/recovery and
ambiguous-outcome cuts, followed by the reclamation, placement, scheduling
and final combined-source gates listed above. PR #167 remains draft.


## Endpoint repair and current PR checkpoint

PR #167 is open and ready for review. This checkpoint supersedes the older
statements above that called the PR a draft and excluded endpoint repair.
The branch now integrates stacked base `447c35e7`, endpoint component
`35ebb664` and the physical-admission fixture correction `1bdb2c86`.
Temporary startup and maintenance probes remain excluded.

Endpoint discovery is keyed by the selected authenticated node boot, without
turning a physical endpoint change into range or membership discovery. A
healthy leader can continue serving while an unrelated follower's endpoint
metadata is unavailable. Reads repair their selected remote endpoint under
the original call deadline and attempt budget.

The controlled in-flight regression reproduced twice on source `74faf168`.
A real group-archive PUT held an admitted S3 command while another follower's
new NodeView was accepted. The selected leader and configuration stayed the
same, but whole-route pointer identity discarded the leader's completed reply
as ambiguous `TransportStalePeer`. Source `ccd8ab5b` captures a bounded cache
lifetime independently of immutable endpoint snapshots. It still checks the
selected peer, schema/configuration revision, current Fleet/NodeView trust,
physical membership and explicit remove/reinstall. Entered work pins the
refreshed cache entry until physical exit; leader-advice generation remains
independently fenced. No mutation outcome is reclassified or retried with a
new identity, horizon, deadline or capacity limit.

The component native campaign `endpoint-native08-ccd8ab5b` passed strict
library/routing Clippy, CI-profile build, lifetime/removal/configuration/boot
negative checks, four public endpoint cases, read cohort/preferences,
cached-origin authority and static checks. The endpoint ledgers cover six
caller/image interactions per write case and twelve read interactions
(SDK/native HTTP/Axum, materialized/extents, Strong/AtLeast). This component
predates the latest primitive base and does not qualify the combined source.

The actual RustFS follow-up `endpoint-rustfs08-ccd8-20261111` passed all
three available backend cases with the same retained component executable
and CLI: selected restart repair in 39.017 seconds (six observations), the
unrelated-follower negative case in 31.370 seconds (six), and read-first
repair in 34.158 seconds (twelve). Logs, complete ledgers and executable
hashes were retained. RustFS kept its pinned image, one CPU, 2 GiB and no
additional swap in the owned Colima VM; voters were host-native TLS
processes. The controlled in-flight case was native-only at that checkpoint; the
later RustFS entry point requires its own runtime evidence. This older
component result does not qualify
the latest combined primitive source or constrained Linux voters.

## Combined revision verification

Revision `6bff2a13`, source `191b46fc`, includes both endpoint repair and
the latest primitive/Storage base. The `pr167-combined-final-191b46fc`
record retained five test executables, the operator CLI, full source inputs
and executable hashes. Rust 1.98.1 used one build job and the CI test profile
(opt level one, debug information zero, assertions and overflow checks on).
**FAILED**: 28 checks passed; 3 failed.

| Check | Result | Seconds |
| --- | --- | ---: |
| `clippy` | Passed | 186.864 |
| `unit-router_refresh-tests-` | Passed | 0.054 |
| `unit-leader_cache-tests-` | Passed | 0.015 |
| `unit-leader_probe-tests-` | Passed | 1.864 |
| `unit-performance-tests-` | Passed | 0.012 |
| `unit-query_wire-tests-` | Passed | 0.053 |
| `unit-storage-erase-tests-` | Passed | 0.012 |
| `unit-raft-effects-outbox-tests-` | Passed | 0.015 |
| `unit-cell_query_lifetime_fences_removal_configuration_and_selected_endpoint` | Passed | 0.562 |
| `unit-process_refresh_is_atomic_across_routes_grants_and_capacity` | **Failed** | 0.108 |
| `inflight` | Passed | 27.675 |
| `read-first` | Passed | 40.120 |
| `endpoint-unrelated` | Passed | 32.896 |
| `endpoint-public` | **Failed** | 27.195 |
| `read-cohort` | Passed | 61.100 |
| `read-preferences` | Passed | 49.495 |
| `origin-authority` | Passed | 40.878 |
| `periodic_raft_segment_settlement_retries_the_local_receipt` | Passed | 42.562 |
| `raft_segment_settlement_credits_files_after_checkpoint` | Passed | 20.666 |
| `raft_segment_settlement_replays_after_root_cas_uncertainty` | Passed | 2.143 |
| `storage_reads_and_writes_exact_cell_versions` | Passed | 18.530 |
| `ordinary_cell_sdk_resolves_current_histories` | Passed | 31.850 |
| `native-storage` | Passed | 1.271 |
| `format` | Passed | 119.713 |
| `layout` | Passed | 0.391 |
| `dependencies` | Passed | 1.191 |
| `docs` | Passed | 0.575 |
| `performance-tools` | **Failed** | 17.339 |
| `multi-raft-tools` | Passed | 9.746 |
| `design-tools` | Passed | 3.240 |
| `patch` | Passed | 0.029 |

Two driver attempts selected physical file names as module paths after
successful build/library groups. Both failed harness records are retained.
The final continuation selects the actual Storage and outbox module paths
from the executable inventory and uses the same hashed binaries/source.
Those selection corrections fix no product behavior. The final run also
reproduces the CI transport task-count failure locally.

These focused checks do not replace the full failed native matrix or qualify
the combined revision against actual RustFS or constrained Linux voters.


The earlier combined source `06769419` remains failed: membership stopped at
the read-only standby CLI timeout; the busy-leader oracle proved its two
refusals before a later fresh standby setup timed out; probe storm reached an
actual RPC and failed ambiguous `Unavailable`; periodic segment settlement
refused command sequence 14 with non-ambiguous `RuntimeOverloaded`. Six
primitive cases, native Storage, the other ten scheduler cases, strict
Clippy/build and static checks passed. An unchanged retained-binary periodic
repeat passed both modes in 35.347 seconds; this is not a causal fix.

The periodic maximum-input loop now waits for actual ordinary-admission
release before issuing its next new command, matching the existing manual
purge workload. It keeps the original resource bank, command IDs, retry
horizons, call windows and Fleet settlement assertions. A lingering owner
still fails the bounded wait; response delivery alone returns no capacity.

Constrained Linux source `06769419` built successfully as image `ce68dd7e`,
but its materialized campaign failed after maintenance readiness with a Fleet
conflict of unknown CLI verb. The archive-outage and hard-kill verifiers
passed for the retained partial ledger; maintenance completion is unproved.
An independent extents campaign failed initial Node 4 inspection with
`Refused`. Diagnostic-only source `f27ac447`, image `cc38c4ff`, added bounded
read-only error context in one test helper, then failed initial Node 5
inspection before reaching maintenance. It does not identify or fix the
previous maintenance failure. Those temporary probes are not published.
The earlier source `95db97a5` passing Linux campaigns remain historical.

Existing GitHub CI at head `9c9448af` also retained a transport fixture task
count failure (9 versus the asserted 11) and design tests unable to read the
pinned historical source in their checkout. Neither is declared fixed by
these endpoint changes. Current combined-source RustFS/Linux acceptance,
startup and actual-RPC failure causes, complete authority/ambiguity cuts,
held-owner reclamation/restart, placement/fairness measurements and the full
primitive/performance/failure matrix remain open. No full CG-13, CG-17,
OPT-01 through OPT-08, independent failure-domain or speedup claim follows.

## Backend entry and verification-guard follow-up

The next source adds the ignored RustFS held-reply entry and includes it and
read-first repair in the named endpoint suite. Its parser requires all thirty
observations across four cases, with exact caller, image and read-consistency
coverage. Seven new refusal tests and the existing tool suite passed (105
tests, one opt-in skip). The parser also accepted retained native held-reply
and read ledgers and the actual RustFS read ledger. Those parser checks do not
qualify a new runtime source.

The local source-`191b46fc` task-count failure reproduced CI's nine-versus-eleven
assertion. Main's shared-group cutover `d0c2873f` removed
`snapshots::start_prepared` and its owner pair. The fixture now requires three
listener tasks, six peer tasks and two actual pools; all overload, atomicity,
physical-drain and capacity assertions remain. Its compiled regression still
requires verification on the new source.

A controlled depth-one fetch omitted the exact pinned historical source
needed by the design tests; fetching full ancestor history made that same
object readable. The SDK CI job now selects the documented
[`fetch-depth: 0` checkout option](https://github.com/actions/checkout/blob/v6/README.md#fetch-all-history-for-all-tags-and-branches).
The workflow parsed with the intended job setting. A GitHub rerun is still
required. The performance reader now includes all three endpoint owner
counters; all 317 performance-tool tests passed with one opt-in skip.

The source-`191b46fc` endpoint case completed all three materialized interactions,
then failed fresh extents voter-one readiness before the endpoint scenario.
That startup failure remains open. The full current-source RustFS endpoint
campaign, constrained Linux and broader optimization gates remain unproved.


## Current-source RustFS and constrained Linux checkpoint

Runtime artifacts below were built from clean revision `b9ff44ba` with source
manifest `46eca779`. Documentation and workflow follow-ups do not change that
runtime manifest. Temporary HTTP diagnostic probes are excluded from the
published source.

The current native campaign passed formatting (2,214 files), strict library and
routing Clippy, the CI-profile build, all eight directory tests, layout,
dependency, documentation and patch checks. The directory group includes the
corrected nine-worker baseline and lifetime/configuration fences. Multi-raft
tooling passed 105 tests and performance tooling passed 317 tests, each with
one opt-in skip. The first incremental compiler fingerprint panic is retained;
identical-source fresh-state compilation passed. No compiler fix is claimed.

The named four-case RustFS endpoint campaign remains **failed**. After one SDK
interaction, materialized native HTTP command E9/sequence 3 returned ambiguous
`Unavailable`. Its original identity and disks remain retained; a later local
SQLite result is not proof of the required S3 receipt. The three remaining
cases passed independently on the exact same retained executable and CLI:
unrelated follower (six observations), Strong/AtLeast read-first repair
(twelve), and held in-flight S3 replies (six). These 24 observations do not
convert the failed named campaign into a pass.

An isolated error-category probe completed the selected endpoint case's six
observations without reproducing that HTTP failure. It was removed before
returning to the published source; this passing diagnostic is no causal fix.
A subsequent independent repeat of the exact published binaries completed all
three materialized interactions, then failed fresh extents initial attachment
with `Transport(Timeout)`. All three groups were absent, with one retained
preparation per voter and approximately five seconds spent in attachment.
This pre-attachment failure differs from the earlier post-attachment quorum
wait and the original native HTTP command failure. None is declared fixed.

The current constrained Linux image `ef0024789e9f` passed the full existing
`daemon.py` campaign in both image modes. Materialized took 98.726 seconds
(143 ledger events); extents took 88.250 seconds (131 events). Existing
archive-outage/refusal/exact-replay, leader-loss/hard-kill/retained-restart,
maintenance-stop/return/resume, park/wake, no-quorum-refusal and joined-close
verifiers passed. Ten actual live lifecycle observations recorded one CPU,
3 GiB and zero swap per voter. These were three voters observed across their
lifecycles. The pinned RustFS container retained one CPU, 2 GiB and no extra
swap inside the six-CPU/14-GiB Colima VM. The image used dev opt-level zero,
debug zero and one build job; it is not the CI opt-level-one test artifact.
Standby/replacement, independent failure domains, performance and the full
CG-13/CG-17/OPT acceptance remain unqualified.

Retained qualification directories under the checkout's private target are
`endpoint-current-backend-fresh-46eca779`,
`endpoint-current-independent-46eca779`,
`endpoint-http-diagnostic-eb6e2eb1` and
`endpoint-selected46-baseline-repeat`. The separate Linux checkout retains
`linux-current-build-b9ff44ba0582-46eca779` and
`linux-daemon-b9ff44ba0582-46eca779`. Each record binds source, executable or
image hashes, outcomes and the limits of its evidence.

Current GitHub native CI subsequently failed five measurement-runner tests at
`checkout target directory`, before voter setup. Its native job did not export
`CARGO_TARGET_DIR`, which these evidence-retaining fixtures require. The job
now explicitly selects `${{ github.workspace }}/target`, matching Cargo's
existing output location. A fresh CI-profile executable reproduced the exact
error immediately with the variable absent. With the variable exported, the
same executable completed its serial campaign in 467.533 seconds: **19 passed,
two failed**. Warm-verifier-cache, rooted-image reclamation and occupancy-32
archive diagnostics passed. Both failures reached occupancy-128 enrollment
and consumed the original 60-second total window, with 66 and 59 cells present
respectively. The original enrollment identities and fixture disks remain
retained. Evidence is in `ci-perf-target-b9ff44ba`; its profile is opt-level one,
debug zero, with debug assertions and overflow checks enabled.

The first failed fixture's later observations had committed/applied index 68
and archive publication at 66. The second had a different cell count and
publication position. Those observations do not prove one coherent failure
cut or a fixed 64-cell ceiling. Aggregate SQL preparation, staging, publishing
and engine-sync timings identify boundaries for profiling, not a causal
performance fix. The campaign used host-native voters and an in-memory object
store; it does not qualify constrained Linux, RustFS or measured speedup.
This workflow change establishes the cause of the setup failure, not a startup
or protocol fix. A separate attempt selected an
older CLI executable instead of the test harness; it is retained as a harness
selector error and contributes no valid qualification evidence.


## Disposable extent initializer base and pinned-toolchain control

The initializer follow-up uses runtime source manifest `aeffb378`. An unenrolled extent initializer
previously staged and sealed a durable transaction-zero root solely to open its
disposable SQLite workspace. It now uses `ExtentWorkspace::empty`, which checks
the canonical transaction-zero binding without allocating an inventory root.
The private comparison marker remains metadata. Existing published bases retain
their actual sealed images, and initialized results still take the original LTX,
sealed-image, manifest and durability-proof path. Callback admission, workspace
ownership, command identities, deadlines and resource banks remain unchanged.

The regression observes actual inventory usage while an initializer is held.
The old production code reported one root and one admitted workspace; the new
code reports zero roots and one workspace. Both observations use the existing
concurrent enrollment/cancellation case, including its original callback,
reconciliation, conflict and joined-close assertions. The assertion runs after
cleanup so its failure cannot strand the held callback. New LTX public tests
exercise real SQLite writes, last-handle admission retention and invalid
binding/quota refusal before file creation.

A compiler-provenance audit found that the recent 19-passed/two-failed runner
campaign and its OS stack samples used ambient `stable`, which had become Rust
1.99.1. Their optimization/assertion flags matched CI, but they do not qualify
the pinned Rust 1.98.1 toolchain. Embedded compiler hashes independently confirm
that the earlier source-`46eca779` RustFS routing artifacts and temporary HTTP
probe used 1.98.1. The 1.99.1 candidate compilation panic and all diagnostics are
retained; no compiler fix is claimed.

The controlled before/after therefore explicitly selects Rust 1.98.1, with its
compiler hash checked in retained executables. The original production code,
plus the new inventory assertion, reproduced the one-root/one-workspace failure.
It also reproduced the original occupancy-128 enrollment deadline: the total
window ended at 60.000 seconds, with later observations of 54 cells in the main
group. The candidate passed all 19 native enrollment cases and 29 LTX cases
across workspace, source-backed workspace, admission and apply targets. Two LTX
entries are marked ignored: the isolated write-failure child is exercised by its
passing parent, while the retained decimal-10-GB campaign remains unrun.
Strict library/affected-test Clippy, formatting (2,214 files), layout,
dependencies, documentation and patch checks passed. Pinned LTX core-only and
all-feature builds also passed. Their first attempt returned zero from both
compiler commands but failed its source guard after a component README edit;
that attempt is retained as failed. Restoring the exact tested README and
repeating both feature checks passed with the guard intact.

The candidate's original occupancy-128 case still **failed**: enrollment used
60.003 seconds and later observations showed 77 cells in the main group.
The complete failed test, including observations and joined close, took 76.471
seconds; the before test took 67.962 seconds. Those separate observations and
single attempts establish neither a coherent failure cut nor a measured
speedup. The optimization removes the extra empty inventory root; it does not
close the scale gate. Stack samples from the earlier diagnostic exposed
manifest-page, archive-journal and engine persistence barriers for further
investigation. No persistence requirement is relaxed by that observation.

Records are retained under `enrollment128-profile-46eca779`,
`enrollment-empty-base-regression`, `enrollment-empty-base-candidate`,
`enrollment-empty-base-pinned-before` and
`enrollment-empty-base-pinned-after` in the checkout's private qualification
root. The pinned candidate campaign remains **failed** solely at `scale128`;
its other listed checks passed. Source inputs stayed frozen through each
actual job's completion. The before and candidate incremental caches are
retained separately, with the same compiler/profile settings.

Actual RustFS and constrained Linux have not yet qualified `aeffb378`.
The prior source-`46eca779` endpoint results and both passing daemon modes stay
bound to that earlier source. Occupancy-128 enrollment, the distinct endpoint
and startup failures, standby/replacement, authority/ambiguity cuts, held-owner
reclamation, placement/fairness and the complete primitive/backend/performance
matrix remain open. This checkpoint accepts no complete CG-13, CG-17 or
OPT-01 through OPT-08 package.

## Archive reservation checkpoint and recovery control

Runtime source manifest `c273e2b0be74`, committed as `dfc0d424`, removes one
intermediate local journal checkpoint from an ordinary archive upload. The
original reservation intent is already durable before its allocation CAS.
The persistent pending generation reconciles that exact intent to its original
ticket, even after unrelated registry revisions and partial immutable writes.
The journal therefore retains `Reserve` through upload instead of writing a
second `Upload` record. Publication intent remains durable before its CAS;
seed uploads retain their existing checkpoint path. Neither recovered tickets
nor partial objects establish archive coverage or serving authority.

The new regression holds the actual reservation CAS, reads the durable intent,
then loses an accepted immutable-create response and denies subsequent reads.
It joins the original controllers, SQL workers and Raft runtimes before opening
the registry in a new archive execution domain. An unrelated durable allocation
advances the registry, yet the persisted original intent reconstructs the same
ticket without another PUT. Collection preserves its partial generation after
the original local upload handle has exited. Recovery independently reopens the
selected local root, completes the upload and independently verifies the archive
before publication. Published coverage is then audited against the full SQLite
contents. Publication intent is persisted before commit; consumed
ticket replay refuses without another allocation, and all owners join cleanup.
The original ReplicaLog engines remain open: this archive-owner handoff is not
full node/process restart or power-loss qualification.

On unchanged old production code, every recovery, collection, SQLite and
joined-cleanup assertion passed before the final journal assertion reproduced
the extra `Upload` checkpoint. The candidate passes that assertion in both
image modes. Both controls explicitly use Rust 1.98.1; retained executable
compiler hashes were checked. Initial fixture attempts remain retained: one
missed the shared test-intent helper and another tried checkpoint capture after
the runtime had closed. The corrected fixture uses persisted-root recovery.
The before control's initial driver classification was corrected by inspecting
both retained output streams; it was not rerun under a new mutation identity.

The candidate's native campaign remains **failed**. Two archive-journal unit
tests passed. The controller target reported **16 passed, four failed, 23
ignored**, including a passing new recovery case. The archive-protocol target
reported **31 passed, one failed, seven ignored**. Its failure was
`quorum_continues_bounded_s3_outage`, at the fresh remote catalog-discovery
assertion. Original occupancy-128 enrollment also failed within its unchanged
60-second total window; the complete failed test and joined observations took
69.752 seconds. No scale acceptance or measured speedup follows from removing
this checkpoint. Strict library/affected-test/runner Clippy, formatting (2,215
files), layout, dependency, documentation and patch checks passed. The multi-raft
tool suite ran 105 tests with one opt-in skip and passed. Source inputs remained frozen
through actual completion.

A separate paired comparison uses the retained old-production and candidate
executables, with identical test code, profiles and Rust 1.98.1. In both,
`s3_enrollment_requires_the_original_archived_creation` fails at the duplicate
response ownership assertion and
`s3_remote_outputs_preserve_durability_and_creation` fails at the unbound remote
ambiguity assertion. The deadline and slow-delivery cases that failed in the
full controller campaign each pass independently on both executables. Those
repeats neither erase the full campaign failures nor prove their causes. The
archive-protocol outage failure has no paired before control in this campaign.
No assertions, request windows, identities or resource limits were changed.

The committed candidate then **passed** the named RustFS `archive-reservation`
suite: seven signed S3 capability probes and its exact ignored recovery case,
which exercises both materialized and extent modes. Its retained executable
hash matches the native candidate exactly, including the embedded Rust 1.98.1
compiler hash. Backend ownership, pinned image, loopback endpoint and live
one-CPU/2-GiB/no-extra-swap limits were checked before and after the campaign.
The backend runs in the owned Colima VM; these RF3 voters are host-native.
This pass qualifies the recovery case, not the default 23-case archive suite,
constrained container voters, full node restart or the broader S3 failures.

Records are retained under `archive-reservation-before`,
`archive-reservation-before-v2`, `archive-reservation-before-v3`,
`archive-reservation-candidate`, `archive-reservation-paired-regressions` and
`rustfs-archive-reservation-dfc0d424` in the checkout's private qualification
root. The latest constrained Linux evidence remains bound to source `46eca779`
and image `ef0024789e9f`; it does not qualify this candidate. Next priorities
remain occupancy-128 cost, the S3 ownership/ambiguity/outage failures and distinct
endpoint/startup failures, followed by latest-source constrained Linux,
standby/replacement, authority and reclamation cuts, placement/fairness and the
complete primitive/backend/performance matrix. Full CG-13, CG-17 and OPT-01
through OPT-08 remain open.

## S3 output contracts after routing and verification reuse

Qualification source `8eca02f3`, committed as `2010c5c5`, strengthens three
existing S3 scenarios. Production behavior is unchanged; the production Rust
diff corrects the public remote-command documentation. The previous failures
exposed test assumptions that predated admitted verification reuse, authenticated
rejection before admission and local destination-manifest routing.

The enrollment probe observed a successful duplicate with the same creation
record, S3 durability, two retained outputs and one physical verifier. The
scenario now denies new immutable writes while the duplicate shares that proof.
Dropping the first response must leave its final consumer's verifier charged.
An uncovered successor executes its callback but cannot receive S3 output until
that final consumer drops. Original initialization, creation-conflict, namespace
minimum, callback-count and joined-close checks remain in place.

The unbound remote probe observed authenticated `UnsupportedCapability`, no
callback or initializer, zero retained owners and `ambiguous=false`. The scenario
now checks this precise non-commit result and its opposite boundary. One original
invocation is rejected, retried through a bound gateway, then loses its response
waiter after SQL retires while S3 output is pending. After physical completion,
the same invocation receives another authenticated pre-admission rejection.
It must preserve its earlier uncertainty, original command identity, captured
inputs, 15-second deadline and four-attempt bank. Only one callback runs. The
fixture rebinds the gateway while retaining its existing TLS router; this is
client waiter cancellation, not process restart or a claimed network-loss cut.

The outage probe observed selected-destination Quorum success with zero receiver
catalog reads while the direct backend probe failed. The scenario now checks a
separate cold ingress: its actual catalog read must fail before transmission or
callback execution. The selected receiver still uses its active local manifest
and current execution authority. Its successful remote command counts within
the original six-callback load at the unchanged nine-entry ceiling. The local
client's next sequence is consequently six. S3 wait/refusal, stored minimum,
checkpoint and admission assertions remain, and replay retains the original
remote invocation instead of running another callback.

The full ordinary native campaign **passed**: controller **20 passed, zero
failed, 23 ignored**; archive protocol **32 passed, zero failed, seven ignored**.
Strict library/affected-test Clippy, formatting (2,215 files), layout, dependency,
documentation and patch checks passed. Source inputs stayed frozen. Rust 1.98.1
was explicitly selected and its embedded compiler hash checked in both retained
executables. The first build failed with an unstable incremental fingerprint
compiler panic before any runtime checks. Its record and twelve affected
checkout-private incremental directories remain retained. A fresh-state repeat
passed with the same compiler/profile settings; no compiler fix is claimed.
Earlier fixture attempts also remain retained: recreating an existing router
refused storage creation, and a new command at sequence two correctly refused
its sequence gap. The final counterexample uses the original invocation.

The default **23-case RustFS campaign failed** in its first selected case,
`rustfs_quorum_continues_bounded_s3_outage`. Seven signed capability probes and
inventory validation passed. Materialized mode reached an actual container stop,
restart and archive recovery, then command client `[188; 16]`, sequence **6**,
requested Quorum durability and returned ambiguous `Runtime(DeadlineExceeded)`.
This is the post-ceiling command after recovery, with its original 15-second
request window. The selected test process took 26.8 seconds. The runner restored
the same backend container; its pinned image and one-CPU/2-GiB/no-extra-swap
limits remained unchanged. Extents and the remaining default cases were not
reached. The original command outcome remains unknown; later observations or a
new fixture cannot turn this failed campaign into a pass.

Two independent exact RustFS cases then **passed** on the same retained native
executable and source, in a separate fresh bucket/prefix with seven signed probes:
`rustfs_s3_enrollment_requires_the_original_archived_creation` (7.003 seconds)
and `rustfs_s3_remote_outputs_preserve_durability_and_creation` (10.908 seconds).
Both exercise materialized and extent modes. These scoped passes verify the
stronger ownership and uncertainty checks through the actual S3 client; they do
not accept the failed default campaign. Voters remain host-native RF3 TLS tasks,
with RustFS inside the owned Colima VM. Earlier intermittent deadline and
slow-delivery failures did not recur in the passing native campaign, but their
causes are not established.

Records, executable/source hashes, logs and remote object scopes are retained in
`s3-output-contract-native`,
`s3-output-contract-native-fresh-incremental`,
`rustfs-s3-output-contract-8eca02f` and
`rustfs-s3-independent-ownership-8eca02f`. The targeted enrollment, unbound and
outage probes also remain under their `s3-*-probe` qualification directories.

The base branch advanced independently to `74591226` with parked read-cursor
image lifetime fixes. It has been fetched but is not integrated into this
checkpoint, which still builds on `447c35e7`. Integration and combined-source
qualification remain required. The new Quorum recovery timeout, occupancy-128
cost, distinct endpoint/startup failures, latest-source constrained voters,
standby/replacement, authority and held-owner reclamation cuts, placement/fairness
and the full primitive/backend/performance matrix remain open. The latest Linux
daemon evidence is still source `46eca779` / image `ef0024789e9f`. This checkpoint
accepts no complete CG-13, CG-17 or OPT-01 through OPT-08 package.

## Idle Raft connections after archive recovery

The retained Quorum recovery failure now has a reproduced transport cause.
The receiver closes idle TLS streams at the existing five-second I/O bound,
while the sender previously cached them without an expiry between exchanges.
An exact failed delivery reported peer EOF on the data lane to both followers.
The leader had local entry 11 while both followers and committed/applied progress
remained at 10. SQL had executed its callback, but no archive settlement owner
was blocking the command. This explains the reproduced failure; the original
failed invocation remains ambiguous and its campaign remains failed.

Runtime change `cbabde1d` retires cached data and control connections between
exchanges using the preceding batch/frame's original I/O deadline. Completion
of a late reply cannot renew that reuse window. A fresh connection uses the next
exchange's existing bound. Failed or uncertain frames are never replayed, and
active deadlines, route fences, physical reservations and application identities
remain unchanged. The rare delivery-feedback future has an opaque borrowed
boundary to avoid a compiler Send-proof depth overflow without changing compiler
limits or the healthy packet path.

Two new native RF3 TLS regressions wait for actual receiver-task exit before
the next Quorum command or control frame. Both fail against the previous runtime:
the command exhausts its original 15-second window, and the control frame reports
the same peer EOF. The candidate passes both tests in materialized and extent
modes (24.49 seconds in libtest). Failure paths retain fixture disks and join
network/runtime owners before returning; failed commands receive no fresh window.
The tests use in-memory object storage and disabled automatic Raft ticks, so
this selected pass does not qualify the complete RustFS outage campaign.

The controlled candidate source is `753b2a3e`; retained executable SHA-256 is
`98ed6a5ae58a539ebd76e7efbcb7782bb453d7f87f1ca51d09aa8c22a3d71951`.
Rust 1.98.1 is explicitly selected and its embedded compiler hash is verified,
with test opt-level 1, debug info 0, assertions and overflow checks enabled.
An ordinary Send-proof compilation failure and a subsequent unstable incremental
fingerprint compiler panic are retained separately. The fresh-state build passes
with the same compiler/profile; no compiler fix is claimed. Diagnostic traces
are removed from the committed source.

Base `74591226` is integrated without conflicts in `ba1c350e`. Combined source
`f1500526` is undergoing the full ordinary archive targets, actual transport unit
inventory, exact parked-cursor SDK case, original occupancy-128 diagnostic,
strict affected Clippy and repository checks. The complete default RustFS
campaign must then be rerun. These combined checks are pending at this checkpoint;
the selected pre-integration pass cannot substitute for them.

Evidence is retained under `quorum-recovery-exchange-series`, `peer-idle-before`,
`peer-idle-candidate`, `peer-idle-candidate-v2`,
`peer-idle-candidate-v2-fresh` and `peer-idle-native-combined`. Earlier failed
campaigns remain unchanged. Occupancy, distinct endpoint/startup failures,
current-source constrained Colima voters, standby/replacement and the remaining
authority, reclamation, fairness and full qualification matrix remain open.

## Shared archive fairness in both image modes

The archive campaign previously passed all 23 configured cases on source
`f1500526`, but its three-group fairness entry exercised materialized images
only. An audit of the retained log reproduced the missing evidence: a passing
libtest result alone cannot establish both image modes. The old campaign remains
a pass for its actual configured scope; it does not prove extent fairness.

Changes `27e1d706`, `abd76b1d` and `3bb0d605` run the fairness fixture separately
with materialized and extent images. All three extent groups reuse each node's
existing physical extent inventory and quota, and the fixture checks at least
14 roots in each shared inventory. It independently restores all 14 images and
compares their bytes and storage kinds, including the two initial images. The
existing single checkpoint slot, held publication, lost immutable-PUT response,
foreign-hold refusal, corrupt-tail refusal, healthy sibling cut and exact
repaired-tail checks remain. Original request windows and resource limits are
unchanged. Completed profiles are emitted only after runtime close and zero tail
operations. The RustFS runner requires exactly one complete record per image
mode and retains their hashed ledger; it rejects missing or duplicate profiles,
wrong resource values, missing inventory evidence and incomplete owner proofs.
Eight parser tests exercise these acceptance and refusal cases.

The shared archive-intent helper now belongs to its controller-archive fixture
module. This corrects an ordinary missing-import build failure in the execution
target; the first failed build is retained separately. Production Rust is
unchanged by this fairness follow-up.

Qualified source `1c757c7e` / commit `3bb0d605` passed:

- All 22 ordinary archive-controller tests, with zero failures and the 23
  RustFS-only entries ignored in this native invocation. The actual fairness
  output contains both completed image profiles and 14 shared roots for extents.
- Five exact execution regressions: mixed group roles, hot-group fairness,
  materialized and extent TLS apply/bounded retry, and distinct-history batching.
- Strict library/affected-test Clippy and 113 Python tool tests (one optional
  integration test skipped), plus formatting, layout, dependency, documentation
  and patch checks.

Rust 1.98.1 is explicitly selected. Both retained native executables contain
compiler hash `48a229ceaefd4985c50990b14116b6d856af0985`; test opt-level 1,
zero debug info, assertions and overflow checks remain enabled. The controller
executable SHA-256 is
`456d9b824e81c1de4f8d370fedae0a0417b79680412ad51ae830653b6da43308`.
Source inputs remained frozen through qualification. Records are retained under
`archive-fairness-before-coverage`, `archive-extent-fairness-native` and
`archive-extent-fairness-native-wiring`.

The full configured **23-case RustFS campaign passed** on that same source and
exact controller executable. Seven signed S3 probes, real backend stop/restart,
all case selections and the final backend/source guards passed. The fairness
ledger contains both completed image modes, verifies all 14 restored images in
each and records a shared-inventory minimum of 14 roots for extents. Its SHA-256
is `f3d41346572b6ab1d67233f9791401f1ca60d9a376918021f4452343b461cac5`.
The record is retained under `rustfs-extent-fairness-1c757c7e`. Voters in this
campaign are host-native RF3 TLS tasks; RustFS runs in the owned Colima VM with
the unchanged pinned image and one-CPU/2-GiB/no-extra-swap limits. This does not
qualify independent voter containers or the full scheduling/failure matrix.

## Latest performance and constrained-voter limits

On the preceding production source `f1500526`, all 71 transport unit tests,
22 ordinary controller tests, 32 ordinary archive-protocol tests and the exact
parked-cursor SDK case passed, together with strict Clippy and repository checks.
The combined native campaign remains failed at occupancy 128: enrollment reached
its original 60-second window with an ambiguous deadline error. Its snapshots
and later owner observations are distinct cuts. A separate pinned-source
reproduction with three passive stack samples again failed; the stacks show
manifest-page file synchronization and other persistence barriers. Sampling is
diagnostic evidence, without a measured speedup or causal attribution to one
barrier. Records remain under `peer-idle-native-combined-fresh` and
`enrollment128-f150-phase-profile-after-maintenance`.

The matching Rust 1.98.1 Linux image built as `a3c9d314`. The first constrained
runtime attempt was interrupted by a confirmed stopped Colima VM. After restoring
the same VM and exact RustFS container, a fresh materialized run failed at public
`resume-maintenance` with unknown outcome, before standby or extent mode. It
reached 95 events, including retained hard-kill/restart. Its original operation,
request and resume identities and 20-second command window remain authoritative;
a later fixture cannot resolve that mutation. Five redacted voter logs and
mount inventories are retained. The returning voter opened new ingress, while
its failure snapshot queried the old address; that snapshot cannot establish
that the voter was down. Mount inventories alone do not prove retained disk
contents, and forced cleanup is not joined SDK shutdown evidence. Records remain
under `linux-daemon-999a1fbf5c80-f1500526-standby`, its `-restored` sibling and
`f150-qualification-pipeline`.

Base PR #166 has advanced to fetched `9ccccad7`, adding named bindings, target
sequence sealing, leader-preferred archive publication and retry coalescing.
It is not integrated into this controlled checkpoint. Two fixture conflicts
remain in `cell_group_runtime.rs` and `cell_group_sql.rs`; integration and
combined-source qualification are the next prerequisite. Current-source
constrained voters, standby/replacement, occupancy 128, distinct endpoint/startup
failures, authority and held-owner reclamation cuts, measured placement/fairness
and the complete primitive/backend/performance matrix remain open. No full
CG-13, CG-17 or OPT-01 through OPT-08 package is accepted by these scoped results.

## Base-80 combined native and real-backend checkpoint

Merge `a5b5a8b7` integrates base `80bd02b1`, including named bindings, private
sequence retirement, leader-preferred publication and archive retry coalescing.
Its input digest is `06dbb5b1`. The six retained native executables were built
with Rust 1.98.1 (embedded compiler hash
`48a229ceaefd4985c50990b14116b6d856af0985`), test opt-level 1, zero debug info,
assertions and overflow checks enabled. Compilation took 1,682.002 seconds.
Inputs stayed frozen through the native and backend runs.

The selected native runtime checks passed:

- All 71 transport units, 23 ordinary archive-controller tests and 32 ordinary
  archive-protocol tests. The controller includes the new follower-prefix case
  and both image modes of archive fairness. Its 24 RustFS-only entries and the
  protocol target's seven RustFS-only entries are ignored in native invocations.
- Five execution regressions covering mixed roles, hot-group fairness, both
  image forms of TLS apply/bounded retry, and distinct-history batching.
- The exact parked-cursor SDK case and all four named-binding configurations:
  materialized/extents with Quorum/S3 source durability, under the original
  deadlines and with a Quorum target minimum. These do not qualify a target
  namespace with an S3 minimum or the complete HTTP/RPC primitive family.
- One private binding-seal unit, six manifest-index units and four effect-summary
  units. A supplemental run passed all ten actual `raft::receipts::tests` and
  both private class-contract tests. The initial broad receipt-module filter
  selected two fleet-finalization tests; its recorded scope remains those two.

The complete configured **24-case archive RustFS campaign passed**, including
actual backend stop/restart, follower-prefix verification and the two completed
fairness profiles. It used the same retained controller executable as the native
run and passed all seven signed capability probes and final backend/source
guards. A separate exact RustFS named-binding case passed all four configurations
in 75.596 seconds, with a fresh bucket/prefix and seven signed probes. Its
executable matches the retained native primitive executable. These are
host-native RF3 TLS voters with RustFS in the owned Colima VM. Host Clippy and
the separate Linux image build ran concurrently; these runs establish no timing
comparison or performance gain.

The combined native campaign remains **failed** at three checks. Occupancy 128
exhausted its original window at 60.004 seconds with ambiguous enrollment failure;
full process time was 66.442 seconds. Later serialized observations show 76–77
cells in the selected group, rather than one atomic deadline cut. Its disks,
performance snapshots, observations and executable binding remain retained.
SQL preparation consumed about 25.6/30.5/30.7 seconds of measured operation time
on the three nodes; engine file-sync times were about 12.5/12.6/14.1 seconds.
These durations overlap and cannot be summed as wall time or attributed to one
cause. No speedup is claimed.

Strict all-target Clippy found the archive-intent helper loaded twice in the
archive-protocol target. The source preflight also refused two stale reviewed
anchors. Formatting, layout, dependency, documentation, patch and 113 Python tool
tests passed (one optional integration test skipped). All failed records remain
unchanged under `base80-integration-native`; correct receipt-owner coverage is
retained separately under `base80-binding-contract-units`. Backend records are
`rustfs-base80-06dbb5b1` and `rustfs-named-bindings-base80-06dbb5b1`.

The helper correction gives the shared runtime fixture one archive-intent module
and reuses that function from both controller and protocol callers. The reviewed
successor baseline pins `acb84750` and changes only the effects and CI hashes:
private binding-seal contracts alone do not enable alarm/outbox scheduling;
CI fetches historical Git objects and exports its checkout-owned target directory.
The existing source, excerpt and line checks remain. All 44 design tests pass
after that review; the earlier preflight failure remains recorded. Strict
Clippy verification of the corrected helper is still pending at this checkpoint.

## Constrained Linux maintenance and standby checkpoint

Diagnostic follow-up `acb84750` changes three process-harness files while keeping
production code identical to `a5b5a8b7`. It durably records the original operation,
request, revision, arguments and attestation before the maintenance mutation.
Failure GETs sample the current process's ingress once, retain address provenance
and preserve the original mutation error. The 20-second command, 22-second outer
CLI and two-second diagnostic GET bounds remain unchanged.

Source `372b4822` built Linux image
`d76a4cc24afa48e472aa43f9d42170ec448272e72c329b3d173835d9fab00c8d`.
Both executables contain the same pinned Rust 1.98.1 compiler hash. This image
uses the existing dev/test opt-level 0 and zero-debug-info Docker profile; its
results are correctness evidence, rather than a native profile-1 comparison.

The complete existing daemon campaign with **standby passed in both image
modes**, taking 208.670 seconds overall. The materialized and extent fixture
processes completed in 95.977 and 87.159 seconds, with 130 and 100 ledger events.
Both modes include park/wake, archive outage and original S3 receipt replay,
retained hard-kill/restart, public maintenance stop/return/resume, returned-leader
SQL, and the fourth standby's capacity/binding, zero-group observer and joined
startup-cancellation checks. Each ledger preserves all four exact maintenance
requests, including the resume attestation. The failed-voter replacement gate
was not enabled.

Each mode retains six voter containers with the expected exit inventory
`[0, 0, 0, 0, 1, 137]`. Twelve whitelisted post-teardown inspections and redacted
logs retain one-CPU/3-GiB/no-extra-swap limits and no OOM kills. These inspections
are separate from the runner's live cgroup evidence; mount inventories alone do
not establish retained disk contents or graceful SDK shutdown. The run uses the
same owned six-CPU/14-GiB VM and one-CPU/2-GiB RustFS backend, with separate voter
disks and cgroups in one physical failure domain. Records are retained under
`linux-current-build-acb847506c44-372b4822` and
`linux-daemon-acb847506c44-372b4822-standby`.

The earlier interrupted Linux run and the source-`f1500526` maintenance-resume
failure remain unchanged. A passing fresh fixture does not resolve that original
unknown mutation. Replacement still requires the existing 15-GiB parent guard.
The upstream base has advanced again to fetched `d46ecc1d`, adding autonomous
binding recovery; that implementation is not integrated or qualified by these
records. Occupancy 128, remaining CI failures, authority and physical-owner cuts,
release-scale placement/fairness, full primitive/provider coverage and the final
single-source release matrix remain open. No complete CG-13, CG-17 or OPT package
is accepted by this checkpoint.

## Single archive-intent owner and connected binding recovery integration

Correction `f40c5d39`, input digest `ca0d4e85`, passes strict all-target Clippy
for `cellar` and `cellar-logic`, all 44 design tests, 113 tool tests (one optional
skip), formatting, layout, dependency, docs and patch checks. All 23 ordinary
controller tests, 32 archive-protocol tests and five selected execution tests
pass on three retained Rust 1.98.1/profile-1 executables. The intent helper is
loaded once by the shared runtime fixture and reused by its controller and root
protocol callers; its durable file/write/readback behavior is unchanged. The
first successor failed an ordinary unresolved parent import, retained under
`archive-helper-single-owner`. The corrected source and passing run are retained
under `archive-helper-single-owner-import`. These passes resolve the helper and
reviewed-anchor checks; they do not resolve the earlier occupancy-128 failure.

Merge `521fb92c` integrates upstream `b0356fea`, including the connected
source-claim, authenticated target-retirement and source-lane settlement owner.
The routing integration preserves the existing typed selection errors: terminal
refusals stop selection, stale physical placement refreshes only the selected
route within the original session, and bounded retries retain the exact target
history and request. The receiver keeps its existing boxed future boundary while
adding the private authenticated retirement purpose. No compiler limit, request
window or resource ceiling is enlarged.

The standard runner now has a `named-bindings` suite for the ordinary and
interrupted/restarted binding cases. Each case must emit four distinct completed
image/source-durability profiles with its own recovery flag. The runner retains
hashed ledgers and refuses missing, duplicated, foreign, legacy or failed profile
output. Process-isolated keep-going can collect the other case while retaining
the campaign failure. This covers a Quorum target minimum, rather than the whole
HTTP/RPC family or target-S3, cross-class, concurrent-creation and disaster gates.

The reviewed successor baseline pins the merge and updates only the host anchor
and its scheduler comment. Binding classes require bounded shared recovery even
without optional user alarm/outbox scheduling; startup validates the existing
bounds. Source metadata discovery excludes stored business arguments. The new
class-schema seed and committed call due time require fresh matching namespaces
and binaries; markers remain 1 and no legacy reader is introduced. This records
source review and integration. Runtime qualification of this combined recovery
source is still pending, and all complete CG/OPT acceptance remains open.

## Binding allocator and joined-close follow-up

The published transport repair `87d42d77` sends private target retirement through
`RaftTransport`'s shared state, as command and name calls already do. The first
combined pre-publication check at `8272ead9` emitted E0599 for the missing method.
That owned compiler was stopped after the deterministic diagnostic; its terminal
return 101 and interruption reason remain under `pr167-prepublish-8272ead9`.
This was not a compiler panic. Fresh strict checks of the repaired source use the
same pinned toolchain/profile under `pr167-prepublish-87d42d77`; they remain in
progress at this checkpoint.

Reviewed merge `7192455f` integrates base `90e19b34`. Binding maintenance now reads
only its fixed-size system allocator through the existing SQL query owner and a
linearizable exact route/history check. It takes no command-byte reservation or
pending-effect payload; transient contention repeats only that read within its
original maintenance deadline. Retirement and allocator errors preserve their
causes. Shutdown seals admission and joins controller/SQL/runtime owners before
sampling producer quiescence. Failed joins, poisoned admission, retained responses
and unresolved owners still withhold the proof. This neither resolves a lost
business response nor authorizes deletion of old disks.

All 44 design tests pass on the merge with every reviewed anchor unchanged.
New allocator tests deny pending-payload reads and refuse malformed/exhausted
counters without resetting them. Native execution and real-backend recovery on
this combined source remain pending. The next native selection covers query-wire
separately from transport, chooses the actual Raft receipt owner by a unique test
name and requires four profile records for each ordinary/recovery binding case.
The original occupancy-128 failure, source-bound RustFS/Linux checkpoints and all
remaining CG/OPT gates retain their recorded scopes.

Hosted predecessor CI logs confirm both JavaScript worker adapters fail at
`alice/bounds` with an ambiguous outcome (14 other worker tests pass), while the
Typed RPC 3.14 full job reports incomplete host shutdown in the Rust/Rust ABA
peer-cycle case. These failures need their own reproductions; current-source
native Raft tests alone cannot qualify them.

## Typed SQL cursor admission follow-up

On clean `ee8e23e3`, the unchanged Node Workers storage/recovery case reproduces
hosted CI's `alice/bounds` ambiguous failure with Node 24.22.2 and Rust 1.98.1.
Its retained executable uses the CI test opt-level 0, zero debug info, assertions
and overflow checks enabled. The build completes in 753.507 seconds and the selected
case exits 101 in 5.109 seconds. The same binary also fails a stronger fixture-only
negative control at native cursor admission provenance in 3.605 seconds.
Records are `baseline-workers-cursor` and `fixture-negative-control` under the
isolated checkout's `cellar-tikv-cursor-refusal-6aff/qualification` target.

The fixture assumed 128 retained cursors while the native policy permits 64 live
statements. The guest checks its own 128-item allowance; the native SDK's string
conversion discarded the distinction between refusing before SQL execution and
failing after a statement or prefix ran. The candidate preserves a typed native
`SqlCursorRefusal` for the initial count and SQL-text byte checks only. The guest
attaches pre-effect validation only at that direct operation boundary. Failures
inside preparation, binding, prefix execution, row/result copying or buffer
accounting remain unproved outcomes. The public low-level string-returning cursor
entry point retains its signature. No cursor count, byte cap or request budget
is increased, and a refusal says nothing about earlier enclosing operations.

The fixture saturates the effective policy and reports its actual capacity,
checked against the configured native limit by the Rust host. A separate smaller
adapter capacity checks the other owner and repeated release. Shared native/Raft
SDK assertions retain quota text checks, require exact typed initial refusals,
prove rejected prefixes did not execute and require an executed prefix to survive
a later quota failure without acquiring the refusal type. The guest buffer oracle
also checks a real written prefix before cleanup.

On `e114b360`, frozen source `32ffe248`, all 18 combined Node/V8 worker cases
pass, as do all nine Node-only cases; the Node-only dependency graph excludes
V8. The native storage case and all eight ordinary Raft primitive cases pass
(eight RustFS cases are ignored), including independently checked four-profile
ordinary and interrupted-recovery binding ledgers. Strict all-target Clippy for
the affected core, logic, guest and JavaScript adapters, formatting, layout,
dependency, documentation, design and fixture checks pass. Records are
`typed-cursor-workers-with-bundles`, `typed-cursor-workers-node-only` and
`typed-cursor-sdk-and-clippy-with-bundles` under the same qualification target.
These checks use Rust 1.98.1, Node 24.22.2 and CI test opt-level 0; they do not
replace the profile-1 performance and backend matrix. The two failed controls
remain failed. This compatibility fix grants no full CG/OPT acceptance or
performance claim.

The next integration includes base `d290719c` and its bounded completed archive
verification handoff. Both test-helper exports retain the idle-peer regression
and the late-receipt regression. Qualification of the combined source remains
separate from the completed cursor-fix checks above.

## Immutable manifest pack candidate

The native profile-1 negative control at `3a8ed804`, source `0c4e8c34`, builds
in 576.019 seconds and fails the changed-path test's new one-file-barrier oracle
in 0.336 seconds. The original independently sealed 64-image and authenticated
old/new-root assertions remain intact. The observed second insertion completes
two file barriers instead of one; this is evidence of persistence amplification,
not a measured explanation of the entire occupancy-128 failure.

The [manifest pack candidate](manifest-page-packs.md) streams each immutable AVL
update into one bounded file, retains logical page hashes and leases, accounts
for resident encoded containers and compacts within existing page/byte quotas.
Actual source unlink and directory barriers precede credit. Node maintenance
counts physical file deletions and shares a 96-record relocation budget. The
local marker-1 layout is a hard cutover; old page directories are refused.

The core compile check passes. A fixture check initially rejects the new test's
`GroupLog` generic scope; correcting it to the existing native engine fixture's
scope makes the storage, snapshot and execution fixture checks pass. New owning
tests and the full native profile-1 storage/runtime/backend/container gates still
need execution. Earlier occupancy-128 and original CI/compiler failures retain
their recorded outcomes. No performance or full CG/OPT acceptance follows from
these compile checks.

The first complete all-target check at `f1dd6a1a`, source `d7b1fbae`, rejects
the archive/disaster futures: retaining a Send-only host writer made the page
store lose its existing `Sync` contract. The follow-up contains that writer in a
mutex accessed only through exclusive ownership on blocking workers. It also
corrects the independent fixture's logical-page hash to use the established
domain prefix. A retained original occupancy-128 page independently confirms
that domain-prefixed SHA-256 matches its filename and plain SHA-256 does not.
Production page hashes remain unchanged. The first candidate's native runtime
and remaining checks retain their own frozen source; corrected-source runtime
and full all-target validation remain pending.
The corrected-source default RustFS archive suite also requires the new
late-receipt verification-handoff regression, bringing its fixed case set to 25.
The preceding source's 24-case backend evidence retains its original scope.

The retained first native candidate at `f1dd6a1a`, source `d7b1fbae`, builds in
669.528 seconds with verified Rust 1.98.1/profile-1 executable hashes. Its full
owning storage run completes with 19 passed, three failed and two harness-child
cases ignored. The changed-path 64-image test now passes the single-file-barrier
oracle, as do every preceding storage, quota, held-reader, engine-publication and
process-crash case and the new malformed-framing case. The three new compaction
cases fail at the independent helper's plain-SHA condition before reaching their
compaction oracles. This candidate remains failed; corrected-source evidence
must establish the actual physical-copy and held-root contracts.

The follow-up also preserves the existing page-scan unit and ceiling. A bounded
per-pack cursor resumes record inspection across maintenance calls; a new pin
marks its page live if it arrives after an earlier scan step. Physical unlinks
retain their separate file budget. The owning compaction test now requires two
one-record turns without page reads, barriers or credit. It admits a previously
unpinned reader, then
requires the final one-record turn to preserve both real roots during relocation.

At `04e70637`, frozen source `f4bf00d7`, the complete native profile-1 storage
suite passes: 22 cases, zero failures and two harness children ignored by direct
selection (the parent crash cases launch them). Build time is 592.065 seconds;
runtime is 11.443 seconds. Compiler and executable hashes and unchanged inputs
are verified. This includes all four new compaction/framing contracts, the late
pin between bounded scan turns, all original storage/crash assertions and the
single-file-barrier oracle. Strict native core/logic all-target Clippy also passes
in 924.883 seconds. Other repository checks pass except formatting of the new
compaction assertions; that campaign remains failed for formatting, with the
runtime and Clippy results retained separately. The follow-up formats the file.

The follow-up expands the existing process-kill matrix from 30
to 52 cuts: 26 boundaries in each image mode. It retains all original cuts and
adds full-frame writes and both sides of pack rename, plus eight compaction
copy/publication/unlink boundaries. The parent independently authenticates the
replacement container or its written prefix, requires both source and copy
before unlink and requires selected-root recovery from the copy after unlink.
Every cut still verifies exact recovered SQL/receipts and one durable callback
execution. Per-boundary mode/cut evidence is emitted only after those checks.
This expanded matrix needs its own runtime evidence; no broader lifecycle,
backend, occupancy-128 or full CG/OPT acceptance is inherited.

Sealed pack indexes additionally retain only their actual record count in boxed
slices. The writer's maximum 96-record allocation is temporary, including for
one-page checkpoint imports; the unchanged parent admission now charges the
bounded writer/sealed-index overlap. Current-source runtime and all-target checks
must include this residency correction as well as the expanded crash matrix.

At `b92843bd`, frozen source `ed7a05fe`, the full native profile-1 storage suite
again passes all 22 cases (two directly ignored harness children). The retained
driver independently verifies all 52 distinct mode/boundary evidence rows, 26 per
image mode, including the actual replacement/source container checks. Build time
is 393.879 seconds and runtime 20.486 seconds, with compiler/executable hashes
and unchanged inputs verified. The all-target campaign fails only at its new
`println!` evidence call, which violates the repository's forbidden-macro policy;
format, layout, dependencies, documentation, design, tooling and patch checks
pass. The follow-up uses the existing `cellar::note!` output helper. This failed
Clippy record remains failed, and the queued broader native campaign records
`NOT_RUN_PARENT_FAILED`; it performs no runtime tests. The corrected output needs
its own full storage/52-row and all-target checks before that broader campaign.

## Packed manifest native qualification and backend follow-up

At `6b883dd0`, frozen source `3c86d6fa`, the complete owning storage suite
passes all 22 cases and its retained driver verifies all 52 distinct process-kill
boundaries, 26 per image mode. The two directly ignored harness children are
launched by their parent cases. Runtime is 20.839 seconds after a 10.029-second
build. Strict core/logic all-target Clippy, format, layout, native dependency
boundaries, documentation, 44 design tests, 120 tooling tests (one optional skip)
and patch checks pass on the same frozen inputs. No lint or assertion is weakened.

The broader native profile-1 campaign builds and retains seven executables in
954.663 seconds, independently verifying executable hashes, the pinned Rust
1.98.1 compiler, original assertion/overflow settings and unchanged source.
All 389 selected cases pass: 126 component cases, five ordinary snapshot cases,
193 ordinary execution cases, 24 ordinary archive-controller cases, 32 ordinary
archive-protocol cases, eight ordinary primitive cases and the original
occupancy-128 diagnostic. Both named-binding cases retain all four required
image/source-durability profiles; archive fairness retains both image profiles.
Ignored real-backend entries do not become native coverage.

The original occupancy-128 case completes in 92.055 seconds, including its
commands, follower reads, exact receipts, archives and joined close. Enrollment
of the 129-cell population completes in 27.445 seconds within its unchanged
60-second budget. Its observations and post-close encoded pack inventory are
retained with the actual live process, source and executable binding. Earlier
failed occupancy-128 runs remain failed. Another checkout's compiler is active
at the start of this run; neither this single functional pass nor its difference
from an earlier failure establishes a controlled speedup. Repeated equivalent
workload measurements at occupancies 1/8/32/64/128 remain required.

The first current-source real RustFS campaign fails after six passing archive
cases. Its remote-deadline case returns before the required enrollment S3-wait
state is observed, with the original 800 ms invocation deadline. The remaining
18 archive cases are not run. The separate two-case binding campaign passes
interrupted recovery with all four profiles, but ordinary bindings fail in the
materialized/S3-source profile at command sequence three with an ambiguous
`Runtime(DeadlineExceeded)`; its other three profiles pass. These are failures,
not a complete 25-plus-two backend pass.

Three independent fresh-namespace reproductions of the remote-deadline case
then pass on the exact same retained executable and original limits, in 31.318,
34.122 and 37.092 seconds, while the actual matching Linux builder remains live.
They do not reproduce the failure or establish its cause. The full standard
25-case archive and two-case binding suites are queued to repeat after the
owned Linux build and focused reproductions actually finish, with original
failures preserved and external host compiler activity observed separately.
Matching constrained Linux execution requires those complete backend suites to
pass. A successful image build alone supplies no runtime evidence.

The repeat after the owned build finishes passes the previously failing remote
deadline case, then fails in `rustfs_s3_remote_slow_delivery_retains_proofs_and_bounds`
with an unlocalized elapsed deadline after 41.21 seconds. Seven archive cases
pass; 17 are not run. That full archive campaign remains failed. Both named-binding
cases pass on this repeat in 160.546 seconds, each with all four required profiles
and the exact retained native executable. This does not erase the first ordinary
binding failure. Matching Linux image
`sha256:8ce943291bdcdc8e21bd8b3ec8c99f5b440dedcc324c516ed2713a314d8f6bcb`
builds under the original parent limits; both queued runtime pipelines record
`NOT_RUN_PREREQUISITE_FAILED` because their full archive prerequisite fails.

Three fresh-namespace slow-delivery reproductions, with Rust/anyhow backtraces
enabled and the owned build finished, pass in 68.859, 61.793 and 57.878 seconds.
They use the unchanged source and executable, original wire deadlines and S3
envelope, and the standard runner's signed backend checks. The failure remains
unlocalized; these focused passes do not replace the failed full archive suite.

A subsequent public test candidate adds
`recent_source_expiry_keeps_physical_image` through SDK, native HTTP and Axum in
both image modes. It configures a 500 ms source-observation lifetime within the
original five-second invocation and callback bounds. One selected follower's
actual SQL callback holds its immutable image; the invocation must refuse with
`VersionUnavailable` before its outer deadline, retain its read pin and ordinary
admission after refusal, and release them only after physical SQL exits. A new
S3 command and fresh source read verify the subsequent image under the same term,
without a ReadIndex barrier or historical fallback. This candidate still needs
compilation and execution; no new runtime acceptance is claimed for it.

Public wire-authority and ambiguous-response cuts, physical-owner and repeated
movement/settlement qualification, broader fairness and placement measurements,
failed-voter replacement/all-disk-loss, full SDK/provider compatibility and the
single-source release matrix remain open. These checkpoints accept no complete
CG-13, CG-17 or OPT-01 through OPT-08 package.

## Public source expiry and matching packed runtime

At test-only follow-up `3261b2fe`, source `d572b657`, the retained native
profile-1 build completes in 1056.624 seconds. The new public expiry case passes
in 25.678 seconds, with all six SDK/native HTTP/Axum and image-mode profiles
independently checked. Each returns `VersionUnavailable` in 308–464 ms within
its original five-second request budget, retains one physical image pin and one
ordinary admission while SQL remains held, then releases both after physical
completion. The next S3 command and fresh query preserve the exact cell result,
version and receipt without a ReadIndex barrier or historical fallback. The
existing preference case passes in 39.328 seconds. Strict core/logic all-target
Clippy passes in 1121.486 seconds; all eight source/static checks pass.

That first campaign remains failed: the cohort case times out in its prerequisite
`first-group standby` CLI call after 5.971 seconds, before any cohort assertion.
Its original five-second CLI limit and retained operator directory remain intact.
A fresh-process follow-up explicitly retains and selects the matching CLI,
verifies both compiler/executable identities and reuses only those unchanged-input
static checks. Expiry and preferences pass in 23.211 and 42.237 seconds; three
complete independent cohort attempts pass in 42.088, 42.163 and 51.250 seconds.
All six expiry profiles are checked again. The failed CLI attempt is not relabeled
or attributed to the executable path. Hashing/copying observes executable file
data; these passes are not cold-start or controlled speedup qualification.

The complete standard RustFS campaign at packed-runtime checkpoint `6b883dd0`,
source `3c86d6fa`, then passes all 25 archive cases in 215.534 seconds and both
four-profile binding cases in 147.798 seconds. It retains the exact native
executables, signed backend probes, original deadlines, original backend caps
and frozen inputs. Rust/anyhow backtraces are enabled for diagnosis. The first
two failed campaigns and all focused repetitions retain their original outcomes;
their transient deadline causes remain unresolved.

Matching Linux image
`sha256:8ce943291bdcdc8e21bd8b3ec8c99f5b440dedcc324c516ed2713a314d8f6bcb`
passes the complete existing daemon campaign with standby, native/Axum ingress
and real RustFS in both image modes in 203.688 seconds. Both daemon and CLI ELF
binaries have the pinned Rust 1.98.1 compiler identity. Materialized and extent
fixtures exit zero in 86.198 and 95.288 seconds. The original six-CPU/14-GiB VM,
one-CPU/3-GiB/no-extra-swap voters and one-CPU/2-GiB backend remain enforced.
Independent inspection verifies all 12 retained voter-log hashes, run/image
identities, voter caps and no OOM. Post-teardown exits include intentional fault
stops; this inventory is not proof of graceful close for every file or container.

These are distinct source checkpoints: the new read qualification is a test-only
follow-up, while the full native/storage/backend/Linux packing evidence is bound
to `6b883dd0`. The subsequently fetched base `7c893506` adds grouped SQL bindings
and RPC error compatibility changes. Their integration needs its own checks and
cannot inherit these broader combined-source results. Failed-voter replacement,
independent domains, physical hard-storage bounds, repeated performance, full
primitive/provider compatibility and all remaining CG/OPT acceptance stay open.

## Grouped SQL integration and operator certificate I/O

Combined source at `ca5f92df`, digest `296e79b9`, integrates base `7c893506`
with packed manifests and the public source-expiry regression. Its native
profile-1 campaign passes 51 selected cases: two SQL, 15 Raft host, 22 storage,
nine ordinary primitives, two RPC reply-envelope and one public cohort case.
An independent audit verifies all 52 storage crash cuts on its retained
executable. All eight static/source checks pass, including strict core/logic
all-target Clippy. The build and checks preserve the pinned compiler, original
assertions, limits and frozen inputs.

That campaign remains failed. Expiry and preferences stop in their prerequisite
five-second standby CLI call, before their proof oracles, in 37.384 and 7.422
seconds overall. Cohorts pass in 47.234 seconds. A later diagnostic repeats the
exact preference executable and matching retained CLI in three fresh processes:
all pass in 32.993, 34.823 and 34.939 seconds. External monitoring observes no CLI
child long enough to trigger its 2.5-second sampling threshold. These repetitions
do not identify a cause or replace the failed campaign.

The actual compiled dependency graph enables both native and bundled certificate
roots in reqwest. Five alternating isolated construction probes observe native
root loading at 139–176 ms versus 43–48 microseconds with only native loading
disabled. These contextual component measurements are not a workload speedup or
cold-start qualification.

A separate induced control runs the actual retained operator CLI against an
independent loopback HTTP standby service. An empty regular certificate file
allows all three requests in 10–12 ms. Replacing only that file with an owned
unwritten FIFO blocks all three invocations until their original five-second
deadline; none reaches the HTTP service. Process samples locate the wait in
reqwest client construction, native certificate loading and file open. This
establishes an unnecessary certificate-store I/O dependency for HTTP. It does
not establish why the original, uninstrumented standby calls timed out.

The follow-up disables built-in certificate acquisition for HTTP management
URLs and retains it for HTTPS. Redirect refusal remains in force. Three actual
CLI regressions cover the blocked certificate source, explicit HTTPS native-root
trust and rejection of an untrusted HTTPS server. The separate test-only source
`35cc0f95`, digest
`76ec66fe`, passes its native faulty-wire advice case in 87.586 seconds. Its
independently checked 60 rows cover ten advice/authentication faults across SDK,
native HTTP and Axum in both image forms, with no SQL entry during refusal and
once-only S3 replay under the original command identity. Its strict all-target
Clippy check fails because the new responder uses the forbidden Tokio select
macro. The follow-up switches both new fixtures to Cellar's fair polling macro;
the failed lint record remains failed.

The combined checkpoint `89ad604c`, source digest `ec6f482f`, passes its complete
selected native campaign with frozen inputs. All six CLI cases pass in 17.576
seconds, including the three certificate controls. Faulty-wire advice passes in
80.740 seconds, with 60 independently checked observations covering all ten
faults, three ingress paths and both image forms. Entered-source expiry passes in
14.331 seconds with all six profiles independently checked; preferences and the
fresh-barrier cohort pass in 27.787 and 28.276 seconds. These are ten test cases,
not 60 separate test functions. Strict core/logic all-target Clippy and all seven
other static/source checks pass. Retained executable hashes, compiler identity,
native profile-1 settings, source inputs and evidence ledgers are recorded under
`qualification/cli-http-roots-fair-advice` in this checkout's target directory.
The original five-second deadlines, assertions and resource limits are retained.
This scoped pass does not explain or replace the earlier startup failures and
does not qualify a later base integration, current-source RustFS/Linux or
controlled performance.

Removed-member, untrusted-certificate and superseded-boot
coverage remains open. The preceding RustFS/Linux results remain tied to
their exact revisions; no full RO-03, CG-13, CG-17 or OPT package is accepted.

The subsequent PR integration includes base `ab06b48a`, which adds stateless SQL
ingress and reuse of locally applied binding histories. The counter registry
retains both branches' counters, and the baseline retains both review histories;
all anchored file hashes still match the reviewed `89ad604c` checkpoint. Runtime
results above remain bound to their stated revisions.

Combined checkpoint `8d578fba`, source digest `837c0127`, passes all 18 selected
native cases: five performance-registry, two native SQL, one public SQL-binding,
six CLI and four public routing/read cases. SQL bindings cover both image modes
in 113.549 seconds; CLI cases pass in 18.004 seconds. Faulty-wire advice passes
in 86.093 seconds with all 60 observations independently checked. Entered-source
expiry passes in 15.086 seconds with all six profiles independently checked;
preferences and the cohort pass in 27.932 and 26.506 seconds. Strict core/logic
all-target Clippy passes in 1208.402 seconds, and all seven other static/source
checks pass. The build preserves native profile-1 settings, the pinned compiler,
original deadlines and assertions. Seven executable identities, their original
Cargo artifacts, source inputs and both evidence ledgers are retained under
`qualification/pr167-sql-ingress-base-ab06b48a`; its source/HEAD/clean-tree guard
remains unchanged through completion.

This qualifies the selected native integration. Earlier failed records remain
failed, and the original uninstrumented standby timeouts remain unexplained.
Current-source RustFS/Linux, broader native/storage/primitive coverage, measured
performance and full CG/OPT acceptance remain open. The base's recorded RustFS
interrupted-binding failures are not resolved by these native results.

## Certificate and actual-boot wire authority

Checkpoint `0a923923`, source `f43eecff`, passes all twelve selected native cases
and all eight static/source checks. The actual untrusted-certificate case passes
in 12.960 seconds with all six SDK/native HTTP/Axum image profiles independently
checked. Each cached trusted connection is joined before changing certificates;
the real client sends `UnknownCA`, no RPC or SQL producer enters, and restored
trust and permission reuse the original command once with S3 proof. The wire
case passes in 83.081 seconds with all sixty captured request/reply observations
checked against the stricter framing, correlation and authenticator ledger.
Native profile-1 settings, original five-second windows, executable identities
and frozen inputs are retained at `qualification/untrusted-tls-advice-native`.
These runs use in-memory object storage, not a current RustFS/Linux deployment.

Checkpoint `26b045da`, source `3f38fcd6`, passes all thirteen selected native
cases, fresh strict core/logic all-target Clippy, and all seven same-source
repository checks. The latter are reused only after verifying the preflight's
exact HEAD, complete source manifest, clean tree and retained record hash. The
actual issuer-restart case passes in 39.709 seconds across six fresh caller/image
fixtures. Its controlled call/boot changes take 314–601 ms within the original
five-second budget. Public inspection verifies a successor physical boot on the
same engine; all old tails are written to their owned TLS streams after current
trust and a genuine owner query are installed. This records local writes, not a
claim that the client decoded the old reply. No SQL/callback or non-commit proof
is admitted, ambiguity remains, and both the first recovered S3 command and exact
replay use one cached RPC. The matching wire, certificate, origin-revocation,
read-expiry, preference, cohort and CLI controls pass on the same retained
executables. Records are under `qualification/superseded-boot-advice-native`.

The first boot-source preflight fails at the evidence JSON macro's recursion
limit; the next fails at an invalid SDK query variant. Both remain failed with
their seven passing static checks. The corrected checkpoint uses smaller evidence
objects and the existing `Linearizable` API; no request bound or assertion is
relaxed. These are scoped native authority checkpoints, not full RO-03, CG or
OPT acceptance.

The subsequent integration includes base `55fff63a`, with bounded stateless SQL
identities and nonce-bearing RPC envelopes. Its changed production source needs
new combined validation; it cannot inherit the preceding checkpoints. Actual
combined authority coverage, current-source RustFS/Linux, broader native and
primitive/provider coverage, physical-owner accounting and performance gates
remain open.

## Removed-member wire authority checkpoint

Checkpoint `543634b8`, source `2df204aa`, passes the focused native
`removed_member_advice_cannot_grant_authority` case in 62.656 seconds. The
independent ledger checks all six SDK/native HTTP/Axum image profiles. Each
fixture actually replaces and retires an original voter, moves leadership to
member 5, verifies physical inventory exclusion and establishes a successful
linearizable query through the new voter before injecting authenticated advice
that names the removed member. The command enters no SQL producer; its complete
pre-admission refusal preserves a known non-commit outcome. Restored permission
reuses the original command identity once with S3 proof, with unchanged current
membership, term and logical range.

The first candidate `0ce7cad1`, source `aa899b54`, passes its source preflight but
fails its final replay assertion; an unchanged repetition reproduces that
failure. A focused probe shows that both tokens authenticate the same canonical
version and witness, all receipt/callback invariants hold, and only token bytes
differ. Tokens include a fresh issue time on each reply. The corrected oracle
authenticates both tokens and checks their decoded version and witness exactly,
while retaining command identity, result, durability, coverage and callback
assertions. Two diagnostic attempts fail earlier with `StalePeer`; those remain
separate setup failures. The corrected fixture uses the existing current-trust
inspection barrier before membership inspection, without claiming that this
establishes the cause of those earlier failures.

The frozen source, executable identities and six captured wire observations are
retained under `qualification/removed-member-authenticated-replay`; the evidence
file SHA-256 is
`09adeb1943ca02cfd3db3fa0acd91e7030369f6ccfdb3a3fa09d9f1b8900acb6`.
This uses in-memory object storage and the original request/resource bounds.
It does not qualify current-base integration, strict Clippy, RustFS/Linux,
full RO-03, CG-13, CG-17 or OPT-01–08.

## Physical archive reader cancellation candidate

The next RL-02 component regression holds an actual immutable image/extent read
inside checkpoint upload, then drops its asynchronous caller. It advances every
populated cell through the existing Raft/apply fixture and attempts bounded
physical collection. Source and archive reservations must remain charged, and
the old manifest page and physical image bytes must remain exactly unchanged,
until the physical file reader exits. Only then may collection remove the old
closure. A fresh upload and independent receiver must verify the current cut.

The ordinary `archive_cancelled_waiter_keeps_physical_checkpoint` case and its
ignored RustFS counterpart cover both image modes. The new fixed
`archive-owners` suite requires both hash/owner ledgers and retains their actual
runtime provenance. The existing checkpoint fixture accepts an explicit image
filesystem for this cut; its ordinary callers preserve their existing adapter.
No request or parent resource ceiling changes.

This candidate still requires compilation and runtime validation. Synthetic
verifier controls prove only evidence refusal. Other reader/snapshot/transfer
cuts, Fleet settlement, allocated-block accounting, constrained voters and full
RL-02/CG/OPT acceptance remain open.

## Current-base checks and archive physical-owner evidence

Review checkpoint `59288cb2`, source `8ec3cfed`, passes strict core/logic
all-target Clippy and all seven repository checks. The corresponding native
checkout has source `5dfb5855`; its 3,278 shared input hashes match, while the
review checkout additionally contains sixty ignored generated JavaScript
artifacts. These full input manifests remain distinct.

The first native runner aborts after 29 passing performance/SQL/receipt/CLI
functions because its inventory expectation says 22 ordinary routing cases;
the executable contains 21. The corrected runner checks their exact names.
That campaign passes 45 functions, then fails the superseded-boot case at the
original five-second initial-group readiness prerequisite on voter 2. Four
SDK/native HTTP boot profiles complete before startup of the Axum/materialized
fixture fails, before fault injection. The four later routing cases are not run.
Two exact unchanged-binary repetitions complete all six profiles in 42.514 and
45.801 seconds; those diagnostic passes do not establish the original cause or
relabel the failed campaign. The same setup/close path is being isolated for a
temporary repeated-startup diagnostic, with original budgets unchanged.

Archive-owner checkpoint `eaa86194`, source `1d97ce33`, passes its focused
physical-reader cancellation case and all 33 ordinary archive tests; eight
RustFS tests remain ignored. All eight static/source checks pass, including
fresh strict Clippy. The two physical-owner image ledgers are independently
checked. The first full-target runner incorrectly classifies a nested ignored
RustFS test by its name prefix; it retains its failed campaign record with the
actual 33-passed/eight-ignored summary. The corrected runner asks libtest for
its ignored inventory. Exact inputs, executables and records are retained under
`qualification/archive-physical-owner-native-ignored-inventory`.
All 3,281 inputs match the Git archive byte for byte; this establishes source
identity for a future Linux build, not Linux qualification. Fleet byte credit,
other held-owner cuts and full RL-02/CG/OPT acceptance remain open.

## Performance collector registry integration

Hosted CI at `59288cb2` fails its Python 3.14 job before adapter execution, in
`test_metric_contract_matches_the_native_owner`. The same existing regression
fails locally on Python 3.14 and 3.15: the current native registry has 68
counters, and the collector has 67. Its sole missing entry is
`cell_binding_applied_history_hints`; inserting it at native index 54 preserves
every other name and ordering. The collector now includes that existing native
counter. No native counter, parser assertion or request/resource bound changes.
The full 317-test performance guard suite now passes on Python 3.14 in 17.044
seconds (two skips) and Python 3.15 in 16.427 seconds (one skip). The existing
exact-registry regression fails before this change and passes afterward. The
prior eight-check campaigns did not include this separate CI step; these guard
results do not qualify the skipped Python adapter/ABI runtime steps.

## Combined native failure and archive-completion integration

Checkpoint `2c8be2ff`, source `be0c5f1a`, passes all ten source checks,
including strict core/logic all-target Clippy and the 317 performance guards
on Python 3.14 and 3.15. Its physical archive-owner case passes both image
profiles, and all 33 ordinary archive cases pass; eight RustFS cases remain
ignored. The canonical checkout and Git archive have identical 3,281-file
input manifests.

The combined native campaign fails: 49 selected functions pass and
`removed_member_advice_cannot_grant_authority` fails. The runner continues
through every independent ordinary routing case. In the first SDK/materialized
fixture, real voter replacement reaches Joint, but trust refresh to retiring
voter Node5 remains unreachable at the original deadline. The retained snapshot
shows Node5 closing with zero registered groups while replacement voter Node8
remains active. Joined shutdown reports
`unregistered group event`. This occurs before wire fault injection, and no
completed removed-member evidence row is credited. Two exact unchanged-binary
repetitions pass all six profiles in 56.138 and 56.174 seconds. They establish
neither the original cause nor a repair; the failed campaign remains failed.
Independent ledgers verify eleven completed fixed routing cases, with the
removed-member case still pending.

Hosted CI at the same head proceeds beyond the corrected performance guard,
then fails the Python 3.14t bridge lifecycle case `--runtime-delivery` with
`delivery held consumer admission deadline`. Its shutdown report records
`host_failed=true`. The cause remains unestablished; this is separate from the
native controller lifecycle failure and the earlier startup failures.

The next integration includes base `a5f6183c` and its archive completion,
covered-output proof and joined-retirement fixes. The two merge conflicts are
test export lists: both the existing peer-idle probe and the base's deferred
retirement regression are retained. The prior checkpoint results above do not
qualify this new combined source. Integration checkpoint `a3ee932d`, source
`a26d602b`, is running fresh source checks, both joined-owner library regressions
and the ordinary archive/controller targets. This documentation follow-up
preserves its runtime/tool inputs; the canonical checkout remains frozen while
those checks run. Fresh affected-boundary results are pending;
current-source RustFS/Linux, the open lifecycle failures and full CG/OPT
acceptance remain open.

## Archive integration failures and detached-member classification

Integration `a3ee932d`, source `a26d602b`, completes all ten source checks and
both joined archive-owner library regressions. All 33 ordinary archive cases
pass, with eight RustFS cases ignored. The archive-controller target fails:
22 cases pass, three fail and 26 remain ignored. The failed cases are
`archived_progress_wakes_commands_without_maintenance_poll`,
`s3_command_requires_the_sibling_archive_dependencies` and
`s3_enrollment_deadlines_preserve_resolved_ownership`. The aggregate runtime
campaign remains failed with 57 passing functions and three failures; its
source guards remain unchanged.

Two exact unchanged-binary repetitions reproduce the wakeup deadline. The
dependency and enrollment cases pass those isolated repetitions without
establishing their original causes. Running the existing late-receipt control
before the dependency case enables the full cohort's logging configuration and
reproduces `incomplete group must refuse S3` in 2.929 seconds. This is a fast
diagnostic at the original assertions and limits, not a repaired campaign.

The earlier `2c8be2ff` retiring-voter failure also reproduces on the sixth
unchanged-binary repetition: SDK/materialized completes, then SDK/extents
stops at Joint with retiring Node6 closing and replacement Node8 still active.
The preceding five repetitions pass all six profiles. The actual `GroupRoster`
transition regression independently fails for `ParkedRemoved` while its
`ParkedRetired` control passes. The event guard had recognized only the latter.

The candidate now recognizes both physically detached states. After volatile
reclamation, it also accepts a completed typed member-fence receipt, whose
engine lookup validates history, node, disk, storage instance and old-key
absence. Unknown histories and retained apply tickets remain refused. No
serving route, membership authorization or durability proof is created by this
metadata handling.

Managed-checkout source `787fc27c` passes both lifecycle tests and strict library
Clippy with Rust 1.98.1, optimization level 1 and assertions/overflow checks
enabled. Formatting and layout pass. The initial fixed-source build instead
hits Rust's incremental `evaluate_obligation` fingerprint ICE before tests run;
that failed record and the pre-fix executable are retained. Disabling only
incremental compilation produces the passing component result. The managed
input manifest still includes sixty ignored generated JavaScript assets and
is distinct from the canonical manifest. Fixed-source RF3 replacement, member
reclamation/return, all-target integration and RustFS/Colima qualification are
pending. The failed native and archive campaigns above remain failed; no full
CG/OPT gate is accepted.

## Detached-member RF3 checkpoint and SQL base refresh

Committed checkpoint `f9ceaa17`, managed source `787fc27c`, passes three
complete removed-member repetitions through SDK, native HTTP and Axum in both
image modes. Each repetition has six independently validated profile records.
The actual removed-member physical reclamation and return with a fresh member
ID also passes in both images, together with all six matching CLI controls.
The retained executables use Rust 1.98.1, optimization level 1, debug information
disabled and assertions/overflow checks enabled. Incremental compilation alone
remains disabled after the recorded compiler ICE. Source guards are unchanged.
These are targeted native, in-memory checks; they do not repair the earlier
failed combined campaigns or establish current RustFS/Linux qualification.

PR #166 advances to `418cf719`, adding public SQL operator/CLI and interrupted
SQL recovery coverage. The follow-up integrates that base while preserving its
existing routing, archive-owner and named-binding qualification suites. The
Python runner keeps both captured CLI environment contracts and the SQL test
profile record. Its new SQL campaign fixture resolves the temporary root before
comparing paths, matching the runner's canonical output path on macOS. Before
that fixture correction, all three new SQL runner tests fail on `/var` versus
`/private/var`; afterward all 155 runner tests pass, with one optional skip.
This runner evidence is separate from execution of the Rust SQL/RustFS cases.

The admission-scoped S3 verification-reuse candidate remains local to the
managed checkout and under verification. It is excluded from this integration.
Archive dependency refusal, the maintenance-independent S3 wakeup, startup and
Python delivery failures, exact-current-source combined/backend checks and
full CG/OPT acceptance remain open.

## Archive reuse and progress wakeup candidate

The sibling-dependency failure reproduces at `f9ceaa17` with a passive test
observer: the initial result is Quorum, its retry returns S3, the deleted sibling
chunk remains missing and neither chunk nor index reads occur between deletion
and that response. The retained diagnostic source is `1917cd8e`. No archive
closure was repaired during the unsafe acknowledgement.

An unpublished admission-scoped reuse candidate, source `5b860c5f`, passes three
logged dependency/late-receipt pairs, held-retirement replay and enrollment
controls. Its wakeup case still fails after 20.902 seconds, so the campaign
remains failed. All source guards remain unchanged. A subsequent scheduler
diagnostic, source `458973f3`, reproduces the original wakeup failure in 21.528
seconds: proof against cut 5 defers until the 60-second maintenance interval;
publication of cut 6 leaves both retry clocks deferred with a queued waiter.
This rules out lost queue membership in that reproduction.

The candidate is now based on `0a7d6acc`. Physical retirement protection remains
separate from reuse eligibility. A fresh publication seals its admitted cohort
on controller completion; revalidation of an existing cut cannot authorize a
later Quorum-to-S3 upgrade. Already-proved outputs can share their still-live
physical verification. Retry state is per group, and new physical verification
completion wakes that owner's proof lane. A weak identity captured at batch
launch prevents a later completion from restoring older backoff. These weak
notifications retain no physical reader or archive admission and grant no
serving or durability authority. No task, timer or workspace is added.

Temporary diagnostic probes and test counters are removed. Current-source
format/layout/type checks, complete archive-controller and ordinary archive
targets, the four reuse-policy tests and two physical-join regressions are
pending. Existing deadlines, image configurations, resource limits and negative
oracles remain unchanged. RustFS/Linux and full CG/OPT qualification remain open.

The first typechecked wakeup candidate, managed source `c19e4245` and canonical
source `d5a9abc0`, passes nine source checks but fails strict Clippy: carrying
`CoverageUse` as the weak progress notification expands the proof-completion
variant to at least 632 bytes. Its native no-run build is stopped before runtime
tests; the source campaign remains failed and the native build is recorded as
aborted. The revised notification contains only an opaque weak coverage
identity, with no copied hold, new allocation or physical admission. Library
Clippy now precedes native code generation in the candidate validation loop.

## Archive proof reuse and wakeup checkpoint

The compact candidate passes all ten source checks at canonical source
`2a524031`, including strict core/logic all-target Clippy. Managed native source
`5ffcf68b` passes all 25 archive-controller cases and all 33 ordinary archive
cases. Its four reuse-policy tests and two physical-join regressions also pass.
Three complete wakeup repetitions pass in 2.699, 2.703 and 2.629 seconds, within
the original 20-second deadline and with both image modes. Three logged
dependency/late-receipt pairs pass at unchanged fault and refusal assertions.
The full controller run includes the held-retirement and enrollment controls.

The three retained executables use pinned Rust 1.98.1, optimization level 1,
debug information disabled, assertions/overflow checks enabled and incremental
compilation disabled. Full source guards remain unchanged. Canonical and
managed inputs have identical common file bytes; the managed manifest also
includes the same sixty ignored generated JavaScript assets recorded earlier.
These input manifests remain separately scoped.

The broader native campaign is running on the frozen managed source: all twelve
ordinary primitive cases, all 21 ordinary routing cases, and the existing
performance, SQL identity, receipt, native SQL and CLI checks. This expands the
earlier 50-function selection to 61 functions. No result is claimed for that
campaign yet. Current RustFS/Colima/Linux, startup/Python delivery and full
CG/OPT qualification remain open. Earlier failed campaigns and the aborted
pre-compact build retain their original outcomes.

## Queue and payload base integration

The base advances to `7e75c1ad`, adding durable native callbacks, Queue and
namespace-bound immutable payload protocols. Source checkpoint `47dbc26f`
integrates it with the archive corrections. The qualification runner preserves
the existing routing, archive-owner and named-binding suites and both new
Queue/payload suites. Frozen CLI selection covers every relevant suite, and
payload-only reports claim neither voters nor image modes. All 159 runner tests
pass, with one optional skip; all 44 design tests and documentation checks pass.

The reviewed baseline combines the optimization host/CI anchors with the new
catalog/birth anchors. Its complete 24-anchor preflight passes against
`47dbc26f`. The runtime baseline and case acceptance remain unchanged. This
review neither recreates nor waives the missing historical CG-01 evidence.

Fresh combined-source Rust and repository checks are pending. The 61-function
native campaign remains on its unchanged pre-Queue archive checkpoint
(`5ffcf68b`, matching the published `45763d34` code); it cannot qualify this newer
combined source. Current combined runtime, Queue/payload recovery matrices,
RustFS/Colima/Linux and the full CG/OPT gates remain open.

## Combined-source corrections under verification

The first Queue/payload integration, source `9d65f134`, passes ten source checks
and fails strict Clippy because the automatically merged native-work SQL helper
appears after the retained test module. Its body is moved unchanged beside the
framework SQL functions. The failed source campaign retains its original result.

The pre-Queue native campaign finishes with 60 functions passed and one failed:
ordinary named bindings in Extents/S3 at client sequence 3 report
`archive journal changed during read`. All 21 routing cases pass with their
fixed ledgers verified. This campaign remains failed. A narrow probe of the
unchanged production read helper reproduces the error when a real atomic rename
occurs between pathname metadata and opening the file; four bounds/type controls
pass. Selecting size and bytes from the opened descriptor makes all six probe
cases pass. This does not qualify a complete journal, caller or combined host.

The combined candidate retains pathname type/size checks, adds explicit bounds
on the opened descriptor and preserves canonical-envelope and ownership checks.
New full journal tests cover atomic replacement, allocation bounds, malformed or
noncanonical state, foreign identity and lost state. Full journal and public
binding tests, archive regressions and fresh source checks remain pending.

## Verified journal checkpoint and Queue regression

Published review head `45709b74` contains the journal correction and the combined
Queue/payload base. All eleven source checks pass at canonical input `e0183245`,
including strict core/logic all-target Clippy and all 24 source anchors. The 44
design, 159 runner and both 317-test Python performance suites pass with their
recorded optional skips. All 3,297 committed source-input paths match that
canonical snapshot. Managed input `6fdf4c99` includes the same sixty ignored
generated JavaScript assets and remains separately scoped.

The matching native component campaign passes all five journal regressions,
four archive-reuse policy tests, both physical-join tests, 25 controller cases
and 33 ordinary archive cases. Three complete ordinary named-binding repetitions
pass in 59.827/65.035/68.432 seconds with all four configurations verified each
time. Wakeup repetitions pass in 3.086/2.494/2.410 seconds under the original
20-second deadline; all three logged dependency/late-receipt pairs pass. Source
guards remain unchanged. Earlier failed campaigns retain their original results.

The broader 64-function native campaign uses the identical retained library
executable and includes Queue and immutable-payload coverage. It finds an
ordinary Queue failure in Extents: source command sequence 2, the first DLQ
metrics call, receives an ambiguous nested enrollment
`Execution(Refused(Unavailable))`. Independent cases continue, and this campaign
cannot receive a passing result. An unchanged-binary repetition is diagnostic
only. Current RustFS/Colima/Linux remains unstarted pending native correctness;
hosted CI and the complete CG/OPT gates remain open.

The 64-function campaign finishes failed: 63 functions pass and the ordinary
Queue case fails as described above. All 21 routing functions pass, including
all twelve fixed routing ledgers and the source-expiry observations. Source
guards remain unchanged. The first unchanged-binary Queue repetition passes in
153.805 seconds, without establishing the original cause or repairing the
failed campaign. The queued backend/Linux pipeline records
`NOT_RUN_NATIVE_PREREQUISITE_FAILED`; it starts neither Colima nor RustFS.
A diagnostic of the actual first DLQ metrics prefix is pending. No current
combined-source backend, hosted CI or full CG/OPT acceptance is granted.

## Cross-deployment Queue base integration

The base advances to `fe4cd801`, adding deployed application selection and Queue
ownership, bounded callback budgets, and the fault/long-callback/load suites.
Unpublished source checkpoint `35cf990d` combines it with the optimization code.
The merge preserves small-segment and periodic-purge policies and every routing,
archive-owner, named-binding, SQL, Queue and immutable-payload runner contract.
It also removes an automatically duplicated native-work SQL helper, retaining
the previously qualified implementation and placement. Formatting passes and
all 161 runner tests pass with one optional skip. The combined 24-anchor source
review passes against the actual Git checkpoint; the historical runtime baseline
and release acceptance remain unchanged.

The incoming implementation has two callback slots sharing the existing byte
ceiling. Its legacy Queue fixture explicitly keeps a five-second callback
window, and public routing retains its sixty-second maximum; only private native
work admits the separate bounded callback window. This resource profile cannot
repair the failed one-callback campaign. That campaign remains failed, and its
original limits are retained in a separate diagnostic artifact.

The first narrowed metrics diagnostic completes sixteen original Extents
prefixes successfully. Twenty-three internal enrollment refusals all resolve to
an existing target. An initial classifier incorrectly matched that handled
refusal text; an independent audit corrects it to no original outer-failure
reproduction. The raw successful test and initial classification remain recorded.
A complete original-case loop with the same tagged observations is running.
Fresh combined-source Rust and repository checks, the actual Queue correction,
current RustFS/Colima/Linux and hosted CI remain required. No CG/OPT acceptance
is granted.

All eleven combined-source checks subsequently pass at input `43e90629`,
including strict core/logic all-target Clippy, the 24 reviewed source anchors,
44 design tests, 161 runner tests and both 317-test Python performance suites.
Optional skips remain recorded and source guards are unchanged. This source
qualification preserves the separate one-callback failure and grants no new
combined runtime, backend or release acceptance.

## KV and callback-retention base integration

The next integration pins fetched base `da66d967`, including the Queue
callback-retention/wake changes, ordinary shared-group KV and the RPC execution
deadline correction. `origin/main` remains `d0c2873f` and is already an ancestor
of the optimization branch. The five merge conflicts are resolved by retaining
both sets of fixture controls: payload settings beside source-expiry and
small-segment/purge policies, archive/range faults beside payload counters, and
all routing/named/SQL/Queue/KV runner provenance. Existing deadlines, byte
ceilings and ownership assertions are preserved.

Formatting and the merged runner tests pass. This is an integration preflight;
strict combined-source and runtime verification remain pending. The incoming
base's own KV/RustFS and ordinary S3-binding failures remain recorded in its
handoff. Its two-callback profile does not repair the earlier one-callback Queue
failure. The automatic name-discovery correction is being verified separately
against that original profile before combined-source qualification. Neither
integration nor a passing diagnostic grants CG/OPT acceptance.

## Public read-cohort RustFS candidate

The read-cohort fixture gains an explicit RustFS entry point and separate full
and configuration prefixes for each image mode. It retains SDK/native HTTP/Axum
admission, the original persistence/apply cuts, five-second windows and
two-request bound. Fresh post-configuration reads now use the same ingress as
their refused predecessors. Joined fixture completion is emitted only after
the work, node close and execution-domain shutdown succeed.

The `routing-read-cohorts` runner requires all four completed profiles, exact
request/disposition coverage, S3 receipt replay, per-caller cancellation and
recovery, fresh barrier counters, and retained/drained physical image owners.
Memory-backed completion cannot satisfy its RustFS ledger. Synthetic/refusal
tests verify the evidence boundary only. All 168 runner tests pass with one
optional skip; new Rust/native/RustFS qualification remains pending. This
candidate grants no RO/CG/OPT acceptance and does not replace constrained
node-container or independent failure-domain qualification.

## Automatic name discovery and physical membership

The original complete Queue failure reproduces on the retained instrumented
`1afb8949` artifact. Its nested enrollment fallback reports `Runtime(NotReady)`
while resolving the logical target name. Automatic discovery reads only the
ingress route registry, even when the caller owns the physical group. A local
refusal can therefore exhaust the one available candidate without trying its
ready authenticated physical peers.

The correction uses the existing bounded physical candidate selector for an
actively claimed local consensus group. Retained removed-member logs instead
use ingress discovery. Explicit routing, namespace grants, receiver-side fresh
quorum/route/schema checks, original attempt limits and the absolute lookup
deadline remain unchanged. No business mutation is retried by this change.

A public before-fix regression commits a real control write on two voters,
holds another voter's log sync, independently proves its apply index is behind
that committed write, and reproduces `Runtime(NotReady)`. The retained corrected
`c78e3778` artifact passes all four image/Quorum/S3 configurations in 24.040
seconds. Its original Queue case also passes independently on the retained
`1f37e43b` corrective artifact in 150.157 seconds. These component results do not
repair the earlier failed campaigns or qualify the newer combined base.

The initial corrective campaign retains a fault-setup failure: peer fixtures
deliberately use independent direct I/O, so an arbitrary follower need not carry
the first node's fault adapter. The fixture now holds that first node and uses
one bounded setup window to place leadership elsewhere. Its write, apply-lag,
incarnation, no-command and joined-close assertions remain unchanged.

The real removed-member regression also checks that former membership supplies
no implicit read grant, then installs an explicit namespace read grant before
requiring exact-name discovery through current voters. Its first added lookup
correctly refused without that grant; the failed campaign remains recorded.
The evidence verifier requires both refusal and successful authorized lookup.

All eleven combined-source checks pass at `ffd25905`, including strict all-target
Clippy, 24 source anchors, 44 design tests, 173 runner tests and both 317-test
performance suites. The corrected fault-owner/read-grant fixture needs its own
fresh source/runtime qualification. Current RustFS/Colima/Linux, hosted CI and
complete CG/OPT gates remain open.

The corrected original-profile campaign subsequently passes at `bb67c204` with
all source guards unchanged. All four held-follower image/durability profiles
pass in 23.857 seconds; all six removed-member SDK/native HTTP/Axum profiles pass
in 54.055 seconds. Their independent ledger requires actual removed physical
state, refusal without namespace read permission and successful authorized
exact-history discovery. The original history and named-binding cases pass in
25.013/53.470 seconds; all four named-binding profiles independently verify.
Three complete original Queue repetitions pass in 150.745/155.104/149.620
seconds under the original one-callback/two-ordinary admission and request
limits. Native read cohorts pass in 33.347 seconds with all 410 events and four
completed profiles verified. These results close the demonstrated name-selector
defect at its owning boundary; earlier failed campaigns retain their results.

The latest combined-source qualification passes all eleven checks at `4e0b5795`,
including strict all-target Clippy in 110.868 seconds. All 3,311 compiler-source
paths independently match actual Git blobs at `28a95e69`, containing the
name-discovery correction `38af885a` and read-cohort tooling. This combined base
uses two callback slots and has not yet passed its matching broad runtime or
backend campaign. The prepared native selection retains the chained
twenty-second KV control, the original Queue cases, archive/journal/physical
owners and the incoming callback/RPC tests. Hosted CI and all remaining
full-family, constrained-node, disaster, performance and CG/OPT gates stay open.

The upstream base advances to `5ff087c4` during these frozen campaigns. This
checkpoint remains pinned to integrated `da66d967`; qualification is not
transferred to that newer base. `origin/main` is still `d0c2873f`.

Review subsequently confirms that `5ff087c4` changes only six design/evidence
documents. It is merged while retaining all 3,311 compiler inputs at `4e0b5795`
and the same 24 anchored source files. The upstream Queue RustFS result remains
bound to its `9498029b` source; it does not qualify these optimization changes.
Additional documentation/design/anchor checks cover this merge. Recorded task
acceptance and the remaining combined runtime/backend/CG/OPT gates are unchanged.

The post-merge documentation, 44 design tests, 24 source anchors and patch checks
pass. Full `plan.py validate` still refuses because the historical
`CG-01-06-gates-e32115aa-02/CG-01-record.json` evidence is absent from this
checkout. Its failed result is retained; no missing-evidence waiver or task
acceptance change is applied.

## Archive publication at lifecycle boundaries

The matching `4e0b5795` native campaign completes with 147 of 148 selected Rust
test functions passing. The untrusted-TLS case fails because its observation
races the peer reading the client's certificate alert. The whole campaign
remains failed. An unchanged local 128-cell enrollment control separately
exhausts its original sixty-second parent budget; this does not reproduce the
hosted unavailable/closed-runtime failure. No RustFS/Linux qualification is
transferred from either result.

The archive publisher's leader scheduling also blocks two lifecycle cuts.
After a transfer archive binding is created, the only attached replica can be
a follower. It cannot acquire a prefix that no replica has published. Later,
a sealed source still owes its final archive cut after serving leadership is
withdrawn. The candidate permits these initial and sealed cuts through the
existing journal, registry CAS, physical capture and verification. Ordinary
live-prefix production remains leader scheduled. A follower's initial intent
must name no predecessor, including if another publisher advances between its
observations; the reservation CAS still rejects a changed parent.

The focused actual-follower regression fails unchanged production at
`9f8007c1` after its original five-second window and joins cleanup. The
`28e4daec` managed component candidate passes both image modes in 0.932 seconds.
The combined optimized `ee65de6d` candidate passes the original complete
split/merge case in 46.080 seconds. Its full ordinary disaster target passes
seven of eight functions; rooted unpublished-parent recovery still reports an
unknown Fleet outcome during later merge-seal authorization. That failed
result is retained. These snapshots precede the final initial-intent guard and
combined-source qualification; they grant no full CG/OPT acceptance.

The TLS fixture now waits for the actual peer alert within the original
absolute command deadline. Twelve component repetitions without diagnostic
probes pass all 72 independently verified caller/image observations. An earlier
post-format repetition that exhausted this same window remains recorded.
Matching optimized combined-source and external-backend qualification remain
open. A controlled initializer/leadership-loss case passes unchanged production,
so failure classification is preserved; the real lost-SQL-worker-reply control
still requires joined shutdown. Failed performance fixtures now retain their
original error and join every node even when an archive observation is closed.

The first-prefix regression also has an explicit isolated RustFS entry in the
archive-controller suite. It has not yet run against RustFS. The remaining
rooted seal-authorization failure, 128-cell unavailable/closed-runtime failure,
Linux sandbox shutdown failure, full combined native/backend campaigns,
historical evidence gate and broader CG/OPT requirements stay open.

## Demand refresh on the sending node

The managed inspection regression exposes a separate sender revision-floor
defect. Holding its background Fleet read behind a real catalog mutation makes
one physical-inventory request refuse with `StalePeer` before any RPC. The
receiver is independently current, so receiver refresh cannot repair this
failure. Unchanged production reproduces it at `9eae4b64` in 1.185 seconds.

The candidate performs sender discovery in the existing eagerly admitted
transport task, installs the complete trust cut, then binds the peer and sends
the original request. It refreshes only when installed trust is behind the
requested revision. Identity refusals, endpoint checks, process revocation,
metadata ceilings and the original absolute deadline remain enforced. No
caller retry or extra transport owner is introduced.

The strengthened regression retains its receiver and self-inspection phases
and additionally requires sender discovery while the response future remains
unpolled. At `211e3e63`, all five optimized repetitions pass in
0.989–1.040 seconds while both background reads remain held, with unchanged
source guards. This is component evidence with temporary diagnostics still
present. Matching clean-source runtime checks and the original rooted
merge-seal failure remain open; that earlier `Observation(Refused)` has not
been attributed to this `StalePeer` defect. No CG/OPT acceptance changes.

The same frozen candidate subsequently passes all 16 ordinary Raft directory
tests, including real process replacement, retained unpolled waiters and joined
shutdown. Two focused rooted unpublished-parent repetitions pass in
57.506/52.338 seconds. The full ordinary disaster target remains failed:
six functions pass and two fail in 549.733 seconds. Materialized-image
recovery-barrier execution still refuses merge-seal inspection; rooted
unpublished-parent recovery separately rejects a close report with committed
progress ahead of applied progress. These failures remain independent of the
sender-floor regression and require further diagnosis without relaxing either
physical admission or shutdown assertions.

Hosted native Python 3.14t validation at published archive-lifecycle head
`edfc1b0a` also fails the disaster target: four functions pass and four fail
in 263.31 seconds. The recovery-barrier, uncertain-outbox, old-token retry and
complete-closure cases report unavailable/unknown management outcomes in
[job 131913326541](https://github.com/crabbuild/cellar/actions/runs/43433706144/job/131913326541).
That head does not include the sender-refresh candidate. Filesystem release
applications pass on all three configured platforms; the remaining SDK/native
jobs have not supplied terminal results at this checkpoint.

The corresponding native Python 3.14 job subsequently fails three disaster
functions (five pass, eight ignored) in 383.80 seconds. The unavailable/unknown
outcomes occur in rooted uncertain-outbox recovery, old-token retries and
unpublished-parent recovery, as retained in
[job 131913326491](https://github.com/crabbuild/cellar/actions/runs/43433706144/job/131913326491).
Neither native job reaches qualification of the remaining failures farther
along its test sequence.

The sender regression gains a real cancellation control at diagnostic input
`e4e1d923`: a held foreground Fleet read must be withdrawn and its metadata
admission restored after dropping an unpolled response. This supplementary
request has a thirty-second deadline, while cancellation must finish within
five seconds; natural request expiry cannot satisfy the assertion. The complete
receiver/self/sender/cancellation regression passes in 1.031 seconds. Original
inspection and recovery request limits remain unchanged. Physical-refusal
diagnostics and clean-source qualification are still in progress.

At this diagnostic input, five isolated materialized recovery-barrier
repetitions pass before the sixth returns another unavailable CLI outcome in
33.325 seconds. No physical-inventory refusal probe fires in that failure and
its stack does not identify seal authorization, so it is not treated as a
reproduction of the earlier `Observation(Refused)`. The CLI fixture now includes
the first two operation words in failed-success diagnostics, following its
existing timeout-context convention and excluding credentials. The original
recovery matrix remains the next diagnostic boundary.

## Demand discovery of inspection caller views

The original four-case recovery matrix passes once at `65a5e17c` in
227.412 seconds, then reproduces the rooted unpublished-parent seal failure in
123.806 seconds. The operation-aware CLI diagnostic identifies
`transition authorize-seal`. Targeted transport evidence places the refusal in
receiver authorization on node 5 at Fleet revision 58, before physical capture.
This rules out physical-inventory construction for that failure.

A separate deterministic public-node regression holds only the receiver's
background process-view reads, then registers and boots a new caller. A local
inspection independently installs current Fleet trust on the receiver. Exactly
one remote physical-inventory request still fails with `Refused` before endpoint
discovery. Unchanged receiver behavior reproduces this at `9a16f854` in
1.253 seconds with all node/domain cleanup joined. Current Fleet trust and an
installed current process view are independent observations.

The candidate uses the existing bounded endpoint-discovery owner to obtain a
missing or stale caller view only after matching the request's node, disk,
process, engine and label to its current enrolled boot. Foreign scope,
unenrolled or superseded callers remain refused. Discovery reads authenticated
registration data and supplies no group or application authority. The original
deadline and all authorization checks remain in force. Temporary diagnostics
are removed. Repeated receiver regression checks, a cached follow-up, existing
sender cancellation checks and the original recovery campaign still require
verification at this updated input.

At input `10256229`, all five receiver repetitions pass without diagnostics in
0.835–0.943 seconds. Each first request enters exactly one existing endpoint
owner, and its cached follow-up enters none. The existing
receiver/self/sender/unpolled-cancellation control also passes in 1.004 seconds.
The missing-view refusal is therefore reproduced and corrected at its owning
public boundary. These component results do not close the original recovery
campaign, negative authorization checks, or any full CG/OPT gate.

The first new negative-caller unit campaign at `432268db` stops on a malformed
removal fixture (`Corrupt("node descriptor")`); it is not a passing authority
campaign. The fixture omitted the required removal operation ID. Its raw edit
helper also replaced repeated epoch fields inside the retained boot. The
correction supplies a valid operation, changes only top-level record fields,
and verifies that decoding the edited payload equals the intended record.
Production authorization and the negative refusal assertions are unchanged.

At corrected input `b8dbfbf5`, the negative caller test passes in 0.088 seconds.
It verifies refusal without endpoint discovery for wrong node, disk, process,
engine, label and Fleet/catalog/cluster scope, and after removing the formerly
valid caller. All three sibling Fleet transport tests and both metadata wire
authorization tests also pass. The public cold/cached receiver and sender
cancellation controls pass in 1.022/1.011 seconds, and all 16 ordinary directory
tests pass in 27.348 seconds. The full ordinary disaster target is still
running at this checkpoint; source checks and matching broader native/backend
qualification remain open.

The corrected `b8dbfbf5` campaign subsequently passes its complete original
ordinary disaster target: eight pass, zero fail, eight explicit RustFS skips,
in 603.247 seconds. All eleven campaign checks pass with unchanged source
guards. This includes the six authorization unit checks, both public metadata
regressions and all 16 directory tests described above. The previously failed
campaigns retain their results; this pass does not replace broad primitive,
backend or repeated release-scale recovery qualification.

The synchronized managed checkout passes all eleven source checks at
`1441cd2a`, including formatting/layout, strict core/logic all-target Clippy
(699.196 seconds), reviewed anchors, dependency/docs checks and both Python
performance-tool suites. All 3,312 shared compiler inputs independently match
the canonical runtime snapshot. The managed checkout's 60 additional ignored
JavaScript inputs remain separately recorded. These checks close the
demonstrated sender-floor and receiver-view component defects; full native
families, RustFS/Colima/Linux, 128-cell enrollment, sandbox shutdown, historical
evidence and the remaining CG/OPT gates stay open.

## Current combined-source routing and load failures

Published `752f2312`, input `b8dbfbf5`, completes the 66-row optimized native
campaign with 154 selected Rust functions passing and three failing. All
19 ordinary primitive cases, the original chained twenty-second KV control,
33 ordinary archive cases and 26 ordinary controller cases pass. The campaign
remains failed at delayed lookup, read preferences and removed-member advice.
The delayed-lookup SDK scenarios pass in both images before a later native
write to an unaffected range returns an ambiguous unavailable outcome. Read
preferences fail initial group startup, and removed-member advice fails its
final counter oracle in the Axum/extent profile. These failures are retained;
they authorize neither a weaker oracle nor backend qualification.

The matching hosted native jobs also fail in the disaster target. Python 3.14
records four passes/four failures/eight skips in 356.02 seconds; 3.14t records
five passes/three failures/eight skips in 380.05 seconds. Failures include
source capture and seal authorization. The 3.14 job additionally refuses a
retained recovery voter with invalid group provisioning configuration. The
inspection component corrections therefore do not establish complete Linux
recovery. All three filesystem release application jobs pass separately.

The original multi-occupancy control at this committed input again fails at
occupancy 128. Its original sixty-second enrollment window ends with 73 of
128 cells observed on every voter; total test time is 195.639 seconds and node
cleanup joins. Two unrelated compiler processes are observed on the host, so
this is no controlled performance comparison. Earlier 128-cell failures retain
their exact scope and results. The owned Colima profile remains stopped, and
the prepared 62-case RustFS campaign has not run because its native prerequisite
failed.

An independent test/runner candidate adds a four-call Queue load selection,
retaining the existing two-call workload and all resource/request bounds.
All 174 multi-Raft tool tests pass with one optional skip, along with formatting
and documentation checks. At input `4e9139b4`, the original two-call native
case passes both images, while the four-call case reproduces an ambiguous
ingress deadline before its extent image. See the
[Queue load checkpoint](queue-durable-work.md#qualification-and-remaining-scope)
for observed owner state and the remaining diagnostic boundary. No production
budget is raised, and no complete primitive family, CG or OPT package is accepted.

## Bounded SQL progress FIFO follow-up

PR [#167](https://github.com/crabbuild/cellar/pull/167) publishes the Queue
regression and bounded FIFO candidate at `2e1852db`. All 3,314 tracked compiler
inputs match the published Git blobs; 60 unchanged ignored JavaScript inputs
remain separately recorded. Runtime and source checks use input `7c0585ac`
with locked Rust 1.98.1, optimized tests, debug assertions and overflow checks.

The build, formatting, layout, focused strict Clippy, six admission unit tests,
174 multi-Raft tool tests (one optional skip), native dependency and documentation
checks pass. The original two-call Queue load passes both images in 78.729
seconds. The four-call load still fails in its first image in 26.205 seconds
with an ambiguous ingress deadline, measured peak four and unchanged resource
bounds. Its second image is unqualified. The
[Queue checkpoint](queue-durable-work.md#qualification-and-remaining-scope)
retains the earlier failures, targeted admission probes and exact result scope.

This candidate replaces opportunistic progress-slot retries with bounded FIFO
waiting; it does not establish the complete cause or repair of the four-call
failure. The failed native campaign and hosted recovery jobs remain failed.
RustFS/Colima/Linux, 128-cell enrollment, shutdown, historical evidence and all
remaining CG/OPT acceptance gates remain open. No merge is performed.

## Bounded stateless SQL ingress FIFO follow-up

At compiler input `e716fb93`, active stateless SQL callers to the same logical
cluster, namespace and cell share a node-local FIFO before reading the next
command sequence. Candidate incarnation hints share that FIFO but grant no
authority. Each waiter retains its existing metadata admission; the weak
registry removes the entry when its final caller leaves. There is no idle
per-cell owner or task. The original absolute deadline and cancellation cover
the wait, and fresh-quorum sequence queries, exact consumed-other conflict
proofs and ambiguous mutation outcomes remain unchanged.

The locked Rust 1.98.1 build, formatting, layout and focused strict Clippy pass.
All three ingress-lane unit tests and all six existing SQL progress admission
tests pass. Ten exact existing frontend contract cases pass: native/Axum SQL
and KV, interrupted SQL recovery, extent progress after SQL errors, source-proof
refusal, cross-deployment Queue faults, the original twenty-second chained KV
deadline and the original 65-second Queue callback. Frozen executables and
unchanged compiler-input guards bind these results to `e716fb93`.

The original two-call Queue load passes both images in 77.796 seconds. The
first full four-call run fails in 75.761 seconds: the materialized image delivers
all 528 unique messages, then the extent image returns an ambiguous ingress
deadline at sequence 22. Failure counters record peak four, two failed producers
and zero active calls after joining. An uninstrumented repeat of the same full
two-image four-call case passes in 78.888 seconds, with 528 unique messages and
measured peak four in each image. These are mixed same-source results; the
repeat does not erase the failure or establish stable four-call progress.
Both workloads retain their original deadlines, message counts and resource
bounds. These native runs use the in-memory object store, not real RustFS.

Two extent-only diagnostic runs pass, including one with the leader moved to a
different node before load. They retain separate diagnostic scope and do not
replace the original two-image selection. Bounded snapshots observe SQL
preparation, persistence and publication work; they establish neither a single
remaining bottleneck nor a performance improvement. All temporary probes and
fixture changes are removed before publication.

Final pre-publication review also corrects concurrent lane destruction: each
caller releases its strong gate reference while still holding the registry
lock. Otherwise two destructors can both count the other's remaining field
and leave an idle weak entry. A concurrent-drop regression checks complete
registry and admission release. The `e716fb93` results above predate this cleanup
correction and retain their original compiler-input identity.

At final compiler input `73c80371`, the locked build, formatting, layout,
focused strict Clippy, all four ingress-lane tests and all six existing SQL
progress admission tests pass. This includes paired concurrent final drops
across 4,096 distinct targets. The final Queue rerun stops at its activity
precheck after observing a compiler in a different checkout; no Queue case
executes. Its harness record remains failed, with no new runtime result. The
earlier frontend contracts and mixed load results remain bound to `e716fb93`;
the final cleanup correction does not acquire broader qualification from them.

Hosted [workflow 43489533616](https://github.com/crabbuild/cellar/actions/runs/43489533616)
tests published parent `7f900435`, which excludes this ingress FIFO. Native
3.14 fails three of eight ordinary disaster cases at extent seal authorization.
Native 3.14t fails the original multi-occupancy enrollment control at 87 of 128
cells and a separate rooted 32-cell control at 30 cells. Their leaders close
while SQL workers remain live with one prepared enrollment retained. A dedicated
128-cell diagnostic passes separately; that result does not qualify the failed
matrix. A stale enrollment observation is a hypothesis requiring a controlled
regression, not an implemented fix. Python 3.14t separately fails owner recovery
at its original 250-ms handler budget. Python 3.14 and all three filesystem
release application jobs pass; both aggregate framework gates fail.

The prior broad native failures, Linux recovery and shutdown failures, current
RustFS/Colima qualification, full occupancy/performance matrix and remaining
CG/OPT gates stay open. The owned backend remains stopped. This follow-up is a
bounded local fairness change, not full Queue or multi-Raft acceptance.

## Enrollment resolution after an applied observation becomes stale

The hosted `7f900435` enrollment failures motivate a controlled regression at
the existing SQL pool resolution boundary. At test-only input `27b088e7`, both
`concurrent_enrollment_one_cell` and its rooted counterpart capture valid
absence, apply the exact prepared creation, then submit the old observation
for resolution. Both fail at the new assertion in 0.283/0.267 seconds: the
expected uncertain result is `Refused(Unavailable)`. Production code is
unchanged in this red checkpoint; unrelated startup or timeout failures do not
count as reproduction.

The correction keeps applied-state validation and maps its `NotReady` result
to uncertain enrollment resolution, matching existing command reconciliation.
It retains the producer for fresh proof instead of reporting the same failure
used for a lost shared SQL worker reply. All other error mappings and binding
checks remain. The regression also checks equal route/term, the exact committed
creation, retained bytes/operations/workspace, one initializer call and eventual
fresh-proof resolution.

At candidate input `8f0ec3be`, both regressions pass in 0.501/0.506 seconds.
All 17 exact existing enrollment controls pass, including leadership loss and
transfer, disconnected initializers, original deadlines, cancelled shutdown,
remote permission/revocation and genuine lost-worker reply shutdown. The locked
Rust 1.98.1 build, formatting/layout, focused strict Clippy, native dependency
and documentation checks also pass. Frozen binaries and unchanged-input guards
bind these results to this candidate; no temporary probes are introduced.

The complete original `cellar-perf-raft` unit target remains failed: 19 pass,
two fail, zero skip, in 378.399 seconds. The original 1/32/128 matrix reaches
its original sixty-second enrollment deadline with 103 of 128 group-zero cells
observed on each voter; its other group has one cell. The separate 128-cell
diagnostic also reaches that deadline, with 114/113/114 group-zero cells and an
empty second group. Both fixtures retain their disks and join cleanup. Their
snapshots show open controllers and SQL pools, two live SQL workers per node,
zero apply retries, no extent-store quarantine and no collector failures.
The rooted 32-cell control passes. No compiler or other native fixture is
observed at the full target's start.

The first failed fixture records substantial SQL preparation, publication
and engine file-sync time; its timing spans can overlap. Most SQL
resolution calls finish within tens of microseconds, so their high failure
count alone does not establish a costly retry-loop bottleneck. Preparation,
persistence and workspace-close costs need targeted attribution before another
optimization. This closes the demonstrated stale-observation component defect,
not the 128-cell latency gap, hosted Linux enrollment matrix, broader native
failures, current RustFS/Colima qualification or any full CG/OPT gate. No speedup
is claimed and no request budget, resource limit or durability barrier changes.

## Bounded checkpoint receive page packs

A diagnostic run at published `327742fe`, compiler input `8f0ec3be`, retains
the original full 1/32/128 workload and captures three bounded stack samples
during its actual 128-cell phase. Initial competing compiler activity is
recorded; later observations show that compiler activity has cleared. This is
stack-path evidence under observed host load, not an uncontended performance
comparison. The run fails its original test in 189.170 seconds. The prior
uninstrumented 21-test campaign remains failed independently.

The samples repeatedly observe manifest-pack and archive-journal syncs, alongside
Raft engine and image-root publication. Workspace creation and cleanup appear
less frequently in these thread-stack observations; their weights do not measure
nonoverlapping wall time. Inspection finds that checkpoint receipt calls the
page store once per leaf, creating one durable pack per received page.

At test-only input `5c5ab417`, the existing public archive roundtrip fails its new
four-page, one-pack assertion in 0.434 seconds: four physical packs are observed.
Production is unchanged at this red checkpoint. The failure occurs in the first
materialized-image loop, before the extent loop runs; no broader result follows.

The candidate buffers a bounded 64-KiB/96-page window under explicitly charged
existing receiver admission, while leaving the parent limits unchanged. It
preflights destination capacity before a batch writes, preserves all leaf/image
checks and completes each batch's file/directory barriers before page pins or a
prepared root escape. Page counts include buffered leaves and target validation
repeats after the final flush. The existing quota/cancellation control additionally
requires that a refused batch leaves the destination live and writes no pack.
The first component campaign at input `d702ba17` passes all 33 ordinary archive
tests, with eight explicit RustFS cases ignored. Checkpoint execution passes 31
tests and fails two recovery fixtures: they assume a checkpoint-only root page
occupies a different physical file from every current-root dependency. Receipt
batching now correctly co-locates some of these distinct logical pages.

The fixture correction independently repacks that closed store only after every
engine, runtime and pin has gone. It verifies every original page byte and digest
is unchanged, then retains the original assertions that the missing-file fault
damages only the old checkpoint and leaves every current-root dependency intact.
Missing-checkpoint refusal and successful recovery after restoring the file
remain required.

At corrected fixture input `c3accf6b`, all 88 selected ordinary tests pass:
33 archive, 33 checkpoint execution and 22 storage tests, including the existing
crash-cut parents. The eight RustFS cases and two child-only process fixtures
retain their explicit ignores; storage parents launch their owned crash children.
The locked Rust 1.98.1 build, formatting/layout, focused strict Clippy, native
dependency and documentation checks pass. The test profile retains optimization
level one, debug assertions and overflow checks. Binaries are frozen and input
guards remain unchanged throughout each campaign.

The complete original 21-test performance target still fails: 20 pass, one fails,
zero skip, in 425.397 seconds. Its original 1/32/128 matrix reaches the unchanged
sixty-second enrollment deadline at 121/128 group-zero cells on each voter; each
second group has one cell. The failed fixture retains its disks and joins cleanup.
The separate 128-cell diagnostic and rooted 32-cell control pass; neither replaces
the failed matrix. No competing compiler or native fixture is observed at target
start or at the recorded activity samples. These observations do not establish
uncontended execution or a repeatable speedup. The receiver packing component is
verified; the original 128-cell latency gap, stable Queue progress, broader native
failures, matching RustFS/Colima/Linux qualification and full CG/OPT gates remain
open. No earlier failed campaign is relabeled.

## Complete-root directory barrier reuse

At published `bf7af7b5`, the unchanged original 1/32/128 matrix passes a sampled
repeat in 174.567 seconds. Its preceding complete 21-test target still fails at
121/128 cells. These are mixed results; the sampled pass establishes neither
stable enrollment latency nor a speedup. Three bounded samples during the actual
128-cell phase repeatedly observe manifest-pack, archive-journal, image-root and
Raft-engine sync paths. Their thread-stack weights are not wall-time percentages.
No competing compiler or native fixture is observed at profile start or at its
sampled activity checks.

Inspection identifies an unconditional page-directory sync after complete-root
preparation has already obtained each page's pack-level file and directory
persistence proof. The existing public warm/cold/corruption fixture is extended
to count both kinds of barriers. At test-only input `24ac7213`, it fails in
0.062 seconds on the exact extra-directory assertion: 12 calls versus 11.
Production is unchanged at this red checkpoint.

The candidate reuses the complete pins' existing store-bound persistence proofs
at this boundary. Every physical page read and image check remains. Cold packs
still require file and directory barriers; a failed page read invalidates the
pack observation, and failed required barriers still quarantine the owner.
The test additionally requires exactly one directory barrier for its one-pack
cold preparation and no extra directory barrier for another warm preparation.
Partition pre-copy's separate directory synchronization remains. This adds no
new cache, lifetime, memory bank, deadline or changed durability requirement.
Candidate component, original performance and matching backend checks are pending;
the full optimization and release acceptance gates remain open.

The first candidate input `07621ba4` passes the expanded storage regression in
0.138 seconds and all 33 ordinary archive tests. The complete execution target
passes 193 cases and fails the two image modes of
`group_controller_drives_tls_apply_and_bounded_retry`; two owned process fixtures
remain explicitly ignored. Storage and performance do not run after that failure.
The exact same two cases also fail with the frozen published-parent binary
(input `c3accf6b`) in 15.355/15.374 seconds.

Tagged observations at diagnostic input `439bd260` identify the wait: both apply
owners remain active and charged, one reserved progress slot is held, the callback
is entered once and the controller stays open, while `apply_retries` remains zero.
The existing FIFO now retains its second admitted job instead of returning a
slot refusal that increments that counter. Both unchanged retry-counter waits
reach their original deadlines in 15.341/15.343 seconds.

The corrected fixture waits for two actual apply owners and reservations, no
pending ticket and one occupied progress slot behind the held worker. It keeps
all later replication, exact receipt/version, callback-once, capacity and joined
shutdown assertions, with the original deadlines. The probes are removed.
Qualification of this corrected fixture input is pending; the failed full
execution campaign remains failed under its original input identity.

At corrected input `71f4abcb`, all 250 selected ordinary tests pass: 33 archive,
195 execution and 22 storage tests, including their owned crash parents. Eight
RustFS cases and four child-only process fixtures keep their explicit ignores;
the parents launch the owned children. The exact barrier regression passes in
0.057 seconds, and the corrected controller cases pass all later oracles in
0.514/0.529 seconds. Locked Rust 1.98.1 build, formatting/layout, focused strict
Clippy, native dependency and documentation checks pass. Frozen artifacts and
unchanged-input guards bind those results to this input, with no source probes.

The complete original performance target remains failed: 19 pass, two fail,
zero skip, in 337.039 seconds. The original 1/32/128 matrix reaches its unchanged
sixty-second enrollment deadline at 115/128 group-zero cells on every voter.
The separate 128-cell diagnostic reaches the same deadline at 123/128 cells on
every voter. Each second group has one cell. Both failed fixtures retain their
disks and join cleanup. The rooted 32-cell control passes. No competing compiler
or native fixture is observed at target start or the recorded activity check.
These observations do not establish uncontended execution. Removing one redundant
directory call is demonstrated by the controlled fixture; no end-to-end speedup
or stable 128-cell progress is claimed. Stable four-request Queue progress, the
broader routing failures, matching RustFS/Colima/Linux verification and full
CG/OPT acceptance remain open. The earlier failed campaigns retain their scope.


## Main integration after PR-166

PR-166 is squash-merged as `3d6450d4`. Its pre-squash head `8de8e18b`
has exactly the same Git tree. Integration `67a3d6e8` reconciles the actual
shared history at `5ff087c4` and records `3d6450d4` as its main merge parent,
retaining the optimization history without rewriting the published branch.

The combined source preserves bounded committed archive tails and rollover,
quiet-head maintenance and command-prefix filtering from main. Archive reuse
still requires the admitted cohort, a live physical verification, current
birth/journal checks and the exact requested output proof. The existing
reserved SQL progress FIFO composes with worker wakeups and witnessed
pre-enqueue enrollment admission; entered producers and lost replies retain
their original reconciliation and shutdown requirements. Public alarms,
KV payload preparation and their incoming regressions remain present.
The performance reader includes both new KV preparation counters and preserves
the exact vocabularies of retained measurement artifacts.

The reviewed source baseline now pins this combined Git revision. Only its SQL
worker anchor changes; it exactly matches main's reviewed `Port` implementation.
The other 23 anchors retain the optimization bindings. Source preflight grants
no runtime acceptance and does not restore missing historical CG-01 evidence.

Before integration, the complete `dcbce778` native campaign at compiler input
`71f4abcb` finishes **156 passes, two failures, zero skipped selected functions**
across 67 check rows. All three routing cases that failed the earlier
`752f2312` campaign now pass, but the four-in-flight Queue control and the
live overlapping split control fail. The former reaches measured peak four
and then returns an ambiguous materialized-mode producer deadline at sequence
seven; the latter returns an ambiguous SDK command deadline at sequence four.
Both preserve their original five-second windows and retain failed disks;
cleanup completes and source/executable guards remain unchanged. The original
two-request Queue load passes both image modes with 528 unique messages each.
These results remain bound to the earlier source, which lacks this integration.

The integration passes 178 multi-Raft runner tests and 317 performance-tool
tests, with one opt-in case skipped in each suite. Native dependency checks,
documentation checks and formatting pass. Combined Rust compilation, runtime,
Clippy, Linux and RustFS/Colima qualification remain pending; the previous
performance and Linux failures retain their original scopes. No complete
OPT, CG-13 or CG-17 gate is accepted by this merge.

## Ordinary primitive measurements, 2026-11-14 UTC

The [local SQL/KV/Queue report](primitive-performance-2026-11-14.md)
and its machine-readable observations record the real RustFS/Colima experiment
at `b101730f`, compiler input `8edbdba5`, after pulling main `3d6450d4`.
The optimized `lab` artifact runs three native RF3 voters and its HTTP client
inside one three-CPU/9-GiB container; RustFS retains one CPU/2 GiB in the
six-CPU/14-GiB VM. This is an exploratory primitive benchmark, not separately
constrained daemon or independent-domain qualification.

All six primitive/image cases fail their complete matrix. Nine of 40 planned
30-second windows finish without errors and verify exact values/messages;
seven timed windows fail and 24 are unmeasured after earlier failures. SQL
materialized mode observes node-lease fencing. Both modes encounter ambiguous
operation refusals/deadlines and later quarantined-group refusals. Queue can
deliver a timed-out enqueue, so acknowledged-only comparisons do not establish
corruption or loss. Failed logs, disks, original identities, raw latency samples
and kernel resource observations remain retained. Source inputs stay unchanged;
no workload container is OOM-killed and RustFS does not restart.

The report grants no stable throughput, speedup or completed OPT/CG gate. The
next work is focused lease/callback/apply-progress diagnosis, independent
reconciliation of uncertain outcomes and an unchanged full rerun before broader
population or separate-process capacity claims. Admission, lease and deadline
limits were not enlarged to obtain these observations. Subsequent benchmark
clock/reporting corrections leave production Rust unchanged and do not
retroactively qualify the measured source.


## Separately constrained primitive comparison, 2026-11-14 UTC

The [initial three-node experiment](three-node-primitive-comparison-2026-11-14.md)
and [bounded-writer follow-up](three-node-primitive-capacity-2026-11-14.md) measure
all three public SQL/KV/Queue APIs with three independent one-vCPU/2-GiB nodes,
a separate capped client and shared local RustFS. SQL/KV use three RF3 groups of
eight cells; Queue uses three RF3 groups of 16 including declared DLQs. Serving
leaders are balanced across nodes and complete physical enrollment is checked.

The initial profile at `fdbe6961` verifies 62 of 120 timed windows. Its five
complete cases are Fleet/bucket SQL/KV and bucket Queue; all Raft cases fail
at concurrency-four warmup or Queue concurrency-one warmup. A real extent-store
control confirms the fixture's 784-MiB disk bank cannot admit two 592-MiB private
workspaces. The corrected profile at `d09569df` explicitly admits eight writer
workspaces in a 4,928-MiB disk bank; the owned VM disk grows from 100 to 160 GiB.
Node CPU/RAM, five-second deadlines/leases, image/manifest banks and SQL worker
count remain unchanged. This is a benchmark configuration correction, not a
production admission, fairness or collection fix.

All six corrected Raft smoke cases pass at concurrency one/four. The subsequent
30-second, two-repetition plan verifies 21 of 60 Raft timed windows, fails three
and does not reach 36; no full Raft case completes. SQL/KV have verified
one/four-client windows; Queue has verified one-client enqueue, delivery and
settlement windows in both Raft durability modes. Raft overloads/deadlines at
higher concurrency remain required work. Unconfirmed SQL updates can later
replace earlier values, so a readback mismatch after uncertainty is not a
corruption or lost-acknowledgement finding.

The selected cross-mode comparison uses corrected Raft and the original
Fleet/bucket baselines: 75 of 120 timed windows verify, four fail and 41 remain
unmeasured; five of 12 cases complete. Fleet Queue separately fences with
`NodeLeaseExpired` during verification after 415 acknowledged timed batches.
Its timed delivery/settlement window remains unqualified. All failed cases,
original identities, raw latency samples, logs and disks remain retained. The
original RustFS service is restored after measurement.

Rates and percentiles describe these source-bound local windows and differing
acknowledgement/read implementations. Optimized `lab` uses size optimization;
shared-host activity and the distinct disk profiles preclude isolated speedup
or production-capacity claims. No OPT/CG gate or historical failure is reset.
Next priorities remain bounded admission/progress, Fleet lease-renewal progress,
original-identity reconciliation, sustainable complete-root manifest reclamation
and the remaining independent-domain/authority/retirement/provider gates.

## Primitive refusal phases, 2026-11-14 UTC

The [instrumented three-node probe](three-node-primitive-admission-2026-11-14.md)
adds eight bounded observations for SQL node/group/ordinary-worker admission,
staging before enqueue versus received worker replies, private-image opening,
command submission and overloaded settlement. They change no admission bank,
deadline, identity or retry behavior. Native controls distinguish an unopened
callback from a callback that returns the same overload, and hold the original
physical workspace bank full before testing refused opening and returned credit.

Immutable image `16541b6a`, compiler inputs `0df275de`, runs all three public
bindings on the existing separately constrained RF3 topology. Of 20 planned
timed windows, three verify, one fails and 16 remain unmeasured; two warmups
verify, two fail and eight remain unmeasured. All three cases fail, retaining
eight emitted windows and 18 request errors. SQL reaches one group-budget
refusal before enqueue at concurrency 16 alongside 12 deadlines. KV fails a
four-client read with two deadlines; Queue fails four-client warmup with three
deadlines. Neither KV nor Queue increments the new overload counters. The SQL
counter does not explain every deadline, and zero overloads do not exclude
writer contention or other waits.

Queue delivery observations include all 40 acknowledged IDs plus 24 IDs from
the three unconfirmed attempts, with zero reported backlog/DLQ. The failed row
does not retain the complete observed body-hash map, so this is ID reconciliation
only; delivery/body/settlement qualification remains failed. Original uncertain
identities are retained without fresh-ID retries. All nodes/stores close zero,
with no sampled OOM or restarts, and the original RustFS service is restored
and ready. Complete owner snapshots, raw samples, source hashes and host activity
remain available with the report; the earlier comparison is unchanged.

Four native integration controls, two admission unit tests and five registry
tests pass, with strict affected-target Clippy, formatting/layout and 332 Python
performance tests (one existing optional skip). The evidence reader accepts the
new counter vocabulary and complete historical measurement inventories, while
refusing partial inventories and within-run schema changes. These are archive
evidence readers, not runtime legacy-format support. No OPT/CG gate is accepted.
Bounded group admission progress, independent deadline diagnosis and original-ID
reconciliation remain required before another complete qualification campaign.

## Bounded SQL admission wait, 2026-11-14 UTC

The [bounded-wait implementation and three-node evidence](three-node-primitive-admission-wait-2026-11-14.md)
close one received refusal boundary. An owned command retains its finite
invocation, original identity/inputs/clock/randomness, route ticket and deadline
while waiting for a fitting node/group SQL reservation before enqueue. It holds
no SQL slot or physical reservation. Registered credit notifications recheck
bytes and items; controller ticks observe lifecycle without polling a full bank.
Fresh staging revalidates authority. Oversized requests, worker admission,
entered callbacks, private/continuation opening and lost replies grant no new
replay permission. Retirement wakes lifecycle checks without refunding live
physical credits; maintenance claim/settle/seal still releases its command slot.

Real three-voter group-byte and node-item controls fail on the earlier code with
immediate overload and pass with bounded waiting. Four new integration controls
cover credit return, once-only callback/receipt replay, original expiry, joined
shutdown and terminal refusals in both image modes. Six new admission tests cover
byte shrink, item ownership, retained clones, consumed signals, fitting peers
and retirement. Forty-one focused Rust tests pass, alongside strict affected
Clippy, formatting/layout (2,292 files), native dependencies and 332 Python
performance tests with one existing optional skip. The SDK source-proof control
covers both image modes and quorum/S3; this is not a complete qualification run.

Immutable image `976eda7e`, compiler inputs `92d406c1`, measures all three public
bindings on the unchanged separately constrained RF3 topology. Nine of 20 timed
windows verify, three fail and eight remain unmeasured; seven of 12 warmups
verify and five remain unmeasured. All three cases remain failed. Nineteen
emitted windows retain seven request deadlines and zero public overloads. SQL
verifies five timed windows before a second-repetition 16-client read fails;
KV verifies four before its second-repetition 16-client write fails with two
deadlines and readback mismatch. Queue verifies 200 warmup messages, then its
timed four-client window has two deadlines and a delivery-set mismatch.

SQL/Queue complete node snapshots observe four/eight group admission refusals
and overlapping stage counts. They include node-wide maintenance and do not
establish every attempted command's result. KV end observations are unavailable
on all three nodes and its counter deltas stay unknown. All three KV nodes
subsequently self-fence with `NodeLeaseExpired`; the lease warnings and fences
occur after load exit, so they explain missing final snapshots without proving
the cause of the earlier request failures. Five-second lease/operation windows
and all resource/admission banks remain unchanged.

Queue observes all 664 acknowledged IDs and 16 IDs matching both unconfirmed
attempted batches, with zero reported backlog/DLQ. The failed row lacks a complete
observed body-hash map, so only ID reconciliation follows. Original uncertain
identities remain retained without fresh-ID retries. Three KV nodes exit one;
the other six nodes and all stores exit zero. No container OOM/restart is observed.
All failed disks/logs remain retained, source/engine guards pass and the original
RustFS service is restored and ready. SQL failure-snapshot/index recovery is
exercised; KV's missing-node errors stay explicit in the retained evidence.

The unchanged load bytes/profile and additional verified windows do not establish
isolated speedup on this shared host. The earlier selected cross-mode comparison
remains five complete cases and 75 verified timed windows out of 120. No full
Raft primitive matrix or OPT/CG gate is accepted. Next work is constrained
node-lease renewal progress, independent deadline/writer/proof-wait diagnosis,
original-identity reconciliation and sustainable complete-root reclamation.
Authority expiry/revocation, physical retirement/once-only Fleet settlement,
provider recovery, independent domains and missing historical evidence remain
required.

## Main-168 integration and primitive re-verification, 2026-11-14 UTC

Origin/main `50b927d9` is merged at `a16bbdc6`; source `c5444fd2` composes main's
queued writers and primitive execution with bounded capacity waiting, routing
and packing. The old Queue performance helper converts into main's `Minimums`
contract. Opt-in lease tracing adds no deadline, resource or retry permission.

The [updated-main report](three-node-primitive-main-168-2026-11-14.md),
observations and
raw evidence retain the
complete re-verification. Compilation/strict Clippy, format/layout (2,306 Rust
files), native dependencies and Python/documentation checks pass. There are 109
distinct libtest passes, while the complete primitive target fails eight cases
and explicitly ignores 38. Workflow compatibility and the custom three-process
TLS daemon pass; the wrapper's mistaken libtest-footer expectation for the
harness-free daemon is separately assessed without erasing real failures.

All twelve separately constrained RustFS/Colima cases are attempted. Only KV
Fleet and KV bucket complete both repetitions at c1/4/16. Of 120 timed windows,
49 verify, seven fail and 64 remain unmeasured; of 72 warmups, 34 verify, three
fail and 35 remain unmeasured. All 93 emitted windows retain 33 request errors. Three SQL baseline
nodes exit with `NodeLeaseExpired`; their exact fence event timing is unknown.
All Raft cases retain complete end observations. Group refusals increment once
for SQL quorum and nine times for KV quorum, with overlapping staging counts;
other selected phases do not explain every deadline. The failed Queue bucket window has 40 unobserved ACK
IDs and reported backlog 40. Other Queue failures observe extra IDs matching
unconfirmed batches, without a complete exported body-hash map. No data-loss,
body/settlement or complete Raft-matrix qualification follows.

All 60 measured containers stop with no observed OOM/restarts. Failed resources
remain retained, owners are joined and the original RustFS is restored/readied
at HTTP 200. Manifest `b417c200`/image `630bbfa5` remain immutable; 3,350 tracked
inputs match the published source and 60 ignored generated optional-JavaScript
assets are separately hash-bound. Earlier measurements retain their earlier
source scope. Next work is deadline/readiness/writer/proof-wait progress,
constrained SQL-baseline lease renewals and Queue drain progress at unchanged
limits. Complete-root reclamation, authority, physical retirement/once-only
Fleet settlement, provider/independent-domain and historical-evidence gates
remain open; no OPT/CG acceptance is promoted.
The full plan CLI preflight still fails on the missing historical CG-01 record;
plan unit-test passes do not waive that evidence gate.

The subsequent [SQL follow-up](main-168-sql-followup-2026-11-14.md) preserves an
isolated recovery pass and an eight-configuration pass under temporary test-only
diagnostic inputs; the earlier failure does not recur, so no recovery fix is
claimed. Probes are removed and compiler inputs return to `b417c200`. A separate
SQL quorum syscall diagnostic fails at c4 read with two node lease-expiry exits.
It retains 69,391 successful sync calls: 10,627 engine, 30,234 serving-image/manifest
and 28,530 archive-verifier/journal calls. Tracing changes timings and final Raft
observations are incomplete. Counts include failure observation/shutdown, and
summed syscall times overlap. Bounded image/archive batching and isolation are
profiling priorities; original failures and every OPT/CG gate remain unchanged.

## Main-172 integrated qualification and matched control, 2026-11-15 UTC

Source `95244b42` includes main `0e925f6b`; its frozen compiler manifest is
`f8917ffe` (3,413 inputs). The [complete report](three-node-primitive-main-172-2026-11-14.md),
assessment and
raw capsule retain all
attempted windows, including failures. The control reverses only `97b805f3`'s
nine LTX image-publication files. Both images use matching locked Rust 1.98.1
profiles and load bytes under the original three 1-vCPU/2-GiB node limits.

The candidate passes five of twelve cases, while the quorum control passes SQL
and fails KV/Queue. Candidate timed windows record 78 verified, six failed and
36 unmeasured, with 43 request errors across emitted windows. All four Queue
cases fail. No complete passing quorum pair exists. Shared-host compiler activity
and queued source hashing also prevent an isolated causal speedup claim. An
unconfirmed SQL write plus readback mismatch does not establish corruption or
acknowledged data loss; the original verifier does not identify the mismatched
key/value. Missing Queue producer evidence remains explicitly scoped.

The joined native campaign records 1,508 passes, 15 failures and 107 explicit
ignores across eight libtest targets; the separate production daemon passes.
Original failure deadlines and assertions remain unchanged. Some failed native
temporary disks are unavailable after host restart or fixture cleanup; their
logs survive, and disk retention is a separate harness gap. All 75 measured
containers stop without observed OOM/restarts, and the original RustFS identity,
limits and data are restored with HTTP 200 readiness. This accepts no complete
primitive family, OPT/CG package, independent failure domain or release profile.

### Source-journal and verification repairs

Repair manifest `9f483e79` moves destination wire-bound capture to the public
EventStream adapter. The private synchronous journal requires that explicit
bound instead of attempting to acquire an asynchronous dispatch capability.
The real SDK authorization and source-history capture remain mandatory; failed
wire admission rolls back source identity, lanes, events, statistics and work
discovery together. Seventeen outbox tests and the full Cellar library pass
(1,184 passed, two existing ignores). Both public native and Axum EventStream
publication/subscription/outbox recovery controls pass on that same executable.

The Queue load client records acknowledged identities, body hashes and attempted
batches before verification I/O, retaining partial per-node delivery and per-queue
metrics observations on error. The comparison validator independently reconciles
complete probe rounds, IDs, bodies, duplicate counts and settlement. The new
failure-injection and tampering controls fail before repair; 336 performance
tests run with one existing skip after repair. The frozen campaign still uses
its original client bytes and retains its missing evidence and failed status.

Strict all-target Cellar Clippy, format/layout, the reviewed SDK inventory and
13 coverage-validator tests pass. Regenerating the catalog/explorer closes the
new report's initial catalog-coverage failure; documentation then validates 133
documents and 2,650 references. The native dependency graph remains 496 packages
without a JavaScript engine. The failed documentation attempt and two diagnostics
held before execution remain recorded separately from the successful follow-up.

The selected SDK source-proof oracle passes all four image/durability profiles
in 33.69 seconds; its earlier timeout is unreproduced, not attributed to the
journal repair or failure-only snapshot. The removed-member oracle still fails:
its unchanged callback/SQL-stage guard passes, but application RPC attempts remain
two before/after while leader probes rise from five to seventeen. The intended
faulty-peer command is not reached. Membership refresh/background observations
increase; these counters alone do not identify why the seeded leader hint is
unusable. The bounded named-counter failure context preserves this distinction
without relaxing assertions or granting authority. Full-suite interaction and
the remaining SQL/KV/Queue, recovery, authority and feature-mode gates stay open.

### Full native primitive follow-up and SQL/KV evidence, 2026-11-15 UTC

The original complete native primitive target now passes at repair source
`9f483e79`: **37 passed, zero failed and 41 explicit ignores**, in 3,222.81
seconds. All owners join and compiler inputs remain unchanged through the run.
SQL/KV, Queue callbacks/four-in-flight/load, source-proof dispatch, activation
caching, name discovery, retained binding restart and Workflow controls execute
with their original bounds and assertions. The eight earlier primitive failures
at `f8917ffe` are unreproduced in this new run; no isolated cause or performance
improvement is inferred. Ignored RustFS controls remain unrun. This is native
in-process qualification, separate from the failed constrained comparison and
the still-failing removed-member protocol-cut oracle.

After that owner joins, Python repair source `467e179f` adds SQL/KV producer and
partial readback evidence. Six controls reproduce missing records on the prior
client: a lost-response overwrite, verifier construction failure and partial
readback failure in each API. Twenty-six tampered ledgers are incorrectly
accepted by the earlier validator. The repaired controller derives latest keys
and deterministic hashes from per-worker acknowledgement counts, reconciling
every completed readback. Wrong/missing values and incomplete probes still
fail. A body matching an uncertain attempt is not an exact receipt or commit
proof; request failures remain unqualified and cause no fresh-ID retry.

All 340 performance tests run successfully with one existing skip, including
19 owning controls, and documentation/whitespace checks pass. Only the three
Python files and performance guide differ from the native run's compiler
manifest; all other inputs, including Rust, are byte-identical. Earlier frozen
windows and their evidence gaps remain unchanged. Real constrained throughput,
latency, membership/recovery cuts and combined OPT/CG acceptance remain open.

### Main 16c integration and physical-credit wake candidate

The working integration includes main 16c45b26, preserving this branch's
original-deadline, removed-member, uncertain-command and archive-cohort controls
beside EventStream paths, checkpoint continuation and evictable archive caching.
The idle-cache control starts with an independently proved S3 output;
background-only verification still cannot authorize a later Quorum-to-S3
upgrade. The existing missing-sibling negative oracle remains unchanged.

Two corrected native controller regressions at input df4e8b50 reproduce a
progress gap with materialized images. A retained S3 result or explicit
checkpoint occupies the ordinary one-slot source bank. Another original
five-second request reaches an actual capacity refusal. Dropping the last
physical owner does not wake it: both expire with ambiguous DeadlineExceeded,
in 5.403 and 5.317 seconds. The existing 60-second slow-maintenance fault prevents
timer masking. No operation/callback/lease deadline or bank is increased. The
first draft's setup failures remain separate from this reproduction.

Candidate input 362f78db records the exact failed bank and requested bytes in a
job-scoped weak hint. The shared controller registers for its existing release
notification and checks fitting byte/item capacity. Retry still reacquires
physical credit and checks current authority, closure, receipt and original
deadline. Store-lock/remote-conflict backoff remains unchanged. Healthy owners
scan no archive roster and allocate no capacity future. Only blocked owners are
indexed; no task, timer, image pin or permit is introduced.

Strict all-target Cellar Clippy, formatter/layout, native-dependency and
documentation checks pass. Both progress cases now pass both image forms,
alongside four capacity-race/ownership units and three reuse/dependency controls.
The complete default suites pass 1,229 library, 33 archive-controller and 40
archive tests, with 2/30/10 existing ignores respectively. Nine selected real
RustFS cases pass both images; unselected ignores stay open. The matching Linux
image 70b02055 rebuilds both owning libraries and both binaries with the same
locked lab profile/client. See the qualification record.

The new constrained matrix is running. KV+S3 passes all 18 windows with zero
errors. Queue+S3 fails its first C16 enqueue with five unconfirmed errors; all
2,256 acknowledged message bodies are observed, plus 32 exact uncertain inputs,
without duplicates or unmatched bodies. Eight uncertain IDs remain unobserved.
All Queue nodes exit cleanly without lease-expiry/OOM/restarts. Four failed
target attempts start with only 1.2--3.0 seconds of the original five-second
budget left. Inclusive phase counts cannot identify their complete causal
path. This fix therefore does not close every constrained deadline or establish
an isolated end-to-end throughput/latency gain.

The [uninstrumented repeat](three-node-primitive-main-173-2026-11-15.md#uninstrumented-production-image-repeat)
retains five failures across six old-main S3 cases; only candidate SQL passes.
Diagnostic KV and the old-source 1,192-test library passes remain scoped to their
earlier manifests. Current-main constrained SQL/KV/Queue, bucket Queue,
lease/startup diagnosis, physical reclamation/movement, authority faults and
complete OPT/CG qualification remain open.

### Foreground-only physical-credit wakes

The capacity wake introduced at `1d663425` also wakes background publication.
New owning-controller tests at compiler input `525cec94` reproduce an early
retry twice, and reproduce it after a foreground S3 waiter expires. They hold
an actual checkpoint in the ordinary one-slot bank, observe the real Busy
refusal, release its last owner and check the configured GroupLeader 500 ms
minimum cadence. The expired call retains its original five-second deadline
and ambiguous outcome. Before controls fail with materialized images; the
extent loop is not reached. The retained fixtures and failures are not
reclassified as setup errors or timing gains.

Candidate input `1d75a95f` captures failed physical admission only for live
foreground S3 waiters. It prunes expired/cancelled requests before selecting a
publication batch, before installing the completed wait, and on capacity wake.
A background-only release leaves its existing retry time intact. A new live S3
request can still initiate its ordinary immediate attempt. Actual admission,
current authority, exact receipt/closure checks, original deadlines and physical
ownership remain required; no bank, task, timer or proof shortcut is added.

Four focused current-source controller controls pass both image modes: the new
background and expired-waiter cadence cases, and the original held-output and
held-checkpoint release progress cases. Their test wall times include setup and
forced waiting, so they are not per-operation latency measurements. The complete
default suites pass 1,229 library, 35 archive-controller and 40 archive tests,
with 2/30/10 existing ignores respectively. Four capacity units and seven
focused controller controls repeat subsets of those suites. Strict all-target
Cellar Clippy, format/layout and native-dependency checks pass. Nine selected
real-RustFS cases pass both image forms on the same executables; unselected
ignores remain open. See the source-specific qualification.
The matching Linux image and scoped SQL result are recorded below. The remaining
current-source APIs and complete constrained matrix remain unqualified.

The complete [four-mode matrix](three-node-primitive-main-170-2026-11-15.md)
retains its earlier `362f78db` image and four-pass/eight-failure outcome. It does
not contain this foreground-only candidate. The new fix does not establish the
cause of those lease/deadline failures or close any complete CG/OPT package.

### Matching Linux image and current SQL quorum result

The matching Linux image `aecb101f` binds frozen source `bfa06775` and compiler
input `1d75a95f` to public `12eacc18`. Both owning libraries and both binaries
rebuild with exactly the previous compiler/lab profile/client. SQL quorum passes
all 18 original windows, with zero request errors and clean node exits without
lease-expiry/panic/OOM/restarts. This does not qualify the held KV/Queue cases or
an isolated speedup against the earlier incomplete SQL case.

The [source-specific SQL result](three-node-primitive-main-170-2026-11-15.md#foreground-only-cadence-sql-quorum-re-verification)
records rates/percentiles and the separate storage-preflight refusal. After the
SQL case, 3.34 GiB free is below the unchanged 6 GiB guard. No remaining case
starts; the exact original RustFS is restored ready and all owners join. This
owner changes no VM resource; its evidence remains bound to the 160 GiB VM. Earlier
native/backend/cadence checks retain their original source and scopes.

### Dedicated benchmark lease transport

The benchmark frontends cloned their data/archive `Bucket` into
`NodeFleet::lease_bucket`, unlike the normal node CLI's dedicated lease client.
The Raft frontend also uses the data client for operator preparation before
starting the core runtime. A pooled HTTP connection remains driven by the
runtime that opened it; sharing that pool can make core lease CAS depend on
application runtime progress.

Compiler input `3d6c2f48` gives all four measuring frontends separately built
data and lease clients through `tools/perf/object_clients.rs`. Ordinary retries
and each frontend's original conditional retry policy remain unchanged. No
runtime, worker, deadline, capacity bank, authority check or durability proof is
changed. This is a benchmark transport correction, not a measured core speedup.

The actual factory regression uses a healthy independent HTTP server, two
application-owned runtimes and a five-second lease CAS deadline. Once the
application runtime is held, the old shared policy fails twice: warmups reuse
one connection and the lease request never reaches the server before timeout.
Only the factory's lease construction differs in the after source; test bytes
match. The corrected policy passes in all four frontends and strict Clippy
passes. Server and application owners join. Test wall times include setup and
cleanup, and do not establish request latency or throughput gains.

The broader frontend runs pass baseline 8/8, fault-cells 6/6 and cells 4/4.
The first Raft full-suite launcher omitted `CARGO_TARGET_DIR`; five tests refused
their target-directory preflight. With that environment corrected, Raft passes
25/26 and the rooted-128 diagnostic reaches its original 60-second enrollment
deadline. Its retained observations show 128 cells on each group-0 voter.
That test uses an in-memory store and does not call the S3 factory; the broader
enrollment test, which also includes occupancy 128, passes. The failed attempt
remains a reliability finding regardless of later exact-repeat outcomes.
Two unchanged exact repeats subsequently pass in 61.57 and 134.39 seconds
including setup, commands, follower reads and cleanup. These are not enrollment
durations, and neither pass explains the earlier timeout.

The source-specific lease-client record
retains before/after hashes, full-suite failures and exact-repeat outcomes.
The earlier 1,304-test library/archive and nine RustFS passes remain scoped to
the cadence candidate's source. No matching Linux image or new container
measurement qualifies this client change yet. The disk expansion proposal is
unapproved; no VM resource changes, data deletion or deadline increases are used
to proceed. Full primitive, fault, feature and CG/OPT acceptance stays open.

### Enrollment resolution waits for potentially decisive applied evidence

Compiler input `a8b26ff9` removes an unnecessary SQL-worker round trip from the
admitted enrollment owner. Absence at or below the preparation's applied term
cannot resolve that producer. The dispatcher keeps its original ownership and
waits on existing progress. A present record or newer term still goes through
the unchanged SQL owner for exact binding/current-state validation and physical
retirement. No result, authority or admission credit is granted by the skip.
Deadlines, queues, banks, retries, worker counts and durability proofs remain.

The actual controller regression holds physical manifest publication and checks
the original creation, one initializer, retained producer/response admission,
exact duplicate result, conflict refusal and joined close. Formatted before
input `dddf5061` fails twice, each with five unsuccessful SQL resolution calls.
That failure follows the other assertions and joined cleanup; only the
materialized iteration is reached. Afterward, identical test bytes pass twice
in both image forms, with zero failed resolution calls and one exact successful
resolution per image. Only the production dispatcher differs between these
before/after compiler manifests. All failed fixture bytes remain retained.

The complete default execution suite passes 208 tests with two existing ignores;
archive-controller passes 35 with 30 ignores. Focused strict Clippy passes.
Three selected real-RustFS enrollment controls pass ten image/configuration
iterations, covering original creation, serving authority and deadline ownership.
They exercise three default ignores separately. The exact original backend stays
ready, with unchanged identity, image, resource limits, restart count and start
time. This remains host-native RF3 qualification, rather than constrained-node
throughput or latency evidence.

The full Raft tool remains failed: 25 pass and the separate rooted-128 diagnostic
reaches its original 60-second enrollment deadline. Its failed fixture records
111 SQL resolution calls with zero failed checks, alongside substantial SQL
preparation and file-sync time. Zero failed resolution checks does not mean zero
API errors. A separate diagnostic-only before probe records 100 successful
enrollments consuming roughly the complete total budget, leaving 154 ms for the
next call; no completed call exceeds two seconds. Its temporary timing markers
are removed. These observations do not isolate an end-to-end speedup or close
the cumulative enrollment latency gap.

The source-specific record
retains before/after hashes, counters, default-suite failure and real-backend
scope. Formatting/layout and native dependency checks pass. The design tests
first pass 43 cases and reject a stale catalog anchor; after reviewing the
complete seven-line namespace payload binding diff, only catalog/birth hashes
are re-pinned to `cbde019b`, with their excerpts and the other 22 anchors unchanged.
All 44 design tests then pass. Historical runtime evidence, task acceptance and
the missing CG-01 record stay unchanged. No Linux image, new constrained primitive
matrix, independent failure-domain or complete feature/release gate is qualified.

## Local command rejection before transmission

The unchanged sender behavior at input `281f4eb0` returns generic `StalePeer`
both before queue acceptance and before the worker starts an exchange. Both
controlled non-submission assertions fail; the post-authenticated-reply unknown
control passes. Earlier malformed fixture attempts stop at compilation or endpoint
validation and are retained separately, rather than counted as reproductions.

The candidate separates synchronous queue admission from its response waiter and
tracks whether the worker started an exchange. Only stale/missing routes and
capacity refusal before those boundaries may produce local `CommandNotSent`.
It is not a wire result. The invocation retains the original ID, fingerprint,
captured inputs, admission, absolute deadline and finite attempt bank. A local
proof settles that attempt only; earlier uncertainty remains. Once exchange
starts, socket failure, partial transmission, lost reply and post-reply stale
generation remain unknown. No durability, authority or process fence changes.

All three controlled transport cases pass. The native extension initially spends
the original command's eight tickets before its existing final gateway-revocation
check. Its correction places the additional sticky-proof checks in the existing
follower invocation's remaining tickets, preserving all original assertions and
limits. At input `7c39c193`, all three native command lifecycle cases and all four
selected public routing cases pass in both image forms. The original command
still uses exactly eight tickets, and callbacks retain their exact counts.
Strict all-feature/all-target and default recovery-consumer Clippy, formatting,
layout, dependency, documentation and all 44 design checks pass.

The full library attempt reports 1,247 passes, one failure and two existing
ignores. The query-grant control retains original node/peer receive credit after
truncated read-refusal input past its unchanged three-second observation bound.
Both frozen published and candidate isolated repetitions pass with identical
fixture bytes; they do not explain or close that failure. Only the native test
file changes afterward, and Cargo verifies the library executable is byte-identical.
All seven attempts retain their closed fixtures; originals are verified twice
and copies are verified after closure.

The qualification record
keeps these failures, hashes, profiles and scopes explicit. The selected
member-replacement pass does not establish a causal repair of its earlier
full-run original-ID failure. Queue warmup/four-in-flight deadlines, read-credit
release, broader native/SDK/daemon/disaster, reclamation, movement and fairness
gates remain open. The failed Queue profile points to SQL preparation/publication
and persistence for further measurement; its aggregates do not identify one
request's critical path. No throughput or percentile improvement is claimed.
The original RustFS backend has 12.3 MiB free, below its unchanged 6 GiB reserve,
with no restart, resource change, pruning or reserve relaxation. The full original
constrained SQL/KV/Queue comparison and all complete OPT/CG gates remain open.

## Original-ID receipt resolution after unknown remote writes

At compiler input `40723c82`, an originating public command may resolve an
ambiguous remote availability outcome through receipt-only reads. These borrow
the original request and routing session: ID, fingerprint, captured clock/random
inputs, admission, absolute deadline and finite route tickets remain unchanged.
No callback or mutation is resubmitted. Receipt absence stays unknown. Positive
results require the existing exact Quorum receipt or required S3 proof; moving
the result retains its SQL/archive or physical reply custody and grants no local
producer authority. Incoming commands and System/Event admissions retain their
prior contracts.

The frozen old runtime fails the new positive control in both image forms: an
independent read finds the committed receipt, but the original invocation returns
ambiguous `StalePeer`. The rollback/absence control passes. Four new Quorum
controls pass afterward, including the original one-ticket bank and a single
caller admission slot. A new S3 control holds physical publication, invalidates
the original reply and requires the exact S3 result with one callback. It passes
both image forms. Six existing command/receipt ownership controls and seven
existing S3 proof, minimum, deadline and slow-delivery controls also pass.

The full library passes **1,248 tests**, with zero failures and two existing
ignores. Strict all-feature/all-target and default recovery-consumer Clippy,
formatting, layout, native dependencies, documentation and all 44 design checks
pass. The first build's private-helper visibility error and a later S3 fixture's
forbidden `tokio::select!` remain recorded. The fixture now uses the execution
domain's fair select macro. Cargo verifies the final library and execution
artifacts byte-identical to the passed sweep and ten native controls; the changed
S3 case passes again. Production artifact bytes remain unchanged across the S3
fixture revisions. See the
source-specific record.

These are host-native RF3/TLS cases with in-memory object-store adapters, rather
than the constrained RustFS profile. Persistent fixture disks, logs, source and
executables remain private; closed originals are verified twice and copies are
verified. In-memory store contents are not exported. Physical disconnect cuts,
election/member/split repair, automatic local receipt resolution and pending RPC
checkpoint progress still need qualification. The original full public routing
failure, Queue warmup/four-in-flight deadlines, fairness, reclamation, movement
and combined SDK/recovery gates remain open. At 17:33 UTC, the unchanged RustFS
backend has 20.8 MiB free, below its 6 GiB reserve. No throughput or percentile
gain, complete OPT package or remaining CG acceptance is claimed.

## Terminal command-origin denial

The full routing attempt at input `40723c82` reports 20 passes, three failures
and 13 explicit RustFS ignores. Frozen matched controls reproduce two regressions:
origin revocation and superseded-boot routing pass before receipt reconciliation
and fail afterward. Revocation adds nine probes and three membership refreshes
instead of retaining the direct-path counters. Both frozen old-lookup controls
pass; they do not explain that case's original full-run warmup refusal.

Input `b08df720` returns a signed terminal command-origin denial for command,
probe and receipt purposes from a known physical process. The existing frame
binds the result to the complete request-body digest, request tag, sequence and
TLS exporter. Denial drains the bounded body under the existing process owner,
I/O bound and node/peer receive banks. It grants no SQL admission, callback entry,
non-commit proof or mutation retry. Original command ambiguity remains sticky.
Other purposes reject this marker. A wrong physical identity remains `StalePeer`
before namespace permission is inspected; no physical fence is relaxed.

The unchanged revocation and superseded-boot tests pass in both image forms.
Revocation restores one application RPC with zero extra probes or membership
refreshes across SDK, native HTTP and Axum, with no SQL or callback entry. The
full routing target passes 23 tests with 13 explicit RustFS ignores, including
the old-lookup case. That later pass does not establish a causal repair of its
original warmup failure. All 32 wire tests, three sender-boundary tests, ten
native command/receipt controls and eight S3 controls pass. The full library
passes 1,250 tests with two existing ignores. Strict all-feature/all-target and
default recovery-consumer Clippy, formatting, layout, native dependencies,
documentation and 44 design checks pass.

The first qualification attempt correctly stops when an incorrect exported
module filter selects zero sender tests. The next fails a new guard's incorrect
bootstrap-membership assumption. The corrected guard checks terminal missing
permission and stale physical identity; all original sender assertions remain.
Production and routing artifacts are byte-identical across that test-only
correction. Both unsuccessful attempts and their exact sources remain recorded.
Closed persistent fixture originals are verified twice and copied files are
verified; in-memory store contents are not exported. See the
source-specific qualification record.

These host-native RF3/TLS fixtures include six-data-node/ingress routing cases
and a one-SQL-worker fairness case; they do not qualify the requested restricted
three-node deployment or independent physical failure domains. The original
RustFS volume remains below its unchanged 6 GiB reserve. No backend data,
resources, restart policy or reserve is changed. No throughput or percentile gain
is claimed. Queue warmup/four-in-flight deadlines, pending-RPC/local receipt
progress, retained unexplained failures, physical disconnect and recovery cuts,
release-scale fairness/reclamation/movement, combined SDK/daemon/feature modes
and the original complete constrained comparison remain open. No complete OPT
package or remaining CG gate is accepted.

## Cell-specific SQL preparation and active sequence handoff

Final compiler input `54f68d4a` removes two bounded sources of repeated ingress work.
Overlapping, already admitted stateless calls can hand off one successor sequence
only after validating an exact positive receipt for the same cell history and
reserved ingress client. The hint is consumed once, discarded on owner error or
cancellation, and removed with the last active FIFO caller. Cold calls retain the
original quorum sequence query. Conflicting sequences still require the unchanged
fresh-quorum consumed-other fingerprint proof; a lost response never permits a
new mutation identity.

The group owner also permits private SQL preparation when the entire committed,
unapplied prefix contains only validated ordinary mutations of other logical
cells. A 32-entry/64-KiB scan bounds owner work. Gaps, larger prefixes, malformed
entries, enrollment and all control entries keep the existing group barrier.
Current-term leadership, route/membership, recovery, park, seal and activation
fences remain. Exact target image/version checks and the private writer remain;
ReadIndex and acknowledgement proofs do not change.

The same native regression helper fails at the intended `NotReady` boundary
before this owner change and passes afterward in both image formats. It prepares
B through the existing verification helper, holds its actual committed apply,
then stages A through the existing single-worker pool. A commits behind B, a
second A and B remain refused, and untouched C can obtain authority. After
release, all three voters agree on exact receipts and versions; replay does not
rerun A's callback and retained SQL ownership reaches zero. Existing enrollment
and original-deadline controls pass unchanged. The full native execution target
passes 219 tests with two existing ignores.

The native execution and primitive executables are byte-identical to runtime
input `7d62cfdc` across a five-line caller comment correction. The production
library archive hash changes; it is not claimed byte-identical. Final-source
library and remaining native/strict checks are run directly.

The original four-in-flight Queue workload still fails in the materialized
iteration with four failed producers, peak four offered calls and zero active
calls after producer join. Its original command sequence remains retained and
the extent iteration is not reached. Queue proof sharing passes its four native
configurations. These results do not close the Queue progress gap or establish
an end-to-end gain. The nested native ingress policy is three seconds; the
five-second callback timeout and constrained benchmark's five-second operation
bound are separate and unchanged.

The full final-source library passes 1,253 tests with two existing ignores.
Seven of eight selected native S3 controls pass. The deadline/ambiguity control
fails because its expected ambiguous 800-ms expiry arrives before retained
S3-wait admission is observed; that assertion does not observe a successful
acknowledgement. Forward frozen controls pass the published binary and fail the
current command check. Reverse controls fail current enrollment and the same
published command check. This failure is therefore also reproduced on published
source, without a causal repair or complete phase coverage. All original limits
and assertions remain. Strict all-feature/all-target and default recovery-consumer
Clippy, formatting, layout, dependency/documentation checks and all 44 design
tests pass.

The first current-source SQL recovery matrix passes seven configurations and
fails materialized S3 setup before the interruption oracle. The frozen published
baseline and the frozen handoff-only candidate each pass all eight configurations.
An unchanged full-candidate repeat also passes all eight. These controls do not
establish a causal repair; the original setup failure and its data stay retained
and full stable recovery qualification remains open.

The first new regression fixture build used private receipt wrapper fields and
failed compilation. Its next build passed but source guarding refused runtime
qualification because formatting completed after capture. Both attempts remain
retained. The corrected frozen before run reproduces both intended refusals,
releases held work and joins shutdown. Temporary diagnostic correlation is
removed from the final source. A selected legacy chained load control was
already explicitly ignored and ran zero runtime tests, so its count guard stopped
the attempt. That separate twenty-second control remains ignored. A subsequent
archive byte-identity guard also stopped when only the two native executable
hashes matched; the final checks make no production archive identity claim.
All closed persistent originals are verified twice and copied files are verified;
in-memory stores are not exported.

Published parent `504dd25b` has additional hosted failures: the 3.14 native
campaign fails transition disaster closure selection and panics while observing
a drained source collection; the 3.14t campaign fails managed replacement and
S3 seed-refusal cases. Python 3.14 fails held consumer admission/runtime shutdown,
and Node package delivery exceeds its original 250-ms handler budget. Passing
focused diagnostics do not settle these broader failures. These are parent
results, not qualification of this candidate.

See the source-specific record
for exact artifacts, cohorts, failures and scope. Native elapsed times are not
constrained throughput or latency percentiles. The unchanged original RustFS
volume reports 13,623,296 free bytes (about 13 MiB), below its unchanged
6-GiB reserve. No backend pruning, restart, resource or reserve change is made.
Queue progress, combined hosted SDK/recovery, sustained fairness, physical
reclamation/once-only credit, repeated reserve-aware movement, cross-ingress
contention and the original complete constrained comparison remain open.
No complete OPT package or remaining CG gate is accepted.

## Typed pending command settlement

At compiler input `6c280bdd`, a positively observed pending SQL producer can
reuse its unchanged term and complete cell version within one live invocation.
Each settlement attempt still fetches and checks current authority; changed
terms or images force SQL reconciliation. Generic uncertain results retain the
original resume/receipt path. Both image modes turn an intended-red work-count
regression green: four pending checks use one reconciliation and no resume,
then preserve exact commit or newer-term unchanged-image non-commit resolution,
once-only callbacks and joined closure. Twelve focused native checks pass.

Current-source qualification remains **failed**. Full native execution reports
220 passed / 1 failed / 2 existing ignores. The ownership response-deadline
case fails with an unlocalized elapsed deadline. Ordinary SQL fails its concurrent-ingress
race under the original deadline. Original Queue four-in-flight still fails in
the materialized iteration before extent coverage. KV, ordinary Queue, warm and
fragmented KV, Queue proof sharing and the interrupted SQL recovery matrix pass.
Full library: 1,253 passed / 0 failed / 2 existing ignores. All eight selected
native S3 controls pass, including the previously unstable deadline control; this establishes no
causal repair or stable full S3 acceptance. See the
pending settlement record for every check,
profile and failed control. Earlier pipeline evidence
remains historical evidence at its own source, not current-source qualification.

Frozen baseline/candidate runs in ABBA order pass both the response-deadline
and ordinary-SQL cases. Those passes leave the broad failures unexplained
and establish no causal repair.
The instrumented failed Queue workload exercises 766 cached observations and
retains 29 generic-uncertain resumes. Its remaining wakes match the existing
five-millisecond controller retry timer. These are diagnostic observations,
not throughput or latency percentiles. The recorded read-only original-backend
check finds zero available bytes below the unchanged 6 GiB reserve; no constrained run is
admitted. No complete OPT/CG, SDK/release or performance gate is closed.


## Retained producers and RPC checkpoint settlement

Compiler input `f8f9bee8` preserves public uncertainty while reducing two
redundant SQL paths. Ownership of the exact live producer avoids Resume without
caching uncertain authority. A checked same-term intermediate RPC receipt can
reuse one pending view until its term or full cell version changes. Neither
observation grants output, non-commit, retirement or credit release.

At `e0d61c1b`, two isolated work-count regressions turn red to green in both image
modes with identical fixtures. The current source corrects two fixture selector
calls with production inputs unchanged and rechecks all sixteen focused cases.
Four final-RPC observations use one reconciliation and no Resume, followed by
exact output and duplicate replay without another callback. Stale observations
remain uncached. See the source-bound record
for current/historical results, exact executable reuse and failed-fixture retention.

Owning qualification is **FAILED**. Native execution:
225 passed / 0 failed / 2 existing ignores; library:
1252 passed / 1 failed / 2 existing ignores; original Queue C4: FAILED.
Selected public/S3 controls: 16/19 pass. SQL recovery:
5/8 configurations pass. Isolated ABBA controls and the focused
recovery diagnostic establish no repair of failed broad runs. Published-parent
Python lifecycle and current-source full SDK/fault qualification remain open.

The original RustFS has 0 available bytes at the read-only 2026-11-18 06:19 UTC check,
below its unchanged 6 GiB reserve. No constrained campaign is admitted. No
throughput/percentile gain, complete OPT-01–08 package or remaining CG/release
acceptance follows from this component result.


## Disposable shared-cell materialized scratch

Committed source `9c4f830b` includes fetched `origin/main`
`2f083f14`. Shared-cell materialized enrollment and commands
create disposable scratch without syncing its ancestor directories. Fresh
initial-state/image authority, path bounds, private locks, workspace lifetime and
sealed-image/engine persistence barriers remain required. The legacy group
executor retains its original directory policy.

The early corrected regression is intended-red against production `0da514a4`
and green on the candidate with identical fixture and profile. It commits an
initialization and command, checks every replica's version/witness and exact
value through engine-bound readers, and joins the owners. Only three production
files differ in that comparison. The earlier absent-applier-map fixture panic,
its stopped qualification and all failed data remain explicit in the source-bound scratch record.

Selected owning qualification is **FAILED**: 37/38 Rust selections
and 9/9 Clippy/repository checks pass. Failed selections: `queue_four_in_flight_preserves_acknowledged_messages`.
A selected pass does not establish sustained progress or repair earlier full-run
or hosted SDK failures. Ordinary SQL, KV, Queue and fragmented KV pass; native
SQL recovery passes all eight configurations with an in-memory object store.
Queue C4 fails two producers in the materialized run at four offered requests;
the extent iteration is not reached. Original deadlines and admission bounds
remain unchanged.

The original RustFS read-only check at 2026-11-18T08:27:31.405819+00:00 finds 8,589,312 available bytes, below the unchanged 6,442,450,944-byte reserve. No constrained campaign is admitted.
Native work counts, stack samples and test durations establish no throughput or
latency-percentile gain. Complete OPT-01–08 and remaining CG/release gates stay
open. P0 remains repeatable original-deadline SQL/KV/Queue, receipt/RPC and recovery
progress. Next are persistence/archive batching and shared-worker fairness,
physical reclamation/movement, authority/read fault coverage and the complete
source-matched constrained comparison.

## Independent Queue image deadline verification

Test-entry source `bbb93651` keeps production identical to
`29a735c9` and includes `origin/main` `2f083f14`.
Independent materialized and extent-backed Queue C4 entry points now run the
same original load oracle. The original combined C4 and C2 tests remain required;
the canonical load body, identities, assertions, deadlines, banks and one SQL
worker are unchanged. RustFS variants are listed only.

**Latest native qualification is FAILED.** Both independent C4 modes, the
original combined C4 and original C2 control fail with ambiguous deadlines.
Both independent modes observe four offered requests; C2 observes two. Each
failure retains its original identities and data. 8/8 affected Clippy and
repository checks pass. This run qualifies no sustained Queue progress and
does not erase earlier full-run or hosted failures.

The unmodified production baseline reproduces C4 failure. Two phase-triggered
samples contain foreground apply/image persistence, private preview, archive
replay and checkpoint verification; overlapping worker samples do not attribute
exclusive request time. Healthy/outage/outage/healthy controls fail in both
the write-only and refined write-plus-checkpoint-read outage experiments. The
refined outage records actual denied reads/writes in both failed runs. Pausing
archive I/O therefore does not resolve the deadline; in-flight work remains a
limit of this control. All temporary probe code is removed. See the
source-bound Queue record
for source/executable/profile identities, every outcome and retained manifests.

The read-only original RustFS check at 2026-11-18T09:42:09.811621+00:00 finds
10,035,200 available bytes, below the unchanged
6,442,450,944-byte reserve. Identity and limits match with zero restarts;
no constrained campaign is admitted. Test durations and samples establish no
throughput or latency-percentile gain. Full OPT-01–08 and remaining CG/release
gates remain open.

P0 is repeatable original-deadline SQL/KV/Queue and receipt/RPC/recovery progress,
including both Queue image modes and the C2 control. Foreground image preparation
and persistence require causal measurement before another production change.
Then qualify bounded persistence batching and shared-worker fairness, physical
reclamation and movement, combined authority/recovery/SDK faults, and the full
source-matched SQL/KV/Queue constrained comparison.

## Exact live-image reuse and incoming-voter fencing

Committed implementation `53c0fb05` includes `origin/main`
`2f083f14`. A pending producer's independently replayed and
synced image can serve the exact live committed apply on that engine. Engine,
group, encoded input, validation limits, store, result and base checks remain
required. Submission and ticket clones carry weak references. Independent
follower/archive/recovery replay, inventory health, commit/serving proofs,
operation deadlines, banks and workers remain required. A cached result name
without the live capability must independently replay the supplied LTX.

Both image work-count regressions pass after exact replica value, receipt,
version/witness and joined-close checks. Leader base-image reads fall from one
to zero; follower reads stay one each. Materialized apply seals fall from one
to zero. This qualifies reduced duplicate work, **not throughput or latency**.

A full native run exposed outgoing-only application readiness after a Joint
restart. The fix requires incoming membership for application readiness,
producer/enrollment and leader-bound read authority; outgoing followers cannot
campaign. They still age voting leases and vote for legitimate incoming
candidates. Higher-term handoffs retain term evidence without self-voting.
An existing outgoing protocol leader can finish Joint. Six focused cases and
twelve original crash/sync/delivery/image controls pass on their recorded
source. The final combined run rechecks the full suites below.

**Combined owning qualification: FAILED.**
Strict all-feature/all-target Clippy, native-only host/Python facade build,
format/layout/dependency/docs/plan/runner/whitespace checks pass (9/9).
Current-source failed selections:

- `native_cell_groups_primitives--materialized_queue_four_in_flight_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--rooted_queue_four_in_flight_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--queue_four_in_flight_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--queue_load_preserves_acknowledged_messages`.
- `native_cell_groups_primitives--ordinary_kv_bindings_use_managed_shared_groups`.

Full-suite results:

- `extent_apply--full`: PASSED; 15 passed, 0 failed, 0 ignored.
- `cellar--full`: PASSED; 1257 passed, 0 failed, 2 ignored.
- `native_cell_groups_execution--full`: PASSED; 232 passed, 0 failed, 2 ignored.
- `native_cell_groups_storage--full`: PASSED; 22 passed, 0 failed, 2 ignored.

The four original Queue C2/C4 checks retain their exact offered concurrency
and ambiguous deadlines. Independent C4 cases exercise both image modes;
the combined/C2 cases stop at their first failed materialized iteration.
Ordinary Queue and batch-proof sharing, ordinary/Axum SQL, Axum KV,
fragmented KV, ordinary JSON RPC and both selected SQL recovery cases pass.
A failed ordinary KV negative-provider case returns an ambiguous deadline
rather than the required unavailable-payload error; later configurations
inside that test are unrun. See the source-bound record for every
selection, artifact/profile, failed earlier cohort and retained manifest.
The earlier failed membership-crash TempDir auto-cleaned before capture:
its disk state is unavailable, while logs/source/executable and remaining
fixture data are retained. Crash parents now retain their directories.

The read-only original RustFS check at 2026-11-18T11:39:56.787709+00:00 found
0 available bytes, below the unchanged 6,442,450,944-byte reserve.
No constrained campaign is admitted. The native suites use in-memory object
stores. Previous published failures remain; later narrow passes do not replace
them. Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open.

P0 remains repeatable original-deadline SQL/KV/Queue, receipt/RPC and recovery
progress. Then measure persistence batching and shared-worker fairness,
physical reclamation/once-only credit and reserve-aware movement, combined
fault/SDK qualification, and the original three-node SQL/KV/Queue
Quorum/Quorum+S3/Fleet/Bucket C1/4/16 throughput/latency comparison.

## Disposable close checkpoint work and current priorities

The materialized shared-cell close now avoids copying a disposable WAL into the private main file immediately before that file is discarded. The real-close regression independently seals and syncs the result, observes the old main inode across actual cleanup, and rebuilds the correct value from the sealed result. The same fixture fails before the policy change and passes after it. This proves avoided work; it does not qualify throughput or latency.

Committed implementation `aa93bb82` includes `origin/main` `2f083f14`. The full affected library (1,258 passed / 2 existing ignores) and native execution (232 passed / 2 existing ignores) suites pass. All nine strict checks pass. Nine of ten selected SQL/KV/Queue/RPC/recovery checks pass; ordinary Queue fails with an extent-iteration deadline. Combined qualification remains **FAILED**. Original Queue C2/C4 load qualification still fails. Two temporary C2 phase probes reach successful enqueue completion at peak two, then fail the original final consumer drain; one observes 472 of 528 required completions. Their diagnostic sources and all failures remain distinct from the restored candidate. The probe code is removed. Ordinary Queue and batch-proof A/B/B/A controls also reproduce failures on the published parent; they do not establish a candidate-specific regression or close Queue progress.

Completion priorities, in order:

1. **P0: Progress and proof correctness.** Close original-deadline Queue delivery/drain in both image modes, repeatable provider-error handling and remaining receipt/RPC/recovery failures. Published-parent hosted native-framework failures include disaster/token recovery's diagnostic panic and the S3-output pending-response oracle. Classify the actual S3 response before attributing an incorrect acknowledgement.
2. **P1: Complete configured fast paths.** Carry the configured extent apply validation profile into independent image preparation. The public profile currently refuses reuse because execution-derived preparation and committed apply limits differ. Preserve exact-profile equality, negative controls and existing budgets.
3. **P1: Bounded persistence and scheduling.** Measure and reduce SQL queue/image work, physical sync amplification and archive interference. Preserve Ready/sync/completion ordering and progress for leases, elections, control and shutdown; sparse traffic must not wait to fill a batch.
4. **P1: Sustainable capacity.** Prove physical shared-segment deletion and once-only capacity credit, then repeated reserve-aware split/move/merge with retained readers, snapshots and uncertain work.
5. **P2: Combined release and comparison.** Complete authority/partition/wake/recovery, SDK/daemon/feature-mode gates and the source-matched SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 matrix on the original three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared RustFS, 256-byte payloads, Queue batch eight and reversed repetitions.

The latest read-only original RustFS observation still fails the unchanged 6 GiB free-space reserve; no constrained comparison is admitted. All OPT-01–08 and remaining CG/SDK/release/performance gates stay open. See the source-bound close qualification for exact sources, artifacts/profiles, all outcomes, retention limits and capacity evidence.

## Separate S3 output and mutation readiness

Implementation `5546c388` includes `origin/main` `2f083f14`. A committed S3 response could leave while a later unrelated-cell mutation was held at the real group-manifest sync. The trace observes a successful S3 response at 1,243 ms within the original 5,000 ms budget. Mutation preparation intentionally tolerates a bounded unrelated-cell prefix; output had incorrectly reused that exception. Missing archive data or data loss was not established.

S3 command output now requests an atomic whole-prefix authority check from the existing group owner. It retains the exact committed receipt and archive proof until applied equals committed, then performs the existing route, leader/catalog, token and deadline checks. Preparation keeps its original per-cell prefix optimization. Budgets, workers, deadlines, public APIs and wire markers remain unchanged.

The paired regression requires preparation authority to succeed during the hold while retaining every original S3 pending-response, receipt/version/replica, handler-count and cleanup assertion. Before the fix, preparation passes and the original output oracle fails in the first materialized/fresh case; the other three iterations are unrun. The identical fixture/profile passes all four materialized/rooted and fresh/duplicate cases after the fix. All temporary phase instrumentation is removed.

Current source `35fa251497d583e62d8c19dfb611d95ed023dd09f52758f3071b0801c02d8db8` passes the full native effects target (10 passed / 1 existing ignore), eleven selected native execution controls and all nine strict checks. The controls retain unrelated-cell preparation, shared persistence, intervening-read ordering, event capacity, readiness deadlines and exact live-image work counts. This is a scoped run; the prior full library/native execution results belong to source391b and are not reattributed to this source.

Original application-boundary feedback (**FAILED**):

- `native_cell_groups_primitives--ordinary_sql_bindings_use_managed_shared_groups`: **FAILED**.
- `native_cell_groups_primitives--ordinary_kv_bindings_use_managed_shared_groups`: **PASSED**.
- `native_cell_groups_primitives--ordinary_queue_callbacks_use_shared_quorum`: **FAILED**.

The initial rooted concurrent SQL ingress failure records zero S3 coverage/output-check starts on all three nodes. Pinned parent391b/candidate35fa controls in A/B/B/A order: A:PASSED; B:FAILED; B:FAILED; A:FAILED. The additional current-source S3 SQL error/recovery control is PASSED. Initial failures remain recorded; controls do not erase them.

The read-only original RustFS observation at 2026-11-18T19:58:09.774810+00:00 finds 0 available bytes, below the unchanged 6 GiB reserve. Original identity/limits and the inspection pair are stable, with zero restarts. No constrained comparison is admitted.

The original Queue C2/C4 failures on source391b remain open; those minimum-Quorum load cases were not rerun for this S3-only output fix. The published-parent disaster/token recovery failure still requires diagnosis, and local S3 success does not close hosted SDK acceptance. Remaining priorities are original-deadline Queue progress, recovery/SDK faults, actual extent-profile propagation, measured batching/fairness, physical reclamation/once-only credit and reserve-aware movement, then the full original constrained comparison. Complete OPT-01–08 and remaining CG/SDK/release/performance gates stay open. No throughput or latency gain is claimed. See the source-bound S3 fence record.

## Configured rooted preview profile checkpoint — 2026-11-18

Implementation `070e6abe`, compiler inputs `321cde1a365fac246e0796b98c390e8b6234feac76c92fd3b7194d43478fe914`, carries the actual node apply profile into independent rooted preview validation and the exact live-image capability. It preserves execution/workspace/capture bounds, workers, deadlines and proof checks. A source-pinned former-selector control fails the new physical over-limit oracle; the identical fixture passes on the candidate. Existing direct executor and verification-pool constructors retain execution-derived defaults.

The full library passes 1,259 cases with two existing ignores. Native qualification has 27 passed, one failed and one existing ignore: S3/effects, eleven execution controls, three RPC controls, ordinary KV/Queue and disaster retry/old-token refusal pass; ordinary SQL fails on deadlines. The original nine strict/repository checks pass after reviewing the changed SQL pool anchor and rerunning all 44 plan tests. The initial source-drift rejection remains recorded. An additional full plan CLI validation still fails on missing historical CG-01 evidence; that gate remains open. Current Queue success does not waive the older C2/C4 failures. Recovery diagnostics have independent custody; the earlier hosted failure did not reproduce and its underlying cause remains unqualified.

The original RustFS capacity guard still fails at 0 available bytes. No constrained comparison or throughput/latency gain is qualified. Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open. See [current priorities](remaining-work.md#configured-rooted-previews-and-current-priorities) and the source-bound record.


## Cached committed RPC conflict rejection — 2026-11-18

Implementation `6a6878dd` includes main `2f083f14`. The existing bounded native activation now retains one final committed command ID/input digest. A different input at that ID can be refused before reopening private SQLite, after the exact current image/route/leader/class fence, schema, registered admission and authority checks. Matching inputs and other IDs use ordinary receipt inspection. Final commitment/close is required; intermediate checkpoints, result bytes, image custody and next-sequence/non-commit authority are not cached. SQL ingress still requires its fresh quorum consumed-other proof. Original stream IDs, deadlines, workers, activation slots and banks remain.

The same byte-identical native RF3 fixture fails before the implementation in both modes: the first conflict causes three materialized private preparations or one rooted workspace. The candidate rejects two conflicts with zero private preparations, preserves exact duplicate result/version and constructor/cache counts, physically inspects an older ID after a newer commit, and executes later valid writes once. This qualifies avoided work, not throughput or latency. Three earlier observer-setup attempts and every failed fixture remain retained separately.

At compiler inputs `1e9199b4ae9e00076242ba003748c362047c9220e8a6b61a348eaec362a14085`, the full library passes **1,259 / 0 failed / 2 existing ignores**. Full activation lifecycle, full effects (**10 / 0 / 1 existing ignore**), eleven selected execution controls, ordinary KV and all four recovery closure/image combinations pass. The native selection is **26 passed / 6 failed / 1 existing ignore**, separate from the two new regression passes. Combined application qualification remains **FAILED**. All nine strict/repository checks pass; all 24 reviewed anchors are unchanged.

The six failed gates are ordinary SQL, RPC continuation, ordinary Queue, C2 Queue and both independent C4 Queue cases. Materialized SQL and RPC final completion reach ambiguous deadlines; ordinary Queue fails rooted. C2 ends with deadline elapsed. Materialized C4 reaches its first target attempt with only 718 ms left; rooted C4 fails pre-load emptying with 2,408 ms left, leaving its load oracle unrun. The retained parent/candidate/candidate/parent RPC comparison passes all four independent runs and does not explain or erase the original broad failure. On the published parent, hosted native jobs separately fail retained-voter provisioning and an archive-seed cleanup ownership/byte assertion. Local recovery passing does not repair those hosted failures.

The read-only original RustFS observation finds 757,760 available bytes against the unchanged 6-GiB reserve, with stable identity/resources and zero restarts. No constrained comparison is admitted. No throughput/latency gain, complete OPT-01–08 package or remaining CG/SDK/release gate is accepted. See qualification evidence and [current priorities](remaining-work.md#cached-committed-rpc-conflicts-and-current-priorities).


## Provisioning refusal diagnosis — 2026-11-19

Implementation `95e95a35e`, compiler inputs `e92708cebc8ecc202211301c1ea1bf0cb98f9ffe33f6fd4d21acc5525de2bfa3`, retains every provisioning predicate and its evaluation order, the shared preparation banks and the typed `Invalid` error. Static messages now identify the failed guard without including configuration values. Five existing native controls pass: coalesced admission, retained-owner retry, cancelled-close joining, selected-root recovery and refusal isolation. All-feature library Clippy, native-only facade compilation and all seven repository checks pass. These scoped checks qualify diagnostics only; they supply no production recovery or performance fix.

The hosted `607ca35b` disaster sequence again fails retained-voter provisioning in materialized PublishedChildren (12 passed / 1 failed / 8 ignored). Its exact guard remains unknown until the diagnostic source runs. Two temporary SQL phase probes pass; archive cleanup probes produce one failure during replacement reconciliation and one pass. None explains the original SQL deadline or hosted cleanup assertion. Original assertions, budgets and failed data remain; all temporary instrumentation is restored byte-exactly.

The read-only original RustFS check at 2026-11-19T01:33:45.303616+00:00 finds zero available bytes, below the unchanged 6-GiB reserve, with stable identity/resources and zero restarts. No constrained comparison or throughput/latency gain is qualified. Full OPT-01–08, application, SDK, release and performance gates remain open. See the scoped diagnostic record and [current priorities](remaining-work.md#cached-committed-rpc-conflicts-and-current-priorities).


## Queue schema reopen work — 2026-11-19

Compiler inputs `2a7231be5eee34670d16c063fe2ce68ffe030f3a4a1d2cdb3e9e5903b22413d4` retain the original Queue schema and trigger installation while guarding its stats bootstrap aggregate with an existing-row-derived limit. An existing stats row skips the scan; a missing row retains the original single-pass aggregate. No activation/image custody, worker, bank, deadline, identity, API, wire marker or durability proof changes.

The same regression source/profile fails on original SQL and passes with only the schema changed. The missing-stats/trigger control passes both. A direct kernel probe uses the exact linked bundled SQLite 3.53.2: 4,096 messages require 69,759 original versus 122 candidate VM steps; 10,000 require 170,127 versus 122. This qualifies avoided initializer work only. The first driver setup selected unrelated schema test modules and ran neither oracle; that invalid setup and its executable remain retained. The corrected red run uses the same executable.

The candidate passes the full library (1,261 passed / 2 existing ignores), full native broker and SQL/KV/RPC continuation controls in both image modes. All six selected shared-Raft Queue cases fail: ordinary callbacks, batch proof sharing, C2, materialized/rooted C4 and combined C4. Their original concurrency, deadlines, producer/consumer oracles and failed data remain. Combined application qualification is FAILED. All nine strict/repository checks pass, including all-feature/all-target Clippy and native-only facade compilation.

The preceding diagnostic source `e92708ce` at `6783ed04` passes the hosted original serial disaster sequence (13 passed / 8 explicit RustFS ignores). Its broader SDK native 3.14 job fails two disaster cases (11 passed / 2 failed / 8 ignored); both failure sections report duplicate peer address. The rejected condition is now identified, while its origin and repair remain unproven. This is not hosted qualification of the Queue candidate. The read-only original RustFS observation at 2026-11-19T03:03:20.790083+00:00 remains zero available bytes below the unchanged 6-GiB reserve, with stable identity/resources and zero restarts. No constrained throughput/latency comparison is admitted. Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open. See qualification and [current priorities](remaining-work.md#queue-schema-reopen-work-and-current-priorities).

## Retained peer port ownership in managed fixtures — 2026-11-19

Compiler inputs `4164eb74b62e36240adddfa0b688c387ee10c83cd8c9a2a4dfc57094cb4c211d` correct a fixture model of distinct physical peers on one loopback interface. A stopped peer can remain authenticated in the directory after releasing its socket. A real two-node retained boot offered that port to another node and failed the production duplicate-address guard. The identical regression passes in both image modes after the shared fixture rejects foreign historical port ownership before publication. Metadata has no socket custody, and same-node reuse remains allowed. Selection is bounded to 64 candidates; no boot/command retry, resource, deadline, production predicate or durability proof changes.

Five original provisioning controls pass, including the exact typed duplicate-address refusal before filesystem preparation. The full managed target has 37 passed / 1 failed / 15 explicit RustFS ignores; the full disaster target has 11 passed / 2 failed / 8 explicit RustFS ignores. Complete-closure recovery passes all four parent/child and materialized/extents combinations. Three original owning gates fail: extents S3 seed failover settlement, extents fresh outbox-lane settlement, and materialized pending recovery fresh-quorum formation. Available failed data is retained and verified in original and closed-copy manifests. No duplicate-address refusal occurs in the current disaster log. This is not a controlled platform-matched regression comparison.

The library executable/profile are byte-identical to the prior full 1,261-pass / 2-ignore run, which is explicitly reused. All nine strict/repository checks pass. Published predecessor `f82283053` independently fails hosted complete-closure recovery with the duplicate-address refusal (12 passed / 1 failed / 8 ignored). Corrected-source hosted acceptance and all broader failure gates remain open.

Queue's six original-deadline failures on production checkpoint `f82283053` remain unclosed. Next work is request-phase image/persistence/ingress diagnosis and recovery progress, followed by measured batching/fairness, remaining routing/read partitions, physical reclamation/once-only credit, reserve-aware movement and full combined release. The original read-only RustFS observation at 2026-11-19T04:28:57.081176+00:00 still finds zero available bytes below its unchanged 6-GiB reserve, with stable identity/resources and zero restarts. No original constrained comparison or throughput/latency gain is qualified. Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open. See qualification and [current priorities](remaining-work.md#retained-peer-endpoints-and-current-priorities).

## Signed Queue operator reads — 2026-11-19

Compiler inputs `7595d14b9c18bb6efd222f0dd19e192a0424958870d966fd40be58079fe081a4` include `origin/main` `2f083f14`. When local history and initialized Queue schema are available, signed `info` and `peek` use the existing fresh linearizable pinned reader after the original operator authorization. The target namespace's S3 minimum still requires current archive proof of the selected image. Stale consistency refuses. The broker and query share metrics/peek SQL, response shaping, configured retention, limits and console byte budget. Missing local history or Queue schema/statistics uses the existing authorized initialization command with the original admission, arguments and deadline; other read errors do not authorize a mutation retry. Pause, resume, purge and redrive keep ordinary mutation execution.

The same real RF3 regression fails before the change: two signed reads advance an idle Queue's committed index **4 → 6**, under stable leadership. The byte-identical oracle passes after the change with **4 → 4 in both image modes**. Pause/resume remain mutations and subsequent info observes pause without another append. Both modes also pass signed authentication/body-tamper/name-scope/limit refusals, exact stale-consistency refusal, S3 info/peek refusal behind a committed pause with denied archive writes, recovery after archive coverage returns, and refusal after losing two Raft hosts. Quorum read service remains available during the archive fault. These are avoided-work and correctness results, not throughput or latency percentiles.

**Combined application qualification remains FAILED.** The focused shared application selection has **6 passed / 5 failed**. The full library has **1,261 passed / 2 existing ignores**; the full native Queue target passes. The original batch claim/lease proof-sharing case passes. Controls: SQL PASSED, KV PASSED, RPC continuation PASSED. Current failed cases are `ordinary_queue_callbacks_use_shared_quorum`, `queue_load_preserves_acknowledged_messages`, `materialized_queue_four_in_flight_preserves_acknowledged_messages`, `rooted_queue_four_in_flight_preserves_acknowledged_messages`, `queue_four_in_flight_preserves_acknowledged_messages`. Queue producer failures reach their first target attempt with only 844–2,462 ms remaining; the ordinary Queue failure is an ambiguous source-command deadline in rooted mode. These observations locate progress gaps without establishing a causal bottleneck or speedup. Failed identities and available fixtures remain retained and verified against closed copies.

All nine strict/repository checks pass at this compiler-input digest. The prior `7562115bb` full managed/disaster selection retains three failed recovery gates: extents S3 seed failover settlement, extents fresh outbox-lane settlement, and materialized pending recovery forming a fresh quorum. Those full targets were not rerun here. Current-source hosted SDK/release acceptance remains required. See the source-bound qualification for exact profiles, executable/log hashes, excluded build attempts, source binding and scope.

Remaining priorities, in order:

1. **P0 — Queue progress:** close the original producer, source-RPC and delivery/drain deadlines at their existing workers, admission banks and timeouts. Measure ingress allocation/queueing and actual image/persistence phases before selecting another optimization. Preserve ambiguous command IDs and required durability proofs.
2. **P0 — Recovery and hosted acceptance:** close the three retained recovery failures and obtain current-source disaster/full SDK acceptance. The preceding hosted Python 3.14 lifecycle shutdown failure also remains a release gap.
3. **P1 — Sustainable shared execution and capacity:** finish routing/read refusal and partition coverage, measured batching/fairness, physical shared-file release with once-only credit, repeated reserve-aware movement, and interrupted park/wake/transition/recovery (OPT-01–07).
4. **P1 — Combined release:** complete primitive/SDK/Axum/daemon/feature/backend-fault/crash and historical CG evidence (OPT-08).
5. **P2 — Original constrained comparison:** rerun SQL/KV/Queue Quorum/Quorum+S3/Fleet/Bucket C1/4/16 on the original three 1-vCPU/2-GiB Colima nodes, RF3/three groups/shared RustFS, 256-byte values, Queue batch eight and reversed repetitions. The read-only observation at 2026-11-19T06:25:10.843969+00:00 finds **15,716,352 bytes** available against the unchanged **6-GiB reserve**; original identity/resources remain stable with zero restarts. No run is admitted and no end-to-end gain is qualified.

Full OPT-01–08 and remaining CG/SDK/release/performance gates remain open.

## Checkpoint reply progress — 2026-11-19

Compiler inputs 98eba6ce include main 2f083f14. After the existing checkpoint
proof, the SQL owner drives one bounded owned-work pass to relay its reply before
polling the dependent handler. Fresh shutdown, original deadline, closed reply,
activation, term and full executor authority checks surround that continuation.
Contended query and safe-root FIFO writer handoffs remain; reply wakes survive
park. Workers, banks, identities, deadlines, fsync and durability requirements
remain unchanged.

The byte-identical public RF3 test runs three commands in each materialized/
extents × Quorum/S3 configuration. Baseline commands need **3 handler polls**;
candidate commands need **2**. Exact receipts, fresh linearizable values/version/
witness, replay without re-invocation and joined shutdown pass on both sides.
This establishes one avoided poll per checkpoint, not a throughput or latency
gain.

The broader candidate has identical production inputs; final source adds only
the independently compiled regression and root declarations. The full library
passes **1,261 cases / 2 existing ignores** using the exact same executable under
a fresh short temporary root. Initial 13 mock Docker socket failures exceeded
SUN_LEN under a longer root; both attempts remain recorded. Native Queue passes.
Shared application selection remains **6 passed / 5 failed**: signed-read
controls, proof sharing, SQL, KV and RPC continuation pass; ordinary Queue, C2,
both independent C4 images and combined C4 fail their original progress oracles.
Available failed fixtures match verified closed copies. All nine strict/
repository checks pass.

Current priorities:

1. **P0 — Queue progress:** close original producer/source-RPC/consumer deadlines
   and acknowledged-message drain in both image modes. Diagnostic enqueue SQL
   body cost is small relative to writer admission, checkpoint waits, image work
   and durable settlement. Preserve ambiguous command IDs and exact receipts.
2. **P0 — Recovery and lifecycle:** close retained local recovery failures.
   At preceding head 04f728d4, hosted SDK native 3.14 fails both group replacement/
   joined-restart cases with Raft runtime NotFound. Native 3.14t fails the rooted
   replica-routing refusal assertion; separate disaster diagnostics
   fails transfer recovery-slot release. Full 3.14t RPC shutdown also fails.
   These preceding-source observations do not qualify the new source.
3. **P1 — Complete OPT-01–07:** finish routing/read refusal and partition coverage,
   measured persistence/ReadIndex/archive batching and fairness, physical shared
   file release with once-only capacity credit, repeated reserve-aware movement,
   and optimized wake/interrupted-transition/recovery qualification.
4. **P1 — Combined release and performance:** finish current-source primitive,
   SDK/Axum/daemon/feature/backend-fault/crash and historical CG evidence (OPT-08),
   then the original constrained SQL/KV/Queue comparison.

The latest retained original RustFS observation has zero available bytes
against the unchanged 6 GiB reserve. No original Colima performance case is
admitted. Keep three original 1-vCPU/2-GiB nodes, RF3/three groups/shared RustFS,
Quorum/Quorum+S3/Fleet/Bucket, C1/4/16, 256-byte values, Queue batch eight and
reversed repetitions. Complete OPT-01–08 and remaining CG/SDK/release/performance
gates remain open. See source-bound qualification
for exact inputs, separate executable/profile/log hashes, setup exclusions and
retention/reuse limits.

## Import completion — 2026-11-19

Compiler inputs `66f1af10e0ddacf27440ecd3f7e6bb0438ea6649aefd659f97fc34f1debff7bd` include main `2f083f14`.
The engine now drops its consumed transfer seed before waking the import caller,
after durable import and any fatal closed flag. Other prepared-root owners keep
their admission and pins. The causal real-engine regression observes **1 recovery
slot / 84,867,072 reserved bytes at completion before the fix**, then **0 / 0**
with byte-identical test inputs. Held-sync invisibility and no-premature-wake,
exact durable inactive mapping, joined close and retained reopen pass on both
sides. This verifies ownership completion; reserved bytes are not physical RSS
or reclaimed disk, and the empty unmanaged transfer is not RF3 serving proof.

The full library passes **1,262 / 2 existing ignores**. Its first attempt failed
the partial-park fixture's initial direct enter with typed `NotReady`. The fixture
now uses production's existing bounded retry for the same certificate and
original deadline; all persisted-park, fresh-election and late-replay refusal
assertions remain. Full managed qualification remains **37 passed / 1 failed /
15 explicit RustFS ignores**: S3 group replacement/joined restart returns
`Raft runtime: NotFound`. Full disaster qualification passes **13 / 8 explicit
RustFS ignores**. Native reuse requires rebuilt byte-identical executables and
profiles; both source digests and the test-only delta are recorded. All nine
strict/repository checks pass. Original failed fixtures and verified closed
copies remain retained.

The original Queue C2 and both C4 diagnostic runs still fail their progress
oracles. Across 425 phase events, the suspected pure-query handoff never occurs;
that hypothesis was not selected for a fix. SQL/KV/RPC controls from the preceding
application selection pass, but its five Queue failures remain open. No end-to-end
throughput or latency gain is qualified.

Remaining priorities:

1. **P0 — Queue progress:** close original producer/source-RPC/consumer deadlines
   and acknowledged-message drain in both image modes; retain ambiguous IDs,
   exact receipts, current workers, bounds and durability requirements.
2. **P0 — Recovery and lifecycle:** diagnose S3 replacement/joined-restart
   `NotFound`, retained routing/recovery and Python shutdown failures. Published
   parent SDK 3.14 independently has the same S3 failure; its library and disaster
   targets pass. The separate standalone disaster run was cancelled before a
   test footer, with cause unproven. Parent RPC 3.14 passes; other release modes
   still require current-source acceptance.
3. **P1 — OPT-01–07:** finish routing/read refusal and partition coverage,
   measured persistence/ReadIndex/archive batching and fairness, physical shared
   file reclamation with once-only credit, repeated reserve-aware movement and
   interrupted park/wake/transition/recovery qualification.
4. **P1 — OPT-08 and release:** complete primitive/SDK/Axum/daemon/feature/
   backend-fault/crash and historical CG evidence.
5. **P2 — Original performance matrix:** retain the three original 1-vCPU/2-GiB
   Colima nodes, RF3/three groups/shared RustFS; SQL/KV/Queue across Quorum,
   Quorum+S3, Fleet and Bucket, C1/4/16, 256-byte values, Queue batch eight and
   reversed repetitions. At 2026-11-19T13:14:03Z the original RustFS has zero
   available bytes against the unchanged 6 GiB reserve, stable identity/resources
   and zero restarts. No performance case is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open.
See source-bound qualification
for exact source/executable/profile/log bindings, failed attempts, reuse and
native/environment limits.

## Retired-voter drain verification — 2026-11-19

Compiler inputs `029d9a82fe1a7da0c985758ccae9593166c885bb466c64834d316e1c69f15a58` include main `2f083f14`.
All production inputs are byte-identical to published `1ba5fb538`. This follow-up
corrects a managed lifecycle test race and retains its node disks/error phases.

The exact S3 replacement case first passes in isolation, then the first serial
repetition fails after replacement reaches Complete and verifies source data.
Its drain scan requests status from candidate **0**, also the verified obsolete
voter **0**; that runtime detaches between lookup and status, returning
`Raft runtime: NotFound`. The fixture now excludes only this removed voter from
initial drain leader selection and maps the result to the original host index.
Unexpected `NotFound` from any eligible voter still fails. This is a test
selection correction, with no production retry or recovery-policy change.

The same lifecycle oracle passes both materialized and extents modes: exact S3
receipts, replay/fresh values, final membership, retained debt, lost terminal CAS
reconciliation, actual drain handoff/no unresolved producers, invocation counts
six/eight, joined retained restart, cold quorum-read and oldboot grant refusals.
The failed trial's **128 files / 1,766,043 bytes** and candidate's **364 files /
5,203,294 bytes** remain at their original paths and match verified closed copies.
The initial context-only control had no available terminal disks under its prior
TempDir cleanup; it is not represented as retained recovery media.

Full managed qualification now passes **38 / 0 failed / 15 explicit RustFS
ignores**. Library **1,262 / 2 existing ignores** and disaster **13 / 8 explicit
RustFS ignores** also pass; reuse requires freshly rebuilt byte-identical
executables and complete profiles. All nine strict/repository checks pass.
These host-native synthetic-store results do not qualify the original constrained
Colima/RustFS comparison or establish a throughput/latency gain.

Remaining priorities:

1. **P0 — Queue progress:** close original producer/source-RPC/consumer deadlines
   and acknowledged-message drain in both image modes. Preserve ambiguous IDs,
   exact receipts, workers, bounds and required proofs. The preceding application
   selection remains six passed/five failed; SQL/KV/RPC controls pass, while
   ordinary Queue, C2 and C4 progress/drain cases remain open.
2. **P0 — Current-source release acceptance:** obtain hosted routing/read-refusal,
   Python lifecycle and SDK acceptance. The earlier hosted S3 `NotFound` has no
   exact phase context, so the local diagnosis does not independently prove its
   cause. Preserve earlier failures and their source boundaries.
3. **P1 — OPT-01–07:** finish routing/read partition coverage, measured persistence/
   ReadIndex/archive batching and fairness, physical shared-file reclamation with
   once-only capacity credit, reserve-aware movement and interrupted lifecycle/
   transition/recovery qualification.
4. **P1 — OPT-08:** complete primitive/SDK/Axum/daemon/feature/backend-fault/crash
   and historical CG evidence.
5. **P2 — Original constrained performance:** keep the original three 1-vCPU/2-GiB
   Colima nodes, RF3/three groups/shared RustFS; SQL/KV/Queue across Quorum,
   Quorum+S3/Fleet/Bucket C1/4/16, 256-byte values, Queue batch eight and reversed
   repetitions. The 2026-11-19T14:27:07Z read-only original RustFS observation
   still finds zero available bytes against the unchanged 6 GiB reserve, stable
   identity/resources and zero restarts. No performance case is admitted.

Complete OPT-01–08 and remaining CG/SDK/release/performance gates remain open.
See source-bound qualification
for exact source/executable/profile/log hashes, intermittent controls, retained
media, reuse and environment limits.

## Authenticate remote read replies once

The shared-cell query transport authenticates the signed version with the
invocation's immutable key configuration, history and schema. It carries the
observation and signed validity interval together in the decoded reply. The
caller samples its wall clock again and enforces that interval before checking
the requested version floor. Existing route, quorum, and authorization checks
remain part of read admission. Command and receipt wire tests cover their own
identity and durability checks through the shared token representation.

In eight alternating paired samples of 6,000 iterations on the native host,
the median mean cost of reply token validation moved from 101.8 to 50.5 μs.
The harness compiles both versions of the token validation source with the same
dependency artifacts and verifies matching outcomes at the signed time bounds.
This measures token validation in isolation. The source and samples are retained
under `cellar-0d50-quorum/read-freshness-bench` on the workspace target volume.

The 42 query-wire tests passed, along with the full Cellar library suite
(1,429 passed and two ignored), strict Clippy, module layout, formatting,
documentation, and native dependency checks.

The sequential baseline and candidate smoke used three RF3 groups on three
voters, each capped at 1 vCPU and 2 GiB, and a fresh local RustFS per case.
Both builds used the same compiler and harness configuration. Each SQL and KV
read and write window verified at four concurrent clients over two 15-second
repetitions.

| Verified operation | Baseline p99 ms | Candidate p99 ms | Baseline requests/s | Candidate requests/s |
| --- | ---: | ---: | ---: | ---: |
| SQL read | 7.31–9.76 | 6.64–7.04 | 1,135–1,244 | 1,316–1,320 |
| KV read | 11.49–23.08 | 6.57–17.82 | 710–1,067 | 943–1,425 |
| SQL write | 1,130–1,648 | 247–386 | 19.5–21.9 | 26.2–33.8 |
| KV write | 325–1,539 | 389–869 | 17.6–28.9 | 26.3–26.7 |

Write timings also shifted substantially. Attribution of an end-to-end gain
requires more controlled repetitions. Both runs failed overall on Queue. The
baseline failed after 14 acknowledgements and four unconfirmed mutations in
the second enqueue window. The candidate acknowledged 263 batches in the first
window, then hit `Runtime(Closed)` before delivery and settlement could verify.
The pre-shutdown admin probes reported `NotFound` on two voters in each run.
A baseline Queue replay with lease tracing reproduced the closure. Its failing
voter renewed its RustFS lease with roughly five seconds of authority remaining
about 200 ms before exit. The first controller closure remains unresolved.
Ledgers are retained under `cellar-0d50-quorum/read-rf3-baseline`,
`read-rf3-candidate`, and `read-rf3-queue-baseline-trace`.

## Local quorum smoke at main 86cfe5d1

The constrained lab ran three Linux voters and three RF3 groups, with leaders
balanced across voters. The optimized `lab` binaries were cross-built with
Rust 1.99.1. Each voter, the client and the pinned RustFS each had a 1-vCPU,
2-GiB cgroup and zero swap. SQL and KV used 24 extent-backed cells each.
Queue used 24 queues and 24 dead-letter cells. The smoke ran one repetition
at concurrency one and four with eight-second timed windows. Other host work
overlapped the run. All three fresh stores had more than 37 GiB free on `/data`.

| Verified operation | Clients | Requests/s | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| SQL write | 1 | 1.23 | 683 | 2,119 | 2,119 |
| SQL write | 4 | 2.46 | 1,331 | 3,495 | 4,650 |
| SQL read | 1 | 6.00 | 81 | 880 | 1,411 |
| SQL read | 4 | 92.74 | 15 | 180 | 463 |
| KV write | 1 | 0.89 | 840 | 1,964 | 1,964 |
| KV read | 1 | 6.47 | 78 | 567 | 927 |
| Queue enqueue, eight messages per request | 1 | 2.11 | 237 | 4,402 | 4,402 |
| Queue enqueue, eight messages per request | 4 | 5.73 | 634 | 1,330 | 1,377 |

SQL and Queue passed. Queue delivered all 520 acknowledged messages across the
two timed windows, with zero duplicates and zero settled backlog or dead letters.
KV failed its four-client write window with two ambiguous deadlines after 10
acknowledged writes. Its four-client read window did not run. All nine voters
exited zero and the cgroup samples recorded zero OOM events. The overall result
remains failed. The ledgers retain the original attempts and verified windows
under `cellar-0d50-quorum/primitive-early` on the workspace target volume.

An independent smoke after the build finished passed all 16 windows using the
same compiler inputs, executable hashes and client. SQL read p99 was 6.6/15.5 ms
at concurrency one/four and write p99 was 122/492 ms. KV read p99 was 30.1/19.1 ms
and write p99 was 323/2,483 ms. Queue enqueue p99 was 504/1,323 ms and all 1,104
messages in the timed windows were delivered exactly once in the observed run.
The repeat is retained under `cellar-0d50-quorum/primitive-smoke`. Compilation
overlapped the first capture. The runs show sensitivity to the environment
and leave an isolated estimate of that effect open.

The 30-second concurrency-16 Quorum capture passed SQL writes and reads at
17.0 and 833.2 requests/s, respectively, with p99 of 2,022 and 58.8 ms. KV
verified 543 writes at 17.8 requests/s and p99 of 1,959 ms. Its read window
failed two of 23,315 requests with `Version(Expired)` and
`Transport(Authentication)`. Colima adjusted the guest clock during the
load process lifetime. The ledger leaves correlation to the individual refusals
open. Queue warmup recorded 69 acknowledged batches and 11 ambiguous failures
before the timed phase. The consumer saw every acknowledged message plus
messages outside that acknowledgement set and eight duplicate deliveries.
The failed window retains all attempt identities for reconciliation. Evidence
is retained under `primitive-c16`.

The managed daemon fault suite passed on both materialized and extent images
with three Linux voters at the same limits. Two cells shared one physical RF3
group in each case. The ledger verifies native/Axum replies, parking and wake,
an actual leader SIGKILL, successor election before restart and retained receipt
reconciliation. A scoped archive-write fault allowed Quorum operations and
refused explicit S3 acknowledgements until publication recovered. Maintenance
kept the returning voter paused until resume. Losing two voters blocked new
writes and protected reads. Evidence is retained under `managed-daemon-retry`.
The initial daemon package used the benchmark entrypoint and owner-only file
permissions. That attempt stopped before boot and is retained separately.

The managed SQL/KV suite passed all four combinations of materialized/extents
and Quorum/S3 minima. Both image modes exercised two physical RF3 groups with
two cells per group on every voter. The independent verifier checked SQL
rollback, exact readback, archive faults, leader SIGKILL, retained generations,
and minority refusals through the native and Axum adapters on the daemon
listener. The process, limit and operation records are under
`cellar-0d50-quorum/managed-primitives`.

The SQL counters across the full observed interval averaged 80 ms per Raft
engine file sync, 168 ms per image preparation and 171 ms per manifest
preparation in the first smoke. The repeat averaged 7.6, 18.4 and 17.0 ms,
respectively. These populations differ and their intervals may overlap. Split
manifest file sync, directory sync and selector publication on the same request
timeline before choosing another persistence change. The separate native and
Axum EventStream replays, including one using RustFS, reproduced ambiguous
`Consume` deadlines. One Axum capture spent 1.60 seconds between capture and
apply of the source checkpoint before ACK. In the RustFS replay the target ACK
returned before the parent expired. These outcomes retain both source and target
timing as priorities. The local smoke leaves the low-latency and sustained-load
gates open.

The standalone post-build replay failed all three EventStream `Consume`
selections at the original deadline and passed the RustFS Outbox source-proof
control. The earlier Outbox startup timeout remains in the first run. Separate
logs are retained under `native-events` and `native-events-after-build`.

The lab also reproduced a RustFS startup race. Liveness was HTTP 200 while
the first signed bucket request received HTTP 503. The local preparation and
primitive comparison now share the storage/IAM readiness check. A fresh backend
passed all seven signed operations on its first probe after the fix.

## Parse current Raft persistence and apply measurements

The analysis tool now accepts the current native inventory of 64 operation
histograms and 88 counters, including committed image and manifest preparation,
checkpoint phases, and apply prefetch. It retains the previously supported
inventories and the exact inventory before #183. A delta still requires the
same owner, complete vocabulary and bounds at both samples.

The corrected analyzer accepted the retained start/end snapshots of all three
voters in each SQL and KV baseline and candidate case. Mean Raft file sync
spanned 5.2–8.7 ms across those cases, compared with 12.9–22.0 ms for image
preparation and 10.9–17.3 ms for manifest preparation. The samples cover whole
cases and include overlapping operations. Correlate individual requests with
the persistence and apply phases before attributing the write tail. Derived
phase totals are retained in `read-rf3-validated-performance-phases.json`
alongside the original run ledgers.

## Reproduce KV version refusals under a clock step

The [primitive comparison runner](../../../../tools/perf/primitive_compare.md)
can record signed version time refusals at authentication and at the caller's
final check. The load ledger records the failed request's ingress voter,
wall-clock completion and elapsed time within the window.

The four-client control verified its warmup, 66 timed writes and 3,252 timed
reads on three RF3 groups backed by local RustFS. Each voter had 1 vCPU and
2 GiB. The same image and workload verified 104 writes before the fault run
introduced 24 backward clock steps of roughly 104 ms during the read window.
The fault run restored the original guest clock progression after the pulses.

The four failed reads matched their ingress voters' logs. The signed replies
appeared 81–102 ms future-issued. Three requests returned
`Transport(Authentication)` and one returned `Version(Expired)` after reply
authentication. The verifier recorded 1,375 correct read results and refused
qualification of the window. The controlled fault reproduces both historical
C16 symptoms. The earlier run lacks exact timestamps for its failed requests.

Two new C16 attempts stopped in warmup with 12 and 13 unconfirmed writes,
respectively, before the read phase. They retain the existing ambiguity rules
and provide no C16 read measurements. The ledgers and the clock-step samples
are retained under `cellar-0d50-quorum/kv-version-clock-*`.
