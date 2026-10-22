# Cellar multi-Raft delivery package

Status: MR-01 through MR-12 are complete at their recorded native boundaries.
MR-13 through MR-17 remain open. The complete delivery goal is not achieved.

Leaders execute private commands; serving replicas apply committed SQLite/LTX
history and expose explicit follower queries. S3 stores verified committed images,
tails and dependencies. A node can lead some groups and follow others. S3 restores
cell state, not old votes, process identity, leadership or quorum. An opt-in owned
controller repairs redundancy and balances eligible caught-up voters. Committed
alarms and receipt-checked outbox/inbox delivery share an owned leader scheduler.

| Boundary | Qualification |
| --- | --- |
| [Local Colima/RustFS native run](evidence/MR-13-colima-native-progress.json) | Three actual 1-vCPU/3-GiB/no-swap Cellar containers in a separate profile, six Raft groups, 10 acknowledged writes, 18 minimum-version replica checks and six verified archives. Initial Docker metadata failure and setup continuation retained. Functional smoke only; five-node, failure/recovery and performance gates remain open. |
| [MR-13 integration with current main](evidence/MR-13-main-integration-progress.json) | Source `f214ddf`: 664 standard cases across ten binaries, the custom daemon process runner, 15 doctests, Python watchdog and workspace Clippy. Integrates upstream watchdog recovery and runtime/transport performance changes; earlier counts retain their separate source pins. |
| [MR-13 bounded receipt maintenance](evidence/MR-13-receipt-maintenance-progress.json) | Source `817a4ed`: 551 unit/integration cases across five binaries, 15 doctests, Clippy and the integrated daemon process runner. Expired prefixes, inclusive deadlines, latest receipts, atomic command publication and retained-disk floors; dedicated signed-ack/recurrence/archive-floor checks remain open. |
| [MR-13 production CLI startup](evidence/MR-13-cli-startup-progress.json) | Source `0fa09f1`: 984 standard cases across 72 binaries with passing cases, 15 doctests, the custom daemon process runner, focused Python watchdog and workspace Clippy. Tests include three actual daemon processes and retained two-group restart. Serial passes do not close the recorded concurrent CI findings. |
| [Local Colima/RustFS backend](evidence/MR-13-local-backend-progress.json) | Separate profile; digest-pinned RustFS with enforced limits and seven passing S3 protocol operations. No Cellar containers launched by this component. The repository helper in `tools/multi-raft/` targets up to five 1-vCPU/3-GiB nodes and refuses a VM that cannot fit the selected count. |
| [MR-13 image, connection and aggregate resources](evidence/MR-13-resource-views-progress.json) | Source `37aed98`: 610 passing cases across eight binaries with serial fixtures, 15 doctests and workspace Clippy. Physical image/connection owners, bounded complete role/readiness/lag scans and controller views; 110 fixed samples plus 13 with a controller. A parallel outbox callback-count failure remains open despite passing isolated replays and the serial matrix. |
| [MR-13 SQL and transport resources](evidence/MR-13-resources-progress.json) | Source `f89c7b4`: 850 unique unit/integration cases across 26 binaries, four SDK doctests and two recovery processes. Physical SQL and actual directional transport budgets; 68 fixed samples with grouped metric families. Prior intermittent findings remain open. |
| [MR-13 authenticated reservation metrics](evidence/MR-13-metrics-progress.json) | Source `4e25ffa`: 551 cases across six affected binaries, four SDK doctests and two recovery processes. Exactly 14 gauge identities for one versus 32 committed groups; separate scope from the parent result. |
| [MR-13 local readiness and reader resources](evidence/MR-13-readiness-pins-progress.json) | Source `e55e6cb`: 847 unique unit/integration cases across 26 binaries, four SDK doctests and two recovery processes. Local query readiness and physical reader limits/usage; transfer and concurrent fallback findings remain open. |
| [MR-13 retained-state mode guard](evidence/MR-13-mode-guard-progress.json) | Test-only source `33f3c86`: all 15 native operations cases pass; 790 other frozen parent files match `ec312e8`. No combined count across revisions. |
| [MR-13 durable stop receipts](evidence/MR-13-management-progress.json) | Source `ec312e8`: 839 unit/integration cases across 26 binaries, four capability doctests and two recovery process runs. Partial milestone; earlier intermittent transfer refusal remains under investigation. |
| [MR-13 node shutdown component](evidence/MR-13-shutdown-progress.json) | Library source `6784512`: 810 unit/integration cases, 24 binaries and four capability doctests; example-only successor `275c45d`: Clippy, build and two recovery process runs. Partial milestone. |
| [MR-11 automatic repair](evidence/MR-11.json) | Source `cc93ccf`: 248 Rust cases, 12 binaries, 26 checks; six-node repair and eight-group balancing |
| [MR-12 complete business features](evidence/MR-12.json) | Source `a28babe`: 782 unit/integration cases, 21 binaries, four capability doctests, two recovery process runs and 34 checks; all six original feature requirements |
| [Earlier Blob component](evidence/MR-12-blob-progress.json) | Source `8cfc4bd`: 354 cases, 16 binaries, 31 checks |
| [Earlier alarm component](evidence/MR-12-alarm-progress.json) | Source `5a6b606`: 322 cases, 19 binaries, 31 checks |

The [PR91 integration repair](evidence/PR-91-ci-repair.json) merges upstream
Python support and qualifies all 29 archive cases at `3682454`. A slow-capture
probe reproduces an admission refusal mislabeled by the old durability test;
the corrected fixture retains its original deadline and proof/value assertions.
This focused result does not replace or enlarge earlier qualification counts.
The full local workspace tests and doctests subsequently passed at `7c8d908`.
All-target Clippy exposed a unit-test guard-scope warning; the assertion now has
a lexical block before joined cleanup. Its focused test and full-workspace
Clippy pass at the `f8f8958` source change, with no lint allowance added.

The subsequent [Linux fixture repair](evidence/PR-91-fixture-repair.json) records
55 unique native cases at `a128c26`, including the full host and membership
binaries and 12 lease replays. Controlled archive, late-CAS and initial-image
probes retain their failing and corrected outputs. Original deadlines, unpolled
command behavior and proof/lag/leadership assertions remain enforced.

Each record retains its full source pin, executable/output hashes, failed initial
diagnostics and original scope. Counts from different sources are not added.
The [business-feature contract](effects-design.md) describes Blob, alarm, signed
delivery, receiver receipts and private-system boundaries. Stable definition
identities permit self/cyclic bindings; immutable namespace admission still checks
the complete required manifest. All 88 public Node cases and owning Raft/Fleet
regressions pass at the MR-12 source, which predates automatic receipt pruning.
The later MR-13 component adds bounded opportunistic maintenance; permanently
refused deliveries remain retained.

Public Node shutdown now seals Raft admission, tracks accepted application work,
attempts bounded authenticated leadership handoff and joins the registered host.
Its bounded report includes actual engine identity, submitted ambiguous commands,
known archive lag and explicitly unknown parked histories. The
[operations contract](operations-design.md) describes those observations and
retained-disk obligations. Authenticated operator and caller/signal stops share
the driver. Durable management receipts now include actual Raft closure and the
original deadline through physical joining; missing startup/closure proof and
submitted ambiguity cannot yield a complete receipt. Production Raft CLI startup
shares embedded validation, requires persistent storage and binds the active
compiled application before ingress. It pins the initial deployment until process
restart. Remaining management surfaces, receipt qualification, fenced disaster restore
and complete operating qualification remain MR-13 work. Local role/query readiness separates coherent historical images from
source freshness; strong reads still obtain a fresh quorum barrier. Physical SDK
query-reader permits survive waiter cancellation, and fixed-size usage reflects
actual reservations. An authenticated, process-bound internal endpoint exports
those same counters with fixed series identities and refuses unavailable samples.
SQL observations follow real work slots and physical worker lifetime. Transport
observations distinguish directional and auxiliary banks through a weak observer;
retaining an SDK service cannot retain the joined transport owner.
MR-14 is in progress with fair shared persistence batches, a native reference
workload, fixed open-loop arrivals and independent ledger/replica/archive checks.
Its [performance evidence](evidence/MR-14-performance-progress.json) retains both
passed functional checks and missed local traffic objectives. Matched Fleet/bucket
baselines, remaining instrumentation and Q1 density qualification remain open.
The later [five-node evidence](evidence/MR-14-five-node-progress.json) verifies
actual three-voter placement for 150 cells, with 90 replicas and 30 leaders per
node. Quorum completes 1,200/1,200 measured arrivals at 65 ms p99; S3 completes
1,158/1,200 at 233 ms p99 and misses its traffic targets. Both reconcile all
submitted histories, 450 replica observations and final archive coverage.
The shared-host runs preserve separate durability contracts and leave Q1 open.
The [controlled slow-follower evidence](evidence/MR-14-slow-follower-progress.json)
adds bounded node pauses, separate role cohorts and exact healthy-majority command
witnesses. Same-image Quorum/S3 healthy controls meet short-run traffic targets;
the corresponding pause scenarios miss them while reconciling every history,
all 450 replicas and archive coverage. Original timing and network-setup failures
remain recorded. Sustained SLOs, equivalent-contract fault baselines and Q1 stay open.
The bounded archive producer now coalesces payloads from two histories while
keeping independent per-cell physical verification and head publication. The
[archive-bundle evidence](evidence/MR-14-archive-bundle-progress.json) records all
six required MR-14 integration cases, 493 passing cases on the final code,
a separately pinned 677-case broader matrix and three additional bundle replays.
These are native functional checks, not a measured batching speedup. The
[native repair-load evidence](evidence/MR-14-repair-budget-progress.json) checks snapshot reservation bounds while independent commands and follower
queries continue. MR-15 adds release qualification including the 24-hour
soak, MR-16 measured density/physical tiers and MR-17 federation.

Local/in-memory stores, declared zones and small native fixtures do not qualify
physical S3, power loss, failure zones, production performance/capacity, larger
physical tiers or federation. Hundreds/thousands of physical nodes and millions
of cells require measured qualification. Millions of physical nodes are not a
single-cluster promise. All original packages remain in scope.

The internal namespace identity is
`cellar-raft-ltx-1/members-1/seeds-1/s3-1/repairs-1/nodes-1/bundles-1`; numeric markers remain 1.
Class capability/system contracts also enter the immutable application digest.
Use fresh compatible experimental namespaces/prefixes for hard cutovers; there
is no compatibility reader or state migration.

## Deliverables

| File | Purpose |
| --- | --- |
| [Technical design](technical-design.md) | Authority, commit, storage, read, archive and recovery protocols |
| [Implementation plan](implementation-plan.md) | Ordered work packages, source ownership, deliverables and stop conditions |
| [Qualification plan](qualification.md) | Failure scenarios, correctness oracle, performance methodology and release gates |
| [Scaling plan](scaling.md) | Group density, hundreds/thousands of nodes, millions of cells and federation |
| [Application contract](application-contract.md) | Command/query registration, receipts, versions, effects and compatibility |
| [Task manifest](tasks.json) | Dependencies, test cases, command argument vectors and completion evidence |
| [Source baseline](baseline.json) | Source hashes for drift detection |
| [Capacity profiles](capacity-profiles.json) | Explicitly hypothetical scale scenarios |
| [Plan tool](plan.py) | Validate the package, inspect ready work and calculate placement arithmetic |

## Start implementation

Use a complete checkout at the baseline or a reviewed successor. Python 3.10+
and Git are sufficient for the package checks; runtime implementation uses the
repository toolchain and build instructions.

```sh
python3 plans/multi-raft/plan.py validate
python3 plans/multi-raft/plan.py preflight
python3 plans/multi-raft/plan.py ready
python3 plans/multi-raft/plan.py show MR-13
python3 -m unittest discover -s plans/multi-raft -p 'test_*.py'
```

`validate` checks document links, source paths, task dependencies, case ownership,
commands and evidence requirements. `preflight` compares the pinned source
hashes with the checkout and fails on drift. It does not rewrite the baseline.
Review every reported change, revise affected contracts and refresh the baseline
in the same reviewed change before starting dependent work.

`show` prints a task's implementation requirements and future verification
commands. It does not execute shell text, modify source, install dependencies or
mark work complete. The task commands become runnable as their targets are
implemented. The manifest records required test names, so an empty test target
or a filter that runs zero cases cannot count as completion.

Update task status manually after its evidence is recorded. DONE requires all
dependencies DONE, all named tests present and passing, and a linked evidence
record with source revision, commands, outcomes and limitations. BLOCKED describes
an implementation prerequisite, not an instruction to bypass safety.

## Decisions to implement

- One cell per group, three durable voters; optional learners for additional reads.
- `raft` 0.7.0 consensus core and `raft-engine` 0.4.2 as the selected log-engine
  integration baseline, subject to MR-02 compile, filesystem and crash gates.
- Shared node runtime, transport and log engine; no per-cell thread or log engine.
- Private leader execution state; only committed, crash-consistent SQLite
  generations can serve queries or establish successful command results.
- One outstanding data proposal per cell in the first release.
- Quorum durability by default in the new mode, with explicit S3-durable requests.
- Stale, minimum-version and safe ReadIndex linearizable queries.
- Raft owns leadership and membership. No bucket-only fallback after quorum loss.
- An isolated namespace and storage prefix with immutable mode selection.
  Cells in Fleet mode retain their current authority protocol.
- Automatic replica repair and balanced placement are first-release requirements.
- Small materialized databases first. Shared-cell groups, Raft hibernation,
  page-versioned execution and federation are separate scaling milestones.

## Delivery boundary

MR-01 through MR-15 deliver and qualify the first release. MR-16 and MR-17 are
later scaling work, with their own release evidence. Hundreds/thousands of nodes
and millions of logical cells are qualification targets, not established limits.
Millions of physical nodes require federation and are not a single-cluster promise.

The earlier `codex/read-replica-design` proposal preserves the Fleet ownership
protocol and treats read copies as disposable caches. This proposal instead
introduces consensus-backed authority and durable voting replicas. Reuse its
query isolation ideas where useful; it is not a prerequisite branch and this
package contains the required contracts independently.

The [storage integration audit](storage-integration.md) records the host filesystem
adaptation required by MR-02.

## Validation record

Package integrity and negative checker tests are recorded in
[validation](validation.md). These checks do not establish Raft safety, runtime
performance, or deployment qualification. Runtime evidence belongs to the work
packages and [qualification gates](qualification.md).
