<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Three-node bounded SQL admission wait

The bounded admission change passes its focused native regressions, but all
three SQL/KV/Queue runtime cases still fail. Nine of 20 planned timed windows
verify, three fail and eight remain unmeasured. Seven of 12 warmups verify and
five remain unmeasured. Nineteen emitted windows retain seven request errors,
all deadlines, with no public overload error. This diagnostic does not qualify
the complete matrix or replace the
[cross-mode comparison](three-node-primitive-capacity-2026-11-14.md).

| API | Verified timed windows | First failed window | End observations |
| --- | ---: | --- | --- |
| SQL | 5 | Second repetition, 16-client read: 472 successes and three deadlines | Complete on all three nodes; four group/staging admission refusals |
| KV | 4 | Second repetition, 16-client write: 470 acknowledgements, two deadlines and readback mismatch | Unavailable on all three nodes; subsequent node-lease self-fencing |
| Queue | 0 | First repetition, four-client enqueue: 83 acknowledged batches and two deadlines; delivery-set mismatch | Complete on all three nodes; eight group/staging admission refusals |

Queue's four-client warmup verifies 25 batches/200 messages, including IDs,
bodies and settlement. Its failed timed window acknowledges 664 messages and
observes 680 IDs: all acknowledged IDs plus the 16 IDs in the two unconfirmed
attempted batches. Reported backlog and DLQ counts are zero. The complete
observed body-hash map is absent from this failed row, so ID reconciliation does
not independently verify bodies or qualify delivery/settlement. Unconfirmed KV
updates can likewise commit after a lost response and replace earlier values.
Neither failed readback establishes corruption or a lost acknowledgement.
Original uncertain identities remain retained without fresh-ID retries.

SQL's four group-refusal increments are distributed 1/2/1 across nodes; Queue's
eight are distributed 1/1/6. The matching stage counters describe the same
refusals, not additional attempts. Other new phase counters remain zero for
these two cases. These are node-wide observations, including maintenance;
they do not prove which customer command waited or that every refused attempt
later committed. KV end counter deltas are **unknown**, not zero. No observation
attributes every deadline to SQL admission or excludes writer/proof/routing waits.

KV's load exits at `15:11:32.832661159Z`. All three node logs subsequently report
ambiguous bounded node-lease renewals, then self-fence with `NodeLeaseExpired`:
node 3 at `15:11:37.325540Z`, node 1 at `15:11:37.951862Z` and node 2 at
`15:11:38.422809Z`. Those fences explain the unavailable final observations;
their later timestamps do not establish the cause of the earlier workload
deadlines or readback mismatch. The original five-second TTL and fencing rules
remain unchanged.

## Admission behavior and native controls

An owned command may wait only after a received, fitting node/group reservation
refusal before SQL enqueue. Its finite invocation owner retains the original
command ID, input, captured clock/randomness, route ticket and deadline. It
holds no SQL slot or physical reservation while waiting. Credit notifications
recheck both item and byte capacity; controller ticks observe lifecycle changes
without repeatedly attempting a full bank. Actual staging reacquires fresh
authority and reserves against the current banks.

Oversized requests, worker-queue refusals, entered callback failures, private
opening/continuation errors and lost replies acquire no new replay permission.
Existing maintenance claim/settle/seal attempts release their command slot on
refusal. Retirement wakes lifecycle checks without refunding live physical
credits. Phase-facing APIs retain immediate refusal behavior. This closes one
bounded wait boundary; it is not full scheduler/fairness qualification.

Two real three-voter regressions fail against the earlier implementation with
immediate `Execution(Refused(Overloaded))`: one isolates group-byte pressure and
one node-item pressure. The changed implementation passes four new integration
tests in both image modes, covering returned credit and once-only receipt replay,
the original deadline, joined shutdown, and immediate refusal for oversized or
entered failures. While saturated, ten controller ticks produce only one
reservation refusal and no callback entry. Six new bank tests cover retained
output clones, shrink/item limits, consumed notifications, fitting peers,
oversized/retired requests and retirement ownership.

In total, 41 focused Rust tests pass: 20 unit tests, 20 execution integration
tests and one SDK source-proof dispatch test. The SDK test covers both image
modes and quorum/S3 durability. Strict affected-target Clippy, formatting/layout
(2,292 Rust files), native dependency checks (496 packages, no JavaScript engine)
and documentation checks pass. The Python performance suite runs 332 tests with
one existing optional skip. This is not a full native/SDK or OPT/CG campaign.

## Measurement binding and limits

Run `73df0a2958fe` uses immutable image `976eda7e`, compiler input `92d406c1`,
base `6d8299b8` plus the captured capacity-wait patch and two new test modules.
All 3,395 compiler inputs match the image before and after measurement. The
library and both binaries rebuild with locked Rust 1.98.1 and size-optimized
`lab`/thin LTO; load-client bytes remain unchanged.

The profile matches the [preceding refusal probe](three-node-primitive-admission-2026-11-14.md):
three separate one-vCPU/2-GiB nodes, shared one-vCPU/2-GiB local RustFS and a
separate one-vCPU/2-GiB client, with no extra swap. SQL/KV enroll 24 cells in
three RF3 groups; Queue enrolls 24 queues and 24 DLQs in three RF3 groups.
Serving leaders start balanced across nodes. Each case uses fresh storage,
Raft quorum, concurrency 4/16, five-second warmups, two reverse-order
repetitions and 30-second timed windows. Five-second operation/lease/callback
windows, one SQL worker, eight private workspaces and image/manifest/admission
banks remain unchanged.

The verified timed observations below retain individual window boundaries.
Rates are successful operations per actual elapsed second; percentiles are
successful HTTP operation latencies, not delivery latency. No failed window
contributes to this table.

| API | Repetition | Clients | Operation | Operations/s | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |
| SQL | 1 | 4 | write | 23.99 | 157.98 | 271.16 | 342.85 |
| SQL | 1 | 4 | read | 23.27 | 158.01 | 304.89 | 379.60 |
| SQL | 1 | 16 | write | 18.83 | 679.07 | 1851.76 | 2278.29 |
| SQL | 1 | 16 | read | 17.35 | 703.10 | 2311.34 | 2999.89 |
| SQL | 2 | 16 | write | 15.53 | 749.08 | 2641.31 | 3249.40 |
| KV | 1 | 4 | write | 15.36 | 240.22 | 459.54 | 653.74 |
| KV | 1 | 4 | read | 14.76 | 256.86 | 444.69 | 568.47 |
| KV | 1 | 16 | write | 16.92 | 712.71 | 2204.99 | 3225.54 |
| KV | 1 | 16 | read | 14.59 | 876.87 | 2630.13 | 3317.14 |

The previous probe verifies three timed windows and this probe nine. Matching
configuration does not isolate the cause: shared-host activity, source changes
and different execution histories preclude a causal speedup or stable capacity
claim. No new Fleet/bucket measurement is performed here; the earlier selected
comparison remains five complete cases and 75 verified timed windows out of 120.

All three KV nodes exit one after self-fencing. The other six nodes and all three
case stores exit zero; all three failed loads exit one. No measured container
restarts or is OOM-killed, and sampled kernel OOM counts are zero. Failed
containers/logs/volumes remain retained. SQL's sampling failure exercises
window-index recovery and complete failure observations; KV's missing nodes stay
explicit. Source/engine guards pass, all controller/driver handles are reaped,
and the original RustFS service is restored under its unchanged identity and
limits with readiness HTTP 200.

[Machine-readable observations](three-node-primitive-admission-wait-2026-11-14.json)
retain the complete matrix, per-node deltas where available, missing-node errors,
resource peaks and window percentiles.
[Compressed raw evidence](three-node-primitive-admission-wait-2026-11-14.raw.json.gz)
retains all 19 emitted windows, raw latency samples, original errors/identities,
owner snapshots, KV lease logs, source inputs/patch/new test modules and host
activity. Window accounting, source hashes and percentiles are independently
recomputed; missing windows remain unmeasured.

Next priorities are node-lease renewal progress under constrained load,
independent deadline/writer/proof-wait diagnosis and original-identity outcome
reconciliation. Sustainable complete-root manifest reclamation, authority
expiry/revocation, physical retirement/once-only Fleet settlement, provider
recovery and independent-domain OPT/CG gates remain open.
