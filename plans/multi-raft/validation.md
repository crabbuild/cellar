# Package validation

This historical record covers the initial design package and planning checks.
Current implementation evidence is recorded per task, starting with
[MR-01](evidence/MR-01.json).
Runtime implementation, Raft safety and fleet performance remain work-package
deliverables. At this initial validation point, all tasks were TODO.

Validated on 2026-10-19 against source revision
`0efd8f4d871199542f97f9fe0b74086a38606d02`.

| Check | Observed result |
| --- | --- |
| `python3 plans/multi-raft/plan.py validate` | Passed: 17 tasks, 104 proposed runtime cases, 51 source anchors and package-local links |
| `python3 plans/multi-raft/plan.py preflight` | Passed: all 51 source hashes match |
| `python3 plans/multi-raft/plan.py ready` | MR-01 is the only initially ready task |
| `python3 plans/multi-raft/plan.py capacity million-cells` | 3,000 replicas/node, 1,000 leaders/node and 2,000,000 logical heartbeat deliveries/second under the stated assumptions |
| `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s plans/multi-raft -p 'test_*.py' -v` | All 17 checker tests passed |
| `python3 tools/docs/check.py` | Passed: 36 documents, 6 crate READMEs, 10 visual guides, 6 flows and 616 local references |
| `git diff --check` plus a scan of every new package file | No trailing whitespace or missing final newlines |

The checker tests exercise invalid dependencies, duplicate identities, unknown
invariants, unsafe paths, missing evidence, empty test inventories, broken links,
source drift and capacity assumptions. They exercise planning-tool behavior,
not a Raft implementation. The capacity output is arithmetic, not a benchmark.

At that initial validation point, no Rust runtime code or dependencies changed. Rust compilation, the 104 proposed
runtime cases, fault qualification and scale benchmarks have not been run.
This initial result did not establish completion of any implementation package.
Consult tasks.json and the per-task evidence for subsequent implementation work.


MR-04 transport implementation and scoped qualification are recorded in
[MR-04 evidence](evidence/MR-04.json), pinned to source
`24261b84fe9c9b71e18b1e521fb821bd14c0c8d1`. Nine transport cases, eight frame/lifecycle
cases, the embedded-node service case and 37 core regressions passed on the final
Rust source. The broader 88-case node run also passed; its relationship to the
final transport-only queue refinement is recorded in the evidence. Three real
child peer hosts exercise TLS, two groups, independent leaders, one-way loss,
duplication, reordering and delayed Raft responses. These checks do not qualify
SQLite/LTX, S3, end-to-end SDK serving, production performance or capacity.


MR-05 verified image implementation and scoped qualification are recorded in
[MR-05 evidence](evidence/MR-05.json), pinned to source
`fd82ca8deec8c8c56ac9a0e527af5a10956bbc9c`. All 15 image cases and 46 contract, Raft and
existing replication/storage regressions passed. Four SIGKILL cuts exercise
image publication and selection through a fixture marker; this is not yet
qualification of engine apply markers. Actual SQLite queries use injected
readers, retain immutable generations and refuse mutations. LTX core/all-feature
builds, Clippy, formatting, layout, dependency policy, documentation and all 17
planning-tool tests passed. Command/SDK serving, S3 and scale remain pending.


Partial MR-06 image/apply integration is recorded in
[MR-06 apply evidence](evidence/MR-06-apply-progress.json), pinned to source
`c2266dc1818bbd80f653674db93477078cf0e79b`. Four integration cases and 61 regressions passed.
Four SIGKILL cuts cover the actual engine marker and verified images, including
after marker fsync but before public cache completion. Clippy and repository
checks passed. This does not complete MR-06: private command/capture production,
receipts and six named command cases remain required.


MR-06 private command implementation and component qualification are recorded in
[MR-06 evidence](evidence/MR-06.json), pinned to source
`3b526e7bb54e5b40cda314db9341c92298aa3107`. All 14 command/apply cases, including the seven
required gates, passed. Real WAL capture, a multi-file checkpoint cut, durable
receipts, expiry floors and nine SIGKILL cuts are covered. All 61 regressions
passed on the same production code; final owning tests and Clippy also include
the subsequent test-only checkpoint/expiry assertions. Formatting, layout,
dependency policy, documentation and all 17 planning-tool cases passed. This is
not yet SDK/node command serving: MR-07 must bind authorization, live authority,
output gates and cancellation/eviction ownership. S3, repair and scale remain
pending.


Partial MR-07 catalog/bootstrap work is recorded in
[MR-07 catalog evidence](evidence/MR-07-catalog-progress.json), pinned to source
`cff6a84ee446d5538f519eed5cea243be7931ea9`. Seven catalog cases passed, along with 82
regressions and the owning embedded-node lifecycle case. The record identifies
the subsequent catalog-only refinement and final owning reruns. Conditional
creation, lost responses, conflicting modes/identities, missing stable state and
parked voting admission are covered. Public SDK/actor/ingress integration and
five named MR-07 gates remain outstanding; MR-07 is still IN_PROGRESS.
