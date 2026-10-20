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
