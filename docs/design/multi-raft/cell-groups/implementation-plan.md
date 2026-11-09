<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Implementation order

Purpose: Dependency order and status of the sixteen cell-group implementation tasks.

Generated from `tasks.json`. Regenerate with `plan.py index` after status changes.
**13 of 16 tasks are accepted**, covering **147 of 163 required cases**.
The acceptance count includes only DONE tasks. A task closes after its dependencies
and complete source-pinned evidence pass. The [open case audit](remaining-work.md#open-case-audit)
tracks individual passes and remaining gates for pending tasks.
The full release requires a combined run on one source revision.
CG-07 gates the default shared-group backend for new SQL namespaces.
CG-15 and CG-16 gate the full release.

Start with the [quorum SQL delivery priorities](remaining-work.md#first-delivery-of-quorum-sql-through-public-ingress)
for the next release gate and [progress](progress.md) for exact qualification results.

Use `python3 docs/design/multi-raft/cell-groups/plan.py show CG-01` for a complete
executor brief. Replace the ID to select another task. Commands are displayed,
never executed by the plan runner. Read the [design](design.md),
[protocols](protocols.md) and [qualification](qualification.md) first.

| Task | Deliverable | Dependencies | Status | Required cases | Task acceptance |
| --- | --- | --- | --- | ---: | --- |
| CG-01 | Separate logical cell history from physical group consensus | None | DONE | 4 | Accepted |
| CG-02 | Persist complete group manifests with crash-safe cell image ownership | CG-01 | DONE | 5 | Accepted |
| CG-03 | Prepare paged range routing and enrollment admission | CG-01, CG-02 | DONE | 8 | Accepted |
| CG-04 | Execute many independent SQLite cells through one real Raft group | CG-01, CG-02, CG-03 | DONE | 83 | Accepted |
| CG-05 | Snapshot and rebuild complete groups through membership changes | CG-04 | DONE | 5 | Accepted |
| CG-06 | Archive and reclaim the complete group recovery history | CG-04, CG-05 | DONE | 5 | Accepted |
| CG-08 | Preserve per-cell effects and bounded idle execution caches | CG-04, CG-05, CG-06 | DONE | 5 | Accepted |
| CG-07 | Cut public SDK routing and managed fleet over to shared groups | CG-03, CG-04, CG-05, CG-06, CG-08 | DONE | 5 | Accepted |
| CG-09 | Implement irreversible source sealing and destination activation | CG-07, CG-08 | DONE | 7 | Accepted |
| CG-10 | Split hot groups and merge compatible adjacent ranges | CG-09 | DONE | 5 | Accepted |
| CG-11 | Schedule group placement and adaptive partition size | CG-07, CG-10 | DONE | 5 | Accepted |
| CG-12 | Coordinate group hibernation and authority-checked wakeup | CG-08, CG-10, CG-11 | DONE | 5 | Accepted |
| CG-14 | Build reproducible shared-group workload and RustFS harnesses | CG-07, CG-08 | DONE | 5 | Accepted |
| CG-13 | Recover managed groups and incomplete transitions from S3 | CG-06, CG-09, CG-10, CG-11, CG-14 | IN_PROGRESS | 6 | Pending |
| CG-15 | Qualify crashes, split merge, wake and zero-quorum recovery | CG-10, CG-11, CG-12, CG-13, CG-14 | IN_PROGRESS | 6 | Pending |
| CG-16 | Publish the unified group runtime and qualified release envelope | CG-15 | TODO | 4 | Pending |

CG-01 through CG-08 establish the shared runtime and public cutover. CG-09 through
CG-13 add movement, adaptive scheduling, hibernation and complete managed recovery.
CG-08 adapts effects before CG-07 public cutover. CG-14 can proceed after CG-07;
CG-13 requires its real-infrastructure harness.
CG-15 and CG-16 qualify the combined artifact. These milestones are not separate
production architectures. No full delivery claim precedes the final gate.
