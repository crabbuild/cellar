# Cellar read replicas: delivery package

Status: proposed design, ready for implementation planning review. No replica
runtime has been implemented or qualified by this package.

Prepared on 2026-10-19 against `crabbuild/cellar` revision
`0efd8f4d871199542f97f9fe0b74086a38606d02` (`origin/main`, fetched that day).
Implement against the stated source baseline or a reviewed successor and run
the plan's drift checks first. All source paths are repository-relative; the
package has no dependency on the research environment used to prepare it.

## Deliverables

| File | Reader and purpose |
| --- | --- |
| [Technical design](007-technical-design.md) | Protocol, authority, durability, storage, SDK and operational decisions |
| [Implementation plan](008-implementation-plan.md) | Ordered work packages with exact owning paths, dependencies, verification and stop conditions |
| [Qualification plan](009-qualification.md) | Deterministic tests, five-node failure experiments, correctness oracle and release criteria |
| [Application integration guide](010-application-integration.md) | Proposed business handlers, builder registration, native/Axum ingress and version propagation |
| [Task manifest](tasks.json) | Machine-readable work ordering, proposed test targets, gates and status |

Start with the technical design, then execute work packages in the implementation
plan. An executor needs this package and a complete checkout of Cellar; no chat
history or external agent memory is required. `tasks.json` is a task inventory,
not an automation that installs, builds, publishes or deploys anything.

## Decisions made

- One authoritative writer per cell remains the source of mutations.
- Explicit per-class Command/Query contracts drive a shared dispatcher. Ingress
  adapters resolve authorized cell identity; destination business callbacks do
  no owner lookup or forwarding. Operation kind/digest mismatch is refused.
- Applications can register many commands and queries across multiple classes
  and feature modules, with custom types, policies and business helpers.
  Application services/workflows compose operations; cross-cell atomicity and
  a global snapshot are not implied by that composition.
- Durability followers and read replicas are independent sets. The reference
  topology is primary A, durability witnesses D/E, read replicas B/C.
- Preserve the current all-installed-member fleet proof or bucket-proof path,
  ownership CAS, fencing, predecessor recovery and ambiguous-write behavior.
- Publish only exact LTX cuts accepted by the core's durability/authority gate.
- Use materialized immutable SQLite generations and separately registered
  read-only query handlers. Ordinary cell constructors never run on replicas.
- First release: explicitly stale reads and version-qualified reads. Strong
  reads and time-bounded staleness return unsupported until separately designed
  and qualified; no silent weakening of their contracts.
- Read replicas are disposable serving caches in this release. They are not
  counted as durability witnesses and cannot independently certify promotion.
- Policy-based automatic placement and self-healing are first-release
  requirements. Cellar restores the read-copy target and initiates safe primary
  recovery without operator reassignment or incoming traffic. Operators set
  count/pool/domain/budget policy; explicit node assignments are fixture overrides.
- Feature defaults off until qualified. Demand-based replica-count autoscaling,
  arbitrary large paged replicas and consensus replacement are later projects.

## Execution order and status

| Work package | Outcome | Depends on | Status |
| --- | --- | --- | --- |
| RR-01 | Decision contracts and executable invariant model | — | TODO |
| RR-02 | Immutable ticket-to-LTX proof receipts | RR-01 | TODO |
| RR-03 | Core-authorized safe-cut publication | RR-02 | TODO |
| RR-04 | Bounded snapshot and incremental LTX materializer | RR-01 | TODO |
| RR-05 | Read-only storage, context and query registration | RR-04 | TODO |
| RR-06 | Authenticated bootstrap and tail transport | RR-03, RR-04 | TODO |
| RR-07 | Placement, replica lifecycle and query routing | RR-05, RR-06 | TODO |
| RR-07A | Automatic placement, repair supervisor and policy reconciliation | RR-07 | TODO |
| RR-08 | Write-version receipts and version-qualified reads | RR-03, RR-07A | TODO |
| RR-09 | Takeover, epoch bridges and replica reconciliation | RR-08 | TODO |
| RR-10 | Resource budgets, shutdown, administration and rollout | RR-09 | TODO |
| RR-11 | Disjoint-topology fault and performance harness | RR-10 | TODO |
| RR-12 | Qualification, documentation and release decision | RR-11 | TODO |

Status values: TODO, IN_PROGRESS, DONE, BLOCKED with a reason, or REJECTED with a
reason. Keep this table and `tasks.json` consistent. DONE means the package's
verification and review gates passed, not simply that code was written.

RR-04 can proceed independently of receipt integration after RR-01 fixes the
cut contract. RR-05 and RR-06 can proceed independently after their prerequisites.
Neither exposes user traffic until RR-07. This dependency structure permits
parallel implementation; it does not require concurrent edits to shared files.

## Planning verification and limits

Research inspected ownership, output gates, capture/credit paths, recovery,
SQLite opening, native application contracts, peer transport, manifests, CI and
existing test/performance harnesses at the pinned revision. Source excerpts and
verification commands were checked against that source snapshot. A fresh-context
review identified and corrected response-authentication gaps, missing host-level
crash tests, no-change RPC version semantics, same-epoch/multihop ancestry,
proof-posture qualification differences and the distinction between read serving
and authorized promotion. This is design review, not runtime verification.

Validate this package with JSON parsing, local-link/source-reference checks,
dependency-cycle checks, and `git diff --check`. Those checks establish document
integrity only. Rust compilation, runtime tests, fleet fault injection and
performance qualification belong to implementation and have not been run for
this proposed feature. Estimated delivery is a large, high-risk distributed
systems change; no speedup or production loss tolerance is asserted here.

Completed package checks: six deliverables, 13 acyclic ordered work packages,
103 named proposed regression cases, 39 work-package command references and fifteen
local links. Source drift anchors and existing integration-target files were
checked against the pinned source. Every new file passed an unstaged
`git diff --no-index --check` whitespace check.

## Alternatives rejected for this release

| Alternative | Reason |
| --- | --- |
| Add B/C to every write acknowledgment | Couples query capacity to write latency and changes durability membership |
| Run normal cell factories on B/C | Constructors, schema initialization and ordinary contexts can mutate state |
| Advertise the latest captured TXID | Capture is not completed durability plus authority proof |
| Reuse the live owner database as a replica snapshot | It can contain later unproven writes and WAL-only state |
| Promote whichever replica reports the largest number | Does not establish complete acknowledged history or write authority |
| Replace ownership with Raft for read scaling | Introduces a different authority/recovery project without being required |
| Expose strong reads under an eventual implementation | Misrepresents the consistency contract |
| Require operators to reassign replicas after every failure | Superseded: policy-based automatic placement and safe self-healing are required |
