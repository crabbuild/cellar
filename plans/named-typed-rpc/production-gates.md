<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Typed RPC release gates

**Decision: not yet qualified for production.** Named/generated calls across
Rust, V8, Node and Python are implemented. Production qualification requires
the remaining gates below; passing the Counter example alone is insufficient.
This checklist closes [plan 009](../009-named-typed-rpc.md), rather than changing
its definition of completion. The [implementation ledger](implementation-status.md)
retains detailed evidence and failed runs.

## Latest published evidence

On commit `fca2c0c513db4bd319ec317e2ece028bded9c8e1`, both hosted typed-RPC jobs
pass the portable profile, lifecycle suites and expanded 20-suite / 1,064-case
combined driver: [GIL](https://github.com/crabbuild/cellar/actions/runs/40654793964/job/122703106046)
and [free-threaded](https://github.com/crabbuild/cellar/actions/runs/40654793964/job/122703105689).
The JavaScript/Node SDK and Python example workflows also pass. Both broader
SDK jobs fail in the Python runtime-limits fixture with `local reload requires
replication`; this is a shared standalone-host inventory error. The follow-up
returns an empty replicated inventory for standalone hosts and fixes a stale
Python boundary fixture which omitted the 16-byte `rpc_limits` member. The
native regression and exact-size Python fixture pass locally. The broader
follow-up passes the full GIL Python package command, both 576-call profiles,
both five-case proof-wait diagnostics, V8/Node streaming, Node compatibility,
affected Clippy and repository guards. The full free-threaded Python command
fails later: its final contract returns HTTP 500 on the first fleet write,
after both watchdog recovery cases pass. A direct replay of the preserved
executable passes. Nine instrumented direct replays and an instrumented full
bridge replay also pass; the original failure is unresolved.

The same published RPC source has preserved local full-run lease failures.
Earlier hosted lease failures and V8/Node timing failures remain unexplained.
The latest CI success does not establish their causes. Subsequent changes and
integration with newer `main` need their own checks.

The `2e68fac2` follow-up reproduces and fixes a separate synchronous-turn
starvation mechanism: a real SQLite busy wait blocks the only async worker and
prevents an independent timer from releasing the writer. Domain-owned turns
now hand that worker back while preserving ownership and joined completion.
A shared-executor fleet test observes lease progress across two unchanged
one-second lifetimes during a blocked application turn. The preserved Python
contract passes 400 pre-fix replays, including concurrent runs; its original
first-write failure remains unexplained. Implementation `a7b3534c` passes both
full local Python SDK modes, V8/Node streaming and compatibility, JavaScript
typed RPC, 20 focused native checks, affected Clippy and repository guards.
Final serial validation records 3,999 unchanged tracked files. Subsequent
`17ecfb42` hosted GIL RPC CI passes the profile but fails lifecycle after a
worker-owner lease expires. A focused free-threaded probe also reproduces the
first-write 500 on its 222nd fresh fleet with logging disabled. A separate
controlled test identifies synchronous diagnostic-output backpressure; the
qualifier now queues output and refuses incomplete diagnostic evidence. This
does not yet establish the cause of the first-write or hosted lease failure.
The follow-up passes 46 qualifier unit tests and both local 96-case lifecycle
matrices with mode audits, joined runtimes and zero bridge reservations. Its
free-threaded Python contract fails cross-domain context isolation with HTTP
500 before reaching the fleet-write stage; a direct timing-enabled replay passes.
Both failed observations remain release blockers. The GIL contract and the
focused first-write probes pass; their success does not close those causes.
PR #154 is now merged. Its final free-threaded typed-RPC job passes; the GIL
job still fails after owner lease expiry;
phase evidence distinguishes storage admission waits from 1–2 ms timer-delivery
overruns in the final attempts. The new context probe preserves both branches'
errors; passing repeated contracts and first writes does not close either
original failure. A separate pre-admission connection-loss observation is also
open. Previous ignored local artifacts were removed; their historical ledger
references are not a claim that those files remain available.
See the
[current investigation](implementation-status.md#context-failure-provenance-and-current-ci-2026-10-28-release-open)
and the
[latest implementation evidence](implementation-status.md#synchronous-turn-timer-starvation-2026-10-28-release-open).

## Remaining work, in delivery order

| Gate | What is delivered | Required closure evidence |
| --- | --- | --- |
| **P0: reliability and regressions** | Both full hosted RPC matrices and guest examples pass on the source above. Standalone inventory and exact-size fixture fixes pass local checks; full GIL SDK and both live profiles pass the follow-up. Failed local/hosted runs are retained. | Resolve the free-threaded first-fleet-write 500 under its unchanged 250 ms handler budget. Use storage-phase and wait-delivery timing to isolate retained lease stalls and explain V8/Node timing failures. Final SDK, HTTP/WebSocket and lifecycle CI must pass on the release source. |
| **P0: admission and resource bounds** | Peer refusal/loss, source/inbound/response pools, cancellation, proof-wait retention and provider-owned codec limits are tested. | Complete the audit of effective method/runtime/host/peer minima and retained copies. Qualify their composition under saturation, flooding, partial disconnects and cancellation, with bounded allocation/RSS, progress for replies/shutdown and zero owned reservations after cleanup. |
| **P1: supported Protobuf semantics** | An 18-vector profile makes 576 live local/peer calls across all runtime pairs per Python mode. | Finish the descriptor/options audit, explicitly reject unsupported semantics, and run every declared supported feature through generated live clients. Close gaps by semantic requirement, not by increasing Counter repetitions. |
| **P1: Python and application delivery** | Both loaded Python threading modes pass full RPC CI; dynamic codec/schema verification and offline `cellar rpc check` exist. | Finish unified generation, frozen consumer binding/package inventories, executable/descriptor and embedded-Python identity, and reproducible clean-machine Rust/JS/TS/Python examples. Missing dependencies, stale bindings and mixed packages must fail before business work. Broader SDK tests must pass in both modes. |
| **P2: operations and performance** | Typed outcomes preserve uncertainty; receipt examples demonstrate explicit safe retry. | Add registered-method spans and bounded metric labels; publish repeatable typed/untyped baselines, local/peer throughput and durable TPS, latency distributions, CPU/RSS, saturation cleanup and recovery reconciliation. |
| **Final: release artifact** | Source/executable identities accompany qualification evidence. | Re-run applicable gates on the final artifact and intended deployment configuration. Reconcile acknowledged and ambiguous operations after faults; retain shutdown and recovery evidence. Do not infer this gate from an earlier commit's CI. |

The SQLite phase diagnostics apply to the local development object store. They
help distinguish where delay occurs; they do not qualify a production object
store or prove an OS scheduling cause. See the
[diagnostic commands and phase diagram](../../tools/rpc/README.md#diagnose-lease-renewal-delays).

## T01–T19 coverage and open scope

“Covered” below means executable evidence exists for the listed boundary. It
does not mean every combination in the release contract has been qualified.

| Cases | Covered boundary | Remaining release scope |
| --- | --- | --- |
| T01 | Live integer/bytes/Unicode/presence and portable-profile pair matrix | Full declared profile and descriptor semantics audit |
| T02 | Wire/graph parser and codec budget refusals | Composed allocation/RSS and parser amplification qualification |
| T03–T04 | Explicit registration, schema identity, method isolation and reserved scopes | Final artifact/package audit; retain negative tests |
| T05 | Context expiry, retained clients, cancellation and authority replacement | Final lifecycle/resource composition |
| T06–T07 | Parent/child provenance and invalid post-commit replies, including stricter provider policy | Broader call-specific peer refusal and resource composition |
| T08 | Withheld, released and failed output proof | Resolve reliability failures observed while running these fixtures |
| T09–T13 | Process crashes, handoff, receipts, conflicts and concurrent callers | Final artifact/deployment recovery run and client-ledger reconciliation |
| T14–T15 | Deadline/cancellation, self calls and cyclic dependencies | Saturation and cleanup composition under retained resource pressure |
| T16–T17 | Peer framing/loss, Node/V8 lifecycle and Python supervision/cancellation | Wider flood/partial-disconnect combinations and resource accounting |
| T18 | Dynamic codec identity and offline descriptor/catalog checks | Frozen static binding/package identity and clean-machine qualification |
| T19 | Hosted JavaScript/Node regressions and Python examples; follow-up local SDK passes in both Python modes | Preserved timing failures and final hosted SDK/HTTP/WebSocket/lifecycle gates |

No gate permits automatic replay of an uncertain submission, a false rollback
claim, acknowledgement before durability proof, or admission by a stale owner.
