<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Named, typed RPC implementation ledger

Objective: implement [plan 009](../009-named-typed-rpc.md) end to end, with named
business methods across Rust, embedded V8, Node and Python on Cellar's existing
ownership and durability path. This ledger records evidence, not a smaller
definition of success. Status: **in progress; not qualified**.

## Delivery priorities

Pick the highest unresolved correctness or delivery risk for each run. Finish a
bounded implementation, reproduce the relevant failure at its owning boundary,
run the affected checks, and record both the evidence and the remaining scope.
Re-rank when a regression exposes a more urgent risk. A green Counter matrix
does not close schema, resource, packaging or performance requirements.

| Priority | Remaining gap | Deliverable / completion evidence |
| --- | --- | --- |
| P0 — hosted verification | Final hosted gates | Current head `1a2be141` passes both local full matrices and live profiles. Hosted GIL profile and JavaScript container recovery fail on lease expiry; the free-threaded full matrix is still running. Require final SDK, JavaScript, typed-RPC and example gates. Preserve exact failing logs and identities below. |
| P0 | Native and cross-adapter regression timing | A real held-checkpoint regression reproduces a development-store admission bottleneck: the Rust writer mutex remains held after SQLite permits new transactions. Releasing it before COMMIT keeps CAS atomic under SQLite's write lock and lets lease CAS complete within its original 333 ms attempt bound while put/delete checkpoint I/O stays paused. Native I/O reservations and FULL WAL commits remain intact. The final local JavaScript regressions pass unchanged budgets, but their old timing failures and a newly observed Axum lifecycle 503 remain unexplained. Require fresh hosted/original-budget regressions; this controlled cause does not establish the precise phase of earlier CI stalls or resolve the four original 250 ms failures. |
| P0 | Remaining lifecycle and peer/admission provenance | Preserve 55 deadline, 128 proof-wait, 80 cycle and 96 Node/V8 lifecycle cases. Provenance now has 112 cases in each observed Python mode, including raw native envelopes through every generated caller over local/peer routes. The host downgrades entered native failures even when custom cells bypass generated finishing; genuine pre-factory refusals retain their proof. Saturation has 144 cases and peer faults have 112 in both observed Python modes. Receiving overload stays `ResourceExhausted/unknown`; broader call-specific refusal proof, nested-provider and uncertain partial-submission qualification remain. |
| P0 | Resource bounds and parser correctness | Source/inbound request and tunnel admission, dense/cooperative collectors, typed receive ceilings, and shared native decoder/stack allowances retain their existing evidence. A shared response pool now precedes provider factories/codecs and collector growth, normalizes backing, and follows byte aliases through proof/transport waits. Both actual Python modes pass 144 saturation and 128 proof-wait cases with zero cleanup reservations. Dense request backing/alias ownership and native response/decoder handoffs now pass the frozen merged-source full matrices. Wider saturation/fault combinations, effective minima and allocator/RSS qualification remain open. |
| P1 | Language/profile coverage | The response-pool production source passes all 19 Counter/failure suites in both actual local CPython modes: 1,016 cases per mode plus both 576-call live profiles, with independent audits and unchanged sources/executables. Published native-decoder head `fa1543f1` also passes both hosted typed-RPC jobs. Descriptor semantics, broader regressions and packaging qualification remain open. |
| P1 | Reproducible application delivery | Offline `cellar rpc check` now validates descriptors, exact canonical inventories and optional native catalog snapshots. Deliver unified generation, verified consumer binding/frozen package inventories, embedded Python source identity, a published typed-RPC example and clean-machine CI. |
| P2 | Operation visibility | Add registered-method spans, bounded labels and payload-free diagnostics for failures, deadlines and proof waits. |
| P2 | Measured performance | Publish reproducible typed/untyped/native/guest latency, throughput and durable TPS, plus saturation memory, cleanup and recovery results. |

Preserve existing durability evidence while addressing these gaps. Further
recovery-fault variations follow demonstrated correctness risks; expanding the
Counter fixture alone is not the highest-priority deliverable now.

## Shared response admission (2026-10-27; local slice qualified, release open)

Two controlled regressions exercise the real collector and production node with
response infrastructure present but unwired. A retained one-byte reply slice
does not stop a second collector from polling/accepting another body; a held
native handler does not stop a second business result despite a one-provider
encoding allowance. Both runtime tests fail as expected. The collector binary
is `bd97f170f7dec17af912d8e6d95f73f74a7ccd91b15a987cb968312dc5e5551b`;
the provider binary is
`f82e977c42c97bb2b45be82f8181955e22bddeff434874e20e63cde8ab3d577f`.
Sources, lock and logs are preserved under
`.cellar/rpc/diagnostics/response-pool-red-v1/`. Two earlier provider fixture
attempts fail compilation and are excluded from runtime evidence.

The implementation introduces `RuntimePolicy.max_rpc_response_bytes` (256 MiB
by default) and independent `HostServices::rpc_response_usage()` counters.
Typed provider admission reserves its encoding/copy peak before factories;
dense byte ownership then survives output-proof waits, queues, transport and
all byte aliases. Finite typed/ordinary/event collectors reserve before growth
and declared-minimum polling, including reallocation overlap. Unknown external
backing is normalized before a provider result is retained. Quota refusal uses
an exact typed failure envelope with unknown effects, including across peers.
Invalid UTF-8 JSON conversion has its own expansion allowance. Control/refund
paths never wait for another response.

The first source revision passes 735 core tests and 22 selected native tests.
It predates the final peer refusal envelope, declared-minimum pre-admission and
broader qualification assertions. Do not substitute that smoke result for final
source qualification. The extended suite requires 144 saturation cases (32 new
provider-response local/peer cases), retained capacity during all 128 proof-wait
cases, and zero response reservations after cancellation and node join in both
actual CPython modes. Final validation preserves these outcomes:

| Boundary | Evidence |
| --- | --- |
| Native/framework regressions | 738 core tests and 37 selected integrations pass |
| JavaScript regressions | All 126 tests pass under unchanged budgets; six external cases remain ignored |
| Python and generation | 31 Python units plus streaming/typed-RPC integrations; all 13 generator tests pass |
| Actual interpreter modes | Each mode passes 576 live profile calls and the complete 19-suite / 1,016-case driver |
| Resource/cancellation composition | Each mode passes 144 saturation and 128 proof-wait cases, including retained response capacity and zero post-cancellation/join reservations |
| Independent audits | Profile, peer, provenance, saturation, guest-control and actual-mode audits pass; saturation rejects 20 corrupted reports |
| Static/API modes | Core/JavaScript all-target Clippy, embed-only/Node-only builds, formatting/layout, native dependencies and docs pass |

The frozen `response-pool-qualification-v1` run watches 3,745 inputs; all sources
and both copied executables remain unchanged. Its manifest SHA-256 is
`568ee8f2d45fe03f7ea702d36ddf53fdefbfb5a01d003cd46c0144a4cf591540`.
Four of its 41 checks fail compilation because the expanded proof-audit unit
fixture exceeds the JSON macro expansion limit. All live commands pass. Split
that fixture's object construction rather than raising a limit or weakening
assertions. The subsequent recheck passes all 41 guards and all-target qualifier
Clippy in each mode, and rebuilds byte-identical production executables:

| Artifact | SHA-256 |
| --- | --- |
| GIL executable | `e391fa4768c9b9c689bfb41ab98862526b98437e0ff7c7a8131162eb7c1e7a95` |
| Free-threaded executable | `bd8094c0003e9b64268d9c3cda2f4bcbb7839b6b5e9b4e8608efbbde81e90992` |
| GIL full report | `ceffdd8f4d2d503067bb9a368faa737633a2f74e7f070783b9071aecd8804376` |
| Free-threaded full report | `a792105cbc7756763cd32bce61641e8c1cc8e89724e5922be71647abccf94064` |
| GIL profile | `4c88ecd090a79771d5116b4b2ed513ba0ef1408334015221f7a129cb0b808bd8` |
| Free-threaded profile | `9976bd90301693cddfaf1caf0d31e671ada86ca246b04664e925c4ed29a0b39a` |

Published `fa1543f1` passes both hosted typed-RPC jobs in run `40610862296`.
Both SDK jobs in run `40610862321` instead fail compilation at the remaining
Axum caller of the shared lease observer. Pass `None` explicitly; preserve both
original logs in `.cellar/rpc/diagnostics/hosted-fa1543-axum-trace-v1/`.

The first repaired Axum all-feature run exposes a separate lifecycle response
failure: `/hold` returns 503 instead of 200 after five seconds. The handler's
entry state and failed body were not captured, so its cause is not established.
Preserve its executable (`42350d0ba9f00e3e9818079138715f5074afbd94587e19284e394d1f7cac0202`),
sources and log under `.cellar/rpc/diagnostics/axum-lifecycle-503-v1/`.
Ten serial and 32 concurrent original-binary replays all pass; they are not a fix.
Add bounded failure-only head/body, handler-entry, host-work and lease diagnostics
and join node cancellation on that failure. Keep every existing budget and
assertion. The final `response-pool-axum-recheck-v3` passes all eight checks:
35 all-feature and 29 no-default-feature Axum tests, one ignored doctest per
mode, Clippy and static guards. Its sources and both live executables remain
unchanged. Only test fixtures and documentation differ from the qualified RPC
production source; the original lifecycle 503 remains an open reliability case.

This allowance does not qualify whole-process RSS, allocator usable size,
application-retained graphs or untyped leased capability streams. Broader
source-response fault/saturation combinations, retained request backing, native
response-to-decoder ownership handoffs, effective adapter minima,
measured memory/performance, remaining descriptor semantics and packaging are
still open. The original JavaScript timing failures remain unexplained; the
budget-phase diagnostics do not establish their cause.

## Dense request ownership and native decoder handoff (2026-10-27; local slice qualified, release open)

Three controlled observations reproduce ownership gaps on the actual SDK path:

| Boundary | Required observation / red result |
| --- | --- |
| Source proof gate | A one-byte view relinquishes its 8-MiB external backing before waiting; the old gate keeps it pinned |
| Routing queue | Short JSON/scope/name/method strings do not keep 1-MiB spare allocations; the old queue retains a 1-MiB scope for 13 encoded bytes |
| Native response codec | Response and decoder allowances overlap through native decoding; the old codec observes decoder admission with response admission already retired |

Both source regressions fail, and the real-node decoder probe returns the
expected count with `decoder_admitted=true` / `response_admitted=false`.
Preserve the old binaries, sources, lock and logs in
`.cellar/rpc/diagnostics/request-backing-handoff-red-v1/`. Their hashes are
`5bec7a2fdc64e40ad63e1a2856dd42aba80658de1df1f9476087e9e7d7f8e35c`
(source) and
`71919b30fd21378bb837b08820aae0711e0c3b4b19e17ff7071054c90af5f568`
(native decoder).

The implementation reserves before dense normalization, then explicitly drops
original source values before waiting. Shared tickets follow structured inputs
and JSON peer-body copies through every byte alias, independently of dispatch
retirement. Native clients retain their response frame through preflight and
decoding. Raw-byte APIs keep their public owned-vector return contract. Lease
revocation, typed reply ceilings, original deadlines and failure provenance are
preserved; admission/normalization refusal remains before submission.

The first focused source run passes 12 tests and exposes two existing queue
tests' assumption that dropping an explicit ticket frees still-retained input
bytes. Keep that input charged after caller/dispatch completion, then explicitly
drop the owned request bytes and require zero reservations. The corrected broad
run passes all 742 core tests and 37 selected native tests, but the new handoff
probe receives an unobserved non-JSON HTTP response before its codec assertions.
Its exact failed executable is
`172b227663aeed9876e4c685abde030d261d9e1b5230252f3024a03f7d4bd77e`;
preserve it with sources and log under
`.cellar/rpc/diagnostics/request-ownership-probe-http-v2/`.

Add bounded failure-only HTTP status/headers/body, business entry, host work,
resource and lease observations; join node cancellation on probe failure. The
diagnostic run passes both native response tests with the original 250-ms
handler budget, including overlapping response/decoder admission and zero
post-join reservations. All-target core Clippy, formatting (1,252 Rust files),
layout, native dependency and documentation checks pass. This verifies the
ownership fix in that run; it does not explain the earlier HTTP failure.
The merged-source `request-ownership-main-v1` cohort at `1a2be141` includes main
`cd9a9689`. All 3,859 watched inputs and both copied executables remain unchanged.
Its 50 checks contain 49 passes and one failed complete JavaScript regression
command; this is not an all-green release run.

| Boundary | Current merged-source evidence |
| --- | --- |
| Core and native | All 780 core and 38 selected native tests pass, including request backing, alias retirement, native response handoff, decoder/copy bounds and cancellation |
| Generated profile | Both actual CPython modes pass 576 live local/peer calls and independent mode/profile audits |
| Full failure driver | Each mode passes all 19 suites / 1,016 cases, including 144 saturation and 128 proof-wait cases; independent peer, provenance, saturation and guest-control audits pass |
| Language and generation | 31 Python units plus streaming/typed-RPC integrations, all 13 generator tests, JavaScript tooling (139 tests), RPC tooling and TypeScript checks pass |
| Guards and API modes | Both 41-case qualifier guard suites, qualifier/core/JavaScript/Axum Clippy, embed-only/Node-only builds and static guards pass |
| Axum | 35 all-feature and 29 no-default-feature tests pass; one doctest is ignored per mode |

The full driver counts 16 local and 16 remote pair rows alongside its 984 named
failure cases. Both modes preserve actual interpreter identity and zero bridge
reservations after join. These observations qualify the exercised ownership
slice; the full plan's remaining semantics, resource and delivery gates stay open.

| Frozen artifact | SHA-256 |
| --- | --- |
| Source manifest | `530ae9a433e60be30e178e8a74bd1536399a6398773159c59a158d66a3baff16` |
| GIL executable | `8187f54f1c3938704880babed5f47a394590ff34935af1e02568afc56a4d2154` |
| Free-threaded executable | `f11ed311fee0a917c2a00592d3c65ff131a559abb8adfd8d7f2c8bb6e29605d4` |
| GIL full report | `afe4a35eebc434c99c3b2eb6057fd42c127a6d061a9c11a7f960d4ed863cded0` |
| Free-threaded full report | `538927e969ffe13bcc5a0390b5f7d301c25161d18a727493c96771e6607323c1` |

The JavaScript command records 128 passes, three failures and seven external
ignored cases. Two new gateway tests lack the pinned `esbuild` prerequisite.
Install the documented `tools/sandbox/cloudflare-v1` dependencies, verify/build
the gateway, then rerun the same two test executables: Workers entrypoints and
R2 both pass. Their hashes are
`65549c54f4b7011c97f0d5c36b7da7e59215eb39bdf95e9d94fab947dccaefc3`
and `b85582b3ddb3fc2136328d5d20d0669a85c667743c468d3ee8cf2e6ff777d8d0`.
Preserve the original prerequisite failures and rechecks under
`.cellar/rpc/diagnostics/request-ownership-main-v1-gateway-prerequisite/`.

The third failure is a V8 container `/all` turn timeout: `phase=microtasks`,
`observed_wall_us=2002200`, `engine_thread_cpu_us=3298`. Preserve its exact
executable (`5fc58dd89ab4465e9ca24cdf21a3ea0959824c1e00c7b00558b471900c65e4e6`),
sources and log under
`.cellar/rpc/diagnostics/request-ownership-main-v1-v8-containers/`.
Six uninstrumented serial replays pass; none establishes a fix. A stack-sampled
replay fails earlier with a connection close, so it does not reproduce or
explain the original timeout. Low engine CPU narrows the investigation toward
native waits or scheduling; it does not identify a cause.

Published `6addd7eb` JavaScript workflow `40623397481` fails in the container
successor's accepted-create recovery case before adapter tests execute. The
successor reports `Fenced/NodeLeaseExpired`; this is not a typed-codec failure.
Preserve job `122602515937` under
`.cellar/rpc/diagnostics/hosted-6addd7eb-js-v1/` (log SHA-256
`9789c949e904117a8b947b8f2533696eba7dcbfda13469a641549c0abce77af5`).
The job tests the virtual merge into main `cd9a9689`; local integration must
include that main source before asserting an equivalent regression result.

The new published head `1a2be141` fails the same container successor case in
JavaScript run `40628344823`, job `122618585086`. The successor is fenced after
two inspections and no deletion. The preserved log SHA-256 is
`9cf85094e56d05b6be15e415c1f931545dd3a9bcf6c54bffa3755ee335e4a723`
under `.cellar/rpc/diagnostics/hosted-1a2be141-js-v1/`.
Its GIL typed-RPC job `122618587860` in run `40628344860` also fails the live
profile at Python-to-Rust `empty-bytes-and-values` after repeated ambiguous
renewals and both nodes fencing. Cleanup reports zero bridge reservations.
Preserve the log (`7bc4c926c752204203704604f2fbc5dbbc853bff69e4a2cb67a3d3e247ae183d`)
under `.cellar/rpc/diagnostics/hosted-1a2be141-typed-gil-v1/`.

The next test-only diagnostic adds fixed phase marks and the shared bounded
lease observer to the repeatedly failing container fixture. Render observations
only on failure, keep original budgets/assertions, and use existing workspace
tracing dependencies. All five container fleet supervision tests, all-target
container Clippy, formatting/layout, docs and the native dependency guard pass
locally with the observer. Cargo's lock update adds only the two existing
test-dependency edges. This instrumentation is not a causal timing fix.

This does not close all retained-copy, effective-limit, allocator/RSS, protocol
or package qualification. The original timing failures and the preserved Axum
lifecycle 503 still require causal diagnosis.

## Work packages

| Package | State | Authoritative evidence / next action |
| --- | --- | --- |
| RPC-01: Contracts/toolchain | In progress | Counter and portable-profile outputs reproduce byte-for-byte in all four authoring modes; global symbols/import visibility and expanded inventory budgets are validated before output/compiler work; generated APIs pass TS/Python static checks; remaining options/schema/conformance qualification stays open |
| RPC-02: Native typed path | In progress | Native RPC/transport, generated-profile/vector and restart tests pass; codec limits precede allocation; 55 deadline, 128 proof-wait and 80 dependency-cycle cases cover caller/provider pairs, signals/deadlines, local/peer routes and bounded cleanup; 112 provenance cases preserve committed parent writes and fixed categories, including custom native envelopes; broader peer/admission/lifecycle/resource qualification remains |
| RPC-03: Binary bridges | In progress | Shared `cell.call`, Python raw bytes, owned V8 attachments and binary Node framing implemented; native Node framing now shares payload reservations with partial-input/output and decoded-frame lifetimes under test; worker/control/decoder allocations, other adapters' retained copies and full cancellation/flood qualification remain |
| RPC-04: JS/TS services | In progress | Generated clients/handlers, binary dispatch and live descriptor checks work in V8/Node; real startup refuses stale codecs; typed AbortSignal options and native-owned whole-call codec budgets work with bounded cleanup; full schema/resource/deadline qualification remains |
| RPC-05: Python services | In progress | Generated APIs, stale-codec refusal, native-owned budgets and cancellation/recovery work in the described fixtures. Both loaded CPython modes pass the existing complete Counter/failure driver and portable profile locally and in typed-RPC CI; packaging, broader SDK regressions and full qualification remain |
| RPC-06: Fleet/durability | In progress | Local/remote/handoff, 32 failed-recovery admission, 48 SIGKILL and 64 proof cases pass together under both observed CPython modes; broader failure/resource and hosted qualification remain |
| RPC-07: Packaging/performance | In progress | Offline descriptor/inventory/native-catalog checking implemented; unified generation, frozen package/static-binding inventories, clean-machine packaging and performance report remain |

## Required evidence

| Requirement | State |
| --- | --- |
| Typed APIs and runnable examples in Rust, JS, TS and Python | Generated APIs and runnable qualification fixtures implemented; strict TS/Python API checks pass; published example packaging remains |
| Descriptor/profile validation and exact artifact identity | Implementation under test |
| Complete malformed-descriptor semantic checks before invoking message generators | Partial; global symbols, import graph/visibility/indexes, expanded-name budgets, map ownership/shape/key types, synthetic oneofs, enum aliases/reservations, field reservations/packing and both JSON-name namespaces have adversarial tests; remaining option/schema semantics stay open |
| Explicit registration, full method validation and RPC-only cells | Implementation under test |
| Live language codec identity against the native artifact | JS/Python provider readiness and client pre-encode checks implemented and tested, including recursive/map/well-known types; static binding/package verification remains |
| Bounded request/response codec and binary bridge accounting | Native wire/raw attachments, SDK source and receiving request/reply/tunnel quotas implemented; 112 saturation and 48 guest-control refusal cases pass. Flat-control decode reservations include measured scratch growth. Exact retained-copy/allocator/RSS accounting and broader saturation qualification remain |
| Input graph depth/element preflight before allocating language encoders | Rust generated visitors and iterative owned cleanup, Python graph/map checks and JS descriptor-driven type/graph/exact-size preflight tested; broader codec and decoder-allocation qualification remains |
| Decoder layout/default-field allocation and effective limit plumbing | Shared decoded-slot estimate implemented in native wire and Rust/JS/Python encoders; default map layouts, fixed control budgets and native/guest catalog plumbing tested; exact allocation/RSS, saturation and method/runtime/host/peer minimum audit remain |
| Invocation authority, cancellation, deadlines and nested-call provenance | Owner fencing and retained-client refusal cover all 16 pairs; 55 deadline cases cover guest nesting/codec costs; 128 proof-wait cases cover signal/deadline × local/peer × output/sync × all 16 pairs with fresh admission and original-authority checks; 80 self/A-to-B-to-A cases cover bounded cycle cleanup; 96 parent-write/forged-category cases pass with unknown outcomes; broader peer/admission/lifecycle qualification remains |
| Pre-factory contract validation and trusted refusal provenance | Native/Python cold-constructor tests pass through the host catalog; custom wrappers without a forwarded catalog remain conservative; peer/admission provenance remains |
| `cellar rpc check`, pinned Buf workflow and static binding inventory validation | Offline descriptor/inventory and optional native catalog check implemented; pinned Buf integration, verified executable/package identity and consumer binding validation remain |
| Registered-method spans, bounded metric labels and payload-free diagnostics | Pending |
| Business errors separated from infrastructure errors | All backends preserve business variants and post-write invalid-response/unknown locally; entered-handler fixed categories survive with unknown effects in all runtimes; broader host-code mapping and peer/admission provenance remain |
| Output proof after a handler returns without explicit sync | All 16 pairs pass real peer/bucket hold then release, and hold then failure; typed result/local commit, no early response, unknown failures and explicit receipt retries verified under both observed CPython modes |
| Fencing after handler return and during its original sync await | All 16 pairs pass both phases with observed `NodeLeaseMismatch`, joined owner domain, advanced survivor epoch, acknowledged receipt recovery and late SDK refusal under both observed CPython modes |
| Failed recovery before provider construction | All 16 pairs refuse missing and replaced original proof roots: 64 `Unavailable/unknown` calls, zero provider factories/handlers, unchanged retained proof and live recovery after metadata restoration under both observed CPython modes |
| Atomic Counter receipts, exact 64-bit state and retry window | All 16 pairs pass local/remote/handoff, receipt races, owner fences and separate-process kills before handlers, inside count/receipt transactions and after synced handlers; broader recovery and interpreter modes remain |
| Existing HTTP/lifecycle/untyped RPC regressions | Reply-copy source passes 716 core, 17 selected native and 10 selected JavaScript tests, Python's 31 unit tests and streaming/typed-RPC integrations under unchanged budgets. Its preceding diagnostic source passes both complete 952-case failure drivers. Retain the earlier named-container V8, four 250 ms and hosted S3 seed failures; passing repetitions do not establish their cause, and full T19 stays open |
| T01-T19 from plan 009 | All remain open until their actual boundary is verified |
| Standard format/layout/dependency/docs checks and affected tests/Clippy | Reply-copy source passes core/JavaScript all-target Clippy, qualifier all-target Clippy and 40 guards per Python mode, Node-only checking and standard checks across 1,242 Rust files. All 3,738 watched source/fixture/probe inputs and both executable hashes remain unchanged across its 31 recorded checks; full qualification stays open |
| Generated output reproducibility and frozen dependency inventories | Counter/profile reproduce 24 artifacts; tool versions locked; clean-machine/CI inventory verification remains |
| Throughput, latency, durable TPS, memory and recovery report | Pending |

Do not mark the goal complete from native unit tests or a subset of backend
pairs. Preserve the full test matrix, failures and remaining work across turns.

## Current evidence

- **JavaScript timing diagnosis: preserved failures remain open.** Three bounded
  loops use the exact preserved streaming/named-RPC executables. The first
  runs 24 processes with output capture disabled and the named-method test
  isolated; the second restores capture and complete target ordering for 16
  processes; the third runs 32 processes in four-process batches. Every run
  uses fresh private test directories and unchanged handler/engine budgets.
  All 72 processes pass. These are unsuccessful reproduction attempts, not
  evidence that the earlier three timing failures are fixed. Manifests, binary
  hashes, commands, CPU/wall measurements and individual logs are under
  `.cellar/rpc/diagnostics/javascript-timing-{baseline-v1,captured-v2,concurrent-v3}/`.

  To sharpen the next failure signal, the V8 watchdog preserves its first
  observed pump phase and wall timings before current-pass cleanup. The engine
  adds its own thread CPU reading when reporting the error, including any
  engine-side unwind work. Fixed phases cover startup, replies, admission,
  microtasks, completion, cancellation and retirement. The snapshot contains
  no payload, user-controlled method or source text. CPU samples cannot change
  deadlines or interruption causes; unavailable clocks are explicit. The
  original execution-budget root cause survives the extra error context.

  All **83 JavaScript library tests pass**, including actual runaway dispatch,
  microtask and startup interruption under the original 40 ms test budgets,
  requested shutdown and bounded snapshot tests. The complete JavaScript package
  command passes **126 tests**, with six external-provider tests ignored and no
  failures; HTTP streaming, named RPC and existing framework fixtures pass under
  their unchanged budgets. All-target JavaScript Clippy and separate embed-only
  and Node-only checks pass. The five affected production/lockfile hashes are
  unchanged throughout these checks.

  Both actual Python modes pass **16 V8 termination** and **32 V8 shutdown**
  cases, mode auditors and all 41 qualifier guards. All **19 lifecycle/static
  checks** pass, with 3,742 watched inputs and both executable hashes unchanged
  during `.cellar/rpc/javascript-budget-observation-lifecycle-v1-*`.
  The manifest SHA-256 is
  `e07ab36333329937be71d7f4062b203064e28e29376d1fc8636a3d7354705199`;
  qualifier hashes are
  `5c09aeeb3c0be53fa1c450b3d6ef514d0c187131592b62c13d759bccc97c966d`
  and `95bb605375fbdf0d238ea5a1295311071c1e4942285cc0037a027adae2ef98e7`.
  This improves failure evidence without claiming a cause or a fix for V8 SSE,
  Node outbound SSE or typed codec-budget timeouts.

- **Native decoded layouts now have shared heap and per-call stack admission.**
  The main workspace's real generator produces a 512-string inline message.
  Only 256 wire bytes and 386 portable slots can materialize 128 enclosing
  messages and allocate **1,572,864 bytes** despite every child being absent.
  With the new quota infrastructure present but decoder entry points unwired,
  the real provider/client test accepts this message under a 1 MiB policy.
  That failing executable SHA-256 is
  `5d137d3566eed81f8d9d3c738533adc8a0b363cd8c852f92a20b173f90650a04`;
  exact sources, generated artifacts, main lockfile and log are preserved in
  `.cellar/rpc/diagnostics/native-decode-main-red-v1/`.

  Native Router request decoding and Client response decoding now reserve a
  shared host allowance before invoking Prost. The model adds transitive absent
  inline fields, wire/container growth and fixed scratch. Stack accounting
  follows active message nesting, restoring its previous value for siblings.
  Defaults are 256 MiB shared decode capacity and 1 MiB per-decode stack
  allowance; custom executors must supply stack and application headroom.
  Overflow and saturation return `ResourceExhausted/unknown`: a constructed
  provider or previously committed reply cannot establish rollback.
  Request reservations stay with the handler through response encoding, while
  client reservations end when the converted value returns to application code.
  Owned permits refund on another thread and after domain shutdown.

  A second main-lock diagnostic accepts a portable depth-64 recursive oneof,
  then overflows a **256 KiB** OS thread stack (exit 134). The same diagnostic
  passes on 2 MiB; do not claim a default-stack failure. Its binary, descriptor,
  source and both outcomes remain in
  `.cellar/rpc/diagnostics/native-stack-main-red-v1/`. Permanent bounded
  subprocess tests now refuse the deep graph before decoding on a 512 KiB
  thread, accept a shallow graph there, and decode/iteratively discard the
  deep graph under a separate 32 MiB thread and explicit larger policy.

  All **six focused native integration tests** pass, including allocation
  measurement, oneof/map defaults, generated request/client refusal, shared
  saturation, real caller cancellation, fresh progress and zero post-join usage.
  The subsequent frozen run passes **729 core tests**, **35 native integrations**,
  Python's 31 units plus streaming/typed-RPC integrations, **13 generator tests**,
  generator/JavaScript all-target Clippy, Node-only checking and the standard
  checks. Both actual interpreter modes pass 40 qualifier guards, all-target
  qualifier Clippy and the **576-call live profile** with independent audits.

  This run records **24 passed checks and four failed checks**. JavaScript has
  three failures: V8's first SSE chunk hits the engine-turn budget; Node outbound
  SSE and V8-to-Node `codec-budget/encode` hit the original 250 ms host budget.
  Node routing, stale-codec admission and all five Workers-module cases pass.
  Both full drivers then stop at their first proof-wait case: the diagnostic
  subscriber conflicts with the proof fixture's separately installed global
  subscriber. This is a harness regression introduced with the diagnostics,
  not successful qualification of the remaining suites. Core all-target Clippy
  also finds the new policy test module before implementation items.

  All **3,741 watched inputs** and both executables remain unchanged throughout
  `.cellar/rpc/native-decode-checks-v1-*`; source manifest SHA-256 is
  `967cb22d65d5eb51f5f90b5965f60a50c30dafc92b1e283b24433d1cb195a83f`.
  Executable hashes are
  `4b5833b3e8a28ecf22eca708bfec07b8616954b8e8c9b7a7c30ac32b98d62647`
  (GIL) and
  `3b64eb41a70047a0b6bc3ccbc6c4ec12ba24f1d8f103a49b58579c32a20d8754`
  (free-threaded). Logs, manifest and failing adapter executables are preserved
  under `.cellar/rpc/diagnostics/native-decode-regression-failures-v1/`.
  A single externally sampled replay of the preserved V8 SSE executable passes;
  it does not reproduce or explain the original failure.

  The policy tests are moved after the implementations with identical production
  text. Diagnostics and the bounded proof-observation layer now share a single
  subscriber; the stderr filter applies only to its layer so it cannot disable
  proof capture. The exact observer regression passes with stderr set to `off`,
  an INFO renewal recorded from another thread, and two later installs sharing
  the original trace. The corrected policy test passes too. An earlier bare
  `--exact` filter selected zero observer tests and is excluded from evidence.
  The recheck passes the corrected policy test and **18 affected native tests**,
  all **41 qualifier guards in each mode**, core/JavaScript/qualifier all-target
  Clippy, Node-only checking and standard checks. Both modes pass the five-case
  proof-wait smoke run, the **576-call profile**, and the complete **19-suite,
  984-case Counter/failure driver**. Mode, peer, provenance, saturation,
  guest-control and profile auditors pass, including their corrupt-report probes.
  The runner initially gave the provenance auditor an extracted subsection,
  omitting its required runtime-join fields. The two corrected audits use the
  original full reports; no report or verifier assertions are changed.

  All **3,741 watched inputs** and both executable hashes remain unchanged during
  `.cellar/rpc/native-decode-recheck-v2-*`. Manifest SHA-256 is
  `efcf4eaa37d484a9e95816afc12f6945e07a68094591be7348a9c1ed25d25cc1`;
  full-report hashes are
  `efa2d370ce0cedf4716cd68fb7fbd84ed026a27f0f63ee7dcd2ae50212473245`
  (GIL) and
  `82489e3f862093100c047da6ddf19911330a7d6eef1a094d6c4b34017ed1677d`
  (free-threaded). Executable hashes are
  `27435ae939952752d4964c5dc4778d511c80547d88818663256769f31e34825a`
  and `3cc43de4c52d7174646e142da9de89c8c187c16fb5cc059711a19ce2367f8c1a`.

  A separate 60-sample observation of the concurrent correctness runs records
  process-tree RSS ranges of 169,744–368,416 KiB (GIL) and 168,016–376,624 KiB
  (free-threaded). Shared pages can be counted more than once and sampling can
  miss transients. This is unisolated diagnostic data, not allocator accounting,
  a strict peak bound or performance qualification. Its SHA-256 is
  `6b8042d50e00e0fb9c05206f18943276569084b08aaa0e9753505b509b5d72db`.

  Post-run review adds explicit zero-decoder-reservation checks after local
  restarts and forced owner fencing; successful fleet shutdown already checks
  that boundary. All **19 final cleanup checks pass**: both actual modes run the
  16-pair local matrix through two native starts and all **32 forced owner-fence
  cases**, with native decoder usage zero after each observed join. Both modes'
  41 guards, all-target qualifier Clippy and standard checks pass again.
  All 3,741 watched inputs and both binaries stay unchanged during
  `.cellar/rpc/native-decode-cleanup-v3-*`; manifest SHA-256 is
  `ed20d9ac28d5b9c79267069fde3694625323bc26e6d54a26ed4a0383d58ffac3`.
  Final qualifier binary hashes are
  `99ae91af3c6300bc4e703f1b3a358ca2dbdfc607c1d0315f3b5dcedfea1ef7ab`
  and `1cad6c7b95f5bb0562d80eb52ad457fe047c6e91a54ae5df1be263e5c60749b7`.
  Only the two qualification cleanup assertions and ledger differ from the
  full-driver snapshot; production code is byte-for-byte unchanged. Both hosted
  SDK interpreter jobs pass at `4cfee52c`; its separate GIL profile lease expiry
  and the three local JavaScript timing failures remain unexplained.
  This allowance covers standard generated Prost layouts, not custom
  `Message` implementations, application-retained graphs, all response pools,
  allocator backing copies or process RSS; full resource qualification remains
  open.

- **Hosted GIL profile failure retained; free-threaded driver independently audited.**
  At `4cfee52c`, typed-RPC run **40592333578** fails its GIL profile with both
  nodes reporting `NodeLeaseExpired` during the V8-to-V8
  `opposite-boundaries` call. Both bridges join with zero reservations. Artifact
  SHA-256 is `7c3dc380c188760810081158694448e8f3f6fa21e8530f59f2dad68f2555ea5f`.
  The failure contains no lease attempt timings: the qualifier had not installed
  a tracing subscriber. The qualifier now installs it before every suite; CI
  enables existing core lease start/completion records without changing budgets.
  This is diagnostic coverage, not a fix or an explanation for the failure.

  The same run's free-threaded job passes the profile, lifecycle and complete
  19-suite driver. Its actual loaded-mode, provenance and peer-fault reports pass
  the independent auditors locally; full-report SHA-256 is
  `97ff7fd5bb86b97505c39b8bc95962d280942aabea2c0bf042ec1326eef2bfd9`.
  Artifacts and original logs are preserved under
  `.cellar/rpc/diagnostics/hosted-4cfee52c-profile-v1/`. Earlier timing failures
  remain open; passing another mode does not explain the GIL failure.

- **Typed response ceilings reach native routing and peer collection.** The
  real signed-peer relay first observes the owner's proven count/receipt, then
  replaces the reply with an oversized declared body and withholds its bytes.
  Before the fix, the generated Rust caller waits until its original deadline
  and returns `DeadlineExceeded/unknown`, instead of refusing the declaration.
  The preserved red executable SHA-256 is
  `2d4c83eed153253888f84da7a923bbd6d63c4e9525fb8d4b1529996d3c44e641`;
  source, binary, lockfile and log are under
  `.cellar/rpc/diagnostics/typed-reply-cap-red-v1/`.

  Native typed clients now attach their immutable response ceiling to the
  existing shared admission reservation. It survives gates, queues, routing
  and clones without changing public request types or wire formats. Peer
  collection checks declared size before polling and streamed size before
  buffer growth. Local results are checked before publication to the caller;
  typed methods reject leased resources before receiving their transport.
  The ceiling includes the business allowance plus separate 1 KiB Protobuf
  and clone controls. Rejection is `InvalidResponse/unknown`, with no network
  replay tag; generic untyped allowances and deadlines stay unchanged.

  The expanded **112-case signed-peer suite** passes initially with GIL
  CPython. It adds both declared and chunked reply overflow with withheld ends
  across all 16 generated caller/provider pairs. Every case preserves the
  committed count, performs no automatic retry, returns the saved receipt on
  two explicit retries, rejects changed commands and permits fresh reads.
  Both nodes drain, all relay connections/tasks join and the independent
  auditor rejects twelve corrupt reports.

  All **41 frozen checks pass** for the production change: **725 core tests**,
  **23 native integrations** (including large untyped values and SQL), **10
  JavaScript integrations**, Python's 31 unit tests and streaming/typed-RPC
  integrations. Both actual Python modes pass 40 qualifier guards, all-target
  qualifier Clippy, the **576-call profile**, **112-case provenance**, expanded
  **112-case peer-fault** and **112-case saturation** suites with independent
  mode/result audits. Core/JavaScript all-target Clippy, Node-only checking,
  format/layout (1,242 Rust files), dependency and documentation checks pass.
  Original handler budgets stay unchanged. Earlier timeout failures remain
  preserved; these passing checks do not establish their cause or fix.

  All **3,732 watched source/fixture inputs** and both executable hashes remain
  unchanged during `.cellar/rpc/typed-reply-cap-checks-v1-*`. The manifest
  SHA-256 is `998ff32d35ea63525538ad4a2eb01f73a644c07d73e71a7a003cab24f8418d16`.
  Qualification executable SHA-256 values are
  `20e0b96088dc4fc8eee43f0c97d36fee9d14ba87338e61befa4452f1cc1f040a`
  (GIL) and
  `60fe8d80256ca9a675010b512c71e0d34d24bd73b68b44b78c039a19d81f9620`
  (free-threaded). Peer-report SHA-256 values are
  `796af1c5057a364faa8d099e43c0ce3c1ff5fec6eed2de923e37965d0db85179`
  and `d94611bdbbeef559f7a26e478774499fb6034111b6f92834a8166cbea1ec4f73`;
  saturation-report values are
  `fc35dbcd1133925ecaf58d7509341af9a6ad06e86e515071d3b4904a9c920f11`
  and `9a943a97148fe46bb64059dc0061167687060a9000c876ceae181327ccd58215`.
  Profile/provenance hashes match the earlier tables. A subsequent test-only
  addition checks real HTTP lease cleanup when a response is refused at its
  header. All **five lease tests** pass, including unconsumed-body refusal,
  caller drop, server revocation and domain shutdown; both HTTP drivers join.
  Core all-target Clippy passes again. Only that test file and this ledger
  differ from the frozen source; production code remains byte-for-byte equal.

  This closes neither aggregate response/codec pools nor native inline-layout,
  retained backing/copy/RSS and complete method/runtime/host minima evidence.

- **Ordinary peer replies now use dense, finite and cooperative accumulation.**
  Three main-lock regressions fail against the real ordinary response path:
  a ready-frame flood consumes its entire body in one poll, an oversized
  declared body is polled/accepted, and 1,024 one-byte fragments retain all
  1,024 frame owners. The preserved red executable SHA-256 is
  `e3a46275f7515b7a21d59e78dee6a4c5a56ba94408cc2440d840ea35f4a38533`;
  exact source, lock, binary and logs are under
  `.cellar/rpc/diagnostics/ordinary-reply-red-v3/`. An initial command selected
  the physical rather than logical Rust module path and ran zero tests; it is
  not counted as passing evidence. The corrected filter reproduces the failures.

  Ordinary replies now share the dense accumulator and its 64-frame/64-KiB
  cooperation quantum. Their existing generic forwarding allowance remains
  larger than the separate 64 MiB event-control ceiling. Protocol/allocation
  refusal is `ResourceExhausted/unknown` and receives no transport-retry tag;
  event failures retain their original ambiguous operation outcome, and actual
  body I/O failures keep their transport provenance. Fragment ownership falls
  from 1,024 to one; owned fragments are released after dense copying.

  All **722 core tests**, including the **22 collector cases**, pass together.
  Expanded tests cover ordinary chunk overflow/suffix refusal, actual Hyper
  oversized/truncated responses and an untyped value larger than the 64 MiB
  event ceiling. One compatibility fixture initially used 32 MiB; its assertion
  caught that setup error. The corrected input retains the same assertion.
  Typed-RPC CI includes the owning collector suite.

  The frozen continuation records **42 checks: 39 pass, three fail**. Both
  actual Python modes pass 40 qualifier guards, all-target qualifier Clippy,
  the 576-call profile, 112-case provenance, 80-case peer-fault and 112-case
  saturation suites, plus their independent mode/result audits. Fifteen
  remaining native RPC tests, Python's 31 unit tests and streaming/typed-RPC
  integrations, JavaScript all-target Clippy, Node-only checking and standard
  format/layout/dependency/docs checks also pass. All **3,733 watched inputs**
  and both preserved executables stay unchanged. The source manifest SHA-256 is
  `5ee21ffff111c5d3e2fba096757b3bf6ce92703f52e99845288402bebb735825`.

  The original native provenance request `/refusal` reaches its unchanged
  **250 ms** handler budget. The continued JavaScript command passes Node
  compatibility, both streaming backends and stale-codec refusal, then reaches
  that same budget at
  `/v8/codec-budget?target=rust&phase=verify&id=codec-budget-v8`.
  Cargo stops before the later Workers-module target. Both failures remain
  recorded; the continuation deliberately preserves the original native
  failure instead of rerunning it to obtain a green status. The third failure
  is Clippy's byte-character-slice warning in the new fragment-owner fixture;
  its equivalent byte-string spelling is corrected separately. The corrected
  source passes all 22 collector tests and core all-target Clippy. The separate
  Workers-module command also passes all five integrations; it had not run in
  the failed combined JavaScript command. Logs, exact
  sources and failing executables are retained under
  `.cellar/rpc/diagnostics/ordinary-reply-checks-v1-failure/` and
  `.cellar/rpc/diagnostics/ordinary-reply-javascript-timeout-v1/`.

  A separate temporary phase observer passes ten serial and twenty concurrent
  native provenance samples. Those samples neither reproduce nor fix the
  original timeout; the largest observed local routing interval is 50 ms.
  Temporary observer edits were removed, and all original frozen source
  hashes were restored before the continuation. Native wake completion removes
  its operation before polling the handler again; inspection alone does not
  establish a lost notification. The timeout cause remains open.

  This closes the demonstrated collector mechanisms, not aggregate response
  pools, per-method early receive caps, allocator/backing capacity or RSS.

  Hosted [typed-RPC workflow 40576152667](https://github.com/crabbuild/cellar/actions/runs/40576152667)
  passes at the preceding reply-copy head `dda440e4`. Both `3.14` and `3.14t`
  jobs pass fixture/static checks, the portable profile, owned lifecycle suites
  and the complete generated RPC/failure driver. Python examples also pass at
  that head. These are hosted results for the reply-copy change, not the later
  collector source or an explanation of the local handler timeouts.

- **Native inline layouts need an additional allocation boundary.** A bounded
  diagnostic constructs 128 empty messages whose absent optional child type
  declares 512 strings. The 256-byte wire message correctly uses 386 logical
  slots and passes that exact shared slot budget; 385 slots refuses it. Prost's
  inline `Option<Wide>` still makes each outer Rust value 12,288 bytes. Decoding
  allocates **1,572,864 Layout bytes**, even though every child is absent.
  Boxing the absent child in the diagnostic reduces this to **1,024 bytes**.

  The shared slot profile deliberately does not recursively initialize absent
  ordinary message fields; this is evidence that its estimate cannot qualify
  native ABI allocations. No generated API or profile was changed. A native
  allocation/heap boundary remains required alongside the language-neutral
  slot check. Source, binary, separate lockfile and report are preserved under
  `.cellar/rpc/diagnostics/native-inline-layout-v1/`; report SHA-256 is
  `5fe77bc7d178d536e4cf802a3aa85d3bb456f494c4b8838e0ec83b102dc8b03c`.
  This diagnostic is not a main-lock regression or a process-RSS qualification.

- **Both frozen diagnostic full drivers pass without explaining earlier failures.**
  At `fd89b1b4`, both actual Python modes complete all 19 suites, totaling
  **952 cases per mode**. GIL CPython 3.14.6/upb takes 1,427.615 s;
  free-threaded CPython 3.14.5/python takes 1,414.036 s. All twelve recorded
  full-run/auditor commands pass, including mode, provenance, signed-peer,
  saturation and guest-control audits. Runtimes join and bridge/native cleanup
  assertions remain required by the suites. The interpreter, actual GIL state
  and pinned Protobuf 7.34.2 backend match before and after each complete run.

  All **3,730 watched source/fixture hashes** and both diagnostic executable
  hashes remain unchanged. The manifest SHA-256 is
  `9d9ee3e6443e9b53b2ab9d1791beaafd47df512bfcb580ca85bd70b809dc0e36`.
  Full report SHA-256 values are
  `fbc426410c1a16947f540d97cf2bd525f11e14f6d97744600051b2236247d036`
  (GIL) and
  `4294c0e0b370d19ba730f650cb14c3f4ff159301a05dd19f01b02bd75e6dab68`
  (free-threaded). Commands, logs and audits are retained under
  `.cellar/rpc/owner-observer-full-v1-*`; executable identities match the table
  below. Short standalone probe build/link steps overlap this concurrent run;
  it is not an isolated performance baseline. These passing repetitions do not
  establish a cause or fix for the original warmup failures, and precede the
  reply-copy change described next.

- **Framework reply conversion removes two temporary business-sized buffers.**
  Under the main workspace lockfile, the new native allocation regression
  reproduces four failures: successful conversion holds extra copies; wrong
  digests and malformed controls copy the body before refusal; oversized
  business replies and failure text allocate before rejection. The separate
  non-minimal-varint/outcome case passes before the fix. The preserved red
  executable SHA-256 is
  `c31f0404b469b83c1432392297566509c02816bfd3569b4b8654c58b26fd6485`;
  source, lockfile, executable and logs are retained under
  `.cellar/rpc/diagnostics/reply-allocation-main-red-v1/`.

  The decoder now borrows the exact envelope, verifies digest/control wire and
  business size, then copies only the successful body. Large failure controls
  are rejected before their string codec runs. All **five allocation guards**
  pass with the fix: success peaks at no more than one body plus 1 KiB of fixed
  slack; invalid/digest/size failures stay within that fixed slack. Cases cover
  empty through 1 MiB bodies, seven clone versions, non-minimal varints and both
  accepted effect outcomes. Invalid responses retain unknown effects. The
  already-owned input, application decoder layouts and allocator-internal
  backing/RSS are outside this measurement. Typed-RPC CI runs this guard.

  The isolated pre-fix probe measures **3,145,796 new allocator Layout bytes**
  for a 1 MiB successful body, excluding input. Inspection of pinned Prost
  0.14.4 confirms its intermediate bytes copy in addition to the outer wire
  copy. A **5,957-case** frozen private-reply corpus is retained for comparison.
  All **31 recorded checks** pass for the fix: 716 core tests, 17 selected native
  tests, 10 selected JavaScript integrations, Python's 31 unit tests and
  streaming/typed-RPC integrations; both Python modes' 40 qualifier guards,
  all-target qualifier Clippy, **576-call profile** and **112-case provenance**
  matrices with independent mode/profile/provenance audits. Core/JavaScript
  all-target Clippy, Node-only checking, format/layout across 1,242 Rust files,
  815 documentation references and the 466-package native engine boundary pass.

  A supplemental probe observes peak new layouts for a 1 MiB result fall from
  **3,145,796 to 1,048,640 bytes**. Wrong-digest and duplicate-control replies
  fall from 1,048,644 / 1,048,718 to **192 bytes** each. Its 12 allocation cases
  pass, and all **5,957 semantic corpus cases match exactly**, with unchanged
  corpus SHA-256
  `98c428bc6b6553cc98aad018a6d7150571f1c0ee1a0f7cc8179a3a13c083ef7b`.
  This supplemental executable has a separate lockfile; the five owning guards
  above also pass under the main workspace lockfile. The supplemental allocation
  report SHA-256 is
  `241a7aad89b087bd3305cacef18b10d09c4d59883c594ac2277c6b2db7ea8dbe`.

  All **3,738 source/fixture/probe inputs** and both preserved qualification
  executables remain unchanged across `.cellar/rpc/reply-copy-checks-v1-*`.
  This includes seven explicitly watched probe, collector and runner inputs.
  Manifest SHA-256:
  `dc0b0445134851673bffed66609a190934b76af0ab6fd6fd0847e95a864d2456`.
  Executable SHA-256 values are
  `b98ed0aa97050b357b87fc0e4f59780d81598eae7bf5f9b97a02594f38a6d6be`
  (GIL) and
  `9efd3ae43ebd527dd6558f1d8a2ce30f5250b9fd6aee9b557146a658fd7bf637`
  (free-threaded). Profile and provenance report hashes match the earlier
  tables. The full-driver pass above qualifies the preceding diagnostic source;
  current full-driver/hosted qualification remains separate.

  Aggregate response/codec pools remain P0. This earlier source still used
  `body.collect()` for ordinary peer replies. The subsequent collector slice
  above reproduces and closes its demonstrated finite/dense/cooperative
  accumulation gaps while retaining the broader resource work.

- **Original full-driver warmup failures remain unexplained.**
  At `8f8c93d3`, the GIL run closes its HTTP connection during
  `recovery/replaced_root/python->rust` warmup; the free-threaded run receives
  connection refusal during `proof/fence_sync/rust->rust` warmup. Neither has
  yet applied the selected recovery fault or proof fence. Both terminate with
  host failure and zero reported Python tasks, objects, threads, native
  invocations, pending operations, bridge bytes and bridge reservations. Those
  cleanup counters do not identify the underlying owner failure.

  The modes run concurrently; a separate diagnostic Cargo build overlaps part
  of the run. Original deadlines remain unchanged. GIL exits after 672.385 s;
  free-threaded exits after 1,008.161 s. No complete success report is published.
  All 3,730 watched source/fixture hashes and both preserved executable hashes
  remain unchanged. The manifest SHA-256 is
  `8d931857b24bf9517461bb6e437826c03e3ae47d6cbc72a401c345400d22b066`.
  Logs and run metadata are preserved under
  `.cellar/rpc/diagnostics/raw-provenance-full-v1/`.

  Qualification diagnostics now observe the isolated owner's exit status and
  at most 16 KiB of its log before process/temp-directory cleanup. Embedded
  proof owners instead report domain closure, a nonblocking node-outcome
  observation and admission counters. This changes neither deadlines nor
  replay behavior. Both diagnostic executables pass 40 qualifier guards,
  all-target Clippy, 32 recovery cases and 64 proof cases, with mode audits,
  joined runtimes and zero bridge reservations. The complete source/executable
  hash audit passes. These focused repetitions do not establish a cause or fix
  for the full-driver failures.

  | Preserved artifact SHA-256 | GIL | Free-threaded |
  | --- | --- | --- |
  | Original failing full-run log | `ed6d64a5ba9ab393026ae8e3f99f26b2a34e7f6d5ad48f7df31437032de99036` | `def17d74b4ffc74ca095841c46f6da3e07052115b5cda1f30cbfec09899e9ff0` |
  | Diagnostic executable | `1dc8bd1588a37fd1599735b59c3deb274b1007c3521d414632c2bf40e4ca942a` | `2849bc0eb93e4861bbace45ceb3df931709f9f95aa24edefe123abf6f749f271` |
  | Diagnostic recovery report | `ab8907a986011a82a55995ef0642760dbfbcfd258cd838366822f9d0255f337e` | `935cf87afe86ff93be1daf8a247deeffcbef5001a6d2847119f4658b6987f631` |
  | Diagnostic proof report | `df4a6546a877f3297ecd536241ab058eb72072490835a64407c38ae832d8774d` | `ba98aee9bcbc3cbb5976c9ccf3e0063c17fa2b2684ec8f3b3c3905c6e36aff6f` |

  The diagnostic manifest SHA-256 is
  `3605ece9b831e4089ffefb262eb5d0a3f3dc6de20b76113e56f27a2ef2eb6a41`;
  commands, timings and reports are under `.cellar/rpc/owner-observer-v1-*`.
  Separately, the complete [JavaScript/Node SDK workflow at this production
  head](https://github.com/crabbuild/cellar/actions/runs/40561669107) passes.
  This is hosted regression evidence, not an explanation of earlier timing
  failures or a full typed-RPC qualification.

- **Held local replies need separate response/codec accounting.**
  An isolated native-host probe submits four generated typed calls with tiny
  requests, releases their providers, and holds result consumers before reply
  conversion. It observes 2,097,612 bytes of encoded response payload retained
  inside the queued results, four source requests and 264,920 reserved request
  bytes, while receiving-peer counters remain zero. Both consuming those
  results and cancelling the unconsumed futures release all observed payload
  ownership and source reservations; fresh calls progress and nodes drain.

  RuntimePolicy documents a request allowance, so this is not a violation of
  that allowance. It identifies a separate response/codec qualification seam.
  The ignored standalone probe has its own lockfile and is diagnostic evidence,
  not the main locked build, allocator-capacity, decoded-graph or RSS proof.
  Its source, executable, command and report are retained in
  `.cellar/rpc/local-response-probe/`; report SHA-256 is
  `24154d6f47a7f3f058e264a23d7df648eb9eaa7648479231440f243643c643be`.
  Broader local/peer response budgets and effective minima remain P0 work.

- **Raw native replies cannot forge rollback proof after application entry.**
  A real native cell commits and syncs its count/receipt, then bypasses
  `Prepared::finish` by returning either a fabricated typed failure or an
  honest child's raw host refusal. Before this fix, both reach the generated
  caller as `MethodNotFound/not_committed`. The standalone regression observes
  committed state, reopens the node, and obtains saved receipt results on
  explicit retries before asserting the false outcome. Both failing executions
  are preserved; the second original executable SHA-256 is
  `9cf868ac8b2871f55b8e0afc180c404f6afcc2d156708dfbc76ccbb214495c38`.

  The host captures whether the original request uses the exact typed envelope
  before application ownership transfers. An entered cell/application's typed
  failure retains its fixed code but receives unknown effects. Only the host's
  pre-factory catalog refusal bypasses that policy. Constructors and custom
  wrappers cannot borrow a child's proof; ordinary untyped results retain their
  original behavior. Output gates, deadlines and durability policies stay intact.

  Successful response bodies are borrowed and keep their original allocation.
  Only a valid failure within the fixed 1 KiB control allowance is decoded and
  rewritten. Four owning unit tests cover all twelve failure codes, seven clone
  versions, canonical/non-minimal varints, malformed/duplicate controls,
  oversized failure text and unchanged buffer ownership through 1 MiB. Invalid
  controls remain invalid responses with unknown effects. The live native
  restart/receipt regression now passes, including genuine pre-factory refusal.

  The provenance suite preserves its original 96 cases and adds **16 raw native
  cases**: four generated callers, local/signed-peer routes and forged/forwarded
  envelopes. Two dedicated raw-provider objects are warmed before the source
  joins; the resulting six factories remain constant during calls. Host audit
  verifies caller node, owner, scope, epoch, actual committed receipt, two safe
  explicit retries and exactly three Increment entries. An initial expanded
  fixture incorrectly warmed objects after both nodes joined, and correctly
  failed its owner assertion. That failed run is retained; the correction fixes
  setup placement without relaxing route/authority assertions.

  Both GIL CPython 3.14.6/upb and free-threaded CPython 3.14.5/python pass all
  **112 provenance cases**, the **576-call profile**, **40 qualifier guards** and
  all-target qualifier Clippy. The independent provenance auditor rejects **20
  corrupted reports**; the existing mode/profile auditors retain their own
  negative probes. Every report requires joined language runtimes and zero
  bridge reservations. Typed-RPC CI includes the native regression and audits
  provenance within its full combined report.

  The same source passes all **716 core unit tests**, **12 selected native
  cell/typed-RPC/profile tests**, **10 selected JavaScript integrations**
  (streaming, typed RPC, Node and Workers module), Python's 31 unit tests and its
  streaming/typed-RPC integrations. Core/JavaScript all-target Clippy, Node-only
  checking and standard checks across 1,241 Rust files pass. The updated JS
  fixture build and strict TypeScript check also pass. This is affected-boundary
  evidence; the complete failure drivers and hosted gates remain separate.

  All **3,730 source/fixture hashes** and preserved executables remain unchanged
  through the 29 checks in `.cellar/rpc/raw-provenance-checks-v1-*`. The source
  manifest SHA-256 is
  `0f67229461c2c1fc925523fbc7faf383c726e5910091479f961a1b10d32e595d`.

  | Artifact | GIL | Free-threaded |
  | --- | --- | --- |
  | Executable SHA-256 | `822b44528235a6248e665d59e8aac1caa314bf3edfe7b5fdf49d11ad7ecefc89` | `8693f134ffb22d3b9fff4ea8d82f62d1ef194579f56ddc2d88317033498241a4` |
  | Provenance report SHA-256 | `d1ca9eeb7b9631bf313c832442b6348fa24fcaeb4f8812a16d5779ba76b4e824` | `ff2d8d07f4788caae9196a5c2d7079d37debd5fe3905e7106b76294e9ccc4e44` |

  The profile report hashes match the earlier local-mode table. Evidence for
  the failing/passing native executions and placement fixture is under
  `.cellar/rpc/diagnostics/raw-provenance-red-v2/` and `raw-provenance-smoke-v{1,2}-*`.
  Earlier V8/hosted snapshot failures, broader peer/admission and retained-copy/RSS
  bounds, descriptor semantics, packaging, diagnostics and performance remain
  open. No full-qualification flag is set by these selected checks.

- **Native writer admission no longer serializes post-commit checkpoint I/O.**
  A private, named test VFS pauses the actual main-database `xSync` callback
  during an unrelated put. The unmodified backend fails the lease CAS's
  original **333 ms** attempt window in three fresh executions. SQLite has
  committed the transaction and permits other writers, but Cellar's fair writer
  mutex remains held through the automatic checkpoint. This is a controlled
  reproduction at the actual I/O boundary, beyond passing retries or a sleep
  inside a synthetic transport. FULL WAL sync callbacks are not intercepted.

  Put/CAS now release only the fair admission guard immediately before COMMIT;
  SQLite retains its transaction write lock and atomic ETag check. Delete uses
  an explicit immediate transaction with the same handoff. Pre-commit failures
  still roll back/close before returning their admission guard. No checkpoint,
  FULL synchronization, lease, handler, proof or shutdown policy is weakened,
  and no new background task or automatic retry is introduced.

  The regression passes with main-file checkpoint I/O held for either put or
  delete: a following lease CAS finishes in its original window and a separate
  connection reads the committed value before the checkpoint is released. The
  held put keeps its native I/O reservation until completion, then returns it.
  A watchdog prevents test deadlock and cannot establish success; native work
  joins before the private VFS registration and directory retire. The fixture
  is compiled only for native Unix unit tests, outside the external internal
  harness. Focused typed-RPC CI now selects the local-store unit suite.

  A public bucket regression issues **32 same-token metadata CAS contenders**,
  requires exactly one winner and verifies its body/token, stale-token refusal
  and a subsequent valid update after reopen. This checks metadata atomicity;
  the separate Counter matrices remain the business-receipt evidence.

  Before transfer onto main `a9da48ac`, all **21 local-store unit tests**, **15
  public storage/RPC/profile tests**, **40 qualifier guards**, all-target core
  and qualifier Clippy and format/layout/dependency/docs checks pass. The same
  store implementation and regression sources transfer without conflicts over
  main's separate extent-archive change. Broader results and source identities
  are recorded separately.

  The original native target also passes all **93 tests** before this fix, and
  temporary phase probes pass **60** fresh-fixture repetitions in two processes.
  Their slowest observed commit is about 40 ms; they do not reproduce the
  earlier hosted expiry. The probes are removed from tracked source. These
  passes and the controlled checkpoint fix do not establish the precise phase
  of the old CI stalls, resolve the earlier 250 ms failures or qualify broader
  resource/RSS, schema, packaging, diagnostics and performance boundaries.

- **Checkpoint source on updated main, with failures retained.** On main
  `a9da48ac` plus the checkpoint patch, all **712 core unit tests** and **108
  native tests** pass: five local-store, 93 node, three typed-RPC and seven
  native-profile cases. The initial all-feature JavaScript command then stops
  with **79 passed / one failed**: the named-container follow-up test observes
  a disconnected invocation channel. The original helper's panic hides the
  engine's returned failure; it does not establish whether the engine failed
  or only that invocation retired.

  Test diagnostics now propagate the channel failure with the expected fixed
  operation, invocation liveness and bridge counters. They preserve the same
  five-second observation bound and all assertions. No production engine,
  handler, startup, operation or proof budget changes. Twenty fresh exact
  executions and three whole-library executions of the original binary pass.
  Temporary probes also pass with each asynchronous native reply delayed
  150 ms, beyond the unchanged 100 ms engine turn budget. They do not reproduce
  or diagnose the original failure, and are removed from tracked source.

  The resulting `--all-features --no-fail-fast` JavaScript command passes **123
  tests**, with six external tests ignored. Python's 31 library tests and its
  startup, bridge, streaming and typed-RPC integration executables pass; the
  bridge includes real owner-kill/watchdog and acknowledged-state recovery.
  Both rebuilt interpreter modes pass **40 qualifier guards**, all-target
  qualifier Clippy and the **576-call profile**, with independent profile and
  loaded-mode audits. Core/JavaScript all-target Clippy, Node-only checking,
  formatting/layout across 1,236 Rust files, the 466-package engine-free native
  guard and all 815 local documentation references pass.

  The first run's **3,724 source/fixture hashes** stay unchanged, with source
  manifest SHA-256
  `7c92691d381b34befc8b76fbb24399dd35ee3aee3992046efad2dad25fab13ab`.
  The resumed run changes only `crates/cellar-javascript/runtime_tests.rs`;
  every watched source and preserved executable stays unchanged during its
  21 checks. Its source manifest SHA-256 is
  `bb9647780722565cc3d135bce3141440600d8391cf9eec27dc22368c00846c8c`.
  Evidence uses `.cellar/rpc/checkpoint-main-v1-*` and
  `checkpoint-main-resume2-*`; the profile report hashes match the earlier
  local-mode table below. Rebuilt executable SHA-256 values are
  `be9cb68228ce465545ffb68b76e0535a73118ea45df88a6285f4c802b41759f2`
  (GIL) and `754105761dfaabab19e0375dcde9025f0b4ab1af7ac2d54be7e71e27356e8602`
  (free-threaded). The full Counter/failure driver is not repeated against this
  checkpoint source; its earlier local and hosted identities stay separate.

- **Hosted Python modes pass; the complete SDK gate remains open.** Head
  `dcb66fd2` passes both
  [typed-RPC CI jobs](https://github.com/crabbuild/cellar/actions/runs/40534684195),
  including the standalone lifecycle/fault suites, the full 19-suite driver
  and the separate 576-call profile. The reports observe GIL CPython 3.14.8/upb
  and free-threaded CPython 3.14.10/python, with Protobuf 7.34.2. Both uploaded
  evidence ZIPs are downloaded and checked against GitHub's SHA-256 digests;
  the current independent mode/profile/peer/saturation/control auditors accept
  their reports and reject their negative probes.

  | Hosted artifact | GIL | Free-threaded |
  | --- | --- | --- |
  | Artifact ID | `12752972533` | `12753209984` |
  | ZIP SHA-256 | `575497458329ae6fd34324ae1eae8f23fb8ddbe98fd1430e35dee68ca7b452f6` | `3feee142a407107ac54cee748a39f6c177b5984660548e3c885a4c3a7db6aec9` |
  | All-suite report SHA-256 | `dbfea389e8edda36ad9839ea4fcc7ac5dfb7be6f12f2c138372bdca55c92145b` | `53e888f5dd8cdf1886ec79ee6544eeb0b408c8a7cc306904e907d32acadae9ba` |
  | Profile report SHA-256 | `72fe39aabf4288376cf2cee27efa1e5d4a668cda70443bd7bca9bfe842c3275e` | `e71b53b420fcf4ed60e5aa466345302613a5f6f054549963d2ed1af97b79c77e` |

  The same head's SDK GIL job passes. Its
  [SDK free-threaded job](https://github.com/crabbuild/cellar/actions/runs/40534684215/job/122296620494)
  fails `native_s3_seed_crash_matrix`: the child returns
  `NotLeader { leader: None }` on its initial S3 command before the
  after-attachment cut. JavaScript CI is cancelled. These results precede the
  checkpoint fix. The exact S3 seed crash matrix passes on the checkpoint
  source in **29.738 seconds**, from preserved executable SHA-256
  `e0985ee80563e21043768017e32fa4127b5324be953fa52dd9eadba7fec0feaa`.
  All watched sources still match the resumed manifest. This one local pass
  on updated main does not diagnose or close the hosted failure. Evidence is
  retained under `.cellar/rpc/diagnostics/python-mode-ci-40534684195/` and
  `checkpoint-native-seed-v1/`. Earlier timing failures, remaining provenance,
  resource/RSS, descriptor, packaging, diagnostics and performance work stay
  open; qualification flags remain false.

- **Loaded Python modes across the existing full driver.** Separate native
  executables pass `--suite all` and `--suite profile` with GIL CPython 3.14.6
  (Protobuf 7.34.2/upb) and free-threaded CPython 3.14.5 (Protobuf 7.34.2/python).
  Each mode passes all **19 Counter/failure suites**: 904 explicit cases plus
  sixteen local and sixteen peer pairs with restart/handoff/concurrency checks.
  The separate **576-call profile** exercises every generated Rust/Python/V8
  TypeScript/Node JavaScript pair over local and signed-peer routes. The combined
  runs take 1,394.136 seconds (GIL) and 1,413.645 seconds (free-threaded).

  `--python-mode gil|free-threaded` supplies an explicit expectation; its default
  follows the compiled interpreter. The host observes its actual owned CPython
  after application/codec imports, then again after the matrix. Both observations
  require CPython 3.14, consistent SOABI/free-threaded/GIL state, the pinned codec
  and unchanged identity. No subprocess supplies these facts. Reports publish
  only after Python, V8 and Node join with zero retained bridge reservations.
  Private crash/supervision children record startup only and cannot pass a
  completed-matrix mode audit.

  The independent mode auditor rejects **20 corrupted reports**, separately from
  case coverage. Its boolean negative probe initially exposed integer `1`
  passing an equality check; identity checking fixes that auditor bug. An old
  report without runtime facts is refused. Both actual mismatched-mode probes
  fail before either JS bridge starts, leave zero native work and remove a
  deliberately stale success report. The profile/peer/saturation/control audits
  reject **22/10/17/19** corrupted reports under each mode. All **40 qualifier
  guards**, qualifier all-target Clippy and format/layout/dependency/docs checks
  pass for both builds. The first free-threaded guard command fails before test
  entry because the downloaded macOS library has a relative install name and
  Cargo changes the test working directory. Repeating with its absolute library
  directory in `DYLD_FALLBACK_LIBRARY_PATH` passes all guards and remaining
  checks; the failed loader log is retained. Runtime and product budgets do not
  change.

  All **3,697 watched source/fixture hashes** and each preserved executable stay
  unchanged during the two live runs and their checks. The common source
  manifest SHA-256 is
  `abb0d3d6632c98a5df8af9b09144c269f10665be90b8c91562daebe7a971aea9`.
  This evidence belongs to the Python-mode patch on `e60b3fa8`, before transfer
  onto main `784edc1b`; post-transfer checks are recorded separately.

  | Artifact | GIL | Free-threaded |
  | --- | --- | --- |
  | Executable SHA-256 | `38b4cdbe5abae5b13b299e0ff2be60896030e6ce7e7820666039b25d01ed84ab` | `b36d06e0c46542a8d5d4c6a5589449dd7259d029e9b8b2b94d8f9faa971cb248` |
  | All-suite report SHA-256 | `c754ce7a1b183c7dffc3bbb160c2c59bb2221a78a3fbd3b8442228bbbc0e443b` | `fc41357f25227b1b4cc610c2263b58325be31828a60ae20f2c77b50c2d86b97b` |
  | Profile report SHA-256 | `4c88ecd090a79771d5116b4b2ed513ba0ef1408334015221f7a129cb0b808bd8` | `9976bd90301693cddfaf1caf0d31e671ada86ca246b04664e925c4ed29a0b39a` |

  After transfer onto main `784edc1b`, both rebuilt variants pass **40 guards**,
  all-target qualifier Clippy and the **576-call profile** with both independent
  auditors. The three native typed-RPC tests and seven generated native-profile
  tests pass with original budgets. Format/layout check 1,233 Rust files; the
  native dependency guard finds 466 reachable packages and no JavaScript engine;
  docs validate 815 local references. All **3,721 watched hashes** stay unchanged
  through `.cellar/rpc/python-mode-main-v1-*`. Its source manifest SHA-256 is
  `3e88666bc9d33f10a148d3ab4258447b517fc4aa9cb8e0fd8585223ca37a27b2`.
  Rebuilt executable SHA-256 values are
  `68a68361c5bbeba840830cb750e77155095c7631b0d0a0a72418b4f459872733`
  (GIL) and `e3a9f62cff56d3ba48ea558aedcfa87621757de21dd97d113f0cf3e96c9b30be`
  (free-threaded). The deterministic profile reports retain the identities in
  the table. The full Counter/failure runs above precede this main transfer;
  they are not claimed as repeated against its separate recovery changes.

  Reports, inventories and source audits use `.cellar/rpc/python-mode-{gil,ft}-v1-*`;
  the loader-corrected checks use `python-mode-ft-resume1-*`. The new Linux CI
  matrix requires both ABIs, the existing standalone suites, the full combined
  driver and the mode auditor. Its job ceiling increases from 45 to 60 minutes
  to accommodate the additional approximately 24-minute driver; individual
  operation, handler, proof and shutdown budgets stay fixed. Remaining hosted
  regressions, descriptor semantics, call-specific refusal provenance, broader
  resource/RSS/flood limits, delivery identity, diagnostics and performance stay
  open. `all_memory_qualified` and `release_qualified` remain false.

- **Fresh hosted lease-write failures remain unresolved.** Published head
  `e60b3fa8` passes the free-threaded SDK job and Python examples. Its
  [typed-RPC profile job](https://github.com/crabbuild/cellar/actions/runs/40470037576/job/122074672353)
  fails at Python-to-Rust/defaults with lease expiry. Its
  [SDK GIL job](https://github.com/crabbuild/cellar/actions/runs/40470037647/job/122074673286)
  fails in the native lost-append/ack-reply test, before its later ack fault.
  All three nodes start renewals with approximately 666 ms of validity; writes
  time out after roughly 334 ms, then successive shorter attempts, while
  readbacks finish in under a millisecond with unchanged generations. They
  share a development-store instance. This narrows the next probe to write
  admission/SQLite/commit progress; it does not establish which phase stalled.
  Lease expiry correctly fences ownership and must not be suppressed.

  The unchanged exact SDK test passes three serial repetitions and twenty
  repetitions in two concurrent processes using executable SHA-256
  `223939e2d88557c4b8644c6feb31a7302c98dd49974a537bb9d2e0d9f3ec71d3`.
  The native 250 ms RPC probe also passes five exact repetitions, five parallel
  whole-target and five serial whole-target runs; temporary phase probes pass
  twenty exact/concurrent and fifteen mixed adapter/native runs. Those probes
  are removed. These successful repeats are not causal fixes. The four earlier
  250 ms failures, newer lease-write failures and full T19 remain open.

- **Bounded native guest RPC controls and measured scratch allowance.** Python,
  embedded V8 and Node share a decoder before native `cell.call` dispatch. It
  rejects raw controls above 16 KiB before parsing or reserving, duplicate and
  unknown fields before consuming their values, and non-flat or invalid fields.
  Direct owned-value calls share shape and serialized-size validation without
  allocating a serialized copy. Decode capacity is acquired before parsing and
  remains owned by the native future until settlement. Local validation returns
  `InvalidArgument/not_committed`; local decode-capacity refusal returns
  `ResourceExhausted/not_committed`. An enclosing routed/proof failure cannot
  inherit that local rollback claim. Ordinary SDK validation retains its codes.

  Four controlled parser regressions fail with the previous generic JSON
  decoder: oversized malformed input reaches parsing, oversized whitespace is
  accepted, duplicate authority replaces the original, and unknown graphs reach
  recursive decoding. A live Python-to-Rust probe also enters business code with
  an unknown control graph before the fix. Python's scoped UTF-8 producer fixes
  a separate red case where ASCII escaping expands a valid Unicode name to
  27,164 bytes; ordinary SDK value encoding stays unchanged.

  A real allocation-layout probe exposes an insufficient two-wire allowance:
  an escaped 1,000-character name peaks at 3,758 bytes against 3,328 reserved;
  a 16,000-character name peaks at 48,758 against 33,328. The public-boundary
  integration regression fails before increasing the allowance to three wire
  lengths plus 1,024 bytes, then passes fourteen escaped/plain cases through
  the exact 16 KiB wire boundary. This measures requested allocation layouts,
  excluding allocator overhead, runtime heaps and RSS. The regression and the
  live suite are wired into CI.

  The final scratch-aware source passes **1,008 selected live cases**: control
  48, profile 576, peer faults 80, saturation 112, provenance 96 and lifecycle 96.
  The control auditor rejects **19** corrupted reports; profile/peer/saturation
  auditors reject 22/10/17. Refusals have no provider visits or factories; fresh
  generated reads verify the real owner/class/scope/epoch and unchanged count.
  Native reservations reach zero after joins. Guest unit/allocation tests,
  all-target guest/qualifier Clippy, format/layout (1,213 Rust files), dependency
  and docs checks pass. All **3,694 watched hashes** and the executable remain
  unchanged during `.cellar/rpc/guest-control-final-v2-*` validation. Control
  report SHA-256 `8795e521ed54817fa58f386046a03bc4409fbd5e876fae8e7176a19a488d098d`;
  manifest `fe188cf1f4e13800dd33e47362086a247623c3d5ad0fcdb01f13c0fe8d36dbe3`;
  executable `d3655df473b99d2631ec248c278411f10193783ba40778edc8fb56588b9d726b`.

  The preceding two-wire source also passes all 1,008 live cases, **135 affected
  library tests**, **38 qualifier guards**, four Python integrations, 23 Python
  codec tests, 66 JS tooling tests, strict TS, all-target adapter Clippy and
  Node-only checking with 3,693 unchanged hashes. Its complete selected regression
  run retains **four 250 ms failures**: V8 restart, Node restart, the JS fixture's
  Rust-to-Rust first read, and standalone native typed RPC. Streaming/WebSockets
  and stale-codec refusal pass. The native command stops after its first failing
  test target, so its separate generated-profile target is not counted as run.
  These failures remain open; the scratch adjustment is not a timing fix.

  Live runs use GIL CPython 3.14.6. An isolated free-threaded CPython 3.14.5
  installation reports the GIL disabled after importing Protobuf 7.34.2 and
  passes 23 codec tests with its pure-Python Protobuf backend. This is setup and
  codec evidence, not native RPC/failure-mode qualification. Wider effective
  limits, copies/allocator/RSS, receiving refusal provenance, descriptor semantics,
  Python modes, packaging, diagnostics, performance and T01–T19 remain open.
  `all_memory_qualified` and `release_qualified` stay false.

- **Live portable Protobuf profile through generated language APIs.** The
  dedicated `--suite profile` process passes **576 real calls**: 18 vectors ×
  all sixteen Rust/Python/V8/Node generated caller/provider pairs × local and
  signed-peer routes. V8 uses generated TypeScript; Node uses independently
  generated JavaScript. All six registered profile methods are exercised.
  The corpus covers scalar bounds, empty/binary/Unicode values, unknown enums,
  packed/unpacked fields, optional and wrapper presence, maps, selected defaults,
  recursive/nested messages, Empty, timestamp/duration bounds and Struct/Value/
  ListValue mappings. Native observations decode and compare actual requests
  and replies against reference wire messages, and verify owner, class, scope,
  epoch and method. Each actual caller independently compares its decoded
  result. This is live evidence, beyond the earlier codec-only vectors.

  All nodes and language runtimes join, and native/bridge reservations reach
  zero. The independent auditor checks exact coverage and rejects **22 corrupt
  reports**. The run finishes in 12.46 seconds; all **3,689 watched source and
  fixture hashes** remain unchanged. Report `.cellar/rpc/live-profile-v2.json`
  SHA-256 `f46b0e58691e983a1be45351a19c2040eb0bc70a4531b60b5f2b95e1a29fdf43`;
  source manifest SHA-256
  `095708bbca830544713e94733e5a6fe95d277913f001c92a7f4fdb248bafa380`.
  The final executable SHA-256 is recorded alongside the manifest. Independent
  JavaScript and protoc Python corpus checks, strict TS fixture build/types,
  **38 qualifier guards** and all-target qualifier Clippy pass. Added native
  observer regressions refuse lost presence/default selection, a lost NaN and
  a lost zero-valued wrapper. All **66 RPC JavaScript tooling tests** pass.
  The updated executable also passes the existing **80 peer-fault and 112
  saturation cases** with their independent auditors and a second unchanged
  3,689-file source/fixture audit. These are 192 Counter regressions, separate
  from the 576 profile calls and the earlier 647-case checkpoint.
  The new CI gate runs the profile in its own process
  because embedded Python permits one initialization per process; the existing
  Counter `--suite all` contract is unchanged.

  The first live attempt refuses the Python module before host admission.
  A temporary tagged import probe identifies an ASCII-locale `UnicodeDecodeError`
  while reading the UTF-8 corpus. A subprocess regression against the actual
  loader reproduces that exact failure with UTF-8 mode and locale coercion
  disabled. Explicit UTF-8 decoding fixes it; the regression and complete live
  matrix pass with the probe removed. Logs retain both red attempts and the
  green run. This fixes artifact reading rather than changing interpreter or
  handler budgets. Current local evidence uses GIL CPython; supported Python
  mode/failure matrices, remaining descriptor semantics, resource/RSS,
  packaging, performance and T01–T19 remain open. Qualification flags stay false.

- **Published reply payloads share receiving byte admission.** Reply-quota
  commit **`3df78956`** passes **647 selected live cases**: saturation 112,
  signed-peer faults 80, provenance 96, dependency cycles 80, deadline 55,
  proof-wait 128 and Node/V8 lifecycle 96. The sixteen added saturation cases
  exercise every Rust/Python/V8/Node generated caller/provider pair. Each
  original provider commits count/receipt state and returns a codec-valid
  65,544-byte message. Publication is refused as `ResourceExhausted/unknown`,
  no automatic replay enters business code, and an explicit same-ID retry
  returns the saved result. All receiving reservations reach zero after join.
  The independent saturation and peer-fault auditors reject **17 and ten**
  corrupt reports respectively. Saturation report SHA-256:
  `4aae1bca8761fc6ea530c794bd7b81eeeb47bb5ed1daa5812a20ffc439a66793`.

  Four initial regressions fail behaviorally without reply charging; six
  focused regressions and all **122 core RPC tests** pass with enforcement.
  Tests cover pre-header refusal, final transport slices, unknown/inaccurate
  body lengths, stopped ownership and the real authenticated tunnel's fixed
  unknown refusal. Disabling only payload charging also fails the live
  generated-client saturation scenario; restoring it passes. Temporary debug
  bypasses are removed. **35 qualifier guards** and **three Node bootstrap
  tests** pass. The deliberately interrupted coordinator was resumed after its
  original processes ended, without repeating completed live cases. All
  **3,659 source/fixture hashes remain unchanged through the final audit**, manifest
  SHA-256 `cc75bd7ed8b26b42d171a024f195985ca019595ad0548b1485416a4fa42c3ab7`.
  The resumed coordinator is complete. All-target Clippy, Node-only checking,
  Rust/JS formatting, layout, dependency/docs checks, strict RPC types, all 18
  selected native integrations and the original SDK stale-receipt regression
  pass. The latter retains its 30-second lease, three-second call budget and
  assertions. The selected JS command retains **six passed, one failed**: V8
  HTTP/restart reaches the existing 250 ms handler budget. Generated RPC, Node
  compatibility, SSE/WebSockets and Workers port WebSockets pass. The complete
  serial Workers module suite passes all five tests on macOS. Python E2B,
  streaming and Vercel integrations pass; typed RPC fails at the same 250 ms
  budget, with zero native/bridge counts after its failed host result and an
  additional SQLite I/O error whose cause remains unproven. These failures remain in the
  raw logs. A ready-triggered external sample reproduces V8 failure plus lease
  fencing but captures only two samples, so it does not establish a cause.
  Reports and resumed exits are under `.cellar/rpc/reply-byte-bound-*` and
  `.cellar/rpc/reply-byte-bound-resume1-*`. Subsequent isolated changes are
  outside this frozen source and cannot inherit its qualification.

  Known bounds reserve before headers; cumulative frames check actual length,
  owner and deadline. One merged permit follows every byte alias without
  copying the payload. A refusal after headers breaks the body; after a commit
  it proves no rollback. Backing allocations, other copies, decoded graphs,
  allocator/RSS and the wider resource/profile/package/mode requirements stay
  open. `all_memory_qualified` and `release_qualified` remain false.

- **Hosted verification of cache-only head `f88f7f60`.** Typed-RPC lifecycle
  run `40441002537`, SDK run `40441002545` (GIL and free-threaded Python ABI
  jobs), and Python examples run `40441002523` pass. These do not establish
  the complete typed-RPC failure matrix in free-threaded mode. JavaScript run
  `40441002517`, job `121981064617`, retains one failure: the published
  container's `/wake` request times out at five seconds. The workflow explicitly
  sets `RUST_TEST_THREADS=1`; this is a serial failure, not evidence that test
  parallelism caused it. Other listed streaming and generated-RPC tests pass.

  Its lifecycle fixture used a random listener while the unchanged published
  class declares `defaultPort = 8080`. Linux selects the inspected container
  IP and the declared port directly; macOS selects the fixture's mapped host
  port. The test now uses the existing isolated fixed-port fixture helper,
  as the other published-container integration already does. The existing
  fixture regression verifies isolated listeners and Linux port 8080. Wake
  errors identify their cycle; handler/client budgets and lifecycle assertions
  stay unchanged. Local macOS observations cannot exercise Linux address
  selection. Fresh head `f3d5f55b` passes the complete Linux adapter/real-node
  step in JavaScript run `40454527251`, job `122024645088`, including the fixed
  lifecycle test under unchanged budgets. Node-only mode, runtime archive and
  admission boundaries, Pi crash recovery and Vercel SDK task/concurrency
  recovery also pass there; later workflow steps remain in progress. Typed-RPC
  lifecycle run `40454527345` and Python examples run `40454527327` pass. Full
  hosted SDK verification of this head is still running. The local V8/Python
  250 ms failures above remain unexplained; Linux passes do not diagnose them.

- **Bound cached RPC reuse before receiving idle expiry.** The native
  stale-receipt failure is reproduced under its original lease, operation
  budget and assertions. A tagged probe observes held ack/extension requests,
  then receiver expiry at 30,001 ms with zero active requests/replies, followed
  by upstream EOF when the ack is released. The timer expires while the proxy
  holds submission; this is neither a prior dead-sender checkout nor loss of a
  produced business reply. Probe log:
  `/tmp/cellar-rpc-native-settlement-idle-probe.log`; instrumentation is removed.

  A real authenticated socket regression gates the old connection's next read
  while allowing a fresh connection. It fails with `old-socket read held=true,
  connections=1` and connection reset, then passes with an advertised receiving
  interval and a source cache window limited to its first half from dial.
  Parking clean EOF never renews this window. Active exchanges can finish after
  cache expiry; expired/stopped owners cannot contribute an idle sender. Both
  peers require the reuse contract: callers reject invalid/missing interval
  metadata and receivers reject incompatible capabilities before admission.
  Internal protocol markers remain 1, with no legacy pooling fallback.

  The completed frozen attempt passes **116 core RPC tests, 35 qualifier
  guards and all 631 selected live cases**: saturation 96, peer faults 80,
  provenance 96, cycles 80, deadline 55, proof-wait 128 and lifecycle 96.
  Both auditors reject ten corrupt reports. Python streaming/typed-RPC/E2B/
  Vercel, 18 selected native integrations, all-target Clippy, Node-only checking
  and standard checks pass. All **3,656 hashes** remain unchanged; manifest
  SHA-256 `68f41c360567be1a5cd97941ab71b6099d06e76590922bea1c5238ea39a6dbbf`.

  The original SDK stale-receipt case also passes with final mutual-capability
  guards inside the full native SDK attempt, with its unchanged 30-second lease,
  three-second call budget and assertions. That suite retains **92 passed, one
  failed**: metrics export observes both hosts self-fence after an approximately
  60-second pause before its first write. Power logs show no sleep/wake event
  in that interval; the cause remains unproven. Adapters retain **163 passed,
  eight failed and six external ignored**, including native/V8/Node handler
  budgets. A backtrace-enabled generated-client retry and a correctly delayed
  V8 sample pass; an earlier sample reproduces the V8 HTTP timeout but captures
  loader startup. Passing observations and unrelated profiles do not diagnose
  those failures. These are pre-merge cache-fix results, not qualification of
  the subsequent Node-bootstrap integration. Merged-source/hosted verification
  and full resource/profile/package/interpreter qualification remain open.

- **PR #132 conflict resolution with main `fe260689` (#135).** The merged
  startup uses main's fixed launcher and private JSON source/configuration
  envelope, with configuration passed directly to the runtime function. It
  retains the PR's `cellar:node-bootstrap` source label, original startup
  deadline and blocked-pipe regression. Both real argument-limit tests remain:
  main's padded bootstrap admission test and the PR's actual invocation/short
  stack test. The earlier length-prefixed loader evidence below describes its
  original source snapshot, not this merged envelope. All 97 JavaScript tooling
  tests pass, including both private-bootstrap tests. Rust formatting and layout
  (1,203 files), JavaScript formatting (96 sources), documentation links and the
  466-package native dependency guard also pass. Rust runtime tests and hosted
  verification of the merged source remain pending. The independent RPC reuse
  attempt above completed against its unchanged pre-merge source.

- **Integrate main's sandbox bindings without dropping joined cleanup.**
  Merge head **`239796d5`** includes main through **`5db6b475`** (#134).
  The one conflict is in `workers_module.rs`: retain main's sandbox coverage
  and native drain assertion, and use the shared wrapper for its same
  after-join stopped/zero-invocations/zero-bytes invariant. Production changes
  from both branches remain. The earlier 631-case reply pass and final 272-case
  startup pass below precede this merge.

  Post-merge source passes all **272 selected live cases** (saturation 96,
  peer faults 80, lifecycle 96); both auditors reject ten corrupt reports.
  All **3,655 hashes** remain unchanged, manifest SHA-256:
  `3d78edc921d6974c2def2b360fde3a6f213857a39122980529b1bd73bd26d9e1`.
  Reports are `.cellar/rpc/reply-startup-main-{suite}.json`; peer-fault SHA-256:
  `75f683d444ee68effb94349b5d191ee9117a0c925b1861265b81c4a34c8f10ac`;
  lifecycle SHA-256:
  `e26ac5f9eecf7f95dc6c076b121d5aa19b4db58f6af48d28005cb2183c14166d`.
  **111 core RPC tests, 35 qualifier guards**, Python streaming/typed RPC and
  both changed Python provider integrations pass. All-target Clippy, Node-only
  checking, Rust/JS formatting, 1,202-file layout, docs and the 466-package
  engine-free native dependency guard pass. This subsequent ledger edit is
  prose only; it changes no executed source or fixture.

  The full `--no-fail-fast` adapter command retains **169 passed, two failed,
  six external ignored**. `module_http_and_activation_owned_bindings_survive_events_and_restart`
  reaches the original 250 ms handler limit; `rearmed_alarm_waits_for_the_active_handler_and_drains`
  receives HTTP 500 at alarm start. Both observe stopped runtimes, zero native
  invocations and zero bridge bytes after join. Other streaming, container
  lifecycle, input-error and reply-cleanup integrations pass. Do not call this
  a full post-merge adapter pass or infer an alarm failure cause from HTTP 500.

  The exact native SDK stale-receipt case reproduces locally in 30.63 seconds
  under its original 30-second delivery lease and three-second operation budget.
  The held ack returns `Ambiguous` after transport closure; the held extension
  returns `StaleReceipt`. Live-owner and caller-budget assertions pass first.
  This is a local reproducer, not a fix or proof of the idle-pool hypothesis.

- **Reply delivery retains admission until its last transport owner retires.**
  The original `RetainedRpcBody` releases receiving request, byte, tunnel and
  peer reservations when Hyper drops the body, even if a yielded frame slice
  remains alive. A controlled test reproduces all counters falling to zero
  while that alias still owns bytes. `Bytes::from_owner` now carries one shared
  reply lease through every frame clone/slice without copying the payload.
  A scalar-only stalled TCP writer reproduces another premature release:
  Hyper's automatic flat write strategy copies the bytes and drops their owner.
  The production RPC builder now selects its queued write strategy explicitly;
  the same real Hyper/TCP test retains admission through a blocked partial write,
  observes body retirement, expires delivery, joins and verifies zero counters.
  The incomplete response remains a transport error, without rollback or replay
  proof.

  Reply writes expire at the earlier of the original caller deadline and the
  existing tunnel interval measured after handler/proof completion. A separate
  reply lease removes this deadline once bytes retire, even while a native
  cleanup ticket remains. A notification wakes an already-running connection
  timer when publication shortens its wait. Expired publication returns a fixed
  deadline failure before yielding business bytes. The reservation's weak owner
  domain supplies its clock and timers: two additional red regressions reproduce
  foreign-clock contamination and a panic on refund after domain shutdown.
  Both now pass, and cleanup completes without ambient domain installation.

  All **111 core RPC tests** pass, including eight new lifetime/deadline/socket
  regressions. The source-frozen follow-up passes **631 live cases**: saturation
  96, peer faults 80, provenance 96, cycles 80, deadlines 55, proof waits 128 and
  lifecycle 96. Both independent auditors reject ten damaged reports each.
  All **3,650 source/fixture hashes** remain unchanged; manifest SHA-256
  `1a4e25cc494221c0915e62471554f9f218497c25395781c6fe00f90082b8e596`.
  Reports are `.cellar/rpc/reply-bounds-{suite}.json`; proof-wait SHA-256 is
  `2d99aeb50615587ee532b86e681be302b5d228d8c48a74f2c9c610c6abcadbcd`
  and lifecycle SHA-256 is
  `ed35ece3487d8a91b43f039a0d66b0b316db950f1a891f92b28c936731b52a15`.
  Native integrations, Python streaming/typed RPC, 35 qualifier guards,
  all-target Clippy, Node-only checking and standard repository checks pass.
  The full adapter command uses `--no-fail-fast` and records **165 passed,
  one failed, six external ignored**: the failing response test samples 90
  bridge bytes before the still-running Node transport has joined. SSE and
  WebSocket regressions pass under their original budgets.

- **Node startup no longer puts the compiled bootstrap in one exec argument.**
  PR #132 at `1ee378df` fails JavaScript CI job `121896550256` and lifecycle job
  `121896709894` immediately at Node process spawn. The current bootstrap
  argument is **133,023 bytes** before its terminating NUL; Linux limits each
  argument to 32 pages ([execve](https://man7.org/linux/man-pages/man2/execve.2.html)).
  A launcher enforcing the 4 KiB-page boundary rejects an existing real Node
  invocation. A permanent regression then fails with the original launch.
  A small loader now reads the length-prefixed compiled source from the owned
  stdin pipe, leaving the private configuration there for the worker. The
  authentication secret stays out of argv, environment and module URLs; source
  and configuration writes share the original startup deadline and do not make
  another combined source buffer. The real Node invocation and original 100 ms
  blocked-pipe regression pass. This is controlled local evidence of the
  argument-boundary fix; hosted confirmation remains pending.

  Related Workers fixtures retain an application observer across `NodeRuntime::run`
  and share a final stopped/zero-invocations/zero-bytes census after process and
  transport join. Native invocation and cell/work-drain assertions remain inside
  the host closure. This corrects the response test's live-transport census;
  it does not establish a cause for older timing failures. The intermediate
  pipe-based source passes 272 repeated saturation/peer-fault/lifecycle cases,
  both auditors and static/core checks with all 3,652 hashes unchanged (manifest
  SHA-256 `6b2bef51d203ada39c3e152ed09312e37d7a4881267800f6859f5686be73776e`).
  Its full adapter run records 165 passed, two failed and six external ignored:
  native container binding reaches its original 250 ms handler limit, and an
  input producer's original error message becomes `ValueLimit`.

  The input failure exposes the initial loader's data-URL stack expansion.
  A minimal real Node probe measures 187,034 stack bytes without a source label
  and 271 with a fixed label. The permanent startup regression is extended to
  capture a real invocation's stack size and fixed internal location; it fails
  on the original data-URL import. The compiled module now ends with the fixed
  `cellar:node-bootstrap` source label, keeping runtime frames short without
  changing value limits or exposing configuration. Final regression and
  generated-client verification then passes: **167 adapter tests, 111 core RPC
  tests, 35 qualifier guards**, all-target Clippy, Node-only checking, Rust/JS
  formatting, 1,199-file layout, docs and the 466-package engine-free native
  dependency guard. All **272 repeated generated-client cases** pass (saturation
  96, peer faults 80, lifecycle 96); both auditors reject ten corrupt reports.
  These repeat selected cases from the preceding 631-case reply-delivery pass;
  they are not additional matrix dimensions or a second full proof-wait run.
  All **3,652 hashes** stay unchanged. Final manifest SHA-256:
  `dc8f6d8c3009ccd0207f4fd0d986ff095e99abfa430cdefe1895aa091f768e86`.
  Reports are `.cellar/rpc/node-delivery-final-{suite}.json`; peer-fault SHA-256:
  `b4c497aba6a67a99ba037ded39664291056c347c1fef00d945bc57b641fdb27c`;
  lifecycle SHA-256:
  `7826b0b03be61a7752554ee7e877f13cda304c416f2f332c906c737a3c495321`.
  The ledger update that records these results is subsequent prose only. The
  intermediate native timeout remains separate and undiagnosed.

- **Hosted native settlement compatibility remains a P0.** SDK run `40415460527`
  fails the same test in both GIL and free-threaded jobs (`121896552841` and
  `121896553122`):
  `event_settlement_race::settlement_race_expiry_replacement_refuses_old_ack_and_extension`.
  Each records 92 native-node tests passed and one failed. Held original ack and
  extension calls return `Ambiguous` after the peer connection closes, rather
  than the required `StaleReceipt`; named-binding tests pass in both jobs.
  The fixture waits near its ordinary 30-second receipt expiry before holding
  submissions across expiry; receiving tunnels now have a 30-second idle bound.
  Inspect stale pooled connections and the proxy's upstream idle lifetime next.
  This timing relationship is an investigation hypothesis, not an established
  cause or permission to replay uncertain submissions. Preserve the original
  30-second delivery lease, three-second operation budget and exact refusal
  assertions. Main later advances to `5db6b475` (#134); a local merge-tree check
  is clean, while the frozen execution evidence above remains based on main
  through `b3ff49a6`.

- **Intermittent V8 timing remains undiagnosed.** The original streaming command,
  16 fresh-process embedded runs, and eight cold/warm executable-copy comparisons
  pass with unchanged budgets. A temporary fixed-phase watchdog probe does not
  observe a failure and is removed from tracked source. Workspace and system
  copies both pass; executable paging is not established as the cause. The
  earlier failed full-run log remains preserved. Permanent streaming contexts
  identify the static route and response-head/first-chunk/post-release boundary
  without changing the test's flow, assertions or deadlines. Passing reruns do
  not close this P0 or full T19 qualification.

- **Final main integration preserves admission and its qualification limits.**
  PR #132's code head **`71d1d979`** integrates main through **`b3ff49a6`**
  (#128/#131/#133). The one V8 bootstrap conflict is resolved by retaining
  main's callback-metadata function/argument and the startup phase/watchdog
  diagnostics together. The working tree is clean before qualification.

  This head passes **503 live matrix cases**: saturation 96, signed-peer faults
  80, provenance 96, cycles 80, deadlines 55 and lifecycle 96. A separate
  **five-case proof-wait smoke diagnostic** passes; it deliberately claims no
  full matrix. Both report auditors reject ten corrupted reports. The full
  128-case proof-wait run remains evidence from the earlier `80aa3b64` baseline
  below; it is not represented as rerun on this head. All **3,649** recorded
  source/fixture hashes stay unchanged during the final remaining-integration
  run, six matrices, smoke diagnostic and auditors. The source manifest
  `peer-admission-latest-source-before.json` has SHA-256
  `5944b777922bccc0fdd258421a42a56e56d0ef0a53bbd538648e440567120dcd`;
  `peer-admission-latest-source-audit.json` records zero changes. Reports are
  `.cellar/rpc/peer-admission-latest-{saturation,peer-faults,provenance,cycles,deadline,proof-wait-smoke,lifecycle}.json`.
  The final lifecycle SHA-256 is
  `5d306640650e19e6b713f05f02906a5f20cbbce6e401b1be9930547eda36bbfb`.

  Checks pass again after this merge: 103 core RPC tests, 18 selected native
  integrations, 35 qualifier guards, all-target core/JavaScript/qualifier Clippy,
  Node-only library checking, format/layout over 1,197 Rust files, native-only
  dependency isolation and documentation checks. The SDK named-binding CI case
  passes locally alone with `--all-features --test-threads=1` and its original
  one-second lease. That passing attempt does not diagnose the hosted self-fence.

  **The final full adapter command fails.** Embedded SSE exceeds the original
  100 ms V8 turn budget, closes the producer and yields an unexpected EOF in
  the chunked response. Node streaming passes. The 35 integration tests after
  that failing target pass separately, including typed RPC, callback/container
  lifecycle, upgrade and writer cleanup cases. The six external image/fleet
  tests remain ignored. No failed target is hidden or budget increased; the
  earlier complete 166-test pass belongs to its earlier source baseline.
  Logs are `/tmp/cellar-rpc-peer-latest-*.log`; the two check inventories are
  `.cellar/rpc/peer-admission-latest-{checks,final-checks}.json`. The first
  inventory explicitly records the failed adapter command, while the second
  records successful remaining targets and selected live qualification.
  New-head hosted jobs are queued; full qualification remains open.

- **Receiving RPC admission and dense uploads are bounded before dispatch.**
  A real Rust-to-Rust signed-peer call held before the original provider proved
  the missing receiving quota: a second generated call mutated state despite a
  configured one-request policy. The red log is
  `/tmp/cellar-rpc-peer-admission-red.log`.
  Receiving hosts now reserve global/per-authenticated-peer tunnel slots before
  upgrade, and request/control capacity before authority decoding or body reads.
  Uploads reserve dense capacity and a possible copy before growing one buffer;
  fragmented frames retain no list nodes. Empty-frame floods yield to the
  independent upload deadline. Header parsing, upgrade waits and idle pooled
  tunnels are bounded. Defaults and explicit CLI/library configuration are in
  [configuration](../../docs/11-configuration.md#library-defaults).

  The opaque ticket is shared by input, native driver and retained reply body.
  Caller cancellation cannot release another owner's capacity. Peer entries
  retire with their last connection. Durability acknowledgements and cancellation
  endpoints use independent paths. A paired fixed host status/header conveys
  `ResourceExhausted/unknown`, including event ambiguity, without rollback proof
  or automatic replay. Request/input accounting does not measure response or
  decoded graphs, guest heaps, allocator overhead or RSS. Slow readers and the
  remaining effective minima still need qualification.

  The **96 saturation cases** cover every Rust/Python/V8/Node caller/provider
  pair: source local/peer and receiving peer admission, independently by count
  and bytes. Each observes the actual owner/scope/epoch and unchanged state
  before business entry, refuses without a factory/handler, then verifies one
  explicit receipt retry and changed-command conflict. Cases drain calls/bytes;
  joined nodes also drain receiving tunnels and peer entries. Socket and buffer
  regressions cover partial upload, slot reuse, 100,000 tiny fragments, empty
  floods, native-ticket retention and idle retirement. Worker lifecycle cases
  now retain observed services across node join and assert both quotas are zero.
  Failures preserve bounded quota/bridge/authority snapshots and bootstrap errors.

  On main through **`80aa3b64` (#131)**, the frozen receiving implementation
  passes **631 live cases**: saturation 96, peer faults 80, provenance 96, cycles
  80, deadlines 55, proof waits 128 and Node/V8 lifecycle 96. Both independent
  report auditors reject ten corrupted reports. This uses Rust 1.98.1, Node
  24.13.1 and **GIL CPython 3.14.6**. Evidence is
  `.cellar/rpc/peer-admission-main-{saturation,peer-faults,provenance,cycles,deadline,proof-wait,lifecycle}.json`.
  The saturation SHA-256 is
  `0e41f1836df27c42ee17b3643684f94393c08aaa010f08326b74b0680e1167f7`;
  lifecycle is
  `11117f40214d3b316e8e8ad99d610198ba095a8c1f736c8f627a459804b95418`.
  All **3,649** source/fixture hashes remain unchanged during these seven suites
  and their auditors. `peer-admission-delivery-source-before.json` has SHA-256
  `7879539b78970400628869192238236298a27850710ea95e4cda6cc85af7be16`;
  `peer-admission-delivery-source-audit.json` records zero changes. Earlier runs
  also passed 631 cases; they are repeated evidence, not additional coverage.

  Local checks pass: **103** core RPC tests, **18** selected native integrations,
  **35** qualifier guards and the complete **166-test** container/guest/V8/Node
  adapter run, with six external image/fleet tests ignored. All-target Clippy for
  the core, JavaScript and qualifier, Node-only library checking, format/layout
  over **1,197** Rust files, the **466-package** engine-free native dependency
  guard and documentation checks pass. The updated receiving socket assertion
  is separately rerun after Clippy corrections. Logs are
  `/tmp/cellar-rpc-peer-{main,delivery}-*.log` and
  `.cellar/rpc/peer-admission-{main-ready,delivery}-checks.json`.

  Reliability limits remain explicit. Earlier native HTTP/SSE regressions hit
  250 ms, and three parallel V8 unit tests fail budget/liveness before their
  isolated and later complete reruns pass. Temporary timing found one 1.345 s
  stateless poll before any cell constructor; its later phase probe passes.
  These probes establish no cause; their instrumentation is removed. The first
  lifecycle run returns early `Unavailable/unknown`; later diagnostic/full runs
  pass, so its cause stays open. The first peer-fault run also loses an opaque
  V8 startup error. A controlled 40 ms runaway-startup regression now preserves
  fixed startup phase/action and watchdog cause; syntax failures preserve phase
  without source/path contents. This improves diagnosis without identifying the
  earlier transient. The controlled red log is
  `/tmp/cellar-rpc-startup-cause-red.log`.

  The writable integration test observed 49 bridge bytes while sampling a live
  Node engine after native drain. That matches one cancel/retire frame; it does
  not by itself establish a leak. The test retains the native-invocation drain
  assertion and now requires stopped/zero invocations/zero bytes after the
  language runtime has joined. Its original failure and passing corrected run
  remain in `/tmp/cellar-rpc-peer-main-ready-integrations.log` and
  `/tmp/cellar-rpc-peer-writer-joined.log`. Clippy's three assertion-style
  failures are corrected without changing their expected outcomes.
  PR #132's earlier hosted JavaScript `/wake` timeout and SDK GIL named-binding
  self-fence remain separate, unresolved failures. Input bounds, Counter cases
  and successful reruns do not close T01–T19, full Protobuf interoperability,
  supported Python modes, package identity or performance qualification.

- **SDK submission count and length-weighted byte admission precede queueing.**
  Native SDK namespace calls previously entered an unbounded source queue and
  routing futures before per-cell admission. A configured one-call policy's
  boundary regression observed two queued requests. `RuntimePolicy` now pins
  `max_outbound_rpc_requests` (default 128) and `max_outbound_rpc_bytes` (default
  256 MiB; minimum 128 KiB) per execution domain across generations. Admission
  tries both quotas before a source gate or queue wait, without a new waiter
  queue. A valid typed request receives an encoded
  `ResourceExhausted/not_committed` refusal for that unsubmitted child call;
  entered handlers cannot use arbitrary propagated errors as rollback proof.
  There is no automatic retry. The request, caller and local native driver share
  one opaque reservation. Caller cancellation or queue draining cannot free it
  while another owner retains work; an unconsumed reply retains it too.
  Trusted direct host control requests and receiving peers keep separate
  admission, leaving reply/shutdown progress independent of this source quota.
  The byte charge is twice the encoded payload length, four times the combined
  scope/name/method UTF-8 length and 64 KiB of control/frame allowance. It is not
  an RSS or exact allocator measurement; spare/shared backing capacity, response
  copies, guest heaps and peer ingress still require their own accounting.
  The new **64-case source saturation matrix** uses every generated
  Rust/Python/V8/Node caller/provider pair, local and signed-peer routing, with
  independent count and byte policies. V8 uses generated TypeScript; Node uses
  generated plain JavaScript. The count cases retain the default byte allowance;
  byte cases retain the default 128-call quota. A native driver barrier observes
  the real owner, scope, epoch and closed transaction before the original
  provider. While held, the other generated client is refused without factory
  or business entry. Release permits the original handler and proof path;
  explicit same-ID retries apply the refused command once and preserve changed
  command conflicts. Every case and joined node has zero source reservations.
  The independent auditor rejects **ten** corrupted reports. The suite runs in
  the combined qualifier and Linux lifecycle CI.
  On merged main through `cc38acc3` (#130), **375 live cases** pass: source
  saturation 64, signed-peer faults 80, failure provenance 96, dependency cycles
  80 and deadlines 55. Both report auditors pass ten negative probes each.
  All **71 core RPC-filter tests** (including nine SDK boundary tests), **18
  native integration tests**, **35 qualifier guards**, **76 V8/Node unit tests**
  and **six adapter integration tests** pass, including dedicated SSE/WebSocket,
  Node npm/native-addon compatibility and typed RPC/restart checks. Affected
  core/test and full qualifier Clippy checks pass; format/layout cover 1,189 Rust
  files, the native-only dependency guard finds 466 packages with no JS engine,
  and docs pass. The first Clippy invocation caught an unused test-harness
  import and owned comparison; the corrected assertions preserve their meaning.
  The first dependency-guard invocation lacked Cargo on PATH; the normal pinned
  environment passes. Both failed invocation logs are retained.
  Evidence: `.cellar/rpc/outbound-saturation.json` (SHA-256
  `687b257082b99957a4eb836a3200a2e1789ea28974968667d974bc2514257fc0`),
  `outbound-{peer-faults,provenance,cycles,deadline}.json`,
  `outbound-source-before.json` and `outbound-source-audit.json`; all **3,637**
  source/fixture hashes remain unchanged throughout these runs. The source
  manifest SHA-256 is
  `57f645d0b4a07cdbc43949c82836a570f244cdcc8bf6618ce72f0ac7e1818e7b`.
  The original red boundary is `/tmp/cellar-rpc-submission-count-red.log`;
  qualification/check logs are `/tmp/cellar-rpc-outbound-*.log`. This uses Rust
  1.98.1, Node 24.13.1 and GIL CPython 3.14.6. New-head hosted checks remain
  required; merged #130's typed-RPC lifecycle job passes. Successful streaming
  and compatibility runs do not diagnose the prior intermittent failures.
  Broader receiving-peer admission, effective limits, allocation/RSS and
  flood/disconnect/cancellation saturation, full live Protobuf-profile calls,
  supported Python modes and package identity remain open. T01–T19 and full
  `release_qualified` remain open.

- **Receiving size refusal retains its exact-call proof without a body copy.**
  A valid 128-byte message is admitted; a valid 129-byte message under the same
  descriptor previously escaped `Catalog::refusal` as an outer
  `InvalidArgument` error, losing its encoded refusal. The native
  [envelope reader](../../crates/cellar-native/clone/bytes_envelope.rs) now
  borrows its fixed private buffer, bounds metadata separately to 1 KiB and
  retains clone versions 10–16 and the existing primitive/wrapper encodings.
  Host catalog admission validates the borrowed Protobuf body before any owned
  `Prepared` copy. Oversized bodies produce `ResourceExhausted/not_committed`
  through the normal output path; the same check after a factory produces
  `ResourceExhausted/unknown`. Malformed graphs, aliases, extra properties,
  views and resizable/immutable buffers remain refused. Encoding uses the same
  resource-exhaustion category for its byte ceiling.
  The signed-peer matrix now has **80 cases**: all 16 caller/provider pairs
  across reconnect, committed reply loss, method refusal, element refusal and
  byte refusal. A receiving host snapshots 128-byte/four-element limits while
  generated clients retain their default limits. The relay independently
  validates its injected unknown fields against the source contract, isolates
  the exceeded receiving limit, and preserves method, operation/request IDs,
  scope and remaining deadline. Refusals enter no business handler and leave
  state unchanged; two explicit same-ID retries and a changed-command conflict
  prove one eventual mutation. Actual owner/epoch and transaction observations
  guard against replay. Both native nodes and the relay join with zero relay
  connections. The independent auditor rejects **ten** corrupted reports.
  After integrating #127 and merged #129 through `185c397a`, all **81 native
  tests, 60 core RPC tests, 35 qualifier guards, 76 V8/Node runtime tests, five
  adapter integration tests and ten native typed/profile integration tests**
  pass, as do the 65 JS RPC tests, strict TS checks and three SDK streaming
  tests. Native, affected core/test, JS and qualifier Clippy checks pass. The
  initial new encoder assertion needed `.err().expect(...)` because `RpcData`
  has no `Debug`; the corrected assertion keeps the same required failure.
  Qualification source/fixture hashes stay unchanged across all **3,632**
  recorded files during the integrated run. Evidence:
  `.cellar/rpc/receiver-main-peer-faults.json` (SHA-256
  `9084da53d1dbfa85b07eae9654c1d0b5f0f74f468cde136e3f946306c58a35eb`),
  `receiver-main-source-before.json`, `receiver-main-source-audit.json` and
  `/tmp/cellar-rpc-receiver-*.log`. The original failure is preserved in
  `/tmp/cellar-rpc-receiver-limit-red.log`; the initial assertion compile error
  remains in `receiver-main-core.log`. This uses GIL CPython; other receiving
  dimensions, runtime/slot admission, all-language full-profile calls, supported
  Python modes, package identity and broader allocation/RSS qualification remain
  open. It does not close T01–T19 or set `release_qualified`.

- **V8 completion capacity survives byte saturation.** Two original regressions
  show startup accepting no terminal headroom and a saturated result copying
  8 KiB before reservation fails and stops the engine without a completion.
  Embedded admission now reserves 128 bytes for its eventual terminal result.
  Startup requires `max_bridge_bytes >= max_value_bytes + 128`; a larger result
  must extend that reservation before its native String/Vec copy. Quota refusal
  uses the reserved allowance for fixed `ResourceExhausted/unknown`; invalid or
  oversized replies use `InvalidResponse/unknown`. Neither category claims
  rollback. Native output proof and prepared-RPC completion remain on their
  existing paths. Node keeps its independent framed-transport policy.
  A real-isolate regression observes zero refused reply-copy bytes, keeps the
  cell object alive for its next successful call and drains native bytes. Exact
  1,024/1,025-byte UTF-8 results are call-local; byte/queue refusal and joined
  queue shutdown release their completion reservations. All **72 runtime tests**
  and all **five streaming, Node compatibility/restart and typed-RPC integration
  tests** pass. Both-backend all-target Clippy and Node-only/embedded-only library
  checks pass. The initial new queue fixture left exactly 128 bytes available
  and failed its starvation guard; its corrected 1,279-byte pool leaves 127,
  preserving that guard and the zero-retention assertions.
  Evidence: `/tmp/cellar-rpc-v8-completion-red.log`,
  `/tmp/cellar-rpc-completion-runtime.log` (initial fixture guard),
  `/tmp/cellar-rpc-completion-runtime-green.log` and
  `/tmp/cellar-rpc-completion-{integrations,clippy,node-only,embed-only}.log`.
  Effective cross-layer minima, other adapters' copies, allocator/RSS accounting
  and the full saturation/flood/disconnect matrix remain open.

- **Published-container Linux correction verified; WebSocket follow-up pending.**
  The unchanged published module declares container port 8080. Native Docker
  routing on Linux uses the inspected container IP and that port, while macOS
  uses published host-port mappings. The old mock advertises `127.0.0.1:8080`
  on Linux but listens on an unrelated ephemeral port; readiness never succeeds.
  Both hosted heads stop at the original ten-second `/container/alice` deadline,
  while 100 original module-suite runs with two Tokio workers pass locally before
  the fix. Those retries do not explain the failure; the platform-specific
  routing and fixture metadata do. The fixed-port fixture now binds port 8080
  on an isolated Linux loopback IP and reports that address in Engine inspection.
  Other hosts retain the real published-port mapping. A new real-HTTP two-peer
  regression checks endpoint isolation and Linux's declared port. The pinned
  application, existing assertions and handler/client budgets are unchanged.
  All four module tests, both V8/Node container integration tests, both native
  container tests, the explicit Python container mode and the full GIL Python
  bridge matrix pass locally. The latter includes HTTP/storage/SQL/alarm,
  cancellation, restart, remote routing, handoff, shutdown and both process-kill/
  disk-deletion watchdog recovery cases. JS all-target Clippy and the affected
  native/Python test-target Clippy checks pass. Hosted JS run `40353259801` at
  `f0a717ee` passes all four module tests, including the unchanged published
  application and the two-peer endpoint regression. Typed-RPC lifecycle run
  `40353259691` also passes. The JS job then fails the Workers WebSocket fixture's
  first `/open`: that fixture also requests port 8080 but still starts its peer
  on an ephemeral port. Use the same declared-port peer there; the original
  WebSocket integration passes locally with its assertions and budgets intact.
  Its Linux verification remains pending; this is the only other fixed-port
  caller found in the compatibility fixtures.
  Evidence: `.cellar/rpc/diagnostics/workers-module-repeat100/status.json` and
  `/tmp/cellar-workers-port-*` and `/tmp/cellar-rpc-workers-websocket-port.log`.
  The separate native 250 ms regression timing
  failures remain unresolved.

- **V8 admission now precedes native request allocation.** A real-isolate
  saturation regression first fails because an RPC refused by the aggregate
  bridge limit nevertheless copies its entire 8 KiB attachment. The engine now
  inspects the fixed private view and UTF-8 metadata length, checks authority and
  operation admission, and retains the byte reservation before making either
  owned request copy. It rechecks host revocation before native publication.
  The same saturated invocation returns its refusal and releases all charges;
  its attachment-copy probe must stay at zero. Another live regression accepts
  exactly 1,024 UTF-8 payload bytes and refuses 1,025 before native work.
  The initial fix passes 68 runtime tests, all five streaming/Node compatibility/
  typed-RPC integration tests, all-target JS Clippy and Node-only library checking.
  Logs: `/tmp/cellar-rpc-v8-admission-copy-red.log` and the
  `/tmp/cellar-rpc-v8-admission-*` rechecks. Completion-copy accounting, effective
  host/peer minima and full saturation/flood/RSS qualification remain open.

- **Merged baseline and CI fixture follow-ups.** At `b3c276bd`, all 66 runtime
  tests, 35 qualifier guards and 48 generated-client peer-fault cases pass.
  Independent auditing rejects six corrupted peer reports; source hashes remain
  unchanged across the checks. Evidence: `.cellar/rpc/pr126-merged-peer-faults.json`
  and `pr126-merged-head-source-hashes.json`; report SHA-256:
  `6ed5e8a10a73a3bb7892b499a8e0e0b5a4e330db301a8a835dcd88ff7fe3393e`.
  Hosted typed-RPC lifecycle also passes. The broader CI failures exposed a stale
  SDK runner bootstrap and a native cold-factory helper signature. `8fa1acb0`
  shares the production delegate bootstrap between SDK/RPC tests and adapts that
  helper without changing assertions or budgets. All 68 JavaScript tests and all
  three native typed-RPC integration tests pass, including cold-factory deadline/
  cancellation and receipt recovery. JS/Node CI subsequently reaches a distinct
  `/container/alice` timeout in `workers_module`; its cause remains unresolved.

- **2026-10-26 PR #126 integration with main.** Main is integrated through
  `9f1e7df1`, including the new runtime delegates and extent-backed storage.
  The moved V8 engine retains requested-shutdown provenance across dispatch,
  microtasks, lifetime cancellation and background queries. RPC facade tests
  supply the production delegate factories; SDK container cancellation uses the
  existing portable signal interface so ES-only TypeScript builds remain valid.
  All 65 JavaScript RPC tests, strict RPC/SDK typechecks and rebuilt fixtures
  pass. Merged-head native checks and hosted results are recorded in
  [PR #126](https://github.com/crabbuild/cellar/pull/126). The 744-case report
  below predates this integration and does not qualify the merged source.

- **2026-10-26 combined recheck: all 744 cases pass.** The original 696 cases
  and 48 generated-client peer-fault cases finish in 1,354.280 seconds, including
  all 96 Node/V8 lifecycle cases and all 48 Python suppression/supervision cases.
  Success is published only after native drains, interpreter/engine joins and
  required child-process exits. Tracked and unignored source hashes remain
  unchanged throughout the run. Independent checks retain every prior section,
  verify all new fault tuples and reject nine corrupted-evidence probes. This
  does not erase the earlier real-clock deadline failure or qualify the complete
  portable profile, resource bounds, interpreter modes or application delivery.
  Local evidence: `.cellar/rpc/runtime-peer-recheck.json`, its `-status.json`,
  `-source-hashes.json` and `-audit.json`; raw log:
  `/tmp/cellar-rpc-runtime-peer-recheck.log`. Report size: 2,798,907 bytes;
  SHA-256: `07734682663c19d24c2f6edd49b8f985712819ac10f98f67c866c7b75b6fec13`.
  Executable SHA-256:
  `7dd6cd1f12a235fe32d6623fb6b7fe23bb1bbae18d1c4c4582e4687229b3471c`.

- **A separate real-clock deadline failure remains unresolved.** The first
  boxed-dispatch combined attempt stops after 37.144 seconds because a guest
  deadline target does not record its late SDK refusal within the existing
  five-second observation bound. It returns the expected caller timeout, but
  missing target evidence correctly fails qualification and publishes no report.
  The frozen source hashes remain unchanged. A separate `--suite deadline` run
  passes all 55 cases; that retry does not diagnose or fix the combined failure.
  Guest deadline cases now log their language/mode and include native entry and
  transaction-checkpoint observations on failure, with unchanged assertions and
  budgets. Preserve `.cellar/rpc/runtime-peer-final-status.json` and its raw log
  alongside the earlier stack-overflow attempt when assessing any later pass.

- **The combined driver no longer exhausts the Python host stack.** The first
  new combined run reaches all 96 Node/V8 lifecycle cases, then aborts entering
  Python cancellation; no success report is published. The same standalone
  smoke command reproduces it. The macOS trace contains no recursive chain and
  reaches native startup before cancellation logic. Disassembly shows a
  1.34 MiB root poll frame competing with nested startup on the unchanged 2 MiB
  Python host stack. A non-async dispatcher now constructs one boxed suite
  future; the combined loop polls one suite at a time in its original order.
  No new task, stack setting or timeout is introduced. The driver-size guard
  fails against the old sequential future (21,288 bytes of embedded state),
  then passes after indirection, along with all 35 guard tests and the exact
  standalone smoke command. The failed frozen run retains its log/status and
  binary digest under `.cellar/rpc/runtime-peer-full*`; the combined rerun uses
  a fresh report path. Those partial phases alone are not combined success.

- **Requested V8 shutdown preserves the original host result.** Two real-isolate
  regressions stop synchronous dispatch and a running microtask before the
  unchanged turn deadline. Both fail against the old runtime: a requested stop
  becomes a dispatch/budget error. The watchdog now retains its first observed
  interruption cause under its existing deadline lock. Only a private typed
  requested-stop marker is joined as shutdown; genuine expiry, ordinary engine
  errors and panics still fail. The existing runaway test requires the exact
  execution-budget error. Host success and original host failure survive both
  interruption phases, with zero retained native bytes. The 55 library cases,
  real typed-RPC, Node compatibility/restart and streaming tests pass together.
- Focused validation also passes all 35 qualifier guard tests, six native peer
  tunnel regressions, 17 embed-only runtime tests and the Node-only library
  check. Clippy passes all targets for both JS backends and the full qualifier
  with warnings denied. Formatting/layout cover 1,074 Rust files; the dependency
  guard keeps native-only applications free of JS engines. Documentation checks
  pass. These checks do not establish a full portable-profile or release result.
- **The underlying streaming timing failure remains open.** Updated `main`
  passes the original serial streaming/Node commands and 20 serial repetitions
  (40 target executions, 60 test cases) under unchanged budgets. A separate
  concurrency-four probe retains one real `/outbound/sse` native 250 ms handler
  timeout among 24 target executions. That observation does not establish a
  scheduling cause or close the intermittent Node/V8 regression. The shutdown
  cause fix prevents misleading secondary errors; it does not extend any budget.
- **Generated clients now exercise signed-peer faults.** The focused
  `peer-faults` suite passes 48 cases: all 16 caller/provider pairs for forced
  warm-tunnel reconnect, dropping a complete successful owner reply, and changing
  the inner method to an unregistered name. Lost committed replies remain
  `Unavailable/unknown` with exactly one initial handler entry and no implicit
  replay. Real owner refusals preserve `MethodNotFound/not_committed` with zero
  factory/handler entries. Two explicit same-ID retries recover/apply exactly
  one effect; a changed command conflicts. Native owner/scope/epoch and closed
  transaction audits agree with observed descriptor/request/reply bytes. The
  relay bounds its framing and tasks, joins after native drain, and refuses
  missing evidence. Ordinary outer keepalives must close so a subsequent upgrade
  cannot bypass observation. The raw-report auditor rejects six corrupted
  evidence variants. This extends peer correctness evidence, not full resource,
  portable-profile, interpreter-mode or packaging qualification.

- **Peer tunnel reuse now binds the authentication owner and target session.**
  Two real signed-upgrade regressions fail before the fix: a warmed address-only
  cache admits an inner RPC with changed credentials or a different target
  name, bypassing the fresh handshake's refusal. Keys now include address,
  tunnel kind, target and an opaque per-`PeerAuth` scope. Scope clones retain
  identity after the owner drops; no credential material, digest or pointer
  string enters the key. Changing credentials requires a new handshake, and
  wrong-target refusal preserves trusted stale-route classification.
- **The idle cache has one 64-tunnel bound across every peer and kind.** A
  65-live-peer regression fails against the former per-peer bound. The new
  cache evicts the least recently returned idle sender, purges closed senders
  and retains no empty peer entries. Active responses own their senders outside
  this cache and are never evicted. This does not cap all active connections.
- A real pooled-RPC reply-loss regression observes the complete submitted
  request, exact request ID/method/body and a lost reply. The production caller
  reports an ambiguous attempt, opens no replay tunnel, stops route retry and
  never returns the failed sender to cache. The existing positive unsent
  recovery test preserves the exact request and decreases its original budget.
  All six focused peer tests pass, including receiver upload expiry before
  admission. Logs use `/tmp/cellar-rpc-peer-auth-`; the global-bound red log is
  `/tmp/cellar-rpc-peer-pool-red.log`. This is transport/admission evidence,
  not the full forced-guest-retry or business-receipt matrix.
- **Node IPC work now yields between bounded byte slices.** Two real-socket
  regressions demonstrate that the old receive/write pass exceeds 1 MiB before
  yielding. Each direction now has an independent 1 MiB turn quota. Decode
  work charges wire size and permits at most one larger bounded frame in a
  fresh turn. Large partial inputs and outputs retain their complete native
  reservation until handoff/drop. The original 256-frame heartbeat observation
  allowance survives across slices; a quota yield grants no new liveness
  window. Native authority retirement, replies and shutdown are serviced
  between slices, with no added per-slice sleep or changed timeout/capacity.
  Real Node checks include a healthy queued 4 MiB control backlog after a
  300 ms observer pause under the original 100 ms heartbeat setting. An initial
  test expectation used `context.variable` instead of the actual `var` wire
  operation; its failure is retained separately from behavioral regressions.
  These finite cases do not qualify whole-process RSS or all flood conditions.
- The final peer/resource checks pass **six native peer tests**, **53 combined
  V8/Node library tests**, both named typed-RPC/codec-readiness tests, native
  and JavaScript Clippy, the full qualifier build and standard checks. The
  earlier broader pipeline also passes native admission/capacity and selected
  integration tests, **39 Node-only library tests** with HTTP/restart and
  streaming, and **31 GIL Python library tests** with streaming and typed RPC.
  Each pipeline verifies unchanged source hashes during its execution. Stage
  commands and results are retained in ignored
  `.cellar/rpc/peer-resource-validation-status.json` and
  `.cellar/rpc/peer-resource-final-status.json`; the separately run typed-RPC
  target is logged at `/tmp/cellar-rpc-peer-resource-typed-rpc.log`.
- **T19 timing qualification remains open.** The broader combined command
  fails Node compatibility at the original 250 ms handler budget. Its harness
  now joins host shutdown after request failures and records elapsed phases
  and bridge usage without changing requests, assertions or budgets. A later
  diagnostic run passes both restart rounds; that retry does not establish a
  cause or fix. The final combined streaming command fails V8's outbound
  WebSocket phase with both the original 250 ms handler timeout and 100 ms
  engine-turn termination; native work drains on shutdown. The skipped
  typed-RPC target was run separately and passes. Logs retain both failures,
  the initially incorrect `var` test expectation and the subsequently fixed
  Clippy range-loop warning; no limit or timeout was raised.
- **Latest complete prior runtime report:** the control-decoder/WAL revision
  `8bd52c73` passes all **696 cases** in **1,418.560 seconds**, with unchanged
  source hashes and publication after joins. Independent audits cover every
  suite count, 96 Node/V8 lifecycle cases, native reservation cleanup, exact
  receipts and 32 forced retirements before original expiry; three negative
  audit mutations are refused. `.cellar/rpc/wal-index-full.json` is 2,743,842
  bytes, SHA-256
  `1b843ed6231d472f15ea51b44d40292cfb40cbcba68acb7fd6c3e16d3bac4330`.
  This report predates the peer-cache and IPC-work changes above; broader
  release-qualification flags remain false.
- **Development storage preserves the WAL index between operation connections.**
  The original lifted-limit workload completes all 1,002 blob puts and drains
  application work in **3.770 seconds**, versus **46.975 seconds** before the
  fix. Its blob-seeding phase falls from **36.535 to 0.290 seconds**. The
  original 60-second deadline, large values, batch sizes, FULL synchronization,
  automatic checkpoints and writer admission remain unchanged. All five
  native cell-limit tests and all four public local-store tests pass. This
  local result still requires verification on the hosted SDK failure path.
- The public bucket regression writes an 8 MiB object above SQLite's default
  automatic-checkpoint threshold, then performs tiny writes through a surviving
  clone. It fails before the fix because the WAL generation does not recycle.
  After the fix, header salts change and the file does not grow beyond its
  prior large-write size. Reopening preserves every exact object and ETag;
  conditional updates apply and stale updates fail. The implementation shares
  one idle connection, with no active statement or transaction, across clones;
  every operation retains its own connection and existing native I/O lifetime.
- A process-local I/O probe isolates the mechanism with linked SQLite 3.53.2:
  64 tiny writes after a 26 MiB write read **3,523,078,400 bytes** with all
  connections closed between operations, versus **792,832 bytes** with one
  idle connection. Both retain FULL sync and exact data. Rebuilding the WAL
  index loses checkpoint progress and repeats large recovery/checkpoint work.
  The probe and hashes are in ignored
  `.cellar/rpc/node-frame-budget/wal-reopen-result.json`. Public-boundary
  red/green logs use `/tmp/cellar-rpc-wal-recycle-`. The cell-limit fixture now
  retains bounded phase names/timings, completed blob count, pending work and
  its disposable store on failure, and joins its execution domain on errors.
- The WAL-lifetime follow-up passes **18 local-store library tests**, the
  multi-store CAS/pagination test and **30 native integration tests** (one
  pre-existing Raft test remains ignored). V8/Node pass **49 library tests**,
  both HTTP/restart targets, both live streaming targets and both typed-RPC
  tests. Embedded GIL Python passes **31 library tests** and its streaming and
  typed-RPC executables. Affected native, JavaScript and Python Clippy, the full
  qualifier build and standard format/layout/dependency/docs checks pass.
  Source hashes remain unchanged during that validation pipeline. Commands,
  exit codes and logs are recorded in ignored
  `.cellar/rpc/wal-index-validation-status.json`. These passes do not establish
  the historical V8 watchdog/native cold-timing failures' exact causes. The
  combined 696-case rerun is recorded above; hosted verification remains open.
- **Node control decoding now has allocation-free shape and string admission.**
  The private protocol admits at most 16 scalar fields with distinct literal
  ASCII keys of at most 64 bytes and numeric tokens of at most 32 bytes.
  Application values remain encoded payload strings or raw attachments.
  Borrowed input scanning rejects collections and ambiguous keys before
  serde_json allocates, counts exact decoded UTF-8 string bytes (including
  surrogate pairs), and reserves parser scratch separately from retained
  strings/attachments. Scratch returns after decoding; retained bytes follow
  the frame through native admission. The fixed field cap bounds control
  object layout separately; allocator overhead and RSS remain unqualified.
- Four new transport regressions fail on `720ae5c4` and pass after the control
  guard. All **49 combined-feature library tests**, **35 Node-only library
  tests**, two Node real-host HTTP/streaming tests and both combined-feature
  typed-RPC/codec-readiness tests pass, alongside all-target JavaScript Clippy.
  Standard format/layout (1,025 Rust files), docs and dependency checks pass.
  The parser tests include malformed escapes, Unicode boundaries, a generated
  1,024-step Unicode corpus, field/key/scalar limits and scratch cleanup.
  Initial red compilation and a mistyped 33-digit positive boundary fixture
  are retained separately from the corrected behavioral runs; production
  limits and refusal assertions are unchanged.
- The same isolated allocator probe/inputs now refuse the array-valued frame
  before building a JSON collection. Peak Rust allocation above baseline falls
  from **8,789,387 to 400,193 bytes** under the unchanged **4 MiB** byte pool.
  Both the original scalar input and an escaped 400,123-byte scalar control
  remain accepted with unchanged measured allocation peaks; all permits
  return on drop. Old/new source, binary and linked-library hashes are retained
  in `.cellar/rpc/node-frame-budget/control-allocation-differential.json`.
  This finite allocation experiment is not a process RSS or flood qualification.
  The 696-case report below predates this control guard and is not its full
  runtime qualification. Stage results use
  `.cellar/rpc/node-control-preflight-status.json` and logs use
  `/tmp/cellar-rpc-node-control-preflight-`.
- Hosted GIL SDK job **121299416265** on `38bfbb96` now fails
  `lifted_bindings_persist_large_values_batches_and_pages` at its original
  **60-second** deadline, after embedded Python packaging succeeds. The same
  all-feature native integration command passes all five tests locally in
  **46.94 seconds**. The free-threaded SDK job **121299416570** fails the same
  test at 60.88 seconds. Neither hosted failure has phase evidence. The later
  local phase probe and controlled WAL-recycling fix above preserve the
  workload/limits; a passing hosted rerun is still required.
- **Native Node framing now uses the runtime's payload byte pool.** Three
  controlled transport regressions fail against the old implementation:
  incoming input bypasses aggregate admission, input coexists with queued
  output beyond the aggregate limit, and decoding creates an uncharged
  overlapping copy. The new implementation reserves before frame allocation
  or decoding copies, retains the full allocation across partial writes,
  transfers decoded-frame reservations through native admission, and frees
  completed input capacity. It counts metadata serialization without a
  temporary encoded buffer. Delivered invocation buffers are dropped while
  their native authority remains live. Startup refuses a configuration that
  cannot fit one maximum value and its conservatively framed copies.
- All **12 transport tests**, including real socket partial writes, shared
  native owners, partial inputs and disconnect cleanup, pass. Two real Node
  tests verify release of a delivered 64 KiB request while nested native work
  remains held, exact reply slices and zero final reservations, plus refusal
  before process start for insufficient capacity. The combined-feature
  library passes **41 tests**; the Node-only command passes **27 library and
  two HTTP/restart/streaming integration tests**. Its typed-RPC target has no
  enabled tests in that feature mode; the separate combined-feature command
  passes both real typed-RPC/codec-readiness tests. All-target combined-feature
  JavaScript Clippy and formatting pass.
- This accounting change does **not** establish allocator/RSS bounds. Control
  JSON layout and parser scratch, Node worker JavaScript buffers, codec/guest
  objects and other adapters' retained copies require further audit. Effective
  method/runtime/host/peer minima, saturation and control progress under full
  byte admission remain open. No default value, body, heap, deadline or lease
  budget was increased.
- The initial broader combined-feature command fails both streaming tests:
  V8 reports a 250 ms handler budget failure plus engine-turn termination;
  Node reports a `/reset` HTTP 500 without a retained body. A subsequent
  diagnostic command passes the 41 library tests, then fails the V8 HTTP
  fixture's first increment with a **250 ms handler timeout**, startup 1,405 ms,
  client construction 127 ms and a fully drained `Stopped` outcome. Later
  targets in that command are not run. The focused typed-RPC and Node-only
  passes do not close those failures. Streaming diagnostics now retain reset
  phase/status/body/timing and JavaScript native reservation/shutdown state.
  Logs use `/tmp/cellar-rpc-node-frame-budget-`; stage results are in ignored
  `.cellar/rpc/node-frame-budget-validation-status.json` and
  `.cellar/rpc/node-frame-budget-diagnostic-status.json`. The fresh full
  qualifier's first full run fails its first Rust-to-Rust private-method
  preflight with the same 250 ms handler timeout, before the guest matrix.
  Three unchanged standalone local runs pass all 16 pairs in 1.6–1.9 seconds.
  Those passes establish neither a timing fix nor the failing path's cause.
- The unchanged full executable then passes **all 696 cases** in **1,788.685
  seconds**. A source-hash audit confirms its tracked inputs stay unchanged.
  The independent raw-report audit verifies every suite count, all **96
  Node/V8 lifecycle cases**, live nonzero reservations, zero drained native
  reservations, exact receipt recovery and all **32 forced retirements before
  original expiry**. Three negative audit mutations (missing backend pair,
  missing live charge and retirement at expiry) are refused. The report is
  `.cellar/rpc/node-frame-budget-full-recheck.json`: **2,744,037 bytes**, SHA-256
  `ec8764dd469840a69ac96dec41f5ffc9e8aa6be9880dc4578fedbf032ae85442`.
  Its binary hash is
  `5b959011ec8d1ba4243575f0b2192c29b7c1c78d414103ada305a5627cb47089`.
  All broader qualification flags remain false; this passing recheck does not
  erase the initial timeout or the V8 turn-watchdog/streaming failures.
- The corrected embedded-Python streaming and Clippy commands pass, as do
  format/layout, docs and native dependency checks. The initial Python commands
  omitted required `--features embed` and did not run; their errors remain in
  the diagnostic status. The final JavaScript streaming command passes Node
  but fails V8 with watchdog termination and an unexpected response EOF. Its
  snapshot has zero native reservations and a fully drained host. Final stage
  results are in `.cellar/rpc/node-frame-budget-final-status.json`.
- An offline packed-consumer smoke passes outside the checkout's module tree.
  The `@cellar/sdk@0.1.0` tarball contains **16 files** and is **28,707 bytes**.
  A fresh directory installs it and pinned Protobuf-ES 2.16.0 from local cache,
  imports Counter/Profile code, verifies all eight registered methods against
  their artifacts, preserves tested 64-bit/bytes/presence/empty values, refuses
  the wrong contract and passes strict TypeScript checks with negative cases.
  `.cellar/rpc/node-frame-budget/package-smoke-status.json` records the commands.
  This is package-content/static-API evidence, not a clean-machine CI run,
  live profile RPC matrix or frozen host/package identity qualification.
- **A separate allocator probe proves the next native control-decoder gap.**
  Under a valid 256 KiB value / 4 MiB bridge configuration, a 400,049-byte
  array-valued control frame is accepted while retaining **8,389,330 bytes**
  above baseline and reaching **8,789,387 bytes** of peak Rust allocations.
  Its returned reservation is only 400,049 bytes. A scalar-control comparison
  stays below the bridge budget; both release their reservations on drop.
  The probe counts Rust allocator sizes and excludes allocator-internal
  realloc overlap and OS/RSS overhead. This demonstrates structured metadata
  amplification, not a framing-buffer leak. The reproducer intentionally
  fails its refusal assertion; source, linked artifact hashes and results are
  in `.cellar/rpc/node-frame-budget/control-allocation-probe-result.json`.
  Bound control shape and parser scratch before decoding next.
- **Shared descriptor admission now validates Protobuf JSON field names.**
  A 116-case comparison against pinned `protoc` 34.0 finds 23 invalid schemas
  accepted by the previous native validator. The new validator agrees on all
  116 cases: 93 accepted, 23 refused, zero mismatches. It checks default names
  and effective custom names independently within each message; custom
  overrides cannot conceal a default-name conflict. Embedded NUL characters
  and extension-like bracketed names are refused. Case-sensitive, empty,
  Unicode and space-containing custom names remain valid metadata. Native
  wire behavior and canonical artifact identity remain intact.
- The corrected controlled baseline passes the legal-name test and fails all
  three refusal tests; all four pass after the implementation. The initial
  positive identity fixture compared raw dependency-first file ordering with
  canonical sorted ordering; the fixture now starts in canonical order and
  retains strict byte equality. Both red logs are preserved. Six old-generator
  absent/existing-output probes create or overwrite output before compiler
  failure; all six now refuse without changing output. The public generator
  suite additionally covers four JSON-name refusal variants across absent
  and existing output with no compilers on PATH.
- All **57 core RPC unit tests**, nine codegen unit tests, three generator
  integration tests, 14 selected native integration tests, both V8/Node typed
  tests and Python typed-RPC normal/slow-constructor profiles pass. Core
  library/test Clippy, codegen all-target Clippy and standard format/layout,
  documentation and engine-free dependency checks pass. All 24 Counter/profile
  artifacts reproduce byte-for-byte. Exact compiler/input/binary hashes,
  differential and output-preservation results and validation-log hashes are
  retained under ignored `.cellar/rpc/json-name-baseline/`; stage commands and
  results are in `.cellar/rpc/json-names-validation-status.json`. This finite
  naming corpus does not close other descriptor semantics, full-profile
  business calls across all pairs, effective limit/copy accounting, saturation,
  packaging or the historical intermittent startup/connection failures.

- **Descriptor admission now precedes allocating decoder collections.**
  Six new tests fail on the old implementation: file/field/source-metadata
  bounds are checked only after decoding, unknown attributes disappear during
  normalization, unresolved options remain, and invalid MessageSet/lazy
  settings are accepted. The new borrowed-slice walk checks the pinned schema
  and bounds before `FileDescriptorSet::decode`: 256 files, 4,096 message/enum
  types, 65,536 fields, 200,000 descriptor/option/metadata records and 1,000,000
  wire-field occurrences. The independent symbol/name budgets remain intact.
  Poisoned later records prove resource refusal occurs before decoding the
  remainder. Malformed keys, lengths, fixed-width bodies and groups fail
  recoverably. Unknown attributes and unresolved options at all eight option
  scopes are refused rather than erased or reinterpreted by a later compiler.
- Pinned `protoc` 34.0 rejects proto3 MessageSet, scalar lazy hints and unknown
  uninterpreted options. It accepts 31 declaration levels and rejects 32;
  the shared validator now enforces that boundary. The business-message
  recursion budget remains 64. Metadata and valid message lazy hints retain
  their descriptor identity and eager native wire validation. Compiler
  comparisons retain legal/default metadata instead of inferring restrictions
  from option names. The old build tool accepts all four tested invalid inputs
  (MessageSet, scalar lazy, unresolved options and 32 levels), creates output
  directories and overwrites existing contract metadata before a missing-tool
  failure. The new build tool refuses all eight absent/existing-output cases
  without changing output. The codegen integration suite covers these and
  unknown future attributes with no compilers on PATH.
- Native message definitions now borrow the single decoded tree; the old
  validator cloned each enclosing subtree. A pinned-compiler-valid 171,007-byte
  descriptor with 16 message levels and 8,192 leaf fields measures
  48,939,008 / 49,545,216 / 48,955,392 bytes peak RSS before the change versus
  21,102,592 / 19,382,272 / 19,349,504 afterward. Each observation is a fresh
  macOS arm64 process measured with `/usr/bin/time -l`; the intentionally
  unsupported language is checked only after validation/name checks, so no
  output or language compiler runs. This is an isolated descriptor-validation
  diagnostic, not an RPC throughput result or a global RSS qualification.
  Exact input/binary hashes and raw results are under ignored
  `.cellar/rpc/descriptor-admission/`, including `baseline.json`, `final.json`,
  `old-output-refusals.json`, `new-output-refusals.json` and compiler comparisons.
- All **53 core RPC unit tests**, nine codegen unit tests, three generator
  filesystem/admission tests and all 14 selected native typed-RPC, offline-check
  and portable-profile integration tests pass. All 24 Counter/profile artifacts
  reproduce byte-for-byte across Rust, Python, TypeScript and JavaScript.
  V8/Node and Python library/typed-RPC/streaming checks pass, including Node
  runtime lifecycle and existing HTTP/restart cases. Core/codegen Clippy and
  both adapters' all-target Clippy pass. Format, module layout (1,023 Rust files),
  documentation (749 local references) and the native dependency guard
  (465 packages, no JavaScript engine) pass. Stage reports use
  `.cellar/rpc/descriptor-admission-{validation,adapters}-status.json`; logs use
  `/tmp/cellar-rpc-descriptor-admission-`. These changes do not close remaining
  semantic conformance, runtime saturation/copy accounting, supported runtime
  modes, packaging, observability or performance requirements. The earlier
  intermittent startup and connection failures remain unexplained.

- **Native cold construction is now qualified for deadline and cancellation.**
  Two public-node tests hold a real native factory after proving its storage
  handle works. One generated caller observes its 200 ms deadline; the other
  is explicitly cancelled only after the factory enters. Both callers receive
  a fixed failure category with `unknown` effects while construction remains
  held. After release, new storage, new child bindings, a cached write and a
  synchronous transaction are all refused. Generated business handlers have
  zero entries for the ended call. Fresh admission reads count zero and no
  late marker writes; two explicit same-ID increments both return count one.
  Exactly those two increment-handler entries remain after the node joins,
  with ingress, application work and cells drained. The native integration
  helper accepts an explicit policy callback for this test's three-second
  parent; existing language-adapter defaults remain 250 ms.
  The initial two-case run passes in 0.52 seconds after compilation. This is
  native constructor-boundary evidence, separate from the 55-case deadline
  matrix and 696-case cross-language report. Guest cold-constructor matrices
  and the intermittent native streaming latency remain open.
- Final affected checks pass: all three native typed-RPC integration tests,
  Python streaming and typed RPC (including its slow-constructor profile),
  and all five selected V8/Node HTTP, streaming and typed-RPC tests. Native
  integration Clippy and all-target Python/JavaScript Clippy pass, as do
  formatting/layout (1,021 Rust files), docs and the engine-free dependency
  check. Stage results and raw-log hashes are retained in
  `.cellar/rpc/cold-factory-validation-audit.json`.
  These test/fixture changes do not change the production executable behind
  the previous 696-case report.
- The first selected JavaScript command fails its V8 HTTP/restart case with
  connection refused after 2.54 seconds. Its log does not identify the phase,
  node generation or shutdown outcome. Ten unchanged frozen-binary runs pass
  in 0.61–0.78 seconds; those retries do not establish a cause. The fixture now
  joins the node after request errors as well, preserving the primary failure
  with its exact test phase, expected restart count, startup/client-build
  timings and typed node outcome. All five selected tests pass in the final
  retry, without budget changes or mutation retries. This is improved failure
  evidence and teardown, not a fix for the original connection refusal.
  `.cellar/rpc/cold-factory-v8-http-baseline-status.json` retains the baseline
  repetitions; the audit above retains both failed and final logs.
- **The native I/O probe did not reproduce the slow streaming path.**
  A temporary process-scoped macOS interposer measures `open`, `mkdir`,
  `fsync`, `read`, `pread`, `pwrite` and `close`, logging only operation,
  descriptor, process/thread identity and durations above 5 ms. A controlled
  delayed read verifies the probe. Ten startup/streaming pairs pass, followed
  by the full original SDK sequence in 347 seconds; no measured streaming
  syscall exceeds that threshold. These are passing observations, not a
  falsification of an I/O cause in the absent failing scenario. Other syscalls,
  CPU work and native scheduling remain unmeasured. The temporary library is
  unloaded and removed from `/tmp`; its source/binary provenance remains only
  in ignored `.cellar/rpc/diagnostics/cold-io/`.
  `/tmp/cellar-python-sdk-cold-io.log` is 19,895 bytes, SHA-256
  `3c59fb45e216d7de45adb56084350b762b6d629d25f825e9e58b6591622b0552`.
  `.cellar/rpc/python-sdk-cold-io-{status,audit}.json` retain metadata for
  published head `0bf59c34`. No production runtime source changed for this
  experiment; no cause or performance fix is claimed.

- **The codec decoder probe now isolates decoding from cold activation.**
  Its original 100 ms child call also had to construct a new provider. With
  controlled 120 ms Python construction, the native deadline correctly returns
  `DeadlineExceeded/unknown` before decoding; the old fixture incorrectly fails
  its decoder-entry assertion. A separate read-only `GetCount` now activates
  that same provider before the probe. The 100 ms child/250 ms host budgets,
  120 ms decoder delay, committed count and explicit same-ID receipt retry
  remain unchanged. The default test launches an owned fresh-process
  slow-constructor profile with a 60-second kill/join bound before running the
  normal profile. Both profiles pass. Request failures now identify the fixed
  fixture path; checkpoint diagnostics contain only synthetic phase/category
  data. The controlled red log is `/tmp/cellar-python-cold-codec-red.log`,
  SHA-256 `443b2ca362a3f2730151e2683a493810ae2c561cfd5d41e6fdd75a66814bb517`.
  This proves a separate fixture assumption, not the historical 250 ms failure.
- **Streaming remains intermittently slow before native cell dispatch.**
  The full original SDK command fails again after the bridge/startup tests in
  `/tmp/cellar-python-sdk-cold-codec-failed-01.log` (371 seconds). The smaller
  `--test startup --test streaming -- --test-threads=1` command reproduces
  the same `/cell/sse` 250 ms failure in 14 seconds. A new failure-only snapshot
  is captured before node shutdown without entering Python or calling SDK
  storage. Live dispatch handoff peaks at 0.4 ms; a native SDK operation takes
  259 ms and is abandoned on expiry. No child Python invocation has been
  admitted in the live bridge counters. The interpreter sample is 167 ms old
  and reports zero resident objects, so it is not an instantaneous guest stack.
  The smaller failure log is `/tmp/cellar-python-stream-startup-replay-01.log`,
  SHA-256 `0af72531997e48149a16bbbe7a86d0d9609e4778038ef642c67c1cb0b3d1c2ed`.
- Repeated isolated/interleaved tests pass, as do five startup/streaming pairs
  with temporary native phase tracing. A traced full SDK retry passes in 364
  seconds (`/tmp/cellar-python-sdk-stream-trace.log`): cold routing takes 85 ms,
  including 42 ms restore and 40 ms isolate startup, versus about 5 ms routing
  in an isolated run. Temporary tracing is removed. These passing retries and
  timing differences do not establish why the failing native path exceeded
  its original budget. Changing working directory alone does not reproduce
  the failure consistently. Native cold-start/storage/scheduling diagnosis
  remains the highest local timing-qualification gap.
- With temporary tracing removed, the original full SDK command passes in
  383 seconds, including its 66-second build, the lifecycle/streaming tests
  and both typed-RPC profiles. All-target Python Clippy, formatting, layout
  (1,020 Rust files), docs (749 references) and the native-only graph check
  (465 packages, no JavaScript engine) pass. Production code is unchanged.
  The SDK log `/tmp/cellar-python-sdk-codec-final.log` is 19,539 bytes,
  SHA-256 `cc73b2331234433a765bc18acd995e45c66b287b7e46e10cbb2649fb75eae4bc`;
  `.cellar/rpc/python-sdk-codec-final-{status,audit}.json` retain metadata.
  This successful retry does not establish the intermittent timeout's cause.

  ```sh
  RUSTC_WRAPPER= cargo test --locked -p cellar-python --features embed -- --test-threads=1
  ```

  The per-command wrapper override follows an intentionally stopped build:
  two owned compiler/cache-client processes stayed asleep with under one CPU
  second each for 260 seconds, before tests began. Only those owned processes
  were stopped; the shared compiler-cache server was unchanged. The retry
  compiles successfully. The aborted build is recorded separately in
  `.cellar/rpc/python-sdk-cold-codec-build-abort.json`; it is not a runtime
  test failure or evidence for the earlier streaming timeout.
- **Linux follow-ups have current-head evidence on `91bd2675`.**
  Lifecycle run `40221009824` passes its 144-case matrix and JS/Node run
  `40221009873` passes. SDK run `40221009821` passes embedded-adapter tests,
  baseline verification, loaded-threading-ABI and packaging checks in both
  3.14 and 3.14t jobs; broad test/lint checks are still in progress at this
  observation. All four GIL/free-threaded FastAPI and durable-agent jobs in
  Python-example run `40221009862` pass. These are results for the published
  queue/SQLite/observer follow-ups, not the newer local codec-fixture change.

- **Python queue admission now uses a direct condition wait.** Linked SDK
  job `121210984291` on `27977f2f` fails the one-second held-consumer waiter,
  with no surviving tasks/objects or bridge reservations in the final report.
  Twenty fresh-process runs of the unchanged fixture pass locally; those
  retries do not establish the historical CI cause. The old waiter repeatedly
  calls `/state`, which performs two native SQL queries before reading the
  holding flag. A controlled profile adds 1.1 seconds to that reporting route
  while preserving the consumer and two-second handler budget. The original
  waiter fails with the same admission-deadline error.
- The replacement issues one `/holding` request that awaits the existing Python
  event under the original one-second timeout. It performs no SQL polling.
  The normal and slow-reporting profiles pass, including the original live
  cross-context refusal, release, expired-context refusal, exact delivery and
  business-effect counts, restart and zero-work checks. Both profiles run in
  the normal bridge executable. Failure-only snapshots retain native bridge
  counters, interpreter observation age and bounded read-only queue/lease/
  alarm/wake state without calling Python or submitting SDK SQL. The original
  hosted trace lacks these observations; its exact cause remains unproven.
- The exact broader bridge command passes in 333 seconds, alongside all-target
  Python Clippy, with GIL CPython 3.14.6 and Rust 1.98.1:

  ```sh
  cargo test --locked -p cellar-python --features embed --test bridge -- --test-threads=1
  cargo clippy --locked -p cellar-python --features embed --all-targets -- -D warnings
  ```

  `.cellar/rpc/python-delivery-final-status.json` records the stages; raw logs
  use `/tmp/cellar-python-delivery-`. The controlled red log has SHA-256
  `4c5f90f0b0a2e5e26d6121a767da3592ca7c0cf0f1fee875d019f6256e7352f5`; the full
  bridge log has SHA-256
  `f031522889142fee1c58804d08592b1b860a1155f8928d42963b27a22eaf6d2e`.
  This is a queue-test-only follow-up to `6b532bf8`; production bridge/core
  code and its 696-case report below remain unchanged. Updated Linux SDK
  results and the earlier streaming/typed-RPC timing failures remain open.

- **The SQLite and Node follow-ups pass the complete local command.** The
  frozen executable exits 0 for all **696 cases** in 1,747 seconds, after all
  native nodes and guest runtimes join. A separate unchanged proof run passes
  all **64 cases** in 333 seconds. This run uses macOS arm64, Rust 1.98.1,
  Node 24.13.1 and GIL CPython 3.14.6 with Protobuf 7.34.2; it does not qualify
  free-threaded Python or every supported runtime version.
- `.cellar/rpc/node-sqlite-final-full.json` is 2,744,182 bytes, SHA-256
  `04d2eeaa2ee9aa334a7069f24ee7600ca30ec69fc5a95b760c9b7d2176ff8def`.
  The matching status, environment, binary hash and audit summary use the
  `node-sqlite-final-` prefix. Raw logs are under
  `/tmp/cellar-rpc-node-sqlite-final-*.log`. Independent report audits verify
  all 16 suite counts, all 144 lifecycle Cartesian cases, native authority,
  process teardown, zero reservations and receipt recovery. All 32 forced
  Node/V8 cases retire physical native frames before original RPC expiry.
  Broader qualification flags remain false. Reproduce after the fixture setup
  in `tools/rpc/README.md`:

  ```sh
  cargo build --locked -p cellar-rpc-qualification --features full
  "$CARGO_TARGET_DIR/debug/cellar-rpc-qualification" --suite proof \
    --report .cellar/rpc/node-sqlite-final-proof.json
  "$CARGO_TARGET_DIR/debug/cellar-rpc-qualification" --suite all \
    --report .cellar/rpc/node-sqlite-final-full.json
  ```

- The first upgraded-SQLite combined run exits 1 in
  `output proof fail v8->node`: Node watchdog
  diagnostics show a 5,381 ms heartbeat gap, a 5,378 ms observer gap, readable
  input and zero active guest invocations. That failed run publishes no success
  report. A separate unchanged 64-case proof command passes; that retry alone does not
  close the failure. The actual reason for the native thread's delay is unknown.
- A controlled real-Node regression pauses native observation for 300 ms with
  a 100 ms heartbeat threshold. Node keeps its normal heartbeat timer; the
  fixture requires queued input before native observation resumes. The old
  ordering stops the healthy worker and the regression fails. Node now drains
  the existing bounded receive pass before its heartbeat decision, holds new
  starts while heartbeats are overdue, and retains original SDK authority and
  call deadlines. Startup policy and watchdog durations are unchanged. A
  test-only pause gate is absent from production builds. All 14 Node-focused
  tests pass, including genuine runaway, startup and owned-process cleanup.
  Both backends together pass all 31 library tests, two typed-RPC tests and
  both real Node HTTP/npm/restart tests. The unchanged 64-case proof suite also
  passes on the new executable; the combined command above also passes.

- Linux lifecycle run `40207338997` passes on published storage-fix head
  `27977f2f`, including qualifier checks and all 144 Node/V8/Python lifecycle
  cases. JavaScript/Node `40207338923` and Python examples `40207338994` also
  pass. The broad SDK run `40207338981` fails: normal CPython times out waiting
  for a held queue consumer in `--runtime-delivery`; free-threaded CPython
  reaches the native suite and fails restoring an evicted coverage entry.
  The latter reproduces locally with unchanged assertions. Its fixture moves
  only the main object-store database, separating it from committed WAL data;
  moving the whole disposable store directory instead makes both coverage
  tests pass with every original recovery assertion retained. These hosted
  results qualify `27977f2f`; the dependency and observer follow-ups require
  their own hosted results.

- **The SQLite follow-up passes the described local checks.** Workspace
  `rusqlite` is pinned to 0.40.2 with its native statement cache and checked unsigned SQL
  conversions explicitly enabled. `libsqlite3-sys 0.38.2` bundles SQLite 3.53.2.
  The linked-library floor regression first fails on 3.45.0; a header/runtime
  identity check and unsigned precision/range tests accompany the upgrade.
  Main-database names and crash-observer C types follow the new binding API.
  Initial authorizer/interrupt installation errors propagate; scoped restoration
  asserts the immutable owned-handle invariant. Public APIs and policy remain.
  The full qualifier/admin metadata check passes. Tests pass for 11 standalone
  LTX cases, 5 public native SQL/storage cases, 40 SQLite unit cases, 18 local
  store cases (including bounded large-object reads), 49 Raft capture/storage
  cases and 31 qualifier cases. Three ignored Raft process fixtures are
  intentionally launched by their parent tests. The linked engine reports
  SQLite 3.53.2 and matches its binding headers. Six native replication tests,
  the real opt-in vector-search example and all affected Clippy targets also
  pass. Final all-target Clippy also passes for the host, LTX, admin, shared guest,
  both JS backends and the full qualifier. The 31 JS library tests and four
  selected JS/Node integration tests pass. The standalone proof report has
  64 cases, 101,788 bytes and
  SHA-256 `160293b9d2ea2954795314b1f6c4ce2579f46785ebfa3432b4618f3d2ba0cd38`.
  The older 696-case report below qualifies `27977f2f`; the new combined
  report above exercises the upgraded engine and observer fix together.

- The native lease probe reproduces under two concurrent, separate processes:
  Node completes all 16 rounds, while V8 fences in round 5. Its renewal enters
  the blocking worker immediately, spends 559 ms connecting/configuring SQLite,
  then 104 ms waiting for writer admission; commit/close finishes at 715 ms.
  The actor starts with 665 ms of authority and correctly fences at expiry.
  Temporary per-write phase traces are removed from production and fixtures;
  raw diagnostic sources and observations remain only in ignored `.cellar/rpc`.
- A four-thread, 600-operation probe links the same bundled SQLite and changes
  only `SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE`. Default-close runs have maximum setup
  waits of 927/152 ms and complete in 2,286/1,285 ms; no-close-checkpoint runs
  have 48/16 ms maxima and complete in 828/575 ms. This is a local mechanism
  comparison, not JS/TS TPS or a worst-case latency guarantee.
- The public bucket regression first fails with
  `operation close removed the committed WAL`. Its frozen pre-fix production
  library is used by the actual integration-test source. The normal Cargo red
  build is stopped to avoid a redundant feature-graph rebuild. The new behavior
  retains committed WAL frames across real gets/heads and reopen, while testing
  new and stale CAS tokens. The source is also selected by hosted RPC checks.
- Development-store connections disable only last-close checkpoints. FULL WAL
  commit synchronization, automatic checkpoints, CAS, native I/O reservation
  ownership, writer serialization and drop order are preserved. Local state
  documentation now requires preserving the database with its WAL sidecar.
  The public integration test passes through normal locked Cargo execution;
  its targeted Clippy check passes. Format/layout cover 1,020 Rust files, docs
  validate 749 local references and the native dependency graph remains
  engine-free at 465 packages.
- The original combined command finishes all **696 cases**, including the full
  128-case proof wait, 32 failed-recovery, 48 abrupt process-crash, 64 proof/fence,
  96 Node/V8 lifecycle and 48 Python lifecycle/supervision cases. Its report is
  published only after every native/guest runtime drains. The tool handle is
  gone after the conversation interruption, but the complete valid report,
  terminal log, absence of child processes and publication invariant establish
  completion. `.cellar/rpc/local-checkpoint-full.json` is 2,744,096 bytes, SHA-256
  `00e34319fc338d5bc25da97d5ec4f9514d2b4db08d3580151b970ee1c725b20e`.
- Saved-report audits verify all 16 suite counts total 696 and retain every
  broader qualification flag as false. Existing independent audits check all
  144 lifecycle cases, original Cartesian coverage, native authority, process
  exits, cleanup and receipt recovery. Added checks establish physical native
  retirement before original RPC expiry in all 32 forced Node/V8 cases.
  `.cellar/rpc/local-checkpoint-audit-summary.json` records the scope.
- The exact concurrent reproducer also passes after the fix: Node-to-Rust/peer
  and V8/local each finish 16 fresh-owner rounds, both process exit codes are 0.
  Reports `local-checkpoint-stress-node.json` and
  `local-checkpoint-stress-v8.json` retain `qualified: false`; their SHA-256
  hashes are `dce1da8daa7954a4e3bc79f0dc98e98532fffde091d47f52105c2331172f17a4`
  and `02d050e2188861b49bd0416af9b5ad8ca04a84b23e3eed32b90a06edbcd2b35e`.
  No TTL, handler/RPC budget, watchdog, proof, shutdown or outcome assertion is
  relaxed. Broader design qualification remains incomplete.
- Hosted lifecycle run `40179795015` on previous head `c99138ff` completes its
  checks but fails warming `worker-control` in Python-to-Node graceful shutdown:
  the HTTP connection closes before the message completes. It has no native
  storage-phase evidence. Do not assign the last-close mechanism to this hosted
  occurrence without evidence. The later Linux lifecycle pass on `27977f2f` is
  recorded above; broader SDK failures remain separately recorded.

- The unchanged V8 shutdown matrix first passes 32 cases, and an exact
  Node-caller/V8-provider/peer/before-sync diagnostic then passes 16 fresh-owner
  rounds. Passing retries alone do not resolve the hosted failure.
- A controlled regression at that same real SDK/fleet seam waits until both
  original weak native budgets have about 3,500 ms left. The old 5,000 ms drain
  stall then fails the existing forced-cut assertion. Native observations show
  both physical frames retiring at about their original 10,000 ms RPC expiry;
  the engine remains healthy, reservations reach zero and the caller returns
  `DeadlineExceeded`. Shutdown finishes in 3,539 ms with a legitimate
  clean drain. This proves a fixture deadline-ordering race without inferring
  missing work from the report. The archived hosted occurrence has no matching
  timeline, so a shared cause is not asserted for every historical failure.
- Only Node/V8 forced-owner fixtures now select a 250 ms no-progress window.
  The complete eight-second shutdown budget remains available for durability
  cleanup. Normal graceful drain, RPC/handler budgets, lease TTLs and the V8
  watchdog stay unchanged. Injection requires over 1,250 ms of original native
  budget headroom, and both physical frames must retire before their original
  RPC deadlines. Existing unclean proof, pending-work, scope/epoch, revoked
  budget, zero-reservation, engine/process join and receipt assertions remain.
- The Node-to-V8 peer/before-sync case in the ordinary 32-case V8 shutdown
  matrix deliberately consumes setup time to reach that near-deadline window.
  Diagnostics also expose `v8-forced-peer-repro` (16 fresh rounds) and
  `v8-forced-peer-deadline-repro` (one near-deadline case), both explicitly
  `qualified: false`. Bounded observations store original/current headroom,
  physical retirement ages, shutdown duration, guest gauges and a fixed reply
  code; observations never release the held business future.
- The regression passes after the fix: the final 96-case run cuts in 256 ms
  with 3,498 ms still available. Both native frames retire before RPC expiry,
  scope probes refuse `ContextExpired`, the report retains two pending events
  and one pending dispatch at the forced cut, and cold receipt recovery passes.
  The earlier standalone green reproduction cuts in 416 ms with 3,500 ms left.
- Final-source qualification passes all **96 Node/V8 lifecycle cases**, **32
  Python cancellation/shutdown cases** and **16 Python supervision cases**.
  Independent saved-report audits retain every original Cartesian case,
  authority/checkpoint, cleanup, process identity/exit and receipt assertion.
  An added audit verifies original deadline ordering for all 32 forced Node/V8
  cases. A separate 32-case V8 shutdown run, 16 exact peer rounds and the single
  near-deadline regression also pass. These separate commands do not establish
  a passing full 696-case run or close broader T01-T19 qualification.
- Final reports and raw execution logs are under `.cellar/rpc/`, with log
  prefix `cellar-rpc-shutdown-final-`. Report hashes:

  | Report | Cases | Bytes | SHA-256 |
  | --- | ---: | ---: | --- |
  | `shutdown-final-lifecycle.json` | 96 | 286,374 | `92ed34a2499032d2e36398874da4b5daa71028019b12cc24fd7470920bad1cb9` |
  | `shutdown-final-python-cancellation.json` | 32 | 890,939 | `a354f62b76d39f1dd963a640aa7d4027f7fb6ed4c98edd89277def5ec0906d2d` |
  | `shutdown-final-python-supervision.json` | 16 | 537,451 | `eeffa5eda49b8b114995b5237e9852673ee1b8ec16ad474eb84445ead8ef01aa` |
  | `shutdown-final-v8.json` | 32 | 96,434 | `228201f19a7c94ae6e3b34138ffb69cb5ca3858de5d983394995413f930598e0` |
  | `shutdown-final-peer.json` | 16 | 59,701 | `711e49b23ce5023c2d5a1dd86111adbbf5b827bbda49b864efefeceb4f0ce4a2` |
  | `shutdown-final-near-deadline.json` | 1 | 3,859 | `9300e6628d38fbeec72d46255dc4454ac0c021e69f18a92f6a5778c9ca0b2132` |

- The red log `.cellar/rpc/cellar-rpc-shutdown-v8-peer-deadline-before.log`
  is 1,441 bytes, SHA-256
  `58bea7c2e4ec4ebfa485707c865ad5371666f9f8ed869f6e1b3352044a90e3dc`.
  Final all-target qualifier Clippy, all 31 unit tests and format/layout for
  1,019 Rust files pass. The native-only dependency graph remains engine-free at
  465 packages; docs validation covers 749 local references. Temporary debug
  tags are removed. The new diagnostic returns a boxed future to keep its type
  out of the main suite's already large native host future.

- PR #110 merges at `906eb36a`, including the named RPC implementation, offline
  checker and bounded proof-wait lease diagnostics. The generator-read follow-up
  starts from that updated main (including the Raft image-limit change) at
  `fb8b5275`. It passes all nine generator unit tests, three descriptor-admission
  tests, 65 RPC-related native library tests, four CLI checker tests, the native
  typed-RPC integration and 31 qualifier unit tests. All three affected Clippy
  commands pass. Format/layout check 1,019 Rust files; the native dependency guard
  remains engine-free at 465 packages; docs check 749 local references.
- The updated-main rerun again passes all 16 local and 16 authenticated-peer
  pairs, including restart, handoff and receipt recovery. Independent audits
  validate both complete pair sets, digest, topology, epoch/scope and recovery
  flags. `.cellar/rpc/delivery-main-local.json` and
  `.cellar/rpc/delivery-main-remote.json` reproduce the earlier local/peer report
  bytes and SHA-256 values recorded below. All raw command logs are preserved
  as `.cellar/rpc/cellar-rpc-delivery-main-*.log`.
- Updated-main generation again reproduces all 24 tracked artifacts exactly.
  `.cellar/rpc/delivery-main-regeneration.json` records source `fb8b5275`,
  4,741 bytes and SHA-256
  `772620875b1677010842fb6ec6182e200ed749f00054fc7012b2d886cfee3841`.
  The reference files are unchanged. This verifies the bounded reader's valid
  path; the full 696-case command and failing lifecycle/deadline/regression gates
  below remain open, with all broader qualification flags false.

- The shipped CLI adds `cellar rpc check --descriptor FILE --inventory FILE`
  with optional `--catalog FILE`. It uses the runtime's portable-profile
  validator and the generator's exact canonical inventory. Native catalog
  reconstruction requires complete service registration with matching method
  types, digest and limits. Canonical byte comparison rejects duplicate keys,
  extra fields and formatting drift. Input reads are bounded to 4 MiB for the
  descriptor and 64 MiB per JSON artifact; no compiler, guest runtime or node
  participates. Missing catalog observations are reported as `null`, not zero.
- Four executable tests pass using the real Counter/portable-profile artifacts
  with build tools absent from `PATH`. They cover valid native catalog export,
  stale digest/type/limit metadata, partial/unknown registrations, invalid class
  names, ambiguous JSON, malformed descriptors, bounded sparse oversized files
  and duplicate/unknown flags. Successful and failed checks preserve every input
  and unrelated user file. The focused Linux RPC job now runs this CLI gate.
  This checks supplied snapshots; compiled executable/package identity and
  consumer binding verification remain open.
- Final-source checks pass: the four executable CLI tests, both affected
  CLI/qualifier Clippy commands, all 31 qualifier unit tests, format/layout for
  1,015 Rust files, the engine-free native dependency guard (465 packages), and
  documentation validation (749 local references). Earlier in this follow-up,
  all 49 RPC-related library tests and the native typed-RPC integration test
  also pass. Existing vendored Raft warnings remain.
- The final-source combined rerun passes all 128 proof-wait cases and all 32
  failed-recovery admission cases. It completes 34 process-crash cases, then
  stops at `after_count_before_receipt rust->v8`: spawning the next crash owner
  returns `No such file or directory`. Inspection confirms that the qualifier
  executable was removed from its checkout-specific target during the run;
  the whole target directory subsequently disappears. This is an interrupted
  qualification run, not evidence of a new RPC failure or a passing combined
  matrix. No `.cellar/rpc/delivery-final-matrix.json` is published, so no saved
  success-report audit runs. The completed checks predate artifact removal;
  production code and imported fixtures stayed frozen throughout execution.
- `.cellar/rpc/cellar-rpc-delivery-final-matrix.log` preserves that interrupted
  run: 60,793 bytes, SHA-256
  `42da87a72562bfb8c346ff24a3da3ff688ebfefdc9fa59a9bdf59f2c7f2a345c`.
  The native lease reproduction is preserved separately as
  `.cellar/rpc/cellar-rpc-delivery-node-repro.log`: 19,643 bytes, SHA-256
  `0c92bd2be3afa192030e44a438901f07e58da595f161ceeedb858b7df67a343d`.
  Targeted-check logs are copied beside them before any rebuild. A fresh complete
  run still requires retained executable artifacts and a stable build directory.
- At prior head `27d3f978`, hosted
  [JavaScript/Node SDK](https://github.com/crabbuild/cellar/actions/runs/40166912163)
  and [Python examples](https://github.com/crabbuild/cellar/actions/runs/40166912149)
  pass. The expanded
  [lifecycle job](https://github.com/crabbuild/cellar/actions/runs/40166912140)
  fails during the Python-caller/Node-provider/peer/before-sync startup read:
  `/rust/read?target=rust`, connection closed before the response completed.
  Its final native/managed work counts are zero with `host_failed=true`. No
  successful lifecycle report is published, and the subsequent Python groups
  do not run. Its cause remains unestablished; this is separate evidence from
  the local native lease trace and later executable removal.
- After the user confirms that the Cargo directory can be recreated, the
  locked full-runtime build at `3605c5d8` succeeds from the empty target in
  4m59s. The fresh combined command fails in remote Node-to-Node codec-budget
  verification: `/node/codec-budget?target=node&phase=verify&...&remote=true`
  returns HTTP 500 with `JavaScript handler failed`. The separate `deadline`
  suite then fails `guest deadline target did not attempt late SDK work` /
  `deadline has elapsed`. Both finish with zero native/managed work and
  `host_failed=true`; neither publishes a success report. Their cause is not
  established, and passing earlier cases does not close either failure.
  `.cellar/rpc/cellar-rpc-delivery-restored-{build,matrix,deadline}.log` retains
  this evidence. These failures are independent observations from artifact
  removal and native lease expiry; no shared cause is claimed.
- The generator follow-up fixes a separate pre-allocation resource gap:
  `std::fs::read` previously consumed the whole descriptor before the native
  validator's size check. It now requires a regular file, checks the shared
  public `Contract::MAX_DESCRIPTOR_BYTES` cap before reserving input and limits
  the read to that cap plus one byte to detect growth. Output creation and
  compiler execution still follow native descriptor admission. A real FIFO
  regression first fails against the old reader after the test kills and joins
  its own blocked child at five seconds. Oversized sparse input also tests
  refusal while preserving existing user output. The Linux CLI gate now runs
  these generator admission tests too. Final follow-up checks are recorded next.
- Final generator-bound follow-up checks pass: nine generator unit tests and
  all three executable admission tests (including the previously blocked FIFO),
  all 65 RPC-related native library tests, four CLI checker tests, the native
  typed-RPC integration and all 31 qualifier unit tests. Generator, CLI/native
  integration and full qualifier Clippy pass. Format/layout cover 1,016 Rust
  files; the native-only dependency graph stays engine-free at 465 packages;
  documentation validation covers 749 local references.
- Both fresh ordinary business-call matrices pass on that implementation:
  all **16 local pairs** and **16 authenticated-peer pairs**, including native
  restart, handoff and receipt recovery. The peer report records advanced owner
  epochs and preserved scope identity. An independent report audit checks the
  exact Cartesian pair set, contract digest, topology, owner transfer and receipt
  flags. These reports keep their broader fleet/crash qualification flags false;
  they do not replace the failing deadline/lifecycle or full 696-case commands.
- `.cellar/rpc/delivery-bounds-local.json`: 734 bytes, SHA-256
  `c337b1c6f9c997131ed86ad9ff29d266780c7d779d83adf9e2d18328a0d277f9`.
  `.cellar/rpc/delivery-bounds-remote.json`: 1,075 bytes, SHA-256
  `e61e43569c94e28d444bb5ae2f796ba6210bf355f8bf990b5220f4a530627a54`.
  Their raw execution logs are `/tmp/cellar-rpc-delivery-bounds-{local,remote}.log`.
- Fresh generation of both Counter and portable-profile bundles reproduces all
  **24 tracked artifacts byte-for-byte** in Rust, Python, TypeScript and plain
  JavaScript. The first attempt lacked `rustfmt` on the shell PATH; after fixing
  the tool environment, the comparison uses tracked outputs rather than local
  Python bytecode caches. Reference artifacts remain unchanged. The manifest
  `.cellar/rpc/delivery-bounds-regeneration.json` is 4,678 bytes, SHA-256
  `4ceaa5f41f3982fde768df34445b6c43cdb56f35133a87c050b5202abc2238ed`.
- At `3605c5d8`, hosted
  [Python examples](https://github.com/crabbuild/cellar/actions/runs/40173314384)
  pass, and the offline CLI checks pass inside the
  [lifecycle workflow](https://github.com/crabbuild/cellar/actions/runs/40173314418).
  The lifecycle matrix itself fails at Node caller / V8 provider / peer /
  before-sync forced shutdown: `drain_timed_out=false`, both work/cell drains
  true and zero pending events/dispatches. The test requires a forced-cut proof.
  Whether work retired early or accounting missed it remains unestablished;
  the report is not relabelled as a successful forced cutoff. Managed/native
  counts finish at zero with `host_failed=true`. No lifecycle success report is
  published and the following Python groups do not run. The JS/Node and broad
  native SDK workflows were still running at inspection. This result makes
  lifecycle proof the next priority alongside the existing timing failures.
- The unchanged exact V8/local/sync/deadline reproduction passes 16 rounds in
  `.cellar/rpc/delivery-lease-before.json`. Four instrumented runs then pass
  another **64 rounds**, saved as `.cellar/rpc/delivery-lease-trace-{1..4}.json`.
  The original native lease-expiry failure remains unresolved: a passing retry
  is not a causal fix. Proof-wait failures now retain the latest 64 bounded,
  memory-only actor lease observations, including attempt start/completion,
  validity headroom and setup phases. This distinguishes delayed renewal dispatch
  from a slow storage CAS on the next failure without changing TTLs or replaying
  the failed mutation. Temporary debug tags were removed.
- A fresh expanded `all` run fails later in Node-to-Rust/peer/sync/deadline
  proof-wait, during the explicit receipt retry after cancellation. No success
  report was published. `/tmp/cellar-rpc-delivery-full.log` records the failure.
  `--suite proof-wait-node-repro` repeats that topology and fails at round 10
  during warmup, before the third replica starts. Owner and ingress both fence
  with `NodeLeaseExpired`; the bounded trace distinguishes timely renewal start
  from delayed storage completion. Attempts start with approximately 665 ms
  of prior validity, become ambiguous after about 333 ms and fail to restore
  authority before expiry. `/tmp/cellar-rpc-delivery-node-repro.log` preserves
  this evidence. The precise storage/readback phase remains unresolved.
- Temporary LocalStore phase instrumentation was removed after three subsequent
  Node diagnostic commands each passed 16 rounds (48 total, including two
  concurrent processes). Their `.cellar/rpc/delivery-store-{repro,a,b}.json`
  reports retain `qualified: false`. These passes do not fix the earlier failure.
  The retained diagnostic uses only the existing bounded actor lease trace;
  production storage, lease TTLs, deadlines and durability policy are unchanged.
- Three unchanged standalone Python typed-RPC runs also pass in
  `/tmp/cellar-rpc-delivery-python-before-{1..3}.log`. Their earlier 250 ms
  failures and the streaming failure remain recorded below; no handler budget,
  polling policy or runtime behavior was changed, so these retries do not close
  the timing gap.
- The Python follow-up passes **48 focused cases** on the report-contract
  implementation: 32 cancellation/forced-cutoff cases and 16 supervised
  persistent-suppression cases. Every group covers all four caller languages,
  local/signed-peer routing and before/after-sync checkpoints. The same managed
  interpreter serves surviving callers and fresh native owners while the stale
  provider task remains alive. Late storage writes/reads, a retained generated
  client and the raw bridge must refuse `ContextExpired`. Two explicit same-ID
  retries preserve the exact count without duplicate effects; fresh native reads
  inspect both the late-write key and late-RPC target after reactivation.
- Native observations capture the original scope/epoch, closed transaction and
  actual count/receipt checkpoint before injection, then freeze. The provider
  and held Rust child physically retire, and their weak budgets expire. Native
  permits/bytes/operations reach zero while one managed Python invocation remains;
  a still-live Python caller parent is counted separately until it finishes.
  Cooperative release joins that last task. Forced cutoff retains its unclean
  proof and unknown effects; it is never reported as graceful drain.
- Persistent suppression returns typed `Incomplete` with exactly one known
  guest invocation, zero other managed resources and zero native work. The
  supervisor requires the structured report, exit code 1, verified owner/Node
  worker identities and observed disappearance of both process groups before
  fresh-process receipt recovery. A timeout fails the case; its owned child
  handle authorizes cleanup, while a metadata PID never authorizes signalling.
- Final focused reports are `.cellar/rpc/python-pr-cancellation.json`:
  891,901 bytes, SHA-256
  `032a43a36f270cf66f4ecd0c50b87a070ba06a1f759b4c51bf827ffc5d2195da`,
  and `.cellar/rpc/python-pr-supervision.json`: 537,551 bytes, SHA-256
  `2ca87c3fa496c890eb0a9de30ddccc33ae5c35475ffc871e97dc5f26d71709e4`.
  `/tmp/cellar-rpc-python-pr-cancellation.log` and
  `/tmp/cellar-rpc-python-pr-supervision.log` record execution. An independent
  saved-report audit checks every Cartesian case, checkpoint, late refusal,
  held/zero gauge, process identity/exit and explicit receipt retry. Publication
  follows native and caller-runtime joins. Full-qualification flags stay false.
- Both 31-test Rust unit suites and combined all-target adapter/qualifier Clippy
  pass. Format/layout check 1,013 Rust files; the native-only dependency graph
  remains engine-free at 465 packages. Three bridge-report, six performance-proof
  and three baseline guard tests pass; bridge/performance guards also pass with
  Python `-O`. The Linux lifecycle workflow now schedules 96 Node/V8 plus 48
  Python cases and retains their reports/logs. At that revision its hosted run was pending; the later result is recorded above.
- The final-source `lifecycle` rerun also passes **96 Node/V8 cases** in one
  process, preserving worker kills, real V8 watchdog terminations and both
  graceful/forced shutdown matrices after the shared fixture changes. Together
  with the two Python runs, this follow-up passes **144 focused lifecycle cases**;
  these separate commands do not establish a passing combined 696-case run.
  `.cellar/rpc/python-pr-node-v8-lifecycle.json` contains 237,419 bytes, SHA-256
  `60a5cbbe44bf797d0ece7f6349393aa6f83368c0249c21bbe958e08b34d278c1`.
  `/tmp/cellar-rpc-python-pr-node-v8-lifecycle.log` records the run. Native
  authority/checkpoint, cleanup/join and cold receipt assertions remain intact.
- The latest full Python regression command passes all 31 library tests,
  queue-observation tests, isolated startup and the embedded bridge executable,
  including CPU-loop/blocked-native owner kill, disk deletion and deduplicated
  durable-alarm recovery. It then **fails** in SSE streaming:
  `stream /cell/sse failed: Worker failed: handler exceeded 250ms budget`.
  `/tmp/cellar-rpc-python-pr-integration.log` preserves the failure. Its typed
  Python incomplete-shutdown report has `host_failed=true` and zero guest/native
  tasks, admissions, bytes, reservations and pending operations. The full
  command is not a passing regression run. No handler budget was increased;
  this occurrence does not establish a cause shared with earlier Node timing
  failures or the separate native lease-expiry reproduction.
- The separately executed Python `--test typed_rpc` also fails with
  `500 Internal Server Error: Worker failed: handler exceeded 250ms budget`.
  `/tmp/cellar-rpc-python-pr-typed.log` records the failed command and typed
  final cleanup with zero native work. Both streaming and typed-RPC fixtures use
  the existing 250 ms shared native-host handler budget. That common boundary
  narrows the next investigation but does not establish the timing cause.
- At prior head `53f4978f`, hosted
  [Node/V8 lifecycle](https://github.com/crabbuild/cellar/actions/runs/40157814401),
  [JavaScript/Node SDK](https://github.com/crabbuild/cellar/actions/runs/40157814304)
  and [Python examples](https://github.com/crabbuild/cellar/actions/runs/40157814239)
  workflows completed successfully. These results qualify that revision only;
  the expanded Python follow-up still needs its own hosted run.
- The expanded 16-suite `all` command now schedules **696 cases**. Its first run
  failed during V8/local/sync/deadline proof-wait warmup, after the preceding
  suites and 90 proof-wait cases had completed. A fresh success report was not
  published. `/tmp/cellar-rpc-python-full.log` preserves the failure; this is
  **not** a passing combined qualification run.
- `--suite proof-wait-v8-repro` repeats that exact case up to 16 times without
  changing lease or watchdog policy. The first diagnostic run failed at round
  zero: both owner and ingress shutdown returned `Fenced { code: 3,
  reason: NodeLeaseExpired }`; bounded fresh health and typed read connections
  were refused. `/tmp/cellar-rpc-python-proof-v8-repro.log` records the evidence.
  This rules out a stale pooled connection for that occurrence. The timing or
  storage cause of lease expiry remains unresolved; no TTL change, automatic
  mutation replay or causal claim about earlier intermittent failures is made.
  Warmup diagnostics join the native nodes and retain their typed outcomes.
- Python cleanup now independently reports native admission permits through
  `BridgeReport::native_invocations`. Managed Python invocation registries can
  remain nonzero after those permits have released. `Report::complete()` requires
  both to reach zero. Published reports require the bridge section and the new
  field; missing evidence cannot deserialize as a clean drain. The native host
  supplies bridge observations after joining and draining its queues, independent
  of guest claims. Red tests demonstrated both missing invariants before their
  fixes; the report readers and performance guards enforce the same contract.
- Implementation `d4097fa3` adds disposable V8 providers to the existing owned
  lifecycle harness. Generated TypeScript business calls reach native local or
  signed-peer owners before writes or after synced count/receipt checkpoints.
  A separate invocation first reaches a distinct real Rust child, then enters a
  CPU loop. The existing two-second V8 pump watchdog must terminate the isolate;
  the bootstrap must report the exact execution-budget engine failure. No test
  API closes the runtime. The provider, held child and trigger child have
  distinct native scope/budget authority; all three frames must retire and their
  weak sources must refuse with exact `ContextExpired`. Both native owner and
  original caller must still serve fresh typed business methods after failure.
- The focused `lifecycle` run passes **96 cases** in one process with the same
  four surviving caller runtimes: 16 Node kills, 16 graceful and 16 forced Node
  shutdowns, 16 V8 watchdog terminations, and 16 graceful and 16 forced V8
  shutdowns. Each group covers Rust/Python/V8/Node callers, local/signed-peer
  routing and before/after-sync checkpoints. The generalized harness retains
  every Node assertion and adds a fresh-method check for surviving callers.
  Graceful shutdown requires the normal drain proof; forced cutoff remains
  unclean with unknown effects, followed by physical cleanup. Native observation
  still freezes before injection and cannot complete or cancel business work.
- V8 engine and watchdog joins follow the owning bootstrap's return. Retained
  applications must then have zero invocation and byte reservations. V8 makes
  no process-group claim; Node still requires observed ESRCH after joining.
  Fresh isolates/workers and native owners recover exact prior state and verify
  two explicit same-ID retries without duplicate effects. The saved-report audit
  checks all four Cartesian matrices, actual authority/checkpoint snapshots,
  held/zero gauges, joined resources and cold receipts. It preserves the exact
  `9_007_199_254_740_993` value and every unknown-effect boundary.
- `.cellar/rpc/v8-lifecycle-matrix.json` contains 237,413 bytes, SHA-256
  `f491583159257eda806ac52251cc0a7ecc50f96e9e75ac99cc745b4a0cc8a487`;
  `/tmp/cellar-rpc-v8-lifecycle.log` records the run. Publication follows all
  native and caller-runtime joins, including Python's complete-drain check.
  Separate V8 watchdog and forced-shutdown smoke runs also pass. All 29 qualifier
  unit tests, all-target qualifier Clippy, strict fixture compilation/bundling,
  format/layout for 1,006 Rust files and docs with 747 local references pass.
  The native-only dependency graph remains engine-free at 465 packages.
- `.github/workflows/typed-rpc.yml` adds a focused Linux CI gate using locked
  fixtures, Rust 1.98.1, Node 24, GIL CPython 3.14 and protobuf 7.34.2. It checks
  the qualifier, runs the 96-case matrix and retains report/log artifacts. YAML
  parses locally; hosted execution is pending. Full-qualification flags remain
  false. The earlier complete 600-case report below remains tied to `e27ba6dd`;
  the expanded 14-suite `all` run was not repeated for these qualification-only
  changes. Python cancellation suppression, resource/schema bounds, remaining
  language/profile modes, packaging and performance qualification stay open.
- Main's streaming-example commit `9f8de83e` (#118) is integrated at `7f6d66ed`.
  It adds examples, documentation and binary registrations; RPC source, SDK code
  and lockfile inputs are unchanged from the 600-case implementation below.
  On the merged tree, combined adapter/qualifier all-target Clippy, all 29
  qualifier unit tests, the rebuilt executable and streaming JS/TS builds pass.
  Fresh `worker-failure-smoke` and `worker-shutdown-smoke` runs pass, including
  physical cleanup, unknown effects and cold receipt recovery. Reports are
  `.cellar/rpc/worker-main-failure-smoke.json` and
  `.cellar/rpc/worker-main-shutdown-smoke.json`. Format/layout check 1,005 Rust
  files, docs validate 746 local references and the native-only graph remains
  engine-free at 465 packages. The full 600-case run is tied to `e27ba6dd` below;
  it was not repeated for this examples-only merge.
- Owned Node lifecycle qualification adds 16 worker-SIGKILL cases and 32 active
  host-shutdown cases: four real caller backends, local/signed-peer routing,
  before/after sync, and graceful/forced shutdown. A disposable provider uses a
  separate Node runtime from every caller. Generated business calls enter an
  actual Rust child gate; the native provider turn observes count, receipt,
  transaction closure and WAL position before termination. No guest outcome
  claim or file marker establishes that checkpoint. Weak budget sources retain
  no native contexts; physical frame retirement and exact `ContextExpired`
  refusals are checked independently. Observation failures never complete or
  cancel business work, and observations freeze before the trigger.
- Worker death must return `Unavailable/unknown`, leave its native owner able
  to serve a fresh Rust method, and release the held native child. Graceful host
  shutdown must refuse fresh admission before a factory runs while allowing the
  accepted call to finish. A forced cutoff must retain an **unclean** drain proof
  with pending work at the cut and unknown effects; only subsequent physical
  frame retirement, zero bridge reservations and joined owned work establish
  cleanup. The strict graceful fleet-stop assertions remain unchanged. Every
  disposable worker group must report ESRCH after its owning runtime joins.
  A fresh native owner and fresh Node worker recover the same SQLite data and
  verify two explicit retries against the receipt without duplicate effects.
- `JavaScriptApplication::bridge_usage()` exposes native invocation/byte
  reservations and stop state for diagnostics. Fields are independently sampled;
  a stopped runtime alone does not prove cleanup. These gauges exclude heap,
  RSS and cached objects, so this slice does not close resource qualification.
  The pending-operation limit remains one in the new matrix.
- The initial forced-shutdown smoke failed strict graceful-stop validation.
  `/tmp/cellar-rpc-worker-shutdown-diagnostic.log` records pending application
  events/dispatches and `drain_timed_out: true`, with log/local durability closed.
  Native shutdown intentionally cuts stalled admitted work before closing and
  joining its execution domain. Forced cases now preserve that unclean report
  and independently audit cleanup after shutdown returns; they never relabel it
  as a successful drain. An earlier smoke accidentally used an old executable
  after a failed build; `.cellar/rpc/worker-smoke-pre-checkpoint.json` is
  quarantined and is not qualification evidence. Separate 16/32-case reports
  predate the final non-interfering observer hardening; the fresh combined run
  below qualifies that final source.
- The normal-stack run on implementation `e27ba6dd` passes all twelve suites:
  **600 cases**, preserving the previous 552 and adding 16 worker kills plus
  16 graceful and 16 forced host shutdowns. The saved report independently
  verifies every new caller/route/checkpoint/trigger combination, weak-authority
  expiry, held/zero reservation gauges, cold receipt recovery and owned worker
  group exit. All 48 killed owners and their groups exit; all 48 disposable
  provider groups also report ESRCH. Publication follows native and guest joins.
  `.cellar/rpc/worker-full-matrix.json` contains 1,014,205 bytes, SHA-256
  `ebf4a096069b089af74c054a9edc27c4754be1edc55a9704b942eb67b5b077d0`;
  `/tmp/cellar-rpc-worker-full.log` records the execution. Full-qualification
  flags remain false. This is local qualification; no new hosted CI pass is
  claimed.
- All 29 qualifier unit tests pass, including four new adversarial tests for
  complete native transaction/receipt checkpoints and conservative shutdown
  replies. Combined adapter/qualifier all-target Clippy, Node-only/V8-only
  Clippy, 30 JS runtime tests and live adapter regressions pass. The 65 RPC SDK
  and three streaming SDK tests, strict TS checks and fixture build pass.
  Format/layout check 1,001 Rust files; docs validate 745 local references;
  the native-only graph remains engine-free at 465 reachable packages.
- Main is integrated through `e2273a91` (#114/#116), retaining the incoming
  RustFS conversation/history changes and native SSE/WebSocket SDK support.
  Node keeps the RPC binary framing, cancellation controls and fixed failure
  categories; both runners retain streaming and WebSocket dispatch. The new
  streaming unit fixture uses the current metadata/body reply pair and races
  first-write readiness against dispatch completion instead of polling forever.
  Its backpressure/chunk assertions remain intact. The shared native streaming
  fixture uses the Rust 1.98 chunk API without changing its assertions.
- After integration, 13 shared-guest, 30 JS runtime and 29 Python library tests
  pass. Real V8/Node streaming/WebSockets, Node npm/native-addon compatibility,
  restart and both JS typed-RPC tests pass. Python's full process-level
  bridge/watchdog/recovery, startup, streaming and typed-RPC executables pass.
  All 25 qualifier unit tests, 42 native RPC unit tests, eight native integration
  tests and ten generator tests pass. Combined adapter/qualifier all-target
  Clippy and Node-only/V8-only Clippy pass with warnings denied. Format/layout
  check 995 Rust files; docs validate 744 local references; the native graph
  remains engine-free at 465 packages. The 65 RPC SDK tests, three streaming
  SDK tests, 34 Python codec/cancellation/failure tests, 24 runner tests,
  12 encoding tests, three build-identity tests, strict TS checks and fixture
  build pass. The agent app build and all 36 agent unit tests pass; agent sources
  match main exactly. Browser/RustFS live-store checks were not rerun here.
  The fresh normal-stack combined run on the merged implementation at `949ea5a1`
  passes all ten suites: 552 cases across 16 local pairs, 16 peer/handoff pairs,
  55 deadlines, 17 cancellations, 80 cycles, 96 provenance cases, 128 proof waits,
  32 failed-recovery cases, 48 observed owner kills and 64 proof/fencing cases.
  `.cellar/rpc/descriptor-main-full-matrix.json` is published after native and
  guest drains; all 48 owners and their worker groups exit. The execution log is
  `/tmp/cellar-rpc-descriptor-main-full.log`. The earlier intermittent failures
  do not recur, and their causes remain unestablished. Earlier evidence stays
  recorded below. Full-qualification flags remain false. Hosted CI is queued;
  no hosted pass is claimed.
- Descriptor admission now validates one global namespace, including packages,
  enum values in their enclosing scope, fields, oneofs and service methods.
  Every message/enum reference requires the defining file to be visible through
  local declarations, direct imports or public reexports. Import cycles, duplicate
  dependencies and invalid/ambiguous modifier indexes are refused. Symbol names,
  counts and aggregate expanded-name bytes are bounded before index-name
  allocation and message-subtree cloning. The
  [generator guide](../../tools/cellar-rpc-codegen/README.md#descriptor-admission)
  records the portable profile and its additional modifier restrictions.
- Eleven new native tests reproduce admission of malformed descriptors before
  the fix. All 42 native RPC unit tests now pass, including 15 descriptor tests;
  eight native integration tests, nine generator unit tests and the generator
  executable regression pass. Four real CLI refusals use no compilers on PATH
  and preserve both absent output and preexisting artifacts/user files. Native
  library/tests and generator all-target Clippy pass with warnings denied.
- Counter/profile regeneration reproduces all 24 artifacts. Native tests also
  verify exact descriptor bytes, digests and codec inventories after file-order
  changes. A separate three-file public-import example generates Rust, Python,
  TypeScript and JavaScript; strict TypeScript checks and real Node/Python
  message-descriptor/codec checks pass with `9_007_199_254_740_993`. These are
  generation/codec checks; public-import fleet calls remain open. Local raw evidence
  is under `.cellar/rpc/descriptor-admission-67284/` (`generation.log`, inputs,
  outputs and `summary.json`); native/codegen check logs are
  `/tmp/cellar-rpc-descriptor-*.log`. All full-qualification flags remain false;
  option/schema semantics, runtime allocation/RSS and lifecycle work remain.
- The earlier main integration through `2133d124` (#115) preserves copyright
  notices, including the one Node transport comment conflict. At the notice
  merge, all 974 checked runtime, SDK and fixture source files matched the
  ten-suite-tested `73a62b5a` after removing only notice text. New sources receive
  matching notices. The generator applies notices reproducibly while retaining
  producer comments; nine generator tests and generator Clippy pass, and both
  Counter/profile sets reproduce all 24 artifacts with unchanged descriptor
  bytes. SDK, Python sanitizer/runner, three build-identity tests, documentation
  and workflow parsing pass after the merge. That earlier ten-suite runtime
  evidence is from the implementation before the notice merge; it is not a fresh
  package/release qualification or a resolution of the transient failures.
- The earlier normal-stack serial run at `73a62b5a` passes all ten suites and publishes
  `.cellar/rpc/provenance-full-matrix.json` after native and guest drains:
  16 local pairs, 16 remote/handoff pairs, 55 deadlines, 17 cancellations,
  80 cycles, 96 provenance cases, 128 proof waits, 32 recovery cases, 48
  observed process kills and 64 proof/fencing cases. The provenance report
  contains all 96 unique combinations with fixed categories, unknown parent
  effects, exact i64 counts, live parent authority and two receipt retries.
  Full-qualification flags remain false. The earlier timeout/domain-closure
  failures do not recur; their causes remain unestablished.
- Serial regressions pass all 30 JS runtime tests, real V8/Node HTTP and restart,
  Node npm/native-addon compatibility and both JS typed-RPC tests. Python passes
  29 library tests, its full bridge/watchdog/recovery executable and typed RPC.
  Clippy passes in combined, Node-only and V8-only modes. Format/layout check
  986 Rust files, docs check 737 local references and the native dependency
  graph remains engine-free at 465 packages. Builds/tests finish before the
  serial combined executable starts; no policies or assertions are relaxed.
- T06's dedicated `provenance` suite passes 96 cases: all 16 caller/provider
  pairs, local/authenticated-peer parent calls and three post-commit failures.
  Each parent syncs its atomic exact-i64 count/receipt, then receives a real
  zero-timeout child refusal, throws a forged category/outcome, or throws an
  unrecognized private code. The parent retains storage authority after its
  child's refusal. Callers receive fixed categories with unknown effects;
  two explicit same-ID retries recover the saved count without another write.
  Native observations verify ingress, owner/scope/epoch, exactly four warmed
  factories and no constructed child. This is GIL CPython and locally proven
  child-refusal evidence; broader peer/admission provenance remains open.
- The initial test reproduces a code-preservation gap: Rust-to-Python receives
  `Internal/unknown` after the parent receives `DeadlineExceeded/not_committed`.
  A bounded category-only completion path corrects this in Python, V8 and Node.
  Native code independently recognizes only canonical categories and creates
  `Failure::new`, which cannot establish rollback. Exception text, outcome
  claims, accessors, proxy traps, non-scalar and oversized codes are refused or
  reduced to Internal. Ordinary HTTP failure behavior remains unchanged.
- The new sanitizer checks pass with all 65 SDK tests, 34 Python codec/client/
  cancellation tests, 24 Python runner tests and 12 shared-guest unit tests.
  Combined all-target Clippy and qualifier unit tests pass. Python CI includes
  the new sanitizer tests; the JS SDK test glob includes the real-runner checks.
- One concurrent validation attempt stops in cycle 28 with an unlocalized
  `execution domain is closed`, before publishing a combined report. The Node
  compatibility regression exceeds its unchanged 250 ms handler budget twice,
  including an isolated attempt. An interleaved differential then passes all
  three runs of the previous Node completion path and all three current runs;
  the serial JS runtime/HTTP/native-addon/restart/typed-RPC suite also passes.
  These passes do not establish the original causes. Node and cycle-stage
  failure context is added without relaxing deadlines or cleanup assertions;
  subsequent qualification runs serially after all builds/tests finish.
- Main is integrated through `1a1b0243` (#113), preserving the streaming
  React chat and agent-tool recovery work. The single workflow conflict keeps
  both the RPC fixture install/test/typecheck/build and UI install/build/browser
  checks, plus the agent tool-recovery step. The merged workflow parses and
  documentation still validates 735 local references. Main changes no Rust
  implementation, typed-RPC fixtures, SDK sources or Cargo dependencies: the
  nine-suite evidence below is from those identical RPC sources at `c381cb67`.
- The merged agent passes its locked build and all 28 unit tests; the UI passes
  its locked build and all six desktop/mobile browser tests. The native Node
  host builds and passes all six fixture-provider crash/cold-recovery boundaries,
  16 concurrent tasks, cold recovery and 32 unchanged duplicate results. The
  external-tool scenario passes six tools across two attempts with count 13,
  one external effect, two downstream requests and an unchanged duplicate.
  This is fixture-mode integration evidence, not live-model qualification.
  An initial parallel unit attempt reached the static page before UI assets
  existed and returned 503. Building the UI before unit tests, as CI specifies,
  passes all 28 assertions without changing code or weakening expectations.
- With the corrected crash observer, the normal-stack executable passes all
  nine suites and publishes `.cellar/rpc/cycles-full-matrix.json` only after
  native nodes and guest runtimes drain: 16 local pairs, 16 remote/handoff pairs,
  55 deadline cases, 17 cancellations, 80 cycles, 128 proof waits, 32 recovery
  cases, 48 observed process kills and 64 proof/fencing cases. All 80 report
  combinations are unique; native retirement/revocation and fresh A/B counts
  are verified. Full-qualification flags remain false. T06 provenance is next,
  with broader lifecycle, resources, interpreter and delivery gates still open.
  Format/layout check 982 Rust files, docs check 735 local references and the
  native graph stays engine-free at 465 packages. All 25 qualifier unit tests,
  63 SDK tests, 32 Python codec/cancellation tests, strict TS checks and affected
  all-target Clippy pass. The unrelated prior watchdog failure does not recur;
  its cause remains unestablished.
- The first nine-suite attempt passes local/remote calls, 55 deadline cases,
  17 cancellations, all 80 strengthened cycle cases, all 128 proof-wait cases
  and all 32 recovery cases. It then fails the first crash case at the owned
  process-group observation with `EPERM`. No success report is published.
- A minimized real-child probe returns `EPERM` for all 25 exited/unreaped groups;
  100 already-reaped groups return `ESRCH`. The new native regression fails
  against the original observer and passes after the correction. Group cleanup
  now keeps permission denial unproven within the same five-second bound and
  succeeds only when both owned groups report `ESRCH`. Live/unreaped children
  cannot pass cleanup, and broadcast/current-group selectors are rejected.
  Apple’s [signal implementation](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/kern_sig.c)
  is consistent with the observed zombie-group result. The original failing
  run did not retain which group was involved; the deterministic probe establishes
  the observer bug without claiming a surviving worker was present in that run.
- All 25 qualifier unit tests pass with the crash-observer correction, alongside
  all-target Clippy for the qualifier and the JavaScript/Python/shared guest
  crates. The real SIGKILL status, both group-absence checks and five-second
  bound remain required; no permission error counts as proof of process exit.
- The named `cycles` matrix passes all 80 cases, both in its dedicated run
  and as a stage of the combined run: four self-call providers and all 16
  A-to-B-to-A provider pairs, local/split signed-peer placement and explicit
  signal/original deadline. The strengthened combined stage checks distinct
  invocation authority even when outer and inner share a cell, inherited native
  budgets, exact entered-frame inventories and stable owner/scope/epoch.
- Each cycle frame syncs its atomic Counter receipt before awaiting its child.
  The native caller joins cancellation/deadline with unknown effects, then stays
  live through the cleanup audit. Every native frame retires and its captured
  budget independently refuses authority; original Python/V8/Node continuations
  refuse late storage/read/RPC work. Both A and B admit fresh mutations, counts
  preserve the earlier commits, and exact native entry counts reject automatic
  mutation replay. Python intentionally suppresses cancellation until an explicit
  supervisor release. No guest task is detached; JS keeps one pending SDK slot.
  GIL CPython is covered; broader T14/T17 and interpreter/resource gates remain.
- The cycle slice passes 63 SDK tests, 32 Python codec/client/cancellation tests,
  23 qualifier unit tests, strict TS API checks and all-target Clippy for the
  qualifier, JavaScript, Python and shared guest crates with warnings denied.
  Existing vendored Raft warnings are unchanged. The executable is not rebuilt
  while its combined suite or disposable crash-owner processes are running.
- The combined normal-stack executable passes all eight suites and publishes
  `.cellar/rpc/proof-wait-full-matrix.json` only after native nodes and all guest
  runtimes drain: 16 local pairs, 16 remote/handoff pairs, 55 deadline cases,
  17 explicit cancellation cases, 128 proof-wait cases, 32 failed-recovery
  cases, 48 process kills and 64 proof/fencing cases. All proof-wait rows include
  the final fresh-call authority check. Full-qualification flags remain false;
  T15 cycles, provenance and remaining T14/T17/resource/interpreter gates remain.
  Final checks pass in combined, Node-only and V8-only Clippy modes. Format and
  layout check 978 Rust files; documentation checks 735 local references; the
  native-only dependency graph remains engine-free at 465 packages.
- The strengthened `proof-wait` stage passes all 128 cases in the combined run:
  two triggers (signal/deadline), two routes (local/signed peer), two phases
  (returned handler/output proof or suspended sync), and all 16 Rust/Python/V8/Node
  caller/provider pairs. Peer and bucket data proof remain held while the
  source native operation settles and a fresh RPC is admitted. JS has one
  pending operation slot. Another fresh RPC after the target's late-work audit
  proves the original caller's authority stayed usable throughout cleanup.
- Sync cases resume the original callee continuation and refuse retained
  storage/read/RPC work. Native callees also retain a pre-cancellation budget:
  its direct clock/revocation check is independent of the control request's
  executing turn, avoiding a foreign-turn false positive. Gates open only after
  these checks. Two explicit same-ID retries recover the atomic receipt and
  exact i64 count with stable owner/scope/epoch; no mutation retry is automatic.
- A separate five-case `proof-wait-smoke` diagnostic passes both native causes
  and all guest caller forms. Its report remains `qualified: false`. The full
  matrix validator rejects missing/duplicate combinations, optimistic outcomes,
  missing proof paths, weak authority checks and incomplete cleanup evidence;
  both adversarial validator tests pass. All 63 SDK, 22 Python codec/client and
  ten Python cancellation tests pass, alongside the JS HTTP/runtime/restart and
  full Python bridge/watchdog/typed-RPC executables. All-target Clippy passes for
  the qualifier, both adapters and shared guest crate.
- One first 128-case attempt stops in warmup for its 83rd case with a Node
  watchdog timeout while the target is V8. The old message cannot distinguish
  worker silence from delayed host observation. Bounded diagnostics now report
  heartbeat/observer gaps, non-consuming input readiness and active counts.
  The two-second policy is unchanged. The observed rerun passes all 128 cases;
  process memory snapshots do not grow. No cause or fix is established by that
  retry. The strengthened combined run also passes all eight suites, including
  all 128 proof-wait cases, without reproducing that watchdog failure.
- The first combined attempt catches a fixture-local `deadline` Boolean that
  shadows Python's deadline module. Renaming it to `deadline_trigger` restores
  the existing 55-case deadline suite. No runtime guard or assertion is relaxed.
  Non-consuming readiness checks also wait for actual socket arrival; a local
  write does not imply immediate nonblocking readability.
- The branch now incorporates main through `3308a1ac`, including the durable
  Vercel AI SDK agent demo. The JavaScript workflow retains both the RPC fixture
  and agent installs/builds/tests. On the merged branch, all seven normal-stack
  suites pass again: 16 local pairs, 16 remote/handoff pairs, 55 deadline cases,
  17 cancellation cases, 32 failed-recovery cases, 48 process kills and 64
  proof/fencing cases. `.cellar/rpc/guest-deadline-merged-full-matrix.json` is
  published after native/guest drains; full-qualification flags remain false.
  All-target qualifier/JS/shared-guest Clippy passes. Format/layout check 974
  Rust files, docs check 735 local references, and the native graph remains
  engine-free at 465 packages. The merged workflow parses and retains both
  RPC fixture checks and the new agent's process-recovery step.
- Main's agent also passes its locked build, 13 unit tests and all six
  fixture-provider SIGKILL/cold-recovery boundaries on this branch. Its production
  entry passes 16 concurrent tasks, cold recovery and 32 unchanged duplicate
  results. `.cellar/rpc/guest-deadline-agent-merge.json` records seven fixture
  scenarios. This preserves main's integration behavior; it is not live-model,
  throughput, or typed-RPC example-packaging qualification.
- With the expanded deadline suite, the combined normal-stack executable passes
  all seven suites and publishes `.cellar/rpc/guest-deadline-full-matrix.json`
  after native and guest drains: 16 local pairs, 16 remote/handoff pairs, 55
  deadline cases, 17 cancellation cases, 32 failed-recovery cases, 48 process
  kills and 64 proof/fencing cases. All 63 SDK tests, 22 Python codec/client
  tests, strict JS/TS API/build checks, real Node HTTP/native-addon/restart and
  typed-RPC tests, and full qualifier all-target Clippy pass. Format/layout
  check 973 Rust files; docs check 734 local references; the native dependency
  graph remains engine-free with 465 packages. Full-qualification flags remain
  false. This evidence precedes integration of main's new Vercel agent example.
- The expanded signed two-node deadline suite passes 55 cases: three original
  native cases, 12 Python/V8/Node local/remote/receiver-cap/nested cases, 36 remote
  codec failures across all four providers and four real Node control-queue
  failures. `.cellar/rpc/guest-deadline-matrix.json` is published only after both
  native fleets and guest runtimes drain. Full-qualification flags remain false.
- Nested guest providers spend 120 ms in verification before requesting a
  60-second child timeout. Child entry records its actual native budget: the
  first run observes 255–277 ms remaining under a 400 ms parent. Assertions
  require at least 80 ms consumed, plus fixed deadline outcomes, late native
  binding/storage/write/commit refusal, expired guest storage/cached-client/new
  client refusal, zero late writes and usable fresh admission. Actual owner,
  scope and epoch stay unchanged; caller cancellation alone cannot explain the
  measured child cap. Remote decode expiry preserves one saved increment and
  explicit receipt retries; pre-submission expiry admits zero increment handlers.
- The first expanded-fixture attempt fails because warmup also consumes the
  bounded 16-entry followup audit. Warmup now only establishes ownership; the
  fresh-admission check remains required for every deliberately timed call.
  The audit bound and all deadline assertions are unchanged. The fixture retains
  ingress URL/error context for unexpected failures. This is a fixture correction,
  not evidence of a runtime defect or a relaxed qualification gate.
- With V8/Node codec budgets, the combined normal-stack `--suite all` passes
  all seven suites and publishes `.cellar/rpc/js-budget-full-matrix.json` only
  after native nodes and guest runtimes drain: 16 local pairs, 16 remote/handoff
  pairs, three native deadline cases, 17 cancellation cases, 32 failed-recovery
  cases, 48 observed process kills and 64 proof/fencing cases. Full-qualification
  flags remain false. Format/layout check 972 Rust files, documentation validates
  734 local references, and the native-only dependency guard resolves 465
  packages without a JavaScript engine.
- V8 and Node generated calls now start a private budget before codecs run.
  Options are captured before Node's first asynchronous wait; even a throwing
  option getter joins and releases its original control. Verification, encoding,
  native admission/queueing/transport/proof and response decoding share one
  native-owned deadline, capped by the original host and incoming parent.
  The opaque budget travels separately from JSON/business bytes, and native
  refusals retain their outcome. SDK-detected expiry after native success is
  `DeadlineExceeded/unknown`; no retry is automatic.
- V8 reads the original native monotonic clock through private callbacks. Node
  obtains an invocation-scoped native handle through bounded IPC, charges the
  control round trip and one clock tick conservatively, and forwards a ceiling
  relative to that handle's original start. No absolute timestamp crosses clock
  domains. The effective Node window may be shorter than requested. Controls
  use a separate per-invocation bound, no SDK mutation slot, and are reclaimed
  on release, completion and retirement. Foreign replay is refused while
  foreign cleanup can only release the original capability.
- The initial four JS deadline regressions fail before the fix. All 63 SDK
  tests now pass, including five deadline tests and four tests of the actual
  Node control helper. They cover pre-codec expiry, time subtraction, default
  and requested/parent caps, expiry around decoding, options captured before
  the Node wait, foreign scope, capacity reuse and retirement cleanup.
- The real Rust/V8/Node executable passes all nine caller/provider pairs and
  36 slow verification/encoding/decoding probes across two native starts.
  Pre-submission expiry leaves count zero; decode expiry follows one saved
  increment, and explicit same-ID retries recover its original receipt across
  restart. Real incoming parent caps, foreign-budget refusal and separate
  control capacity pass with one pending SDK slot. The 30 JS library tests,
  Node HTTP restart/native-addon tests, strict API/build checks, and all-target
  Clippy in combined, V8-only and Node-only modes pass. These codec probes are
  local evidence; the signed two-node slice above adds guest remote/nested cases.
- The budget branch now incorporates main through `f0414a3d`, including Python
  lifecycle hardening, bounded replication metadata and the console move.
  Conflict resolution preserves explicit RPC dispatch alongside main's direct
  non-RPC event dispatch. Post-merge checks pass 24 Python runner tests, 34 pure
  core tests, 29 Python library tests, the full bridge/watchdog executable and
  the real Rust/Python budget/restart test. Adapter/qualifier all-target Clippy,
  strict Python API checks, format/layout, dependency and docs checks pass.
  The combined normal-stack executable also passes all seven suites and
  publishes `.cellar/rpc/python-budget-merged-full-matrix.json` after cleanup:
  16 local pairs, 16 remote/handoff pairs, three deadline cases, 17 cancellation
  cases, 32 failed-recovery cases, 48 process kills and 64 proof/fencing cases.
  Full-qualification flags remain false. This predates the JS budget slice above.
- With Python codec budgets, the combined normal-stack `--suite all` retry
  passes all seven objects: 16 local pairs, 16 remote/handoff pairs, three
  deadline cases, 17 cancellation cases, 32 failed-recovery cases, 48 process
  kills and 64 proof/fencing cases. It exits successfully and publishes
  `.cellar/rpc/python-budget-full-matrix.json` after runtime cleanup. The
  initial OS-denied attempt is retained below; the successful retry does not
  establish its cause. Full-qualification flags remain false.
- Python generated calls now mint an opaque native `CallBudget` before request
  validation, live codec checks and encoding. `BudgetSource` captures the
  original native execution-domain clock, host policy and incoming RPC cap.
  Limits can only shorten the original start; the same deadline follows the
  owned bytes through native admission, queueing, proof and guest decoding.
  No guest timestamp enters metadata. Sources keep only weak I/O ownership and
  an independent cancellation flag; clock reads cannot become the last owner
  responsible for native I/O cleanup on a guest thread.
- Five requested/default/parent-budget failures reproduce before the Python
  fix. The expanded 22 codec/client tests now pass, including zero-budget
  refusal before any codec, remaining-time subtraction, late-delivery refusal
  before decoding and post-decode unknown outcomes. The real Rust/Python test
  passes 12 slow verification/encoding/decoding probes across both providers
  and two node starts. Pre-submission expiry leaves count zero; decode expiry
  follows one saved increment and same-ID retries recover its receipt without
  another write, including after restart.
- The real host also reads a captured budget clock on a thread without an
  installed execution domain, refuses an expired budget at native admission,
  preserves a real incoming 1,000 ms parent cap against a requested 60 seconds,
  and refuses cross-invocation budget replay as `ContextMismatch/not_committed`.
  The bridge keeps budgets out of JSON and gives them no separate mutation or
  operation slot. Python's 29 library tests, ten cancellation tests, strict
  generated API checks and existing HTTP/lifecycle/remote/handoff/watchdog
  bridge executable pass. Native RPC's 50 tests, restart/deadline executable
  and seven profile tests pass; current Clippy, format/layout, dependency and
  docs checks pass. This Python evidence predates the JS budget slice above.
- One combined run stops at the first `before_handler rust->rust` process-crash
  case with OS `EPERM`, after the earlier suites and all 32 recovery cases pass.
  The isolated crash rerun passes all 48 cases. The first error lacks enough
  context to distinguish spawn, signal or group observation; permanent error
  context now identifies those operations. No process permission or cleanup
  checks are bypassed, and no cause or fix for that OS denial is claimed.
- After integrating current main, `--suite all` passes again with normal stack
  settings: 16 local pairs, 16 remote/handoff pairs, three deadline cases,
  17 cancellation cases, 32 failed-recovery cases, 48 process kills and 64
  proof/fencing cases. The executable exits successfully and publishes
  `.cellar/rpc/pr-full-matrix.json` after runtime cleanup. Full-qualification
  flags remain false; the ordered follow-ups below are still required.
- The implementation branch incorporates `origin/main` at `e0035689`, including
  Python completion-handoff telemetry and log shutdown publication fixes.
  Post-merge checks pass 29 Python library tests, 21 qualifier tests, 54 JS SDK
  tests, 10 Python cancellation tests and 17 Python codec/client tests. Both
  contracts regenerate all 24 checked-in artifacts byte-for-byte; JS/TS plugin
  output now has one final newline. Strict TS checks/build and all-target Clippy
  for the generator, qualifier and affected adapters pass. Format/layout check
  964 Rust files; the native dependency guard and its three regressions pass.
- CI prepares the pinned Python codec dependency before the new typed-RPC test
  and builds RPC JS fixtures before adapter tests. Native workspace test/doc/lint
  selections exclude the opt-in full-runtime qualifier, preserving their engine
  boundary. Both workflow files parse and the actual Cargo tree for that native
  selection contains neither V8 nor the JS adapter. These local checks do not
  establish a passing hosted CI run or complete clean-machine qualification.
- After JS/TS controls, the combined `--suite all` passes with normal stack
  settings and publishes all seven objects after node/runtime drains: 16 local
  pairs, 16 remote pairs before/after handoff, three native deadline cases,
  17 cancellation cases, 32 failed-recovery cases, 48 observed SIGKILL cases
  and 64 proof/fencing cases. Cancellation includes five Rust callers and four
  each for GIL CPython, embedded V8/TypeScript and real Node/JavaScript.
  Every probe refuses late storage/commit work; guest cases refuse foreign
  SDK authority and admit a fresh RPC after cleanup. Full qualification flags
  remain false; these are the documented fixtures, not T01-T19 completion.
- This JS slice passes all-target Clippy for both JS backends, the shared guest
  crate and the full-runtime qualifier with warnings denied. Format and layout
  check 960 Rust files; docs validate 734 local references. The native dependency
  guard resolves 465 reachable packages without a JS engine and its three
  regressions pass. The only added npm dependency is a pinned build/application
  fixture for the closed V8 bundle; the core SDK remains dependency-free.
- JS/TS generated clients now accept `RpcAbortSignal` with `timeoutMs`. The
  shared type requires no DOM libraries and accepts native Node signals and the
  pinned bundled V8 AbortController implementation. Controls are captured once;
  cancellation is checked before encoding/submission and around reply decoding.
  Reasons never enter metadata. Native outcomes remain authoritative;
  late-reply cancellation stays unknown.
- Each JS RPC admission creates a private cancellation capability for that
  operation. A shared signal can fire from another continuation, but cannot
  grant that continuation new SDK authority. V8 binds the capability's native
  callback data to one pending operation; Node captures the original operation
  identity and sends a dedicated bounded control. Both retain existing native
  futures/reservations and wait for their acknowledgement. Completed/retired
  operations remove the capability and signal subscription.
- `--suite cancellation` passes 17 cases: five native caller cases and four
  each for Python, embedded V8/TypeScript and real Node/JavaScript callers.
  Eight new JS cases cover local, signed remote, nested and earlier synced
  writes. Controllers fire from a different HTTP invocation. Foreign raw RPC
  admission is refused; the original parent remains live through late SDK
  attempts; a fresh RPC is admitted after cleanup. These guest callers target
  Rust providers, not the full provider/proof-wait matrix.
- V8/Node runtime controls pass with one total/outstanding SDK operation slot:
  cancellation needs no second operation, byte ownership survives until native
  acknowledgement, and return/retirement reclaim all byte permits and listeners.
  The full JS library run passes 30 tests. SDK tests first reject native signals
  as `InvalidArgument`; the new pre-aborted call now returns `Cancelled` before
  encoding. Synthetic-event tests exposed and fixed a consumed-listener bug.
  Native composed/timeout signals, protected Node delivery, listener cleanup and
  late-reply cancellation are covered; all 54 SDK tests and JS/TS API/build
  checks pass. HTTP/runtime and named-method restart regressions also pass.
- Python task cancellation now reaches an invocation-scoped native AbortSignal
  through a dedicated control, without another SDK operation or mailbox permit.
  The original native future retains its controller, mutation and byte guards;
  the task waits for its acknowledgement even under repeated cancellation.
  Completion/drop removes the control without relying on Python callbacks.
- The expanded `--suite cancellation` passes nine cases: the existing five Rust
  cases plus four GIL CPython caller cases (local, signed remote, nested and an
  earlier synced write). Before the fix, the live-parent test reports all four
  late SDK checks still allowed; after it, all are refused. The parent stays
  live through those attempts. Foreign-invocation cancellation is rejected and
  a fresh RPC is admitted after cleanup. This is Python caller-to-Rust provider
  evidence; it does not qualify every provider, interpreter mode or proof wait.
- In the earlier Python-only control slice, `--suite all` passed with normal stack
  settings and publishes all seven evidence objects after node/runtime drains:
  16 local pairs, 16 remote pairs before/after handoff, three native deadline
  cases, nine cancellation cases (five Rust and four GIL CPython callers),
  32 failed-recovery admission cases, 48 observed SIGKILL cases and 64
  proof/fencing cases. Earlier synced writes survive; every late write/commit
  in the cancellation probes is refused. Full qualification flags remain false.
- Ten Python cancellation tests cover repeated cancellation, already-ready
  success/failure, retirement, cancellation before the first poll, timeout and
  TaskGroup cancellation, and unchanged non-RPC cancellation behavior. They
  require an empty pending set and no unobserved asyncio failures, including
  a ready failure racing retirement. Seventeen generated Python codec/client
  tests and 29 Rust Python-library tests pass, including three operation-control
  registry tests. The full embedded Python bridge executable and mixed
  Rust/Python typed-RPC restart executable also pass.
- That Python slice passed all-target Clippy for `cellar-guest`, embedded
  `cellar-python` and the full-runtime qualifier with warnings denied. Format
  and layout check 959 Rust files; docs validate 732 local references.
  The native dependency guard still resolves 465 packages without a JS engine,
  and its three regression tests pass. Earlier native library/all-test Clippy
  evidence remains unchanged.
- Native RPC cancellation now has a monotonic, host-owned authority flag in
  the original I/O context, installed before provider construction/first poll.
  Caller signals are watched outside application turns; native and embedded-host
  paths retain their original request cancellation guards. Pending local
  descendants inherit revocation; source host waits propagate cancellation
  over signed peer aborts without waiting for a blocked parent application poll.
  The private capability is not caller-provided JSON or rollback evidence.
- The native `--suite cancellation` fixture first fails with all four late SDK
  operations still authorized. It now passes five cases: local signal, remote
  signal, dropped remote call, cancelled nested child and a write synced before
  cancellation. Each cancellation follows the observed target transaction
  checkpoint. An 800 ms blocked poll stays below the 5,000 ms request deadline;
  fresh calls confirm late writes/transactions do not persist and an earlier
  synced write does persist. Owner, scope and epoch observations stay stable.
  Nodes drain on success and audit failure. Guest API controls and full T14/T17
  qualification remain open.
- Nine cancellation-authority tests cover descendant/sibling boundaries,
  cancelled registration races, iterative 20,000-scope revocation/drop,
  registration cleanup, wake registration, pre-submission operation drop,
  permanent authority after wake consumption and reused request IDs. Actual
  RPC reply completion detaches its caller link: parent retirement/late abort
  cannot cancel valid callee background work; final driver retirement revokes it.
  The selected cancellation/lifecycle run passes 25 tests; the RPC run passes 50.
- The real native input-gate test now cancels an RPC behind an unrelated live
  critical section and observes its actual `Bytes` payload owner drop before
  opening the gate. Business entry counts stay unchanged. Worker/input-gate
  queues wake from RPC revocation and retire their original driver lifetime.
  Existing HTTP abort delivery survives this change. Native concurrency,
  restart/deadline and all seven generated-profile tests pass.
- The first native run after adding source-side cancellation exposed a default
  Tokio worker stack overflow in nested RPC dispatch futures. The macOS crash
  trace and disassembly identify the dispatch poll frame; a larger-stack run is
  diagnostic only. Boxing the routed operation/native inner invocation removes
  those large future copies from outer select frames. All five native cases
  pass with normal stack settings; no production stack-size increase is used.
- After peer deadline forwarding, the combined `--suite all` executable passes
  and publishes all six evidence objects only after node and parent-runtime
  drains/joins: 16 local pairs, 16 remote pairs before/after handoff, three native
  deadline cases, 32 failed-recovery admission cases, 48 observed SIGKILL cases
  and 64 proof/fencing cases. The 49 RPC unit tests, both peer-tunnel tests,
  11 event-wait tests, native restart/deadline test and seven profile tests pass.
  Clippy passes with warnings denied for the native library/all tests and all
  targets of the full-runtime executable. The native dependency guard still
  resolves 465 packages without a JS engine; its three regressions pass.
- Authenticated peer RPC now transmits a remaining duration immediately before
  sending, refreshes it after tunnel establishment/provably unsent retries, and
  caps it by receiver policy before body collection. Receiver upload, routing,
  handler and proof waits share that immutable local deadline; independent host
  timers own cancellation. A paired host status/header returns only the fixed
  `DeadlineExceeded/unknown` code and never creates retry/rollback evidence.
- The new native `--suite deadline` executable reproduces expired remote SDK
  authority before the fix and passes three real two-node cases afterward:
  a 400 ms direct call whose poll blocks 800 ms, a nested call requesting 60
  seconds whose child still expires with its 400 ms parent, and a 5,000 ms call
  capped by the receiver's 1,500 ms policy during a 2,200 ms poll. Actual owner,
  scope and epoch remain stable. Each expired handler refuses child binding,
  new storage access, cached writes and transaction commit; a fresh invocation
  confirms rollback, zero late writes and usable storage. Node execution domains
  start at different times, so source timestamps cannot be copied as deadlines.
  Both nodes drain even on audit failure. This is native deadline evidence,
  not full guest cancellation or dependency-cycle qualification.
- Six native deadline unit tests cover canonical/malformed/duplicate durations,
  receiver clock/policy caps, overflow, paired timeout markers, conservative
  outcomes, expired queue admission, pending-operation drop and synchronous
  overrun. Both peer transport tests pass: a real Hyper unsent request retains
  its identity/body while its budget shrinks, and a signed tunnel with no caller
  timer refuses malformed/zero budgets and expires an unfinished upload without
  routing it. The fixture retains its actor sender through the empty-queue audit;
  an early upload refusal is allowed to close the tunnel without ending that
  audit. Eleven existing event-wait tests pass, preserving their separate policy.
- `cargo test --locked -p cellar --lib rpc`: 50 RPC-related unit tests pass.
  `cargo test --locked -p cellar --test native_typed_rpc --test typed_rpc_profile`
  also passes the real-node test and seven generated-profile/vector tests.
  The node test runs two startup/shutdown
  cycles, recovers the same counter and receipt, verifies exact i64 values above
  JavaScript's safe-number range, rejects conflicting IDs and overflow, refuses
  retained contexts and already cancelled/expired calls, and enforces the deadline
  after a deliberately slow synchronous response decoder. Unknown methods,
  mismatched contracts and malformed requests are refused before the business
  factory runs, with trusted `not_committed` outcomes.
- Native local admission now carries the source typed call's absolute deadline
  into the target invocation. The original start precedes Rust graph sizing and
  encoding; nested calls take the minimum of requested, host and inherited
  budgets. Two unit tests cover deadline minima, zero and overflow. The real-node
  test reproduces a blocked target retaining authority after its caller times
  out, then verifies child binding, new storage access, cached storage writes and
  transaction commit are refused. A new invocation on the same cell confirms the
  expired transaction rolled back, no late write persisted and storage remains
  usable, before and after restart. The cached-handle regression also fails with
  a Context-only deadline check; the budget now belongs to the shared I/O context.
  Aborting after creating a call but before its first poll is refused with
  `Cancelled/not_committed`. This establishes native local boundaries only;
  peer duration forwarding and broader guest cancellation remain open.
- After the local deadline change, the combined `--suite all` executable passes
  again and publishes all five evidence objects after runtime drains/joins:
  16 local pairs, 16 remote pairs before/after handoff, 32 failed-recovery
  admission cases, 48 observed process-crash cases and 64 proof/fencing cases.
  The 42 native RPC unit tests, native restart/deadline test and seven profile
  tests pass. Clippy passes with warnings denied for the native library and
  affected integration targets, plus all targets of the full-runtime executable.
  The dependency guard still resolves 465 packages without a JavaScript engine;
  its three regressions and the documentation/layout checks pass.
- `cargo test --locked -p cellar-guest --lib`: 10 tests pass, including raw RPC
  byte charging and conservative failure outcomes; existing HTTP charging tests
  still pass. The operation is connected to Python and the V8/Node binary paths;
  all backends now exercise generated named service clients end to end locally.
- The generator produced the checked-in descriptor, JSON inventory, Rust
  client/handler/registration and Python message/stub/service files. Fresh
  `--languages rust,python,typescript,javascript` generation reproduces all 24
  Counter/portable-profile artifacts byte-for-byte, including JS declarations
  and TS service code.
- Nine generator tests pass, including language-name/helper collisions,
  statically importable Python module paths, typed message-initializer mappings
  and the reused repository formatting policy.
- The portable-profile fixture generates all scalar wire types, enum numbers,
  nested/recursive messages, optional presence, real oneofs, maps and
  `Empty`/`Timestamp`/`Duration`. Native, JS and Python codec checks preserve
  scalar boundaries, Unicode/BOMs, maps and selected empty variants. Native
  services compile/register nested and well-known method types through the SDK
  re-exports. A valid `Empty` method exposed a Rust generator panic; qualified
  trait decoding fixes it. These are codec/registration tests, not additional
  durable fleet evidence.
- Native descriptor tests mutate the protoc-built profile to refuse illegal
  map keys/entry shapes, foreign/orphan map entries, malformed synthetic oneofs,
  enum alias/reservation failures and field reservation/packing failures.
- `cargo test --locked -p cellar-python --features embed --test typed_rpc`
  passes with CPython 3.14.6 (GIL build) and Protobuf 7.34.2. A mixed application
  runs Rust→Rust, Rust→Python, Python→Rust and Python→Python calls through real
  native nodes across two restarts. It verifies exact i64 values above 2^53,
  duplicate receipts, conflicting operation IDs, retained-context rejection,
  cold-constructor refusal and a committed mutation whose invalid Python reply
  becomes `InvalidResponse/unknown`. A duplicate after restart returns its saved
  result without applying another mutation. This is local restart evidence,
  not abrupt-crash or remote-fleet qualification.
- Seventeen Python codec/client tests pass, covering optional presence, i64 bounds,
  pre-serialization graph/map budgets, owned bytes, wrong input/timeout refusal,
  expired clients and malformed replies with unknown effects. The existing
  Python runner's 22 tests and embedded library's 26 unit tests also pass.
  Shared wrapper/Struct vectors preserve actual wire bytes and decoded values;
  reducing depth, element or decoded-slot budgets below the vector's boundary refuses
  encoding. Generated initializer typing qualifies built-ins so a legal `object`
  field does not mask the mapping's value type; strict mypy passes after fresh
  generation.
- Generated Python services use static imports and a typed `cellar_rpc` stub.
  Mypy 2.4.0 in strict mode checks Counter/profile services and concrete async
  result assertions, including nested/well-known types. Wrong requests, handler
  signatures, result assignments and undeclared helpers must produce actual
  errors. Standard generated message-initializer mappings are parameterized;
  no missing-generic exception is needed. This does not qualify every SDK module
  or free-threaded Python.
- Python's full real-node `bridge`, isolated `startup` and `typed_rpc` executables
  pass. The existing bridge harness covers HTTP, storage, SQL, alarms, authority,
  cancellation, remote routing, handoff, resource limits, watchdog owner loss and
  shutdown. Those are adapter regression results; the new named RPC fixture is
  still qualified only locally with orderly node restarts.
- `cargo test --locked -p cellar-javascript --features embed,node --lib
  --test typed_rpc --test node --test node_runtime` passes 27 library tests, two
  named-RPC integration tests and both real-host
  HTTP/storage/restart tests. Runtime-only RPC fixtures verify raw attachments
  through Start/Operation/Reply/Complete in both backends, exact typed-array
  slices, mutation after submission, empty results and reservation cleanup.
  The runtime-only fixtures deliberately use identity codecs. Separately, the
  named-RPC test runs all nine Rust/V8/Node pairs using real generated Protobuf
  services and SQLite, checks exact integers/Unicode/BOM receipt identities and
  repeats after native-node restart. It refuses private helpers before business
  construction and rejects post-commit wrong response types with unknown effects.
- The opt-in `cellar-rpc-qualification --features full` executable starts Python,
  V8 and Node together and composes four native providers. All **16 local pairs**
  pass generated named calls, business refusals, duplicate/conflicting receipts,
  exact i64 values, Unicode/BOM identities, expired clients, cold refusals and
  post-commit invalid replies. Two native-node starts recover acknowledged state
  and saved results. Guest runtimes remain alive across those node starts; this
  does not qualify process crash, owner transfer or remote routing. The report
  `.cellar/rpc/local-matrix.json` is written only after every runtime joins and
  Python reports a complete drain. A failed rerun removes the prior report.
- `--suite remote` runs all 16 pairs across real signed peer RPC with fleet
  durability enabled, a shared development bucket and distinct native node
  directories. Owner A is seeded before ingress B joins. After A drains with
  `Handoff`, ingress C repeats the matrix against B. Native observations prove
  actual ingress/execution nodes, all 16 remote pairs, stable scope identity and
  advanced owner epochs. Receipts replay their saved results and acknowledged
  counts recover after transfer. Two guard tests reject local/incomplete or
  cross-domain/scope/epoch observations. The same business corpus includes private
  method refusals and post-commit invalid replies; the observation proves remote
  execution of registered business methods, not a remote hop for every cold
  refusal. No user payload is inspected by the observer.
- The remote suite races eight distinct operations per provider across the four
  callers, checks the exact delta sum and replays each receipt to detect mixed
  results. Eight same-ID racers per provider return one saved value, apply one
  mutation and preserve changed-delta conflicts. Observed concurrent work stays
  on the transferred owner's scope/epoch. The local and remote suites use five
  native starts in the supervised parent process. `--suite all` also runs
  the deadline, cancellation, recovery, crash and proof suites below; `.cellar/rpc/full-matrix.json` contains all seven evidence
  objects and is published after complete parent runtime drains. Parent guest
  runtimes stay alive across their native node starts; each crash owner has
  separate runtimes. Broader fleet qualification remains open.
- The initial `--suite crash` added 32 separate owner-process SIGKILL cases: all 16 pairs
  before business handlers and all 16 after atomic count/receipt writes, awaited
  sync and a validated typed reply held before host output. Each case observes
  owner/class/scope/epoch and stored state at its barrier, verifies an unknown
  original-call outcome with no silent survivor replay, recovers on the surviving
  node and explicitly retries the same ID. Count/receipt replay, changed-delta
  conflicts, exact i64 totals and advanced owner epochs pass. Read-only routing
  refusals are polled with a finite recovery bound; mutations are never polled.
  The native owner exits with SIGKILL; its independently grouped Node worker is
  identified from the fixture and must exit after IPC loss. Both groups are
  observed gone. Surviving nodes drain and the parent runtimes join before
  publication. The guard requires both points for every pair and rejects
  incomplete/duplicate cases, rollback claims and unproven termination/replies.
  This covers pre-handler and synced-handler/reply-loss boundaries with normal
  GIL CPython on macOS. Native transaction midpoints, live owner fences and proof
  failures have additional coverage below; broader recovery and free-threaded
  qualification remain. `abrupt_crash_qualified` stays false.
- Embedded V8 now installs native-backed text encoding before application
  descriptors execute. Tests cover UTF-16 replacement/encodeInto counts, labels,
  BOMs, strict errors/reset, incremental UTF-8/Shift_JIS and safe decoder overload.
  Decoder handles follow weak JS function owners and have a separate finite cap.
- Node frame tests cover fragmented headers/metadata/body, coalesced frames,
  empty-vs-missing attachments, announced lengths, contradictory presence,
  outbound budget refusal and disconnect during each frame region. Retained
  outbound frame allocations stay charged until the entire frame is released.
- Six Node codec/input tests pass; generated message declarations and a negative
  i64-number type assertion pass strict TypeScript checks with ES2023-only libs.
  Protobuf-ES otherwise replaces lone surrogates during encoding and strips a
  leading BOM during decoding: the SDK's internal guard refuses the former and
  its codec factory explicitly preserves the latter. The generated package
  supplies its own pinned runtime; the core SDK has no new runtime dependency.
- Schema-driven JS preflight tests reproduce and prevent plain-JS numeric
  i64 precision loss, misspelled fields and scalar coercions. The codec now walks
  actual descriptors, checks exact scalar ranges/types and oneof cases, refuses
  accessors/cycles/sparse arrays and unsupported byte storage, snapshots accepted
  containers, and calculates complete wire size before `create`/`toBinary`.
  Spy codecs verify invalid/oversize inputs never reach those allocators. Tags,
  varints, packed/unpacked elements, map entries, optional/selected defaults,
  wrappers and `Struct` mappings are covered. Shared vectors agree with native
  wire preflight and Python graph checks. Unknown wire fields are discarded and
  caller `$unknown` metadata is never serialized. Native descriptors now refuse
  `jstype` overrides before startup/generation. Aggregate retained-copy accounting
  remains open.
  After rebuilding both JS fixtures, `--suite all` again passes all 16 local
  pairs, all 16 authenticated remote pairs before/after handoff and the 32
  observed SIGKILL boundaries. Broader fleet/crash qualification stays false.
- Eight SDK RPC client/registration tests also pass. The new `@cellar/sdk/rpc`
  helper checks invocation lifetime, sends method metadata separately from
  bytes, preserves business unions and unknown reply failures, and freezes an
  explicit registration table. Three service-plugin/generated-JS tests also pass
  (42 total npm tests, including seven live-descriptor/runner checks and three
  portable-profile checks). Strict TS checks exercise concrete TS and JS declaration
  clients, requiring compile errors for numeric i64 inputs, undeclared helpers
  and invalid call-option types. Application fixtures pass full strict checks.
- Pinned build tools: Prost/prost-types/prost-build 0.14.4, Buf 1.75.0,
  Protobuf-ES/protoc-gen-es/protoplugin 2.16.0, TypeScript 7.0.2, declaration
  transpiler API 5.4.5, Python Protobuf 7.34.2
  and protoc 34.0. Node tools are locked in
  `tools/rpc/package-lock.json`; Buf and protoc-gen-es version commands run.
- The native contract inventory now projects logical codec shapes once per
  bundle, independently of its exact artifact digest. JS/Python providers check
  live descriptors before readiness, and clients check before encoding with
  their native catalog's limits. Recursive references are memoized; maps,
  wrappers, `Struct`/`Value`, presence, oneofs, packing and enum values are
  compared. JS reflection records are frozen. Python checks the descriptor's
  registered message class as well, so copying a `DESCRIPTOR` onto an old class
  does not establish codec identity. Consumer-only Source metadata and the real
  JS runner's consumer-only path are tested. Stale schemas retain the copied
  type names/digest in negative tests but never encode or submit. Real V8/Node
  runtimes refuse a mutated generated descriptor before their host callbacks;
  Python's cold runner refuses a stale provider without calling its constructor.
  Node startup failures no longer print uncaught exception stacks. The freshly
  rebuilt all-runtime executable contains the current Python verifier and again
  passes local/remote/handoff/crash suites. All 24 artifacts, including projected
  inventories, reproduce byte-for-byte. Static binding and frozen package/SDK
  identity checks, including the added Python runtime sources, remain open.
- Native generation now emits `MessageGraph` implementations from actual Prost
  declarations/attributes. Both clients and response dispatchers run graph and
  raw-byte checks before recursive sizing, check complete size before writing,
  and discard owned children through iterative iterator frames. A codec spy
  proves failed graph admission reaches neither sizing nor writing. Generated
  and well-known messages 20,000 levels deep are refused and cleaned up on a
  256 KiB stack. Profile tests cover packed/unpacked costs, selected/optional
  defaults, scalar-only oneofs, omitted map defaults and tag/varint overhead.
  Generator tests cover Rust keyword paths, nested oneofs, files without
  services and multiple files sharing a package without duplicate visitors.
  A real native node refuses oversized input before construction, preserves
  unknown effects for oversized output after synced count/receipt writes, and
  recovers the saved result on explicit same-ID retry across restart.
- Native map-default vectors also exposed a JS decoder mismatch: re-encoding
  can add default fields that Prost omitted. The decoder now relies on native
  incoming wire preflight and does not apply outgoing encoding costs to incoming
  values. The new regression accepts the valid one-occurrence/depth-one Rust
  message; JS outgoing encoding still checks its larger cost. All 24 outputs
  reproduce byte-for-byte after adding the native visitors and scalar-oneof
  fixture. Rebuilt runtime tests and the complete local/remote/handoff/32-case
  SIGKILL executable pass again. These are encode/interop boundaries; exact
  decoder allocation/RSS, aggregate accounting and broader fault/lifecycle
  qualification remain open. The decoded-slot and catalog work below adds
  conservative resource guards without changing that qualification boundary.
- Native incoming wire preflight and Rust/JS/Python outgoing graph checks now
  share a decoded-slot estimate: declared physical fields plus an object for
  each materialized message, serialized occurrences/packed elements and omitted
  map-message default layouts. Native tests refuse 2,000 empty 512-field messages
  carried by only 4 KiB at the default 1,000,000 slots, and accept the exact
  1,028,002-slot boundary. Shared wrapper/Struct vectors and omitted map-default
  vectors assert exact limits across codecs. JS constructor and Python
  sizing/serialization spies prove layout refusal happens before allocating
  codecs. This estimate bounds work/amplification, not actual allocator bytes.
- Framework replies use a fixed private control allowance independent of tiny
  business depth/element/slot limits. A real trusted refusal survives limits of
  one; unknown/duplicate control fields and multiple reply alternatives fail.
  Incoming byte/depth/element/slot exhaustion is `ResourceExhausted/not_committed`;
  malformed Protobuf remains `InvalidArgument/not_committed`.
- Native workers snapshot the application catalog once and share it with their
  invocation contexts. Generated clients resolve matching application limits
  before encoding; generated handlers receive those same limits through the
  router before response encoding. A stricter provider catalog no longer leaves
  native codecs on generated defaults. Real-node spies verify both request and
  response sizing/writing never run beyond an eight-slot declaration, through
  both node starts. Native consumer-only declarations use `Registry::rpc_contract`.
  JS runner and Python client tests also enforce native slot overrides before
  submission. Custom native entries without a matching catalog retain explicit
  contract limits; static binding and effective peer/host minimum audits remain.
- After regenerating all 24 artifacts and rebuilding both JS entries and the
  all-runtime executable, local/remote/handoff matrices and all 32 observed
  SIGKILL cases pass again. The report is published after the supervised
  runtimes drain; broad fleet/crash qualification flags remain false.
- The crash executable first added the actual native Counter transaction
  after writing count and before writing its receipt. It checks the admitted
  connection's epoch and `in_transaction`, records the uncommitted count and
  absent receipt through an atomically renamed file, then waits for SIGKILL.
  The crash-only run passes 36 cases: the original 32 plus all four callers to
  this Rust midpoint. Each new case recovers count zero before explicit retry,
  then verifies one exact mutation, saved receipt and conflict behavior at an
  advanced owner epoch. Guest-provider observations extend this below.
- The expanded crash-only run passes 48 SIGKILL cases: all 16 language pairs at
  each of the three boundaries. Every guest midpoint observes the actual native
  `storage.apply` transaction through a SQLite PROFILE trace installed only in
  the disposable owner. It requires an admitted connection epoch, a completed
  absent-receipt read, the exact count-write SQL/scope/value, one changed row,
  an exact total-change advance and an open transaction. No SDK calls or SQL
  execute inside the callback, and no production fault API is introduced.
  The observer follows the original native future's polls across worker changes
  and restores thread-local state between polls. All 16 midpoint cases recover
  count zero before explicit retry, then recover exactly one mutation, receipt
  and conflict behavior. The report now sets
  `all_provider_transaction_checkpoints_verified` true while broader crash
  qualification stays false.
- Three real-SQLite observer tests verify the completed write before rollback,
  cross-thread poll state, receipt presence/missing observations and refusal of
  autocommit writes. The crash evidence guard requires every provider's observed
  midpoint and rejects the old 36-case subset, incomplete SQLite observations
  and forged change counters. All 13 qualification/observer tests pass.
- The proof suite omits explicit handler sync for tool-owned commands and
  observes returned typed results matching local SQLite state before the native
  output gate. It reuses the SDK's real authenticated log and object-store fault
  gates across three actual nodes. All 32 cases pass: every pair delivers its
  original typed result after release, and every pair returns
  `Unavailable/unknown` after proof failure. Both proof paths entered their hold,
  the original future stayed pending, and no mutation was automatically replayed.
  Explicit same-ID retries, changed-delta conflicts, exact i64 counts and scope
  identity pass after recovery. Unacknowledged atomic work may be retained or
  absent; the test never infers rollback from a lost/failed response. Reports
  publish after native drains and parent runtime joins.
- Staggered proof-suite startup exposed a genuine fast posture-driver issue:
  it skipped recruitment whenever the serving floor was healthy, even with only
  one follower. The second follower then waited for the 30-second maintenance
  pass. The deterministic `proof-recruitment` diagnostic fails before the fix
  with the actual owner at one follower and the new replica already at two.
  The fixed driver retains the one-follower serving floor and uses its normal
  repair cadence to reach the two-follower target. All proof cases first observe
  the ingress follower, then start the new replica and require actual signed
  append acknowledgements from both before arming faults. Diagnostic reports
  explicitly leave `qualified` false; the complete proof report records its
  verified matrices. Temporary debug instrumentation is removed.
- Recovery read polling accepts only named typed RPC admission refusals through
  the fixture's ordinary HTTP application-error channel (500/503), under one
  finite budget. Unrelated server errors still fail immediately, successful
  counts are checked at once, and submitted mutation futures are never polled
  or silently repeated.
- The new focused `--suite fence` run passes 32 cases: all 16 pairs after
  handler return with held output proof, and all 16 with the original handler
  still awaiting sync after its atomic count/receipt batch. The coordinator
  changes the actual published node lease, requires `NodeLeaseMismatch` and a
  joined domain, then verifies refusal of a new request. The original call must
  fail with unknown effects. Recovery uses the same scope on a surviving node
  at an advanced epoch, replays an already acknowledged warmup receipt, and
  verifies explicit retries and conflicts without automatic mutation replay.
- Live fence probes retain the original context and bound generated client.
  V8/Node resume their original JavaScript continuations; Python deliberately
  suppresses cancellation until controlled release and also tests its raw
  native sender. Rust observes the dropped original handler future and tests
  its retained context/client. All late storage reads/writes and typed calls
  are refused; native admission observations reject additional business calls.
  Probe cleanup drops retained contexts before the next case.
- The live V8 case exposed a real engine bug: cancellation dropped pending
  promise resolvers without settling awaiters. The pump now removes authority
  and settles abandoned operations with `ContextExpired/unknown`. Cleanup can
  run, including after another application promise, while new SDK calls fail
  with `ContextExpired/not_committed`. The owning-boundary regression fails
  before the fix and passes invocation/owner revocation with both queued and
  later replies. Late replies cannot revive work and all bridge reservations
  return. All 28 V8/Node library tests pass.
- The first expanded combined run correctly failed when its process-wide audit
  reached the fixed 4,096-entry bound. Proof cases now reset that audit only
  before starting their nodes, after the preceding sequential case/suite has
  joined all native nodes and captured its evidence. The per-case bound remains
  unchanged; active cursors are never discarded. The combined rerun passes.
- The current combined `--suite all` run builds the executable and
  publishes all seven evidence objects after shutdown: 16 local pairs, 16 remote
  pairs before/after handoff, three native peer-deadline cases, five native
  cancellation cases, 32 failed-recovery admission cases, 48 observed
  SIGKILL cases and 64 proof cases:
  release, failure, fenced output and fenced live sync, 16 pairs each. All pass
  together. Twenty-one qualification/observer tests
  reject incomplete matrices, unproven transaction/proof state, forged rollback,
  unrelated HTTP failures, incorrect replica-member observations, missing live
  authority and missing late SDK refusals. Guest epoch/count evidence must keep
  its exact decimal-string representation, rather than becoming JSON numbers.
- The new `--suite recovery` matrix passes 32 cases: all 16 pairs with the dead
  owner's proof root missing, and all 16 with it replaced by a fresh lineage.
  A large count/receipt write acknowledges through an actual signed follower
  while bucket publication is held. After SIGKILL, the witness keeps its real
  batches but cannot claim the selected cell/root. The live successor observes
  each fault and refuses two deliberately issued mutations with
  `Unavailable/unknown`. Native factory and handler observations stay at zero;
  retained bucket and follower proof remains byte-for-byte unchanged.
  Restoring the original root lets the same successor recover the acknowledged
  count and receipt at an advanced epoch without restarting. No refused mutation
  replays automatically; explicit retries apply once and preserve conflicts.
- The first combined recovery run exposed a fixture publication-cut race: an
  already-admitted warm upload could finish after the inventory snapshot. The
  shared fault gate now cuts new admission and joins admitted ordinary/multipart
  writes and in-flight parts before returning; snapshots follow that drain.
  Held waiters release their drain permits, and node leases remain independent.
  The delayed-upload regression fails before the fix. Unit checks also cover
  multipart completion/abort/drop, queued writes and lease independence. The
  combined rerun at that stage passes all five suites. Both shared Python
  proof-failure and reset-recovery modes pass; the latter observes its original
  queued read refused with 503, then recovered state with one business attempt.
  Clippy passes for the qualification executable and Python bridge harness.
- After the posture-driver change, all 78 existing node-log unit tests and seven
  native log-protocol tests pass. The native restart/receipt test and seven
  generated-profile tests pass, as do the 28 V8/Node library tests, both named-RPC
  integration tests, both real-host HTTP/restart tests, the 26 embedded Python
  unit tests and its real-node typed-RPC executable. The 42 npm tests, strict TS
  checks, 17 Python codec/client tests, 22 runner tests and strict mypy checks
  also pass. Native library/integration and all-runtime qualification Clippy
  checks pass with warnings denied for the affected packages.
- The native dependency guard passes with 465 reachable packages and no JS
  engine. Three regression tests resolve real temporary Cargo workspaces and
  reject a native application that pulls V8 through the explicit qualification
  root. They also reject default-enabled qualification runtimes and run in SDK
  CI. Documentation and local-reference checks pass at this stage.
- Clippy passes with warnings denied for the native library/native RPC test,
  shared guest crate, code generator, both JS backends/all targets and Python
  library/new RPC test and the full qualification executable. Layout and formatting
  checks cover 949 Rust files. Build-tool filesystem/output exceptions follow the existing
  xtask convention and remain outside runtime crates.

These checks run locally with Rust 1.98.1 and the checkout-specific target
directory. They establish the described local/remote Counter, graceful handoff,
concurrency, observed SIGKILL boundaries in every provider's transaction and
original-owner fencing with GIL CPython, plus the described missing/replaced-root
recovery cases. They do not establish every recovery failure, all schema/runtime
modes, full cross-language parity, performance or the completion of T01-T19.

## Next implementation boundaries

The implementation remains incompletely qualified until the remaining
correctness and delivery boundaries pass. Deliver these slices in order; each slice
must preserve the existing recovery, crash, proof and receipt evidence.

1. **P0: resolve hosted native settlement and remaining streaming/RPC failures.**
   Reproduce the stale-receipt transport closure observed in both SDK interpreter
   jobs. Distinguish source idle-pool reuse from receiving idle retirement and
   fault-proxy delay; retain original budgets and typed refusal assertions.
   Do not retry a call after uncertain submission to make the test pass.
   Preserve Linux's passing direct-admission/slow-reporting adapter checks.
   The controlled failure proves the old waiter depends on reporting latency;
   it does not establish the original SDK run's exact cause. Reproduce the
   remaining streaming/typed-RPC 250 ms handler failures; the decoder probe's
   independently proven cold-constructor assumption is corrected separately.
   Observe broker/native admission, Python wakeup/pump progress and storage/sync
   timing under the original budgets. Add a regression at each demonstrated
   cause and rerun the original commands; a successful retry alone is not a fix.
   Native cold-factory deadline/cancellation now has public-node authority,
   zero-late-handler and receipt-retry evidence. Keep the latency diagnosis
   distinct from this correctness boundary; guest cold matrices remain open.
2. **P0: observe the remaining Linux gates.** The SQLite 3.53.2 dependency, Node
   observer ordering and complete-store outage fixture pass their local
   regressions, all 696 combined cases and the affected static checks. Require
   final broad SDK and example results. On `91bd2675`, JS/Node and all 144
   lifecycle cases pass; both SDK variants pass the embedded-adapter step.
   The upstream SQLite fix and
   measured connection-setup stalls are distinct; no corruption was observed.
3. **P0: close remaining lifecycle and peer/admission provenance gaps.** Preserve
   the 96 parent-write/forged-category cases, 80 self/A-to-B-to-A cleanup cases
   and 128 proof-wait cases. Qualify broader call-local peer/admission evidence.
   Keep the 96 Node/V8 lifecycle cases and the Python cancellation/supervision
   matrices green. Persistent suppression must retain typed incomplete shutdown,
   require observed process teardown and verify cold receipt recovery. Forced
   guest peer retries and broader nested-provider paths remain next.
   The Node observer ordering has a controlled real-worker regression; the
   reason for the original native observer pause remains unknown. Retain its
   diagnostics when investigating any recurrence. Trusted pre-admission
   refusal may establish
   `not_committed` for that call; it cannot reclassify the parent's earlier
   writes. A lost response remains unknown. Preserve the 55 deadline cases;
   remaining T14/T17 boundaries and all supported interpreter modes stay open.
4. **P0: qualify parser and resource boundaries.** Keep global symbol/import
   validation and expanded-name budgets green; close remaining option/schema
   semantics before invoking message generators. Verify effective
   method/runtime/host/peer minima, account for retained binary copies, and
   measure decoder allocations and RSS under saturation, flood and disconnect.
   Preserve the 64 generated-client SDK source saturation cases and the
   gate/queue/routing/undelivered-reply reservation tests. Their length-weighted
   request allowance does not qualify receiving-peer or response allocations.
   Completion requires bounded queues and reservations that return to baseline
   after cleanup, while retaining graph, slot and control-budget guards.
5. **P1: qualify the portable profile and supported runtimes.** Run real business
   calls across every local/remote backend pair, including free-threaded Python
   where supported, and close the remaining lifecycle/regression requirements.
   Codec vectors alone do not qualify a caller/provider pair.
6. **P1: make application delivery reproducible.** Retain offline `cellar rpc
   check` for descriptors, canonical inventories and native catalog snapshots.
   Ship unified generation, verified static consumer bindings, frozen package
   inventories, complete embedded Python
   source identity, a published example and clean-machine CI. Require generation
   drift and stale package/codec identities to fail before serving traffic.
7. **P2: add visibility, then publish performance evidence.** Add registered-method
   spans and payload-free diagnostics with bounded labels. Measure typed versus
   untyped/native/guest latency, throughput and durable TPS, including saturation
   memory, cleanup and recovery. Record commands, environment and raw evidence
   so the results can be reproduced.

Keep all 32 failed-recovery cases, 48 crash cases, 64 proof cases (including
32 owner fences), the late-recruitment regression and local/remote/handoff
receipt recovery audits green. Expand those fixtures when a new failure requires
it; prioritize the unqualified boundaries above over more Counter permutations.
