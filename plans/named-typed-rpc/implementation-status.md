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
| P0 — next | Remaining lifecycle and peer/admission provenance | Delivered: whole-call budgets, 55 deadline cases, 128 proof-wait cases, 80 cycle cases and 96 parent-write/child-refusal/forged-category cases. Next slice: owned Node worker failure during typed calls and host shutdown, proving joins and reservation cleanup; then remaining V8/Python lifecycle waits and broader peer/admission provenance. Retain every delivered matrix and investigate recurring timeout/domain-closure diagnostics. |
| P0 | Resource bounds and parser correctness | Global symbol/import validation and bounded expanded-name inventories are delivered. Close remaining option/schema semantics; audit effective method/runtime/host/peer minima, retained binary copies and decoder allocations; demonstrate bounded queues, reservations and memory under saturation, flood and disconnect. |
| P1 | Language/profile coverage | Exercise the portable Protobuf profile as real business calls across all local/remote pairs; qualify supported Python interpreter modes and remaining lifecycle/regression boundaries. |
| P1 | Reproducible application delivery | Deliver `cellar rpc generate/check`, static binding and frozen package inventories, embedded Python source identity, a published typed-RPC example and clean-machine CI. |
| P2 | Operation visibility | Add registered-method spans, bounded labels and payload-free diagnostics for failures, deadlines and proof waits. |
| P2 | Measured performance | Publish reproducible typed/untyped/native/guest latency, throughput and durable TPS, plus saturation memory, cleanup and recovery results. |

Preserve existing durability evidence while addressing these gaps. Further
recovery-fault variations follow demonstrated correctness risks; expanding the
Counter fixture alone is not the highest-priority deliverable now.

## Work packages

| Package | State | Authoritative evidence / next action |
| --- | --- | --- |
| RPC-01: Contracts/toolchain | In progress | Counter and portable-profile outputs reproduce byte-for-byte in all four authoring modes; global symbols/import visibility and expanded inventory budgets are validated before output/compiler work; generated APIs pass TS/Python static checks; remaining options/schema/conformance qualification stays open |
| RPC-02: Native typed path | In progress | Native RPC/transport, generated-profile/vector and restart tests pass; codec limits precede allocation; 55 deadline, 128 proof-wait and 80 dependency-cycle cases cover caller/provider pairs, signals/deadlines, local/peer routes and bounded cleanup; 96 provenance cases preserve committed parent writes and fixed categories; broader peer/admission/lifecycle/resource qualification remains |
| RPC-03: Binary bridges | In progress | Shared `cell.call`, Python raw bytes, owned V8 attachments and binary Node framing implemented; runtime attachment/cleanup tests pass; aggregate copy accounting and full cancellation/flood qualification remain |
| RPC-04: JS/TS services | In progress | Generated clients/handlers, binary dispatch and live descriptor checks work in V8/Node; real startup refuses stale codecs; typed AbortSignal options and native-owned whole-call codec budgets work with bounded cleanup; full schema/resource/deadline qualification remains |
| RPC-05: Python services | In progress | Generated async APIs, live codec checks and dispatch work with receipts/restart; stale providers/clients are refused; task cancellation reaches native/peer calls and joins cleanup; native-owned budgets cover validation, encoding, transport and decoding; packaging and free-threaded qualification remain |
| RPC-06: Fleet/durability | In progress | Local/remote/handoff, 32 failed-recovery admission, 48 SIGKILL and 64 proof cases pass together; further interpreter modes and broader qualification remain |
| RPC-07: Packaging/performance | Pending | Reproducible generation, clean-machine packaging, CI and measured performance report |

## Required evidence

| Requirement | State |
| --- | --- |
| Typed APIs and runnable examples in Rust, JS, TS and Python | Generated APIs and runnable qualification fixtures implemented; strict TS/Python API checks pass; published example packaging remains |
| Descriptor/profile validation and exact artifact identity | Implementation under test |
| Complete malformed-descriptor semantic checks before invoking message generators | Partial; global symbols, import graph/visibility/indexes, expanded-name budgets, map ownership/shape/key types, synthetic oneofs, enum aliases/reservations and field reservations/packing have adversarial tests; remaining option/schema semantics stay open |
| Explicit registration, full method validation and RPC-only cells | Implementation under test |
| Live language codec identity against the native artifact | JS/Python provider readiness and client pre-encode checks implemented and tested, including recursive/map/well-known types; static binding/package verification remains |
| Bounded request/response codec and binary bridge accounting | Native wire checks and raw attachments implemented; exact retained-copy accounting and saturation qualification remain |
| Input graph depth/element preflight before allocating language encoders | Rust generated visitors and iterative owned cleanup, Python graph/map checks and JS descriptor-driven type/graph/exact-size preflight tested; broader codec and decoder-allocation qualification remains |
| Decoder layout/default-field allocation and effective limit plumbing | Shared decoded-slot estimate implemented in native wire and Rust/JS/Python encoders; default map layouts, fixed control budgets and native/guest catalog plumbing tested; exact allocation/RSS, saturation and method/runtime/host/peer minimum audit remain |
| Invocation authority, cancellation, deadlines and nested-call provenance | Owner fencing and retained-client refusal cover all 16 pairs; 55 deadline cases cover guest nesting/codec costs; 128 proof-wait cases cover signal/deadline × local/peer × output/sync × all 16 pairs with fresh admission and original-authority checks; 80 self/A-to-B-to-A cases cover bounded cycle cleanup; 96 parent-write/forged-category cases pass with unknown outcomes; broader peer/admission/lifecycle qualification remains |
| Pre-factory contract validation and trusted refusal provenance | Native/Python cold-constructor tests pass through the host catalog; custom wrappers without a forwarded catalog remain conservative; peer/admission provenance remains |
| `cellar rpc check`, pinned Buf workflow and static binding inventory validation | Pending |
| Registered-method spans, bounded metric labels and payload-free diagnostics | Pending |
| Business errors separated from infrastructure errors | All backends preserve business variants and post-write invalid-response/unknown locally; entered-handler fixed categories survive with unknown effects in all runtimes; broader host-code mapping and peer/admission provenance remain |
| Output proof after a handler returns without explicit sync | All 16 pairs pass real peer/bucket hold then release, and hold then failure; typed result/local commit, no early response, unknown failures and explicit receipt retries verified with GIL CPython |
| Fencing after handler return and during its original sync await | All 16 pairs pass both phases with observed `NodeLeaseMismatch`, joined owner domain, advanced survivor epoch, acknowledged receipt recovery and late SDK refusal; GIL CPython |
| Failed recovery before provider construction | All 16 pairs refuse missing and replaced original proof roots: 64 `Unavailable/unknown` calls, zero provider factories/handlers, unchanged retained proof and live recovery after metadata restoration; GIL CPython |
| Atomic Counter receipts, exact 64-bit state and retry window | All 16 pairs pass local/remote/handoff, receipt races, owner fences and separate-process kills before handlers, inside count/receipt transactions and after synced handlers; broader recovery and interpreter modes remain |
| Existing HTTP/lifecycle/untyped RPC regressions | V8/Node real-host HTTP restart and runtime lifecycle tests pass; full T19 remains open |
| T01-T19 from plan 009 | All remain open until their actual boundary is verified |
| Standard format/layout/dependency/docs checks and affected tests/Clippy | Current local checks pass; clean-machine/CI qualification remains |
| Generated output reproducibility and frozen dependency inventories | Counter/profile reproduce 24 artifacts; tool versions locked; clean-machine/CI inventory verification remains |
| Throughput, latency, durable TPS, memory and recovery report | Pending |

Do not mark the goal complete from native unit tests or a subset of backend
pairs. Preserve the full test matrix, failures and remaining work across turns.

## Current evidence

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

The implementation PR is a draft until the remaining correctness and delivery
boundaries are qualified. Deliver the following slices in order; each slice
must preserve the existing recovery, crash, proof and receipt evidence.

1. **P0: close remaining lifecycle and peer/admission provenance gaps.** Preserve
   the 96 parent-write/forged-category cases, 80 self/A-to-B-to-A cleanup cases
   and 128 proof-wait cases. Qualify broader call-local peer/admission evidence.
   Next, qualify owned Node worker failure during an active typed call and host
   shutdown: require native joins, retired invocation authority and reservations
   returning to baseline. Then cover remaining V8/Python lifecycle waits,
   forced guest peer retries and broader
   nested-provider paths. If the unexplained Node watchdog failure recurs, use
   its observer/heartbeat/input diagnostics to establish the actual cause before
   changing policy. Trusted pre-admission refusal may establish
   `not_committed` for that call; it cannot reclassify the parent's earlier
   writes. A lost response remains unknown. Preserve the 55 deadline cases;
   remaining T14/T17 boundaries and all supported interpreter modes stay open.
2. **P0: qualify parser and resource boundaries.** Keep global symbol/import
   validation and expanded-name budgets green; close remaining option/schema
   semantics before invoking message generators. Verify effective
   method/runtime/host/peer minima, account for retained binary copies, and
   measure decoder allocations and RSS under saturation, flood and disconnect.
   Completion requires bounded queues and reservations that return to baseline
   after cleanup, while retaining graph, slot and control-budget guards.
3. **P1: qualify the portable profile and supported runtimes.** Run real business
   calls across every local/remote backend pair, including free-threaded Python
   where supported, and close the remaining lifecycle/regression requirements.
   Codec vectors alone do not qualify a caller/provider pair.
4. **P1: make application delivery reproducible.** Ship `cellar rpc generate/check`,
   static binding checks, frozen package inventories, complete embedded Python
   source identity, a published example and clean-machine CI. Require generation
   drift and stale package/codec identities to fail before serving traffic.
5. **P2: add visibility, then publish performance evidence.** Add registered-method
   spans and payload-free diagnostics with bounded labels. Measure typed versus
   untyped/native/guest latency, throughput and durable TPS, including saturation
   memory, cleanup and recovery. Record commands, environment and raw evidence
   so the results can be reproduced.

Keep all 32 failed-recovery cases, 48 crash cases, 64 proof cases (including
32 owner fences), the late-recruitment regression and local/remote/handoff
receipt recovery audits green. Expand those fixtures when a new failure requires
it; prioritize the unqualified boundaries above over more Counter permutations.
