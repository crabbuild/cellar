<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cell-group implementation progress

The executable design and implementation plan is delivered. Remaining
end-to-end implementation and hard cutover work is specified in the
[executor handoff](handoff.md).
CG-01 through CG-03 have passed their scoped gates;
CG-04 is IN_PROGRESS and CG-05 through CG-16 remain TODO. Intermediate contracts
do not qualify the public shared runtime. No production legacy/shared mode flag
is being added.

## CG-04 catalog-routed command phases

Qualified source: `1f014f42264c73b1d5e3cd387f65ffed45140ecb`.

`CellGroupDispatcher::command` freezes logical command identity, input, namespace
minimum durability, original routing budget and domain-captured time/randomness.
Preparation retains its original runtime, SQL pool, exact route and physical
preparation IDs before entering SQL. A cancelled response or routing retry resumes
that producer instead of executing a second destination's callback. Submission
reuses exact shared acceptance and pre-append seals. Acceptance is provisional;
settlement requires the exact receipt, while absence requires the existing newer
applied-term barrier. Errors preserve command identity and conservative ambiguity.
Returned receipts retain their output admission and sign the cell observation.
Both request and namespace S3 minima refuse until real shared archive proof exists.

Two new three-voter cases cover both image modes, held-callback cancellation,
retry through a follower while retaining the original producer, duplicate proposal
acceptance, same-term uncertainty, exact receipt recovery after another cell
advances the group, and a real leader transfer. A proven non-commit can retry with
the same captured time/randomness. S3 refusal precedes callback execution.
The leadership fixture checks the caller's actual cell and uses the production
applier where installed; the held newer-term barrier assertion remains unchanged.

All 105 native cases pass across execution, catalog, read and runtime suites;
one runtime helper is invoked by its parent. All eight commands pass, including
all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied,
formatting, layout, dependency isolation, docs, source preflight and all 37
plan-validator cases. The partial record at
`.cellar/cell-groups-evidence/CG-04-routed-commands-qualified/checkpoint.json` binds
229 source inputs and four selected executables. The failed gate at
`CG-04-routed-commands` is retained: all native cases passed, then Clippy rejected
the large command error. Its typed cause is now boxed, preserving all assertions.
A second gate at `CG-04-routed-commands-final` retained the passing native tests
and fixture lint failure; the direct Tokio select now uses Cellar's required
fair-polling macro. Earlier compile and fixture failures are retained too. No RustFS
or performance qualification was run. Latest fetched `origin/main` at `8a93ffd7` is included;
the requested MR-13/MR-14 JSON files remain excluded.

CG-04 remains IN_PROGRESS. The critical next gap is production controller ownership
of these phases after ingress cancellation, including an explicit bounded
reconciliation lifecycle beyond the response deadline. The current tests retain
the invocation while cancelling its preparation future; they do not qualify a
controller after the caller drops. Adapt the existing transport's cell-scoped
identity to physical group history, drive transport/apply/completion events, and
qualify exact resolution after caller disconnect. Then complete routed enrollment
and public SDK binding. Missing-origin follower receipt routing, configuration and
cache lifecycle remain unfinished. Reuse existing workers and resource owners;
complete-group snapshots/archive, effects, topology, real RustFS/load and the final
hard cutover remain later task gates.

## CG-04 catalog-routed query invocations

Qualified source: `8723db2df7436478cbc5efc9147d65d3b7651247`.

`CellGroupDispatcher` binds a registered class to its configured namespace,
version keys and execution domain. Queries freeze bounded input, operation and
consistency before discovery. Clones share invocation admission, and the canonical
fingerprint includes logical history and routing limits. Deadline capture precedes
validation and hashing. Existing catalog sessions perform lookup, cached routing,
member retries and exact redirect confirmation without replenishing deadline or
attempt count. Each attempt is consumed before asynchronous destination work.

Local execution authenticates AtLeast tokens, checks the destination's full route
and logical history, and reuses existing runtime read authority and registered SQL
workers. Foreign domain clocks and engines refuse. The response token signs the
selected cell observation. Dropped routing releases only invocation admission;
entered SQL retains its independent input, image and worker ownership through
joined shutdown. Directory metadata cannot create a physical group owner.

The two new three-voter cases cover both image modes, real directory lookup,
linearizable execution, a lost response retry, follower AtLeast, foreign
cell/domain/class/engine rejection, invalid redirect ticket invalidation, original
timeout/attempt exhaustion, a confirmed replacement with no physical owner, and
cancelled routing while a callback is held. The replacement case tests only
metadata confirmation and refusal; it does not qualify live group transfer.

All 103 native cases pass across the shared-group execution, catalog, existing
read and runtime suites. The runtime process helper is invoked by its parent.
This targeted gate complements the prior 319-case registered-query checkpoint at
its original source; it does not rerun the complete previous selection.
All eight commands pass, including all-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,821 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases. The partial record is
`.cellar/cell-groups-evidence/CG-04-routed-queries/checkpoint.json`, binding
227 source inputs and four selected executables. The initial missing type
qualification, corrected targeted run and formatter logs are retained separately.
No RustFS or performance qualification was run. The branch contains latest fetched
`origin/main` at `8a93ffd7`; the requested MR-13/MR-14 JSON files remain excluded.

CG-04 remains IN_PROGRESS. The most critical gap is routed command/enrollment
execution with retained ambiguous outcomes and exact receipt/creation settlement.
Next extend the same dispatcher and qualify enrollment, command, lost response and
linearizable query together. Production node-controller event/transport integration
and replacing the public SDK's old host binding remain required; the native
fixture still pumps shared-runtime events. Retry/cache/configuration, shared effects,
snapshots/archive, physical fault and real RustFS/load gates remain unfinished.

## CG-04 registered queries with exact cell observations

Qualified source: `7a79511d4b8606d11177778a3fe8adf71a8d901a`.

`CellGroupSqlPool::query_registered` dispatches compiled queries through the
existing coherent query owner and fixed SQL workers. It validates operation
kind/contract and full schema before opening the selected image, bounds retained
operation/input storage, and preserves the original authority deadline. The
callback receives the exact `CellHistory` and `CellVersionObservation` returned
in its result, including the selected cell's decision witness. It never derives
a mutation sequence from group log progress.

The two new three-voter cases exercise materialized and extent images. An older
A pin returns its historical state after A and B mutate; a fresh linearizable A
query returns A's sequence and witness independently of B's later group progress.
Tokens round-trip the selected observation. Invalid contract, kind, operation,
schema and input refuse before callback execution. Read-only restrictions,
contained panics and SQL deadline interruption remain enforced. Cancelled waiters
retain input/image/worker ownership, and shutdown joins entered callbacks before
releasing reservations.

The temporary old adapters explicitly use `QueryVersion::GroupCut` and
`RaftClass::invoke_group_query`. These preserve the actual old in-memory group cut
without fabricating a cell sequence. CG-16 requires removing both with the old
host; the final context exposes the selected cell observation directly. This is
an integration checkpoint, not a second permanent production mode.

All 319 selected Rust cases pass: 280 native cases across thirteen suites,
30 Cellar library cases, five pure cell-group core cases and four LTX image-store
cases. The existing native read suite additionally covers current-term barriers,
minority refusal, follower catch-up, delayed replies and version authentication.
Six process helpers run through parent cases; three RustFS-specific cases remain
unrun. These results do not qualify the new shared dispatcher or infrastructure.

All seventeen commands pass, including all-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,819 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases. The retained checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered-queries/checkpoint.json`, binding
225 source inputs and sixteen selected executables. Pre-commit library, targeted
query, format and Clippy logs are retained separately. The branch contains latest
fetched `origin/main` at `8a93ffd7`; the requested MR-13/MR-14 JSON files remain
unchanged from main.

CG-04 remains IN_PROGRESS. The most critical gap is catalog invocation sessions
and production SDK dispatch into registered enrollment, command and query workers,
with namespace-authorized classes, exact receipt settlement and original deadline,
fingerprint and attempt budgets. Next qualify that actual routed path under lost
responses and redirects. Reuse the existing owners instead of adding a worker
pool. Retry/cache and configuration handling, shared effects, complete-group
snapshots/archive, process-kill cuts and real RustFS/load gates remain unfinished.

## CG-04 registered commands and logical effect identity

Qualified source: `b28078afbd243c18ac7c671a481a3e369c086ab2`.

`CellGroupSqlPool::stage_registered` now dispatches a real compiled `RaftClass`
on the existing fixed workers. Its retained input is bounded by the producer
budget. The worker verifies operation kind/contract, full class schema, normalized
input digest and authenticated inbox/reserved client identity before receipt
lookup, including duplicates. It uses the existing private receipt transaction,
prepared submission and exact committed settlement paths. The shared path obtains
logical identity from `CellExecutionAuthority`; it never constructs `GroupScope`.

`CommandContext::identity()` and `RaftEffectTarget` carry `CellHistory`. System
clients and signed effect lanes/requests bind the logical cell, so normal physical
group reassignment preserves identity and a fresh cell incarnation separates it.
Canonical marker-1 records omit physical groups and use distinct signature/lane
and compiled-class domains. Old group-bearing payloads and old signatures refuse;
there is no compatibility reader. The temporary old host projects its actual
logical binding into the same command/effect interfaces.

The new real three-voter tests use two cells and production image preparation in
both storage modes. Callbacks see the exact cell history and persist alarm state.
A's receipt survives B advancing group progress; duplicates do not run callbacks.
Wrong contract, input digest, operation and schema produce their specific typed
errors even on duplicate requests, without retaining cell admission. Unit cases
cover placement-independent signed identity, new-incarnation separation, and old
wire/domain refusal. This does not qualify live transfers or shared effect dispatch.

All 295 selected Rust cases pass: 256 native cases across twelve suites, 30 Cellar
library cases, five pure cell-group core cases and four LTX image-store cases.
The native gate includes the existing alarm/outbox/inbox, extent-host and host
regressions. All seventeen commands pass, including all-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,818 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases. The retained checkpoint is
`.cellar/cell-groups-evidence/CG-04-registered/checkpoint.json`, binding 221 source
inputs and fifteen selected executables. Failed compile, missing test delimiter,
Clippy type-alias and omitted test submission-transition attempts are retained
with the corrected targeted runs. Six process helpers run through parent tests;
three RustFS-specific cases remain unrun. The branch contains latest fetched
`origin/main` at `8a93ffd7`.

CG-04 remains IN_PROGRESS and CG-08 remains TODO. The most critical next gap is
registered query context and version observations, followed by catalog/SDK routing
into these workers with original deadline, fingerprint and retry budgets. Query
contexts still carry the old committed group version; never invent a cell sequence
from that index. Production cache/retry and configuration handling, process-kill
cuts, shared effects and later snapshot/archive/RustFS/performance gates remain.
The opportunity is to complete one dispatcher over the existing consensus, SQL,
read and recovery owners, then remove the superseded production host.

## CG-04 physical apply faults and selected-image recovery

Qualified source: `1ec2eeaab2beaa8d8e82c834cbb5dbf692b5e7d1`.

`CellGroupApplier::recover` reconstructs only the engine-selected complete root.
It verifies every selected physical image, logical binding and geometry, and
retains the same manifest-page owner for subsequent apply. Materialized IDs use
the existing deterministic derivation; filenames supply no authority. Extent
roots verify the full cell history and schema. Missing or corrupt selected closure
refuses. Startup must still consume the catalog permit and establish current-term
quorum before serving.

Six new native cases exercise both storage modes. At actual image and manifest
directory-sync cuts, cancelling preparation and then cancelling close retains
worker, budget and completion ownership. A replacement pool refuses until cleanup
joins; exact preparation can then retry. Injected directory-sync failure preserves
the old selected root and releases preparation reservations. Cold restart closes
all prior engines, stores and execution domains, selects the published cell A,
ignores prepared but unpublished B, and replays B from the durable committed log.
It refuses serving authority without fresh quorum and refuses a missing selected
image directory without changing the marker.

This is joined cold restart, not whole-process kill or power-loss qualification.
The first recovery run hit an ownership refusal because the fixture still held
engine-owned manifest pins. Closing the complete owner before reopening fixed the
fixture; ownership rules and assertions were preserved.

All 260 selected Rust cases pass: 233 native cases across ten suites, 23 Cellar
library cases and four LTX image-store cases. Fifteen commands pass, including
all-target Clippy for Cellar/cellar-logic/cellar-ltx with warnings denied, formatting
(1,815 Rust files), layout, dependency isolation, docs, source preflight and all 37
plan-validator tests. The partial checkpoint is
`.cellar/cell-groups-evidence/CG-04-apply-recovery/checkpoint.json`, binding 205
source inputs and twelve copied executables. Intermediate failures and passing
targeted runs are retained. Six process helpers run via parent cases; three
RustFS-specific cases remain unrun. The branch contains latest fetched
`origin/main` at `8a93ffd7`; the requested MR-13/MR-14 JSON files remain excluded.

CG-04 remains IN_PROGRESS. The most critical gap is the registered SDK/catalog
dispatcher: callback contexts still expose legacy cell-scoped group identity.
Next carry logical cell identity and selected-version observations through command,
query, receipt and effect validation, then use the existing shared worker path with
original deadlines, request fingerprints and retry budgets. Production cache/retry
lifecycle, configuration-entry handling and process-kill cuts remain open. Group
snapshot/compaction, RustFS recovery, performance and public cutover remain later
gates. The opportunity is to connect the existing owners, reads and recovery into
one production dispatcher rather than build another runtime.

## CG-04 committed image preparation on shared workers

Qualified source: `c9100ec774d66f30c14eae009f9a9c76d2bc0c33`. Runtime implementation: `2085cc96`.

`CellGroupApplier` owns the same manifest-page store used to verify startup. It
checks the exact persisted committed entry, engine, group and preceding root
before preparing cell-addressed images. Enrollment and mutation work support
materialized and extent storage, derive bounded alarm/outbox hints from the actual
SQLite image, preserve logical versions and exact commit witnesses, and refuse
malformed scheduling state. Membership/topology administration remains refused
pending its owner integration.

`CellGroupSqlPool::prepare_apply` uses the existing fixed SQL workers and their
reserved progress lane. Apply has a separate byte/item budget, so retained SQL
producers cannot consume its admission. `CellSqlLimits::node_bytes` bounds each
of the producer and apply budgets independently; account for their combined
ceiling. Usage exposes both budgets separately. The returned preparation retains
its reservation across clones, delivery and owner completion. Completion queues
publication; only durable owner apply establishes the new selected root.

Materialized duplicate preparation now reopens the exact verified image through
`GenerationStore::pin_prepared`. It does not reset retirement or infer publication.
Cold images require independent byte verification and persistence barriers.
Regression tests cover restart, corruption, mismatched expected state, reader pins
and reclamation. Both real three-voter cases cover enrollment, mutation, derived
hints, forged/foreign/stale work, duplicate preparation, saturated command admission,
old-root selection before publication and retention during a held log sync.

All 254 selected Rust cases pass: 227 native cases across ten suites, 23
Cellar library cases and four LTX image-store cases. All-target Clippy for
Cellar/cellar-logic/cellar-ltx with warnings denied, formatting (1,812 Rust files),
layout, dependency isolation, docs, source preflight and all 37 plan-validator
cases pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-apply-final/checkpoint.json` binds 202 source
inputs, 12 copied executables and fifteen successful commands. The failed gate
at `CG-04-apply` is retained: the newly inlined apply branch overflowed an existing
recovery fixture's stack. The branch is now heap allocated; normal stack limits
and assertions remain unchanged. Earlier fixture, materialized retry and Clippy
failures are retained too. Six process helpers run through parent tests; three
RustFS-specific cases remain unrun.

CG-04 remains IN_PROGRESS. The most critical next validation is cancellation,
shutdown and crash injection while this new image/manifest preparation is held;
the held publication sync does not qualify those earlier cuts. Then integrate the
registered SDK/catalog dispatcher with logical cell identities and observations.
Production retry/cache lifecycle and configuration-entry integration remain open.
No complete shared-group snapshot, compaction, RustFS or performance claim is made.

## CG-04 durable selected-version commit witnesses

Qualified source: `ed84df5daa0ae65fdfb09e9556f21f3e1ea12ec2`.

Authenticated cell manifest pages now require a complete selected-version
`CommitWitness`: original group history, term/index and proposal identity. The
local installation/mutation position remains separate for future group transfers.
Marker-1 pages without this required field refuse; there is no legacy decoder or
proposal inferred from later group progress. Shape checks bound IDs, namespace,
positions and initial enrollment identity. Ordinary update preparation requires
the current group and exact mutation position; the physical Raft owner additionally
matches the witness to the actual committed entry's proposal before publication.
Repeated enrollment and unchanged-cell progress preserve the selected witness.

Immutable-query results expose their exact `CellVersionObservation`; committed
receipt results expose the selected-version observation, which can cover an older
command receipt. Both use the authenticated leaf rather than the entry cache.
Native tests reject a forged witness despite otherwise correct apply/image data,
preserve older pinned observations after a newer cell mutation, round-trip signed
tokens, and recover exact witnesses after restart and image collection in both
physical image modes. Codec tests reject missing/malformed witness fields.

All 214 selected Rust cases pass: 193 native cases across eight suites and 21
library cases covering SQL workers, receipts, budgets, options and manifest pages.
All-target Clippy for Cellar/cellar-logic with warnings denied, formatting (1,806
Rust files), layout, dependency isolation, docs, source preflight and all 37
plan-validator tests pass. The retained partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-witness/checkpoint.json` binds 194 source inputs,
nine copied executables and thirteen successful command logs. Pre-commit compile
and formatting logs are retained separately. Four process helpers run through
parent tests; three RustFS-specific cases remain unrun.

CG-04 remains IN_PROGRESS. This proves witness persistence and observations at the
owning storage/apply/read boundaries, not actual log compaction, group snapshot
installation, registered SDK serving or S3 recovery. Next integrate registered
callback identity, query/version-token dispatch and catalog invocation sessions;
complete the production apply adapter, retry/compaction and cache lifecycle.

## CG-04 read-pin capacity and repaired qualification

Qualified source: `3da4481e09c3767face30469ffc855d302a7c142`.

Read-image retention now uses `RaftRuntimeOptions::max_read_pin_bytes`, independently
of transport request capacity. The default remains 64 MiB. Node and group read
reservations use the explicit pin ceiling; ordinary peer/apply admission remains
separate. Validation bounds configured capacity by the native image-size envelope
and pending-read count using wide arithmetic. The new unit case admits the full
native image limit without raising transport limits, accepts the minimum metadata
reservation and upper envelope, and refuses out-of-range settings. The native
read fixture now uses `cellar::asyncrt::select!`.

All 204 selected Rust cases pass: 186 native cases across seven suites, 11 SQL
queue/extent-worker tests, four receipt tests, two budget tests and one runtime
configuration test. All-target Clippy for Cellar/cellar-logic with warnings denied,
formatting (1,806 Rust files), layout, dependency isolation, docs, source preflight
and all 37 plan-validator tests pass. The checkpoint at
`.cellar/cell-groups-evidence/CG-04-reads-final/checkpoint.json` binds 194 source
inputs, eight copied executables and twelve successful command logs. It retains
the failed full gate at `88c4717d` and earlier targeted compile/stack diagnostics
separately. No test limits or assertions were relaxed. Four process helpers run
through parent tests; three RustFS-specific cases remain unrun.

This qualifies the low-level coherent-read and immutable-query boundary in both
image modes. CG-04 remains IN_PROGRESS. Registered SDK/catalog dispatch, callback
identity and version-token integration, real compaction/retry dispatch and cache
lifecycle remain required. In particular, the selected leaf retains its last
mutation position but not the corresponding mutation proposal ID: preserve a
complete authenticated cell commit witness before issuing SDK version observations
from that image after log compaction. Group progress is never that witness.

## CG-04 coherent cell reads and immutable SQL queries

Implementation source: `88c4717d`.

The shared node runtime now owns bounded read requests with original absolute
domain deadlines. Linearizable requests use unique nonce/counter contexts and
current-term ReadIndex, then wait for contiguous group apply before pinning the
target cell. Raw or forwarded read requests cannot borrow those internal contexts.
A minority leader cannot substitute a cached commit position. A read of unchanged
A waits behind committed B while retaining A's own version. Stale/AtLeast can
select committed follower images; AtLeast compares logical cell history and
sequence/image, never unrelated group indexes.

Separate node/group admission retains waiting reads and delivered pins. Image
selection grows the existing item reservation to its logical byte size without
consuming ordinary peer/apply capacity. The pin is consumed once by an immutable
query on the existing SQL workers. Queries can coexist with an unresolved writer
and read only committed images; an older pin remains coherent across a later
publication of the same cell. SQL output retains its own reservation through
borrowed result accessors. Wrong engines, routes, histories, expired deadlines
and revoked strong-read terms refuse.

SQL progress hooks check the domain deadline, waiter cancellation and shutdown.
Callbacks retain worker and pin ownership until they return; callback panics are
contained and close joins entered work. Deadline checks also cover queued replies,
so a ready result cannot bypass expiry when the receiver resumes late.

Both image modes pass targeted strong-read and lifecycle cases. The first native
compile used an unavailable clock accessor; fixtures now run clock reads inside
the owning domain. Expanding the strong cases with a pending AtLeast read and
an older same-cell pin exposed unnecessary async forwarding layers overflowing
the default fixture stack. Reexports remove those layers; a visibility error in
that intermediate change was corrected. The enhanced cases pass on the normal
stack without weakening assertions.

The retained gate at `.cellar/cell-groups-evidence/CG-04-reads/gate-commands.json`
completed four test commands: 203 selected Rust cases passed. The fifth command,
all-target Clippy, failed because `tests/support/cell_group_sql_reads.rs` uses the
forbidden `tokio::select!` macro. Subsequent commands in that gate did not run.
This source was unqualified at that point; the repaired source and full gate are
recorded above. No task status advances.

The repair required replacing that fixture macro with
`cellar::asyncrt::select!` and separating the logical read-image pin budget from
`RaftRuntimeOptions::max_request_bytes`: the current transport ceiling is 512 MiB,
whereas valid native images can reach 10,000,000,000 bytes. The repair above adds
the explicit bounded pin-byte option and validation test, preserves node/group accounting, count limits
and cancellation ownership, and retains this failed gate separately.

Registered SDK dispatch, callback identity and version-token integration, catalog
sessions, production retry/compaction and cache lifecycle remain required. This
is partial CG-04 implementation, not the production cutover or release gate.

## CG-04 prepared submission and rejection cleanup

Qualified source: `0cf89c403f4a500d18116499b8fc0f219195cd54`.

A prepared SQL attempt now carries its engine, group and preparation term, with
one submission decision shared by all clones. Exact accepted retries return the
original provisional position/digest, including after apply. Only an actual
owner's definitive pre-append capacity refusal seals the preparation and returns
a rejection proof. A queue overload remains uncertain. A lost rejection response
can be recovered from the local seal without contacting a closed runtime.
The first prepared submission closes raw byte-only proposal paths on that owner;
an uncertain append observation fails the owner closed rather than permitting
another append or manufacturing a rejection proof.

SQL workers match rejection proofs to the exact preparation, not merely the
business proposal ID. Foreign engines, other cells and replacement producers
refuse old proofs. Cleanup closes the writer before releasing logical-name
admission. Queued submissions, prepared outputs and retained proofs keep their
original byte/operation reservations through cancellation and lost responses.
The real three-voter tests cover initialization and mutation in both image modes,
including a fresh preparation with the same proposal identity after rejection.

Intermediate failures exposed two fixture accounting issues. A one-slot runtime
could refuse a read-only status poll while retaining admitted peer work; the
fixture now retries that poll while pumping the cluster under its existing
deadline. Completed enrollment outputs then correctly exhausted the SQL pool's
four retained-operation reservations. The fixture now asserts that accounting,
drops the completed phase's outputs, and asserts zero reservations/bytes before
preparing mutations. Resource limits and rejection assertions remain unchanged.
All 197 selected Rust cases pass at this source: 182 native cases across seven
suites, 11 SQL queue/extent-worker unit tests and four receipt unit tests.
All-target Clippy with warnings denied, formatting (1,802 Rust files), layout,
dependency isolation, docs, source preflight and all 37 plan-validator tests pass.
The checkpoint at `.cellar/cell-groups-evidence/CG-04-submission/checkpoint.json`
binds 189 source inputs, ten successful command logs and eight copied executables.
It retains the initial failures, diagnostic backtraces and repaired targeted run
separately. Four process helpers run through parent tests; three RustFS-specific
cases remain unrun.

The two new cases raise the CG-04 contract to 12 required cases and the package
to 92. No task status advances. Registered SDK/catalog dispatch, coherent reads,
production retry/compaction integration and cache lifecycle remain required;
this checkpoint does not qualify the complete CG-04 or release gate.

## CG-04 bounded proposal acceptance and enrollment capacity

Qualified source: `446dd9057f10c8221122d0d3a9a21254adeb448f`.

The actual group owner now counts pending accepted creations alongside applied
cells before appending a new
enrollment. Existing cells continue accepting mutations at the configured limit.
Leader-local acceptance records bind a proposal to its exact bytes and original
position; retries cannot append a pending mutation twice. Changed bytes under the
same pending identity refuse, and independent proposals for a pending logical
name wait. Mutation bases are checked against the selected cell image. After
apply, callers resolve receipts or creation records instead of resubmitting stale
images; a refusal does not prove that the original request failed.

Acceptance retains a separate shared node budget until durable apply, role/term
loss, terminal failure or owner destruction. It retains no LTX payload or image pin and cannot
consume Ready/completion admission. A current-term applied barrier remains
mandatory before a new leader admits work, so inherited enrollments count even
when the previous process's pending map is gone.

The preliminary native run passed 25 cases and exposed a restart-fixture issue:
restarting only the prior leader leaves live followers' leader leases intact, so
its immediate pre-vote need not elect it. The fixture now restarts all voters from
their retained disks before requiring inherited apply. A later run exposed an
extra async constructor layer overflowing the fixture's default stack; that
layer was removed. A larger-stack diagnostic is retained only for diagnosis,
not qualification. It also exposed the fixture transport discarding overloaded
peer admission. The fixture now retains and retries those exact frames under its
existing deadlines, as the direct-owner fixture already does. All 27 shared-group
execution cases now pass on the normal stack.

All 195 selected Rust cases pass at the qualified source: 180 native cases across
seven suites, 11 SQL queue/extent-worker unit tests and four receipt unit tests.
All-target Clippy with warnings denied, formatting (1,800 Rust files), layout,
dependency isolation, docs, source preflight and all 37 plan-validator tests pass.
The checkpoint at
`.cellar/cell-groups-evidence/CG-04-proposal-admission/checkpoint.json` binds 187
source inputs, ten successful command logs and eight copied executables, with
intermediate failures and the larger-stack diagnostic retained separately. Four
process helpers run through parent tests; three RustFS cases remain unrun.

Four distinct admission cases are now required by the CG-04 executor gate,
bringing the package to 90 required cases. Their oracles are separate from the
existing SQL fairness and receipt-settlement cases. Registered dispatch, catalog
sessions, coherent reads, production retry/compaction and cache lifecycle remain
required. No task status or release claim advances.

## CG-04 shared-worker initialization and enrollment

Implementation source: `6e09bbc7`.

`CellEnrollmentAuthority` now binds an actual leader, current-term applied prefix,
route and logical cell name before a cell image exists. Initialization uses the
existing fixed SQL workers and admission budgets. One logical name reservation
covers commands and initializers, so a competing incarnation cannot bypass a
running or unresolved producer. Worker-owned SQLite initialization captures and
verifies its complete initial LTX image; followers apply those bytes without
running the callback. Root workspace preparation is shared with ordinary commands.

Prepared SQL outputs now carry their preparation term. `propose_at_term` checks
that fence on the real RawNode owner in the same turn as proposal admission.
Retained output cannot be appended under a newer term even after the original
node becomes leader again. The raw unfenced protocol API remains a verification
boundary; registered dispatch must use the fenced path.

Committed retries return the immutable creation record without opening SQLite or
rerunning initialization after later mutations. `CellEnrollmentState` observes
verified current-term applied creation state on followers as well as leaders.
Exact records prove commitment; missing/conflicting records retire old producers
only after a strictly newer applied term. Resolution closes the writer before
releasing name admission, while retained outputs keep their byte reservation.
Cancelled request and close waiters cannot release an entered initializer.

Four native cases cover both image modes: competing histories of one name,
cancelled initialization and exact resume, another cell committing while its peer
initializer is blocked, three independent cells under one physical group on each
voter, creation retries after mutation, conflicting creation refusal, cancelled
close and workspace release, follower resolution of absent/conflicting/committed
creations, and rejection of old prepared output after leadership returns.

All 191 selected Rust cases pass at `6e09bbc7`: 176 native cases across seven
suites, 11 SQL queue/extent-worker unit tests and four receipt unit tests.
All-target Clippy passes with warnings denied. The retained partial checkpoint
at `.cellar/cell-groups-evidence/CG-04-initialization/checkpoint.json` binds 184
source inputs, eight copied executables and the command logs. The gate stopped
at two formatting differences; that failure is retained. `82ae6877` changes only
line wrapping in those two authority checks. Successor verification is recorded
separately in `CG-04-initialization-format/gate-commands.json`: formatting
(1,798 Rust files), layout, dependency isolation, documentation, source preflight
and all 37 plan-validator tests pass. No Rust behavior changed and the Rust tests
were not repeated for line wrapping. Four process helpers execute through parent
tests; three RustFS cases remain explicitly unrun. No task status advances.

Configured group capacity policy, catalog routing sessions, registered SDK
creation/command callbacks, coherent reads,
production retry/compaction integration and preview/cache lifecycle remain open.
This is partial CG-04 implementation, not the final hard cutover or release gate.

## CG-04 receipt lookup independent from admission

Qualified source: `91c3ecfc020e0d42738e4a6395045eb8a44c51f9`.

Reconciliation now uses a read-only identity lookup that preserves sequence/floor,
request identity and result-allocation checks independently from capacity for new
clients or retained receipts. The command path still enforces both ceilings and
uses the same identity inspection; no extra client lookup is added to normal
admission. A full identity table can no longer leave an absent older-term
producer unresolved merely because it cannot admit a new client.

Both native image-mode fixtures fill the selected cell's identity capacity with
another committed client after leadership changes, then reconcile the original
absent request. The changed cell image forces actual receipt lookup. A subsequent
new-client command still refuses at the original limit; increasing its explicit
capacity allows a fresh command from the committed base. Separate cases replicate
bounded receipt maintenance through the real group and return CommandExpired for
the pruned original receipt, and CommandConflict for a different request that won
the same client sequence. They prove callback suppression, producer admission
release, retained-output accounting and a valid next command after the refusals.
Private SQLite tests preserve capacity admission, sequence gaps, floors and result
allocation bounds.

All 187 selected Rust cases pass: 172 native cases across seven suites, 11 SQL
queue/extent-worker unit tests and four receipt unit tests. All-target Clippy
with warnings denied, formatting (1,795 Rust files), layout, dependency isolation,
docs, source preflight and all 37 plan-validator tests pass. The checkpoint at
`.cellar/cell-groups-evidence/CG-04-receipt-lookup/checkpoint.json` binds 181 source
inputs, ten successful command logs and eight copied executables. The preliminary
four-case native pass is retained separately. Four process helpers execute through
parent tests; three RustFS cases remain explicitly unrun. No task status advances.

This remains partial CG-04 implementation. Concurrent initialization, coherent
reads, registered dispatch, production retry and actual compaction integration,
and preview/cache lifecycle remain required before its complete gate.

## CG-04 applied-barrier receipt reconciliation

Implementation source: `8a0cf640ae70f3280f02e281f81b55a8823205fd`.

The existing SQL pool now reconciles the exact pending proposal against the
original request identity in a selected immutable cell image. The new
`CellReconciliationAuthority` is issued from a verified durable current-term
applied group root, including on followers. It grants no write, strong-read or
S3 authority. A matching receipt may resolve from a later cell image without
requiring the original log entry. Its position identifies the inspected image.
A missing receipt releases the old producer only behind a strictly newer applied
term. Expiry/conflict remain distinct refusals; missing producers report uncertainty.

The worker validates engine, route, cell history and proposal identity, rechecks
pin freshness after receipt inspection, and closes the disposable writer before
releasing cell admission. Reconciliation uses reserved progress capacity; retained
receipt outputs keep their existing byte reservation.

Both image-mode tests use real three-voter elections and SQLite/LTX. They hold
the new-term apply on the old leader and reject premature reconciliation; advance
B again on the new leader while its first producer remains on the old follower;
then recover B's original receipt and retire A's absent older-term proposal.
They also cover same-term absence refusal, stale and foreign proofs, mismatched
proposals, unknown producer uncertainty, retained-output budgets, duplicate
callback suppression and reopening A from its committed base.

All 181 selected Rust tests pass: 170 native cases across seven suites and 11
SQL queue/extent-worker unit tests. All-target Clippy with warnings denied,
formatting (1,795 Rust files), layout, dependency isolation, docs, reviewed source
preflight and all 37 plan-validator tests pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-reconciliation/checkpoint.json` binds 179
source inputs, nine successful command logs and eight copied executables.
The initial fixture failure is retained: `MsgTransferLeader` is a local
administrative request, correctly rejected by the peer boundary. The corrected
fixture delivers the current leader's `MsgTimeoutNow` directive and drives the
real election, persistence and apply. Four process helpers run through parent
tests; three RustFS cases remain explicitly unrun. This remains partial CG-04
implementation; no task status advances.

Next, split read-only receipt lookup from new-client capacity admission and
qualify saturated identity tables, expiry floors and conflicting identities.
Then integrate real snapshot/compaction, production retry dispatch, concurrent
initialization, coherent query serving and preview/cache lifecycle. No full
CG-04, RustFS, performance or hard-cutover qualification is claimed.

## CG-04 fixed node SQL pool

Qualified source: `51dff0e5abec7c2355a80e84d1b2e3ae2504e3b1`.

`CellGroupSqlPool` now uses the existing fixed blocking worker pool and fairness
queue. Requests have typed logical cell keys; the old host adapter remains
separate during the staged cutover. The node creates no worker per enrolled cell.
A cell's admission lease remains with queued work, its physical callback and its
unresolved proposal, including when a caller cancels. A lost stage response can
resume the retained proposal without running another callback. Exact settlement
releases that cell; unrelated group progress cannot do so.

Node/group quotas reserve capture capacity before queueing. Once capture ends,
the reservation shrinks to the retained entry/result bound while keeping its
operation slot. Prepared-response clones and receipt delivery retain that charge.
A separate progress slot per worker permits settlement without needing ordinary
queue admission. Extent workspaces are created and destroyed on the admitted
blocking worker, so rejected requests cannot release their last physical workspace
on an async executor.

The pool validates the exact engine behind execution authority and claims an
exclusive SQL lease on that engine. Its cleanup task owns every worker join;
cancelling a close waiter cannot release a blocked callback. The engine lease
also survives through retained response admission, preventing a replacement pool
from bypassing live output reservations. Queue/stage/publication timers use the
existing node performance registry.

All 184 selected Rust tests pass: 168 native cases across seven suites, 11 SQL
queue/extent-worker unit tests and five budget tests. Both new pool cases use
actual three-voter replication and cover blocked/cancelled callbacks, independent
cell progress, group quota refusal, exact resume, duplicate receipts, engine
fencing, cancelled close and retained-response ownership. Four process helpers
are invoked by parent tests; three RustFS-specific cases remain explicitly unrun.

All-target Clippy with warnings denied, formatting (1,794 Rust files), module
layout, dependency isolation, docs, reviewed source preflight and all 37 plan
validator tests pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-sql-pool/checkpoint.json` binds 178 source inputs,
ten command logs and eight copied executables. Intermediate logs retain the
initial verification-export compilation error and direct-Tokio-select Clippy
failure, as well as passes preceding the final ownership refinements. Only the
final gate qualifies the committed source; no task status advances here.

Registered SDK dispatch, concurrent enrollment, strong-read serving, preview/cache
lifecycle and receipt reconciliation after leadership changes or compaction
still need integration.
Closing SQL does not prove an ambiguous submitted command failed; the owning host
must reconcile against committed receipts before admitting a new producer. CG-04
and the full hard-cutover goal remain incomplete.

## CG-04 per-cell SQL producer

Implementation source: `f3d48bdc`; qualified successor:
`f807b2f70e01e3cd8c741557889e1965fd14ec49`.

The synchronous `CellPrivateExecutor` executes one cell's private SQLite and
receipt transaction, captures its physical LTX update and encodes the actual
cell-addressed group entry. Materialized and extent-backed images share the
existing receipt admission, maintenance and transaction implementation. Physical
capture no longer requires a synthetic cell-scoped group identity.

The real leader issues an authority pin for the selected cell, route and term.
Another cell can advance without invalidating that base. Leadership loss, owner
failure or owner drop revokes the observation; the producer checks again after
capture. Settlement requires fresh authority, the exact committed entry bytes
and expected cell image/version. Duplicate receipts come from the selected
immutable cell image without another callback. An unresolved proposal remains
busy; closing its writer does not establish a failed proposal outcome.

The two new real-three-voter cases use the same client/sequence in two distinct
cells, commit B before A, refuse settling A with B's authority, then verify each
receipt and independent SQLite value on all voters. Both image modes also stop
the consensus owner while a SQL callback is blocked, then require a typed
Unavailable refusal and unchanged selected cell version after release.

All 133 selected native cases pass: 13 shared execution, 15 catalog, 15 active
storage, 21 prior execution, 50 rooted and 19 prior runtime cases. Four ignored
process helpers are invoked by their parent tests; three explicitly ignored
RustFS cases were not run. All-target Clippy with warnings denied, formatting
(1,790 Rust files), layout, native dependency isolation, docs, reviewed source
preflight and all 37 plan-validator tests pass. The partial record at
`.cellar/cell-groups-evidence/CG-04-cell-sql/checkpoint.json` binds 173 source inputs,
eight successful command logs and six copied executables. It retains preliminary
runs and the initial two-fixture Clippy failure; the successor removes only an
unnecessary result wrapper and borrow, without changing assertions.

The fixed SQL pool, fair per-cell admission, concurrent initialization,
registered SDK dispatch and coherent read serving still require integration.
The producer currently reopens after settlement and conservatively refuses
exact-entry settlement after compaction; receipt reconciliation, preview
retention and cache lifecycle must be integrated with the owning host. CG-04
remains IN_PROGRESS. No full cutover, RustFS drill or performance qualification
is claimed by this checkpoint.

## Earlier plan delivery and SQL draft handoff

The implementation package contains 16 dependency-ordered tasks, 86 required
cases, 14 invariants and nine qualification profiles. Each task specifies owning
paths, implementation steps, acceptance oracles and executable test commands.
The plan runner prints those commands and checks evidence; it does not execute
the implementation or provision infrastructure.

At plan delivery the working tree retained an unfinished per-cell SQL producer,
leader-issued execution authority, shared receipt helpers and tests for both
image formats. That draft has since been committed and qualified at the partial
SQL checkpoint above. The plan delivery itself advanced no task status or
runtime qualification.

Plan delivery checks pass: package validation, reviewed source preflight, all
37 validator unit tests and repository documentation validation (113 documents,
2,314 local references). `show CG-04` emits the full executor brief. The release
command correctly refuses incomplete implementation evidence. The branch
contains the latest fetched `origin/main` at `8a93ffd7`; neither requested
MR-13/MR-14 progress JSON exclusion differs from main.

## CG-04 attachment admission and main integration

Source: `aec4029e77124848dc6b155c098f8eec9ccdacc4`.

Attachment now reserves ordinary node capacity before catalog revalidation and
RawNode construction, retaining it through dispatch or failure. A public-runtime
regression holds a real vote's engine sync and fills the sole request slot; a
second attachment refuses with overload before claiming an owner. Releasing sync
allows the queued protocol work and shutdown to finish at the same limits.
The first fixture attempted the normal multi-peer pump with one slot and refused
transport overload before the target assertion; the final fixture isolates the
startup boundary without relaxing admission.

The branch includes `origin/main` at `8a93ffd7`. Its documentation consolidation
moves this package to `docs/design/multi-raft/cell-groups/`. Commands, links,
catalog entries, source-root discovery and the plan-only release-input exclusion
now follow that location. Validator tests exercise both the new design path and
historical `plans/` bookkeeping. Existing evidence retains its original source,
paths and hashes; moved documentation is not fabricated runtime qualification.

All 60 selected Rust cases pass: 11 execution, 15 catalog, 15 active storage
and 19 active existing-runtime cases. All-target Clippy with warnings denied,
formatting (1,786 Rust files), layout, dependency isolation, documentation,
source preflight and all 37 plan-validator tests pass. The partial checkpoint at
`.cellar/cell-groups-evidence/CG-04-runtime-admission/checkpoint.json` retains
165 source inputs, eight command logs, four executables and intermediate failure
logs. The larger source inventory includes main's documentation/tooling changes.
CG-04 remains IN_PROGRESS; this does not qualify per-cell SQL integration, SDK
serving, real RustFS recovery, performance or the final hard cutover.

## CG-04 shared-node runtime

Source: `865f253da0a14d730b62f77b17cefc6d1497890b`.

One execution-domain task now multiplexes physical cell-group owners on each
node. It uses the existing engine, mailbox and admission primitives, with a
single in-flight persistence batch containing writes from multiple group
histories. Exact engine receipts complete the corresponding owner tickets.
Group turns and deferred transport/proposal work remain bounded; ticks coalesce.
An admitted transport frame is not a persistence or application acknowledgement.

Apply events retain admission through image preparation and physical root
publication. Completion requests have reserved capacity. Already-started physical
phases can finish when ordinary event delivery is full; one reserved settlement
slot permits releasing an apply reservation without admitting more ordinary
output. A two-slot test exposed the original circular wait, and the fix changes
phase progress rather than increasing the limit.

Shutdown closes admission and joins submitted engine work before releasing node
runtime ownership. Cancelling the caller's close waiter cannot release that
ownership. The existing runtime shares the terminal completion primitive with
the new worker; its behavior remains covered by its native regression suite.

All 59 selected Rust cases pass: ten execution, 15 catalog, 15 active storage
and 19 active existing-runtime regressions. Two process helpers are ignored by
direct execution and exercised by their parent tests. All-target Clippy for
`cellar`/`cellar-logic` with warnings denied, formatting (1,786 Rust files), layout,
native dependency isolation, docs, source preflight and all 37 plan-validator
tests pass. The retained partial record is
`.cellar/cell-groups-evidence/CG-04-runtime/checkpoint.json`, binding 85 source
inputs, eight command logs and four native executables. The initial two-slot
failure and successful intermediate repeats are retained without claiming a
source-bound intermediate qualification. CG-04 remains IN_PROGRESS.

This is the shared consensus/persistence scheduler. Per-cell SQL preparation and
fair admission, concurrent initialization, exact receipt settlement and SDK read
serving still need integration. The full CG-04 gate remains required. Complete
group snapshots, membership/topology, managed startup, RustFS qualification and
the final hard cutover remain unfinished.

## CG-04 physical consensus owner checkpoint

Source: `51e30eabde1ca76bf0b23963dd06d0e8130f8ee7` (successor to `0951440e`).
CG-04 remains IN_PROGRESS. The physical owner now attaches one real RawNode to
each group replica using the catalog's initial-member permit and a completely
verified selected root. The permit is revalidated before attachment. Each
GroupLog shares an exclusive owner lease; persistence requests retain it until
physical engine work finishes, even if the owner and its awaiting future are
dropped. A duplicate attachment cannot race an outstanding write.

Ready and LightReady persistence completes before dependent messages, committed
application or read barriers are released. Image preparation is an external
effect and cannot advance Raft apply. Completion must match the exact owner,
entry digest/position, preceding root, engine and page-store owner. The new root
is published through the existing engine transaction before advancing the
contiguous prefix. No-ops preserve all cell versions. ReadIndex output remains
a barrier, not SDK serving authority.

The local three-voter harness replicates three independent SQLite histories and
interleaved LTX updates. It holds both followers' first apply while two entries
commit, rejects foreign and incorrect-image completions, then restarts one
follower and replays its committed tail. The restart joins the engine; it is not
a process-kill or power-loss test. Separate tests exercise minority read refusal,
cancellation while a real log sync is held, and 40 versions with a backpressured
follower that later reaches the exact final SQLite bytes. A peer counter-boundary
regression rejects input before changing the owner's term or applied state.

Ready collection accounts for small-entry overhead and the two in-flight batches'
maximum entries. A saturated apply queue backpressures new protocol requests.
Ticks coalesce behind pending Ready work so a stalled sync does not accumulate
unbounded heartbeat/election messages. Large physical effect and phase payloads
are boxed, reducing the space paid by every lightweight event.

All 36 selected Rust cases pass: six physical-owner cases, 15 catalog cases and
15 active storage cases. The ignored process fixture is run by its parent crash
test. All-target Clippy for `cellar`/`cellar-logic` with warnings denied,
formatting (1,783 Rust files), layout, dependency isolation, docs, source preflight
and all 37 plan-validator tests pass. The retained partial record is
`.cellar/cell-groups-evidence/CG-04-owner/checkpoint.json`, binding 82 source inputs,
eight successful command logs and three executables. It is not a CG-04 DONE record.

The initial owner fixture compilation and pre-vote persistence
expectation failed and are retained. Pre-vote has no durable vote; the corrected
sync test holds an actual data append after election. An initial Clippy run
rejected oversized enum variants; the implementation fixes their layout without
suppressing the lint. That source-bound failed gate remains at
`.cellar/cell-groups-evidence/CG-04-owner-failed-0951440e/gate-commands.json`;
earlier uncommitted fixture failure logs are retained under the final checkpoint's
`intermediate/` directory and are not treated as source-bound qualification.

The existing shared scheduler and SQL worker still consume the prior single-cell
owner. Next, integrate this physical owner with those node resources, bounded
per-cell producers, fair admission, exact receipt settlement and coherent reads.
The complete CG-04 six-case gate remains required, including real enrollment
concurrency and callback ownership. Snapshot/configuration/topology requests
currently refuse; their complete-group protocols, managed startup, RustFS,
independent failure domains and final hard cutover remain unfinished.

## Plan delivery and working-state audit — 2026-10-29

The package has 16 tasks, 86 required cases, 14 invariants and nine qualification
profiles. Its contract/evidence validation and CG-04 brief generation pass.
Freshly fetched `origin/main` is `1be27d234d4674d4d014785c60cecbfe394bdab9`,
already an ancestor of the delivery branch. The supplied TiKV checkout still
matches the investigation pin. Neither excluded MR-13/MR-14 progress JSON file
appears in the branch diff.

There is additional uncommitted CG-04 owner work in this checkout. It is not
part of the reviewed baseline or completed evidence. The new execution test
target currently fails compilation: `GroupRoute` is ambiguous through glob
imports and the fixture calls the private `ImageCommitment::sha256` method.
The global preflight and its unit test refuse the changed `cache.rs`; the other
36 validator tests pass. Preserve this refusal until the implementation is
reviewed and a successor baseline is recorded. Do not reset those edits or
silently update hashes to make the handoff appear qualified.

In a separate checkout of `fde3e6f9` with this plan update and the three retained
DONE evidence directories, source preflight and all 37 validator tests pass.
The release command exits nonzero with `implementation tasks/evidence incomplete`,
as required. Repository docs validation passes with 869 local references. These
checks qualify the committed plan handoff; they do not qualify the dirty runtime
prototype. An initial export-based validation attempt had incorrect Git isolation;
its temporary local metadata changes were restored before this independent rerun.

The [handoff](handoff.md) separates this working state from the committed
starting point and lists the remaining CG-04 acceptance cases. No runtime task
is advanced by this plan delivery.

## CG-04 durable enrollment checkpoint

Source: `c187c47773fd22d4658de1d912b6096f92a8e26d` (successor to `9f937f22`).
CG-04 remains IN_PROGRESS.

Each authenticated cell leaf now requires the original creation operation,
logical cell history, schema and initial image commitment. Publication includes
that record in the same complete root as the current image. Mutations refuse a
changed record; sequence one must match the initial image and later versions
advance its LTX position. Page decoding rejects missing/foreign creation records
at marker 1. No old-layout reader or inferred enrollment is introduced. The
physical pin-tree budget includes the record's additional cell identity.

Ordered enrollment preparation checks the route, preceding complete root,
contiguous apply position and committed lifecycle phase before invoking a physical
image producer. Exact retries keep the latest image, receipts and cell version
and advance only the group prefix. This unchanged-root path reuses image/page
pins without page I/O and refuses quarantined stores. Engine publication still
requires the exact preceding branch and committed position.

Native recovery exercises actual SQLite/LTX and Raft Engine in materialized and
extent modes. It mutates a cell, restarts, reclaims the original image, then
replays enrollment without invoking the image producer. The latest values and
receipts survive another restart, while the neighboring cell remains unchanged.
Sealed/retired ranges, competing creations and invalid apply positions refuse.
Existing process-kill cases also verify creation identity after recovery.

The new page-wire tests preserve canonical JSON ordering when corrupting identity
fields, so format ordering cannot mask a missing identity check. A quarantine
fixture fails the actual manifest directory-sync barrier and checks fault
consumption. Its initial run selected an image-file-only fault and failed because
no page I/O was interrupted; that failed run is retained separately.

All 32 selected Rust cases pass at this source: 15 catalog cases, 15 active
storage cases and two page-wire unit cases. The ignored process fixture is run
by its parent crash test. All-target Clippy for `cellar`/`cellar-logic` with warnings
denied, formatting (1,781 Rust files), layout, dependency isolation, repository
docs, source preflight and all 37 plan-validator tests pass. The retained partial
record is `.cellar/cell-groups-evidence/CG-04-enrollment/checkpoint.json`, binding
79 source inputs, nine successful command logs and three executables.
The failed fault-injection run remains under
`.cellar/cell-groups-evidence/CG-04-enrollment-failed-9f937f22/`.

This is an ordered preparation/storage checkpoint. The existing `Replica` and
shared worker still consume the staged single-cell log/apply types. The next
implementation must connect physical `GroupHistory` and complete-root application
to that real consensus owner, then wire independent SQLite execution, fair
admission, receipts and reads. The six CG-04 real-Raft acceptance cases remain
required. No live shared owner, RustFS, load or hard-cutover qualification is
claimed by this checkpoint.

## CG-03 completion audit and CG-04 continuation

The audit checks the four CG-03 steps against retained evidence at
`0df009a24d88408a8efc40e356f794ca35513a95`:

- Paged directory tests exercise exact coverage, bounded discovery, conditional
  publication and recovery from lost replies using real conditional operations.
- Enrollment tests check pure creation identity and sealed/retired admission;
  the task explicitly assigns real SQLite enrollment to CG-04.
- Native member bootstrap exercises synced physical genesis, permanent claims,
  restart, unknown outcomes and refusal after claimed local records are removed.
  Storage boundary tests separately verify physical identities and inventory.
- Route tests check trusted-catalog confirmation, cached operation during catalog
  failure, original deadlines and attempt budgets, cancellation and command ambiguity.

The full required command gate, source/log/artifact hashes and dependency evidence
validate. CG-03 is DONE only at that catalog/routing/admission-preparation boundary.
The record retains 77 source inputs, 20 successful commands, three executables and
44 distinct Rust cases. It does not qualify live shared dispatch, network peer
authentication, managed startup, current-term serving authority or RustFS.

CG-04 now addresses original enrollment identity in the durable manifest, followed
by shared group ownership and execution. Its full six-case real-Raft execution
gate remains required; storage-only tests cannot substitute for that gate.

## Plan handoff at b3b7eabd

The package contains 16 dependency-ordered tasks, 86 required cases, 14 invariants
and nine qualification profiles. Each task supplies source anchors, owned paths,
implementation steps, exact test commands and acceptance oracles. CG-01 and CG-02
retain their historical DONE evidence. CG-03 remains IN_PROGRESS pending the
implementation executor's completion audit; CG-04 through CG-16 remain TODO.
This handoff does not advance runtime status merely because a plan validates.

The reviewed source is `0df009a24d88408a8efc40e356f794ca35513a95`. Existing routing
sessions now retain the original logical request, command identity, deadline and
attempt budget across retries. Relocation requires exact confirmation against the
configured trusted catalog; cached requests need no catalog read. Cancelled
lookup cannot refund an attempt, and a possibly submitted command retains its
ambiguous outcome. Catalog confirmation does not authenticate a network peer or
grant serving authority. Actual shared dispatch remains CG-04/CG-07 work.

The retained record at
`.cellar/cell-groups-evidence/CG-03-routing-final/record.json` binds 77 source
inputs, 20 successful command logs, three test executables and 44 distinct Rust
test cases to that revision. Its gate includes all-target check and Clippy,
formatting, layout, dependency isolation and docs checks. All 37 plan-validator
tests pass. The earlier Clippy rejection of a forbidden test macro is retained;
the final source replaces it with a permitted selector without suppressing the
lint. These are existing local qualification results, not a RustFS drill or a
shared runtime performance claim.

For this handoff, package validation, reviewed-source preflight, validator tests,
executor-brief generation and repository docs checks are rerun. The release gate
must still refuse incomplete implementation. The branch includes freshly fetched
`origin/main` at `1be27d234d4674d4d014785c60cecbfe394bdab9`; the supplied TiKV checkout
matches the investigation pin. The requested MR-13/MR-14 progress JSON files
remain excluded from the PR.

## CG-03 physical bootstrap and inventory checkpoint

Source: `5d0d8e5ede3b4efedc211c40122a1a26b43968b4`. CG-03 remains IN_PROGRESS.

Physical handles now carry `GroupHistory` independently of application cells.
Real-engine genesis atomically syncs the mapping, initial membership and complete
empty group root. The same persistence worker owns both typed storage paths,
quotas, sync and cache publication. Required mapping application domains refuse
old untagged records; an engine cannot mix legacy and cell-group application
families. Existing single-cell consumers remain staged until the final cutover.

The initial unmanaged-member bootstrap API verifies the current selected birth,
permanent node-storage pin and exact member claim after actual genesis sync.
Claims bind group history, birth, member, node, disk and engine instance. Unknown
confirmation grants no permit; same-identity restart reconciles it. Tests remove
one owned group's records while retaining the original engine identity, then
verify that the permanent claim refuses fresh genesis. A separate blank-engine
fixture exercises the node pin. Foreign nodes/catalog owners and retired routes
also refuse; managed admission still requires its later rooted fleet integration.

Physical inventory now captures every shared history, including unattached or
retained members, and an optional initializing slot. A domain-separated digest
binds all four physical IDs without a cell name. The bounded existing wire checks
the complete occupied count; an already durable pending slot counts once. Legacy
cell-scoped lookup cannot match a group-native row. Real-engine tests verify
inventory across restart and while genesis sync is held, including wire refusal
when the node count omits a slot.

All 216 distinct Rust cases pass across selected genesis, physical inventory,
membership, core, directory, catalog, manifest, host, snapshot and storage suites.
The shared manifest recovery fixtures now use actual cell-group genesis for both
image representations and process-kill cuts. The RustFS-specific snapshot case
remains explicitly ignored; child fixtures are invoked by their parent tests.
These results do not qualify a live shared-group host or independent failure
resilience. Source preflight, all 37 plan-validator tests, formatting, layout,
dependency isolation and repository docs checks pass. The dependency script's
initial missing-PATH failure is retained separately from its successful rerun.

All-target Clippy for `cellar` and `cellar-logic` also passes with warnings denied.
The partial evidence record is retained at
`.cellar/cell-groups-evidence/CG-03-bootstrap/checkpoint.json`, binding 76 source
inputs, 14 successful command logs and eight executables to the source revision.
This is a checkpoint, not a CG-03 DONE record.

A catalog bootstrap permit is not complete image recovery or serving authority.
The runtime still must verify the selected image/manifest closure, revalidate its
startup permit and obtain current-term quorum. Authenticated bounded redirects,
the shared runtime, managed fleet integration and CG-04 through CG-16 remain
required. No RustFS disaster drill, load result, performance improvement or full
CG-03/release qualification is claimed by this checkpoint.

## Plan handoff at 98aff95a — 2026-10-29

The package supplies the target architecture, protocol state machines, dependency
ordered executor briefs, exact test commands and source-bound acceptance gates.
CG-01 and CG-02 retain their historical DONE evidence, CG-03 is IN_PROGRESS, and
CG-04 through CG-16 remain TODO. The final release gate remains closed.

The branch includes freshly fetched `origin/main` at
`1be27d234d4674d4d014785c60cecbfe394bdab9`. The supplied TiKV checkout matches the
investigation pin `b3995dab28308f09cbb7c125bf32f789dbc203fe`.

Uncommitted physical bootstrap, replica-handle and permanent member-claim changes
remain in this checkout, including two new Rust files and native test changes.
They are preserved for the next implementation executor and are not qualified by
this documentation handoff. Review their full diff and test results before
committing them or advancing the source baseline. Do not restart this work from
the earlier metadata-only checkpoint.

Package structural validation passes: 16 tasks, 83 required cases, 14 invariants
and nine profiles. Working-tree source preflight correctly refuses drift in
`crates/cellar-logic/consensus.rs`; 34 of 37 validator tests pass, with the three
source-sensitive checks failing against that unfinished source. These failures
are not waived. The unchanged committed source is checked separately from the
draft runtime.

Handoff verification: a temporary read-only export of committed source
`aead5cc04a9788ab2f2ee5f3fba3d6468fde5b75` passes the source preflight function and
all 37 plan-validator unit tests. This export excludes the draft runtime and
does not requalify historical DONE evidence. Working-tree structural validation,
CG-03 brief generation, repository documentation checks and `git diff --check`
pass. `release` exits nonzero for incomplete implementation evidence, as required.
The requested MR-13/MR-14 progress JSON files have no diff from `origin/main`.

## Plan handoff at 462aa2eb — 2026-10-29

The package describes the complete replacement architecture, ordered implementation
briefs and source-bound qualification gates. CG-01 and CG-02 retain their existing
DONE evidence; CG-03 remains IN_PROGRESS and CG-04 through CG-16 remain TODO.
The next executor starts with physical group identity, real genesis/member-claim
integration and bounded redirects, then follows the task graph. This handoff adds
no runtime implementation or new runtime qualification claim.

The branch includes fetched `origin/main` at
`1be27d234d4674d4d014785c60cecbfe394bdab9`. The supplied TiKV checkout remains at
`b3995dab28308f09cbb7c125bf32f789dbc203fe`. Existing local DONE evidence must
accompany a fresh checkout to validate those checkpoints. Final release remains
closed; a validated plan does not prove shared-group performance or recovery.

Handoff checks pass: package validation (16 tasks, 83 required cases, 14 invariants,
nine profiles), reviewed source preflight, all 37 validator unit tests, repository
documentation checks and `git diff --check`. The CG-03 executor brief renders
successfully. `release` exits nonzero as expected because implementation evidence
is incomplete. Runtime suites were not rerun for these plan-only edits.

## Historical plan handoff — 2026-10-29

This section records the state at `dcc52840`; the directory checkpoint below
supersedes its working-diff and source-preflight limitations.

The package validator passes: 16 tasks, 83 required cases, 14 invariants and nine
profiles. Task briefs include source excerpts, file scope, dependencies, exact
commands and evidence requirements. The TiKV investigation pin still matches the
supplied checkout. The branch includes freshly fetched `origin/main` at
`1be27d234d4674d4d014785c60cecbfe394bdab9`.

Uncommitted CG-03 directory work remains in this checkout: immutable range pages,
root publication and mutation journals, with native catalog tests. The observed
native run passes seven tests. This is not a complete CG-03 qualification and is
not included in a DONE record. Preserve the working diff for its next executor.

The source preflight correctly refuses the draft `catalog.rs` module exports
against the recorded baseline. Consequently the current-checkout validator suite
passes 36 of 37 tests; its actual-source preflight test refuses that drift. Do not
blindly refresh the baseline or mark CG-03 complete. Review and qualify the draft,
commit its source, then record a successor baseline with updated excerpt lines.
Birth/disk-claim integration and bounded redirects remain outstanding. The final
release gate remains closed.

## CG-01 — complete at the contract boundary

Qualified source: `ee7f4a8d2dec220904f5a6860041f12c14db1cf5`.

- Dependency-free cell/group histories, validated cell IDs, monotone counters,
  canonical routing hashes and collision-safe half-open ranges.
- Logical cell versions and signed tokens independent of physical log position.
  Normal transfer preserves history; disaster recovery changes incarnation.
- Materialized and extent commitment adapters with geometry validation.
- A bounded, domain-separated entry codec for cell-addressed mutations and typed
  enrollment, membership, checkpoint, seal/install/activate, park and retire.
  Enrollment carries replayable initial LTX bytes; decoding borrows payloads.

The [entry codec contract](../../../../crates/cellar/application/consensus/group_protocol/README.md)
documents wire layout and checks that remain the runtime owner's responsibility.
The contracts are exposed through the unstable verification surface. Production
consumers still use the old contracts until CG-07; legacy removal follows in CG-16.

The retained source-bound record is
`.cellar/cell-groups-evidence/CG-01-ee7f4a8d2dec/record.json`. It binds the tested
Git revision, source inputs, native test executable and all 13 command logs by
hash. The complete gate passed:

- All-target checks and Clippy for `cellar` and `cellar-logic`, with locked builds
  and Clippy warnings denied.
- All four required CG-01 cases selected individually by exact name, plus test
  inventory verification to reject an empty selection.
- All 7 native cell-group contract cases, 6 existing native Raft contract cases
  and 18 core library cases: 31 distinct Rust tests.
- Repository formatting, module layout, native dependency isolation and docs.

Tests include SQLite reconstruction from enrollment bytes, an independent wire
vector, truncated/oversized metadata, cross-cell/history refusal, token tampering
and transfer versus disaster identity. These are contract tests, not proof of a
running multi-cell group, a RustFS drill or a performance claim.

The earlier short-ID token-bound failure and intermediate logs remain under
ignored local evidence. The corrected regression passes. Code review also caught
and removed a root-only enrollment gap before the final gate: nonempty enrollment
must include its initial replayable LTX chain.

## Remaining implementation

CG-01 through CG-03 have passed their scoped task gates. CG-04 is IN_PROGRESS;
CG-05 through CG-16 remain TODO. Historical entries below retain the intermediate
checkpoints and their limitations.

The reviewed source baseline is the CG-04 enrollment checkpoint at `c187c477`.
A new executor must receive all three ignored DONE-task evidence directories
before validating those historical checkpoints. Current CG-04 source changes
remain subject to their own verification and recorded review.

No shared runtime, split/merge, hibernation latency, real RustFS recovery, measured
density or final hard cutover is qualified. The release gate remains closed until
all task and deployment evidence passes at one final revision.


## CG-02 — manifest pages in progress

The first storage slice implements a bounded immutable AVL manifest and an
exclusively owned filesystem page store. Leaves bind logical versions, geometry,
last mutation and effect summaries; local image filenames do not affect roots.
Updates compare the old cell version and copy only the changed tree path.
Page publication syncs files and directories and quarantines uncertain writes.

The new native storage target uses real SQLite/LTX and sealed generation pins.
All three current cases pass: path-copy bounds across 64 independently sealed
SQLite cells, unchanged image commitments and old-root reads; restart and
malformed/missing page refusal; conflicts/capacity, replica-independent roots and
injected page-directory sync failure. These are page-store checks; the complete CG-02
gate has not run. The first build failed on an unavailable error derive and a
follow-up build caught argument mismatches. The first running suite exposed a
missing link-aware metadata method in the fault fixture; it now delegates to the
real filesystem instead of weakening the page-store inventory check.

The engine root marker, image adapters for both representations, physical root
pins/reclamation and actual engine crash-cut tests remain outstanding. No
manifest can yet grant shared-group serving authority. The [component contract](../../../../crates/cellar/durability/raft/cell_groups/README.md)
records this boundary. Checkpoint logs are retained under
`.cellar/cell-groups-evidence/CG-02-manifest-pages/`. The CG-02 scope includes the consensus module's unstable
verification exports so the native tests exercise the actual component.

Clippy passes for the library and new native target with warnings denied. The
36 plan-validator tests, source preflight, native dependency isolation and docs
checks also pass. These checks do not substitute for the still-missing CG-02
engine publication, pin reclamation and both-image-mode qualification cases.


### CG-02 engine publication checkpoint

Complete-root preparation now checks each leaf against an actual image pin,
syncs the manifest closure and holds image pins through engine persistence. The
root binds group history, range/epoch, birth/schema, membership revision and the
exact applied position. Raft Engine atomically selects that root with log/hard
state and publishes its cache only after sync. Legacy image accessors refuse a
shared-group root, and normal apply cannot skip an index or change its route.

The local application record uses explicit marker-1 image/group domains; old
bare-image record shapes refuse. No disk migration or deletion is performed.
The existing image variant remains a staging contract until the public cutover.

New native tests exercise a stalled real-engine sync, restart, missing-cell-image
refusal and rejection of preparation from a previous engine instance. Five
manifest cases and the 19-case existing storage suite pass, including the
engine-instance check. All-target Clippy passes after boxing the larger metadata
variant; the failed lint output is retained. Final checks are recorded under
`.cellar/cell-groups-evidence/CG-02-engine-root/`.
The standalone storage child-process fixture is intentionally ignored and is
launched by its parent persistence tests.

Incremental apply preparation, durable root/pin reclamation, root-specific process
kill cuts and full extent-mode recovery tests remain CG-02 work. The production
shared-group host, topology and final release gates remain pending.


The engine-root checkpoint passes all-target Clippy, formatting and module layout,
24 directly selected native tests, dependency isolation and docs checks. The
reviewed source baseline is `4387fedd`. All 37 plan-validator tests pass. Updating
the baseline exposed a verifier bug: historical evidence was compared backward
against the moving review baseline. Evidence now covers every runtime change
since the frozen plan starting revision. A regression test ensures later baseline
reviews neither invalidate historical inputs nor waive earlier changed files.
This correction does not make CG-02 DONE or relax the final release-source gate.

### CG-02 incremental preparation checkpoint

Ordinary cell mutations now prepare against a complete prior root without scanning
or copying all cell pins. The physical pin tree mirrors the authenticated AVL
manifest, shares unchanged subtrees and copies changed paths/rotations. Each
mutation checks the changed image, expected cell version and next group position.
Page reads and allocations are logarithmic; lookup comparisons are bounded by
O(log² cells). Root clones are constant-time and memory admission charges the
complete owned tree, including node and cell-ID allocations.

The engine checks the exact base root, so a staged successor from a competing
branch cannot replace the selected branch merely because its index is next.
Identical already-selected roots remain retryable. Capabilities also bind the
page-store instance; reopening requires complete preparation. A coherent root can
return a selected cell leaf plus an independent physical image pin.

Native cases cover one update among 64 real SQLite cells, bounded page reads and
writes, competing-branch refusal, identical retry, page-owner replacement, and
96 incremental enrollments across AVL rotations. Reclamation checks keep the old
image while its reader survives, reclaim it after release, and retain all 64
selected images through the engine's root owner after caller handles are dropped.
The first incremental run exposed an overly strict retry base check; the corrected
run is retained beside that failure under ignored local evidence.

This is not the complete pin/reclamation gate. Page reclamation, authoritative
retention across uncertain engine outcomes, root-specific process-kill cuts and
full extent-mode recovery remain CG-02 work. The production shared-group host and
the full hard-cutover goal remain pending; CG-02 stays IN_PROGRESS.

Runtime checkpoint `b71f62b5` passes all 7 native manifest cases and 19 existing
Raft storage cases, all-target Clippy with warnings denied, formatting, layout,
native dependency isolation and docs checks. All 37 plan-validator tests and the
reviewed source preflight pass. The initial source-drift refusal correctly required
review of the changed cache anchor before advancing the baseline. Source and
executable hashes, failed/corrected test output and final check logs are retained
under `.cellar/cell-groups-evidence/CG-02-incremental-pins/`.

### CG-02 reclamation and uncertain publication checkpoint

Prepared pin-tree nodes now lease their physical manifest pages and keep the
directory lock alive. Bounded collection scans an explicit inventory window,
removes only unleased pages, and refunds page/byte quotas only after the directory
sync. Remove or sync failure quarantines the owner without speculative refunds.
Every reopened store requires root inventory reconciliation before collection.

The engine now reserves old/candidate closures before writing and selects the new
retention reference only after sync and cache publication. Dropping an armed
reservation on failure keeps both page/image closures and blocks collection until
recovery; pre-write refusals cancel unarmed reservations. The page owner retains
uncertain images even after request and engine handles disappear. Complete
recovery automatically retains a root only when it matches the selected engine
marker. Additional snapshot/archive/transition roots have an explicit bounded
retained set, independent of disposable caller handles.

The owning shared host must still supply the complete catalog/checkpoint/transition
inventory to `reconcile_retained`. This is a trusted owner contract; page filenames
cannot discover authority, and these storage fixtures do not qualify that future
host integration. CG-02 also still requires the root-specific process-kill matrix
and full extent-mode recovery qualification. It remains IN_PROGRESS.

Runtime checkpoint `f41f470f` passes 11 native manifest cases and 19 existing Raft
storage cases. New checks cover independent snapshot/transition retention,
collection during stalled apply, quotas after removal/barrier failures, and
two-group batch refusal before any write. A real failure injected after fsync
retains both images after request/engine handles drop; restart selects the new
durable root before reclaiming the superseded image/page.

All-target Clippy with warnings denied, formatting, layout, native dependency
isolation, docs, source preflight and all 37 plan-validator tests pass. The first
test build required owned fixture paths; the first lint run required replacing
an unnecessary clone used to construct a one-element slice. Neither fix weakened
an assertion. Source hashes, copied test executables and failed/corrected logs
are retained under `.cellar/cell-groups-evidence/CG-02-reclamation/`. This is a
storage checkpoint, not the complete CG-02 evidence gate or a RustFS qualification.


### CG-02 — complete storage qualification

Qualified source: `ec9d277e50416f786fe3c1cd851b4147f9cd4b43`.
The retained record is `.cellar/cell-groups-evidence/CG-02-ec9d277e5041/record.json`.
It binds 45 source inputs, 16 successful command logs and three copied native
executables. All five required cases were present in the inventory and each
passed when selected individually. The complete gate includes all-target checks
and Clippy, formatting/layout, dependency isolation, docs and 37 validator tests.

The 14-case manifest suite covers both materialized and extent-backed SQLite
images. Thirty actual child-process kills span image and manifest writes/sync,
engine sync, publication and reclamation. Recovery selects a complete old or new
root, checks both cells' independent values and receipt rows, and verifies that
selected data remains readable after orphan reclamation. Missing or corrupt
manifest pages, sibling images, extent roots and non-header extents refuse
complete recovery. Extent updates also preserve an unchanged shared tail extent.

All 19 existing Raft storage cases and the generation-store image sync regression
pass. The latter exercises five additional child-process cuts with the shared
fault adapter's original behavior. Standalone child fixtures remain intentionally
ignored by direct suite execution and are launched by their parent tests.

This completes CG-02 at its storage boundary. Process kills do not simulate power
loss. Receipt rows are physical-state checks, not SDK deduplication qualification.
The shared host must still integrate authoritative root inventories, admission,
quorum, snapshots and archives. No RustFS drill, split/merge, hibernation latency,
density claim or final hard cutover is qualified by these tests. CG-03 is next.


## CG-03 — enrollment admission in progress

The first CG-03 slice adds deterministic admission using the existing bounded
cell-addressed entry codec. Requests verify group history, route epoch, birth,
schema and the cell's half-open range. Sealed or retired lifecycle state refuses
delayed enrollment. Concurrent preparations must be checked again in committed
log order; only the selected original creation record can resolve a retry.

The bounded canonical `EnrollmentRecord` binds logical cell history, original
creation operation, schema and initial image commitment. It survives normal
transfer without binding to the old physical group. The host must retain it
atomically with enrollment, preserve it through mutations and include it in
snapshots/archives. Returning an existing record grants no quorum/read authority
and must never restore the initial image over newer state.

Three native admission cases pass: creation conflict/retry and route identity,
sealed/retired refusal, and bounded canonical record decoding. Negative record
checks preserve field order so rejection cannot be attributed only to JSON map
reordering. Empty records require the canonical empty image and zero checksum.
The initial build required explicit image wire fields; an intermediate running
suite exposed the absent empty-image checksum check. Failed and corrected logs
are retained with the checkpoint rather than suppressed.

This is a pure admission boundary. The paged range tree and conditional namespace
root journal, lost birth/root response reconciliation, permanent blank-disk
refusal integration and bounded route discovery remain CG-03 work. CG-04 must
connect admission and durable creation records to the real shared group host.
CG-03 remains IN_PROGRESS; the production hard cutover is not complete.


Enrollment checkpoint `b1d27582` passes its three native cases, all-target Clippy
with warnings denied, formatting/layout, native dependency isolation, docs,
source preflight and all 37 plan-validator tests. Evidence under
`.cellar/cell-groups-evidence/CG-03-enrollment/checkpoint.json` binds 40 source
inputs, nine successful checks and a copied native executable. The failed initial
build/test outputs and a dependency-check environment failure remain retained;
the latter passed after restoring the Cargo path. This is a partial checkpoint,
not a CG-03 DONE record.

### CG-03 range-directory checkpoint

Source `a0c11969763edbe86ee08fc7d1650129058c8ee0` adds bounded immutable AVL range
pages, path-copy replacement, a conditional head selecting the exact mutation
journal and operation/group indexes, and permanent genesis pinning. Historical
retry confirmation survives later root updates. Duplicate IDs, stale parents,
changed coverage, wrong epochs, reused operations and missing/corrupt selected
dependencies refuse. Retired group IDs remain reserved. A cached discovery path
does not refresh S3 metadata or list the namespace.

All eight native cell-group catalog tests pass. These include concurrent CAS,
lost response/restart, initialization racing a later mutation, 128-range bounded
lookup/path-copy checks, and 80 alternating split/merge metadata transitions. An
independent flat partition checks every resulting boundary plus unrelated cell
keys; original views remain readable throughout churn. The 13 existing native
fleet-catalog regressions also pass with the shared fault adapter. The adapter's
new barriers are inactive unless explicitly armed for directory operations.

The directory has no serving authority. Birth/disk claims, managed birth
authorization, deadline/hop-bounded client redirects and the real shared host
remain outstanding. No live split/merge, Raft quorum or RustFS qualification is
claimed. CG-03 remains IN_PROGRESS and CG-04 through CG-16 remain TODO. The full
hard-cutover goal is active; the earlier plan-only handoff does not satisfy it.

The partial record is retained under
`.cellar/cell-groups-evidence/CG-03-directory/checkpoint.json`. It includes source
and executable hashes, command output and the corrected initial fixture compile
failure. This checkpoint is separate from the still-incomplete CG-03 task gate.
The record binds 63 source inputs, nine successful command logs and two copied
native executables. All-target Clippy with warnings denied, formatting/layout,
native dependency isolation, docs, source preflight and all 37 plan-validator
tests pass at this checkpoint.

### CG-03 cell-independent birth checkpoint

Validation source: `40c3c0b143f72496f0345be890413ed855c241ae` (declaration-spacing successor).

Source `9db4ba8fc91e6d67de244c1a684001393bcc9e56` binds a group birth to physical
history, range/epoch, creation operation, namespace policy and canonical initial
members. It has no single-cell ownership field; range boundaries still carry full
cell routing keys. The bounded catalog encoding uses
the distinct kind 186; the provisional overlapping tag was corrected before this
checkpoint. No older birth layout is decoded or migrated by the new API.

Global group-ID reservation uses the existing permanent key and the entire
canonical birth. Different namespaces, incarnations, creation IDs or placements
cannot reuse that key. The range directory now verifies every new route against
its exact reserved birth before publication. Losing a topology CAS retains the
reservation. Selected ranges can remain unmaterialized until their first use.

Lazy materialization checks current range selection and the reservation before
conditional creation, then checks selection again. Unknown outcomes reconcile
with the original encoded request. A verified birth observation remains metadata:
it refuses wrong catalog owners, missing or changed immutable data and retired
routes, and grants no permission to initialize a voter. Managed namespaces refuse
the unmanaged APIs before I/O; their rooted fleet authorization remains to be
integrated.

All ten native catalog tests and all 16 existing node-directory regressions pass,
including the distinct wire-tag assertion. The new cases cover canonical placement,
lazy creation, missing confirmations after actual writes, exact retry after reopen,
global cross-namespace conflicts, retired-range refusal and managed-policy refusal.
Existing bounded routing, races and the 80-transition churn oracle still pass.

Physical `GroupBootstrap`, engine mappings and replica identities still carry the
old cell-scoped identity. The next step is to make that durable log path explicitly
group-scoped, then connect the verified birth to real synced genesis, node storage
pins and permanent member claims. A synthetic cell name is not an acceptable
adapter. The required `birth_unknown_and_claimed_blank_refuse` integration case
remains absent and CG-03 remains IN_PROGRESS. Redirect budgets, the shared host and
all later task/release gates remain outstanding.

The partial evidence record at
`.cellar/cell-groups-evidence/CG-03-birth/checkpoint.json` binds 67 source inputs,
nine successful command logs and two copied native executables. All-target Clippy
with warnings denied, formatting/layout, native dependency isolation, docs, source
preflight and all 37 plan-validator tests pass. The failed declaration-spacing
check is retained with its source revision and corrected result. This is not a
CG-03 DONE record or proof of shared-group disk-loss recovery.


### CG-03 physical engine registry checkpoint

Source: `d72ea068a527d2027e806ca78ab390256e8634fa`.

The engine catalog now keys histories by `GroupHistory`, excluding the application
cell name. Synced mapping records use the explicit `cellar-group-mapping-1` domain
and four physical identity fields. The old bare `scope` mapping shape refuses on
recovery; no compatibility decoder or disk migration is introduced.

The staged single-cell host retains its real cell binding separately as application
metadata. Lookup, create retries, seeded attachment, pending-slot accounting and
physical inventory reject a different cell binding for an existing physical
history. These checks prevent an adapter caller from turning another cell name
into a second handle or phantom slot. The inventory wire still uses its existing
cell-scoped digest until its consumers undergo the later cutover.

This is a storage prerequisite, not the full physical bootstrap conversion.
`GroupBootstrap`, replica handles, node pins and permanent member claims still
need the cell-independent path before the new birth API may start a voter.
The required claimed-blank-disk integration case remains absent. CG-03 stays
IN_PROGRESS; CG-04 through CG-16 and the final release gate remain pending.


Validation at this source passes 19 pure-core cases, 27 selected library cases,
all four genesis cases, 14 native manifest cases, 19 native storage cases,
16 node-directory cases, 10 cell-group catalog cases, 33 host cases and
54 snapshot cases. The new engine tests inspect the stored physical mapping,
restart the actual engine, reject cell aliases at lookup, creation, pending-slot
and inventory boundaries, and rewrite both catalog and group records to prove old-domain, unknown-domain,
zero-history and invalid-cell refusal. Existing managed genesis sync-barrier and
bootstrap claim tests still pass. The snapshot suite includes seeded restart,
lost-claim confirmation and selected-payload refusal; its explicitly ignored
RustFS case was not run. Parent-launched process fixtures remain intentionally
ignored by direct suite execution.

All-target Clippy with warnings denied, formatting/layout, native dependency
isolation, docs, source preflight and all 37 plan-validator tests pass. The partial
record is retained under
`.cellar/cell-groups-evidence/CG-03-physical-identity/checkpoint.json` with source,
executable and command-output hashes: 72 source inputs, 13 successful command logs,
eight executables and 194 distinct Rust cases. An interrupted build using the machine's
older default compiler and a command naming a nonexistent test target are retained
as non-qualifying attempts. Final checks use Rust 1.98.1 and actual test targets.
These results do not qualify the shared host, new group member claims, RustFS
recovery, hibernation, performance or the final hard cutover.
