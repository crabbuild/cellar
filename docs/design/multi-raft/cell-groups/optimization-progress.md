<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# TiKV-informed optimization implementation

Purpose: Track implementation and validation against the
[optimization design](tikv-informed-optimization.md), independently of historical
CG acceptance and release qualification.

## Recovery PR integration

PR [#166](https://github.com/crabbuild/cellar/pull/166) includes this implementation
through committed source `19982a1b`, merged with managed S3 recovery at
`79f2ccc5`. The integration retains the source-seal routing fence and the narrow
published-child discovery authorization. It passes 63 selected Rust cases and
focused strict Clippy with the CI test profile, including all six public routing
cases and cached-source SQL after child publication in both image formats.
See [combined-source progress](progress.md#combined-recovery-and-tikv-optimization-source)
for exact scope and retained records. At `b85c609c`, three combined-source recovery
suites pass real RustFS in both image formats with host-native RF3 voters, including
closure selection, seeded SDK retries and uncertain outbox recovery. Seven archive
safety cases and full strict all-target Clippy pass. See the
[current-source checkpoint](progress.md#cg-13-checkpoint-held-receipt-replays-and-current-source-rustfs)
for held-proof reuse, failed controls and fencing limits. Full CG-13 and release
profiles remain open. This checkpoint does not accept a complete OPT package,
qualify any CG-17 primitive runtime case or establish a measured speedup.

## Current boundary

Implementation starts from `d0c2873f` (the shared cell-group runtime merged into
`origin/main`). OPT-01 and OPT-02 are in progress. None of OPT-01 through OPT-08 is accepted
as complete by this record.

The first change implements the command outcome and authenticated advice
foundation:

- The Raft owner samples history, applied membership revision/configuration
  digest, term and a legal incoming-voter leader in one owner turn. Advice is
  absent during park/wake fencing; command discovery first completes the existing
  fresh-quorum wake. Explicit stale queries do not wake a group for advice.
  Digest scratch consumes the existing 64-KiB membership observation allowance;
  local status-only selection retains its smaller reservation. Advice is withheld
  when the configuration revision differs from the applied manifest.
- Command replies distinguish `Applied`, `RejectedBeforeAdmission` and
  `AdmittedOrUnknown`. Only construction/admission failures before controller
  ownership can report rejection. Once the controller owns the invocation,
  errors retain uncertainty and physical ownership. Exact receipts and the
  requested Quorum/S3 proof still determine success.
- A fresh command sent to a follower is rejected before callback entry. A
  rejection after an earlier uncertain transmission cannot clear that command's
  ambiguity. Command IDs, captured inputs and fingerprints are unchanged.
- Command, probe and query replies carry optional fixed-width advice under the
  existing authenticated request/response binding. Senders reject foreign
  history; probes claiming local leadership cannot advise another member.
  Decoding remains bounded by the existing reply reservation and frame limit.
- Aggregate performance counters expose command RPC attempts, leader probes,
  proven pre-admission refusals and unconfirmed application replies. Attempts
  count entry into the transport exchange, including exchanges that later fail;
  they do not count committed commands or prove physical transmission.

This changes marker-1 response layouts and the query TLS-exporter/MAC domains
(`query-advice-1`) with a hard internal cutover. Rebuild
matching peers and clients. There is no predecessor response decoder. The
test-only value helpers use the same mandatory envelope decoder.

## Remaining optimization work

The [implementation sequence](tikv-informed-optimization.md#13-implementation-sequence-and-owning-boundaries)
and its named RO/RL cases remain the acceptance contract:

| Package | Outstanding delivery |
| --- | --- |
| OPT-01 | Finish smallest-scope negative-path repair and validate membership, public authority isolation and ambiguous outcomes; RO-01/02 runtime cases pass below, while broader RO-03/04/07/08 gates remain open |
| OPT-02 | Selected public RO-05 interval-repair and RO-06 SDK/native HTTP/Axum split oracles pass below; recovery lookup cuts and remaining demand-refresh qualification stay open |
| OPT-03 | Automatic preferences, bounded fallback and public entered-source expiry/pin checks pass below; public partition coverage and full RO-10 acceptance remain open; the public RO-11 cohort oracle passes at its source checkpoints below |
| OPT-04 | Physical shared-file/segment release proof and once-only accounting qualification; RL-01 through RL-05 |
| OPT-05 | Load telemetry, reserve-aware placement and repeated release-scale movement; RL-06 |
| OPT-06 | Measured scheduling fairness, batching and queue isolation; RL-07 |
| OPT-07 | Optimized wake/transition/recovery routing; RL-08 and retained CG-13 gates |
| OPT-08 | Combined-source full primitive, backend-fault, crash and performance qualification; selected local RustFS/daemon checks pass below |

## Direct-routing candidate

The current candidate adds a leader index beside the transport's existing route
registry. Each entry charges 4 KiB and one item to the outgoing control bank.
Its capacity is the minimum of the route limit and one quarter of the parent's
byte/item capacities. Saturation evicts advisory metadata or falls back to
discovery; no parent limit is enlarged. Entries retain weak route references,
and shutdown clears them independently of physical query/SQL ownership.

Managed entries bind a verified quorum configuration digest/revision and legal
incoming voters. Unmanaged startup can bind the immutable initial configuration
as historical metadata; a response must match it exactly, and it grants no
current membership or startup permission. Metadata changes require new verified
configuration rather than learning a peer/address from advice.

Advice has a configuration-specific term floor, sticky same-term conflicts and
monotonic age measured from the physical attempt's admission. Its lifetime is
bounded by the existing transport I/O timeout. Re-observing unchanged membership
does not renew that age. Every request captures a cache generation; eviction,
replacement, endpoint change and trust fencing prevent late replies from updating
the replacement entry. Enrolled endpoint changes preserve verified membership
while discarding advice. Control-bank snapshots include these charged bytes/items.

Warm ingress-only commands use the cached leader directly. A proven pre-admission
NotLeader response may repair advice and retry the same invocation, original
inputs, command ID, deadline and finite bank. Unknown outcomes remain ambiguous.
Incoming commands use the actual applied local manifest instead of another range
lookup; the existing Raft execution authority still checks readiness, cell/history,
route, seal/activation/recovery state and ownership before SQL and proposal.

The new `native_cell_groups_routing_optimization` target's
`cached_leader_direct_write` passed: ordinary SDK plus native/Axum ingress, both
image modes, exact Quorum/S3 writes and replays, callback counts and all-replica
values. Each measured warm invocation had one command RPC, zero probes, zero
command-directory lookups, zero demand membership refreshes and one advice hit.
Background membership observations have a separate counter and were observed
while the warm path remained direct. This is a routing-work oracle, not a
throughput or latency comparison.

`election_repairs_leader_only` and `metadata_outage_on_cached_path` passed in
the final combined public suite. The former checks authenticated
pre-admission handoff, stopped-leader uncertainty, explicit same-ID reconciliation
and exact original result with the current covering cell version/witness. A prior
receipt is not required to carry its old covering version after later mutations.

The outage case denies real range, Fleet and node-directory reads after warmup.
Quorum requests exercise the direct path. The archive publisher independently
requires a current catalog birth check, so this implementation cannot promise S3
progress during that cut: the case requires no S3 acknowledgement, then restores
metadata and reconciles the same ID with S3 proof and no duplicate callback.
Removing the publisher's revalidation would need its own authority proof.
The outage case does not close RO-07's broader expiry/revocation and unknown-route
qualification. Cache race/quota tests and strict focused Clippy also passed.
OPT-01 and combined release qualification remain open.

## Candidate checks

The table below qualifies the preceding direct-routing checkpoint. The newer
interval-repair candidate described next has separate checks pending.

| Check | Current result and scope |
| --- | --- |
| Cache unit cases | 8 passed, including quota plateau, eviction/endpoint/trust generation races, term/configuration/role checks, original-attempt expiry and unchanged-configuration route reinstallation preserving the term floor/conflict |
| Authenticated query/command wire cases | 24 passed on the cache build; malformed shapes, outcome semantics and Quorum/S3 downgrade refusal |
| Performance-registry unit cases | 5 passed |
| `native_cell_groups_execution`, `shared_remote_` and `shared_nonmember_` | 14 passed on the new cache/direct-destination implementation; cancelled/expired work, revocation, replaced-route replies, exact receipts and previously uncertain rejection remain covered |
| Public routing suite | Final combined run: 3 passed in 80.96 seconds, each covering both image modes. Direct SDK/native/Axum writes and replays; live leader handoff with one pre-admission rejection, two RPCs, zero probes/directory lookups/demand member refreshes; stopped-leader uncertainty and explicit same-ID reconciliation; real metadata outage with direct Quorum progress and correct S3 refusal/reconciliation. No deadline/capacity increase or warning suppression. |
| Source-seal and child-publication boundaries | 3 passed: `seal_authorization_cancel_race`, `source_seal_blocks_delayed_work`, `child_requires_install_and_publication` on the new implementation |
| S3 physical delivery ownership | `s3_remote_slow_delivery_retains_proofs_and_bounds` passed in 44.29 seconds: both image modes, command/receipt replies and drain/disconnect/revocation/timeout cuts, original quotas and actual TLS/filesystem ownership. In-memory conditional object store; no RustFS qualification. |
| Strict focused Clippy | `cargo +1.98.1 clippy --locked -p cellar -p cellar-logic --lib --test native_cell_groups_routing_optimization --test native_cell_groups_execution -- -D warnings` passed. The first candidate run's `field_reassign_with_default` was fixed with a complete initializer. The separate all-target failure recorded below remains an open gate. |
| Module layout and format check | Final checks passed for 2,138 Rust files using the built `cellar-xtask` (`layout` and Rust 1.98.1 `fmt --check`, the Cargo aliases' implementation) |
| Native dependencies, docs and plan checkers | Passed: 496 reachable native-only packages; 117 documents / 2,451 references; 42 Python checker tests. These do not establish runtime or backend qualification. |

## Interval-repair candidate

Ordinary directory publication now retains its exact canonical mutation bytes at
`changes/<request-digest>` before the selecting journal/root CAS. The existing
journal already binds that digest. This adds proof storage without changing a
marker or retaining a predecessor wire decoder. Unavailable proof selects full
cache invalidation rather than assuming an unchanged range.

Repair verifies at most four intervening publications, sixteen parent/child
routes per publication and 128 immutable pages. Namespace/schema, identities,
epochs, complete interval coverage, exact selected parents/children and the
predecessor chain must check. A bounded ordered Merkle comparison also proves
that nothing outside each declared interval changed; claimed parents/children
alone are insufficient. Verified overlaps are removed atomically. Unaffected
entries retain the root and revision where they were actually checked.

Each asynchronous lookup captures a weak cache generation before metadata I/O.
Insertion and proof completion recheck it under the cache lock. Changed recovery
or import fences participate independently of routing revision; the fence digest
is streamed without an extra serialized buffer. A changed fence or unverifiable
chain clears all entries. Stable fence observations can retain newly checked
unblocked routes. Session revision floors survive invalidation and cannot be
satisfied by relabeling an older unaffected observation.

Cancellation review found that the first candidate advanced its generation only
after proof completion. The updated candidate records the verified head and
retires the old generation before target lookup or proof I/O. Earlier rows remain
ineligible while proof is pending. A drop guard preserves that negative boundary
and removes unverified rows on timeout/cancellation; newly verified current rows
survive. A second publication during pending repair selects full invalidation.
The lookup receives the exact generation from the observation's locked commit,
rather than sampling a possibly newer generation after I/O. A new deadline case
holds an old lookup, expires a newer publication's proof read, then requires the
old lookup to refuse and a fresh lookup to select the child. All 30 catalog cases,
including that deadline case, passed on the interval-observation build.

One active repair borrows 512 KiB and an item from the original invocation bank.
Admission temporarily retains one maximum normal invocation reservation before
admitting the proof, then returns that headroom for business progress. A one-slot
or byte-tight parent falls back; it cannot be monopolized by repair. The cache
envelope and parent capacities are unchanged. Busy proof ownership,
bank exhaustion, missing/corrupt records, excessive chain distance or page work
select full invalidation. The original invocation deadline also bounds proof I/O.
No new per-group worker or unbounded invalidation history is introduced.

The interval-observation build passed all 30 catalog cases. The 17 selected
execution regressions passed (`shared_remote_`, `shared_nonmember_`, cached
catalog-outage authority and both controller replica-routing variants), as did
three source-seal/child-publication cases and the S3 slow-delivery ownership case
(49.10 seconds, both image modes; in-memory conditional store). Public election
and outage cases passed together. Direct-write startup timed out in that combined
run before the test exercised its routing oracle; its unchanged isolated rerun
passed in 33.74 seconds. No timeout or capacity was increased. The emitted interval library test executable
also passed all 40 selected private cases: three interval proof/fence/headroom,
eight leader-cache, 24 authenticated wire and five performance-registry cases.
That makes 94 selected interval-build cases, with the startup failure and isolated
rerun recorded separately. Logs are under `interval-observation-*`. Final-source
Clippy remains pending; these runtime results precede the coalesced-discovery
changes described below.

These metadata-boundary cases do not replace RO-05/06's public live split/recovery
and original cell-version checks or the remaining OPT-01 negative-path work.
Recovery qualification must additionally verify propagation of namespace fences
to dependent transport membership/advice owners; the cache proof alone does not
establish that integration.

## Coalesced-discovery candidate

Demand membership refresh now selects an exact history plus weak transport-route
and verified-configuration generation. Concurrent requests for that key share one
operation in the existing joined directory worker. Route/configuration replacement
selects a different key; no allocation identity is reused while a key retains its
weak references. Completed work is removed from the index instead of becoming a
reusable authority or freshness result.

The first request's existing 32-KiB/item reservation covers the physical owner,
first waiter, index slot and result. Every additional waiter charges 4 KiB and an
item to the same original control bank. The shared reservation survives all caller
cancellations and remains charged through undelivered results. Each waiter keeps
its original absolute deadline; joining cannot extend the first physical owner's
deadline. Queued close/drop publishes a typed Closed result. The directory drains
queued reservations before releasing its registered service lease; entered work
still finishes before that lease is released. There is no new task per group or
increase to a parent bank.

Two counters distinguish `cell_membership_discovery_owners` and
`cell_membership_refresh_joins` from the existing demand counter. Six private cases
cover sharing/cancellation, deadlines, actual byte/item saturation, the minimum
managed control envelope, generation/history isolation, completion with delayed
delivery and queue closure. The new public `refresh_storm_is_bounded` candidate
uses a cold ingress and a held real birth-record read, SDK/native and SDK/Axum
pairs, original controller-bank saturation, cancelled SDK waiters, exact same-ID
reconciliation and separate held-discovery close cuts in both image modes.
Source review found that the original controller limit is two commands, tighter
than the dispatcher's eight invocations. The first eight-waiter test candidate
could not reach discovery in that envelope; it was corrected before runtime
qualification without increasing either limit. The case checks both HTTP adapters'
typed overload refusal while the two admitted requests share one discovery owner.

The full discovery build and focused Clippy passed before the public pair
correction; the corrected public target also built successfully. Six
coalesced-owner cases and the existing 40 private proof/cache/wire/performance
cases then passed in the serial driver.

The paired public run proved two demands, one owner and one join, typed SDK and
both HTTP overload refusals, exact reconciliation and cancelled first-caller
retention for both image modes. The close scenario incorrectly requested the
Raft service close while a full HTTP application request was still active, then
expected a complete node-drain proof. Both the original and a narrowed repeated
reproduction correctly returned conservative `Unobserved`. Moving the observation
after the controller join did not change that result; that production change was
reverted. Held application work still makes the proof unavailable.

The candidate now drives public `Node.shutdown(Preserve)` under an independently
owned test task, cancels only an observing waiter, and keeps the actual node drain
alive. It requires admitted work to finish, exact same-ID reconciliation at a live
voter, complete physical control release, and matching first/repeated close reports
with a drained local-producer proof. The narrowed corrected case passed both
image modes in 27.36 seconds. Temporary diagnostic selection and tagged logging
were removed, and the complete SDK/native/Axum matrix was restored.

The final build, focused Clippy, format/layout (2,143 Rust files), native
dependency check (496 packages) and docs check passed. All 46 selected private
cases passed. The first full public attempt completed six refresh scenarios,
then failed enrollment in the next fixture while Clippy was still active.
An unchanged serial rerun after build and Clippy completion failed in the second
fixture's group startup. Original five-second request/fixture bounds were kept;
failed logs and disks remain under `node-drain-final-*` and
`node-drain-static-ready-*`. RO-09 and the combined 101-case run remain failed,
with native startup stability and broader busy-peer/fault/load cuts still open.
This candidate is being checkpointed for the requested constrained Linux tests,
not accepted as a complete optimization package.

The final runtime driver waits for its complete successful build, flushes build
writes, requires exact test selections and executes all selected cases serially.
The earlier interval catalog/public run overlapped independent fixtures,
contrary to the qualification guide; its startup failure and isolated rerun
remain recorded above.

## Transient-refusal candidate

Source `0cef5350` separates a probe set containing only overload or valid
no-leader observations from the existing membership-discovery fallback. Such
observations do not establish a changed placement. Commands, pre-submission
enrollment and receipt reads back off using the controller's configured cadence
and retry the cached route within the original session. The shared destination
selector keeps its existing return contract for all three operations.

An authenticated command rejection for overload may retry only while the
invocation is unambiguous. A previously unknown outcome remains sticky. The
retry retains the command ID, captured inputs, fingerprint, original absolute
deadline, attempt bank and physical owner. Backoff uses the owning execution
domain, clamps to that deadline and refuses after owner closure. No parent bank,
worker or request budget is added. IO/stale-member fallback still needs the
remaining membership/endpoint/range scope separation; this change does not close
OPT-01.

The new public `busy_leader_keeps_placement` candidate retains two real destination
commands waiting for S3 proof, observes an authenticated rejection at an
ingress-only origin, releases archive work, and requires the same invocation and
exact replay with once-only callback entry. It requires zero probes, command
directory lookups and demand membership refreshes for that retry. Three private
cases cover probe-failure scope, the original deadline and closed-owner refusal.
Focused Clippy passed in the isolated source checkout. The source-pinned CI-profile
run at `729b006f` subsequently passed focused Clippy, format/layout, native
dependencies, docs, all 42 plan tests and 49 selected private Rust cases. Its new
public overload case failed and stopped the remaining serial campaign.

Review found that this new fixture submitted its two concurrent destination
commands with one client identity, violating the documented per-client sequence
contract. Setup correction `729b006f` gives each concurrent command its own client
and preserves exact replay of the routed command. All overload, routing-work,
durability and callback-count assertions are unchanged. The first CI-profile
build at `0cef5350` completed, but its validation was invalidated before any
runtime test; the in-flight Clippy process was cancelled. The corrected source was
used for the `729b006f` campaign above. No invalidated run is counted as a pass.

The next diagnostic showed that one of the two held S3 commands completed early
with SQL `Busy`: both targeted the same SQLite cell. The intended node-level
capacity hold therefore did not survive until the incoming request. That request
was admitted and returned an ambiguous `Unavailable`, correctly preserving its
identity and refusing automatic replay. The corrected fixture uses two independent
cells, establishes initial S3 coverage, and explicitly refuses any held call that
completes before archive release. It keeps the same two-slot node bank and five-second
request deadline; callbacks and exact per-cell results remain checked.

With a valid capacity hold, all four original request tickets were consumed by
authenticated overload responses in approximately fifteen milliseconds. S3 work
completed about 890 milliseconds after release. The controller's five-millisecond
poll cadence alone did not give that work time to free capacity. Retry spacing
now divides the original remaining window geometrically across the remaining
tickets and final execution, with the configured cadence as its minimum. Each
retry still consumes an original ticket and every sleep clamps to the original
absolute deadline. No retry count, timeout or admission limit is increased.
This follows the bounded exponential pacing principle in the
[TiKV Rust client](https://github.com/tikv/client-rust/blob/master/src/backoff.rs),
while deriving Cellar's spacing from its own existing invocation bounds.

The paced public case passed both modes in 29.79 seconds before diagnostic cleanup.
Materialized images observed three proven refusals before success; extents observed
two. Both preserved the command identity, one callback, exact replay and zero
probes, directory lookups and membership refreshes. Temporary instrumentation was
removed. A fourth private case checks time reserved for progress and final
execution without deadline renewal.

Final source `0bf9571506fe5207215cb8055cea739c3eeacd06` passed 83 selected
regressions under the SDK CI test profile after all builds and static checks
completed. This includes the complete SDK/native and SDK/Axum refresh-and-close
matrix in both storage modes, the public overload/direct/election/catalog-outage
cases, and selected routing, enrollment, receipt and wire regressions. Focused
Clippy, formatting, layout, dependency isolation, documentation and all 42 plan
tests passed. The source manifest digest is
`10160068de81411681829c06df350994bb023acaaf6768e4513cd6709ea18644`.
Source, test binaries, operator CLI and logs are retained outside the repository
at `qualification/native-paced-routing-0bf95715` under this checkout's workspace
target. Its `record.json` SHA-256 is
`6a912e7262f6aad8cd3b170ec71fbb2d254ed7424726b80ee3d81d1e9a3f0478`;
all 15 recorded artifact hashes were rechecked. These selected regressions do
not close every RO-09 fault/load cut, any complete OPT package, or performance
and release qualification.

Artifact inspection found the preceding failing native startup runs used
optimization level 0 and debug information level 2. The qualification guide and
SDK CI require test optimization level 1, no debug information, and enabled debug
assertions and overflow checks. The new build and serial driver use those exact
settings, verify the emitted Cargo profiles, wait for all static checks and flush
build writes before fixtures. Original timeouts, assertions and resource limits
remain unchanged. This corrects the reproduction setup; it does not yet establish
the cause of the earlier failures or supersede their failed results.

## Fresh ReadIndex cohort candidate

Already-queued linearizable cell reads may share one newly initiated barrier
for the exact group history, term and applied configuration revision.
Initiation seals the cohort; later arrivals need another barrier. Each logical
read retains its original node/group reservation and absolute deadline, selects
its own coherent target-cell image, and receives its own consumed authority and
pin. Cancelling the first waiter leaves the shared context available to remaining
members. Last-member removal discards correlation state. Term or configuration
changes refuse pending selection. No lease or completed-barrier cache is added.

The first candidate gathered up to 32 adjacent reads per group turn. The
current worker takes the existing queue prefix through its last linearizable
cell read and processes intervening work in FIFO order. Later arrivals cannot
extend that prefix. The cohort stays open while the owner waits for readiness
and seals at the first ReadIndex submission. The existing group and node
admission limits bound the queue, and each read keeps its original deadline.
Separate counters report first submissions, all submissions and joins.

Source manifest
`986cbfd6d2318897b09730fae39e8368819480ab7e8f047ed76dfd56bfe91f43`
passed 33 selected regressions under the SDK CI test profile. These include three
private cohort tests, actual consensus tests in both storage modes for fresh
arrival boundaries, cancellation and independent image pins, selected strong,
follower, source-age, membership and query-lifecycle regressions, and all five
public routing optimization cases. Focused Clippy, format/layout, dependency
isolation, documentation and all 42 plan tests passed. Source patches, binaries
and logs are retained at `qualification/read-cohort-candidate-986cbfd6` under
this checkout's workspace target; its `record.json` SHA-256 is
`afe4f8e1aa01585e4c9a5550fb1c1cba0935fc02ce269dadd040e59ac93b5d6d`.
Four additional actual-consensus term/configuration invalidation cases passed
in both storage modes, alongside eight companion barrier, pin-cleanup and
follower-read cases. The corrected fixture uses the stored manifest's public
membership-revision accessor; the preceding compile-only failure ran no tests
and is retained separately. This 12-case run passed all six static checks at
source manifest
`48c54ec3943737d9a9663f466b3f846684424bda3a11e82225229949642ecaee`.
Its source, executable and logs are retained at
`qualification/read-cohort-invalidation-48c54ec3` under this checkout's workspace
target. Comparing all source inputs with the 33-case campaign confirmed changes
only in five test files; production inputs are identical.
The retained 12-case `record.json` SHA-256 is
`8c4d5e42d2b0bcae0e4345f0e18decdeee17ba8411f61c1104c6cee00ec4eeb2`.
At that checkpoint, replica preference ranking and the public SDK/native/Axum
RO-11 oracle remained open; those component checks did not close OPT-03 or
release qualification.

A focused test reproduced premature sealing when readiness prevented ReadIndex
submission. The same test and the three existing cohort unit cases pass after
the fix. The native fixture now inserts a peer heartbeat response between
queued reads, and the public fixture waits for the actor's queue observation
before releasing its storage sync gate. At source `fa64fda8`, four actual crate
unit cases, all six consensus cohort cases and the public read-preference case
passed. The public cohort case failed during its late Axum arrival.

The late-arrival fixture could hold an engine metadata sync while the selected
leader remained ready. An assertion at source `498e582b` reproduced that boundary.
The filesystem gate now selects a sync while the owner reports a pending Raft
phase. At source manifest
`ffc61abd512b2c0e7d1a96e6b85f7eb07362649de29aa94b56ae64c8391687c5`,
the complete public cohort case passed both image modes across SDK, native HTTP
and Axum. Its independent request ledger accepted 406 events. The runtime log
and ledger are retained at `qualification/pending-late-ffc61abd` under this
checkout's workspace target. Formatting and combined qualification follow this
checkpoint. Original deadlines, read quotas and barrier assertions still apply.

The final focused read checkpoint at source manifest
`f72af1e57d4409d19fe69142fd9c0c3cea256194a9412515a125fd90baffe2fc`
uses Cellar's selector in the sync fixture and passed all twelve selected Rust
cases. These comprise four cohort unit cases, six actual consensus cases and
the public cohort and replica-preference cases. Both public ledgers passed
with 406 and 204 events. Focused Clippy, formatting, module layout, native
dependency isolation and documentation validation passed. Records are retained
at `qualification/cohort-queue-f72af1e5` and
`qualification/cohort-static-f72af1e5` under this checkout's workspace target.
Combined-source and broader RO-10 qualification remain open.

## Public read cohort verification candidate

The ordinary SDK, native HTTP and Axum candidate holds actual Raft filesystem
sync and actual application query callbacks in each storage mode. It requires
two pre-initiation reads to share one barrier, later arrivals to initiate another,
and independently retained image pins and original two-slot controller admission
after an entered caller is cancelled. Signed observations must match each cell's
own version under the registered namespace schema. Higher-term handoff cuts
require refusal before query callback entry and exact S3 command replay.

This public drill exposed a production handoff failure: the former leader relayed
another member's ReadIndex request through raft-rs with the original sender.
Cellar's authenticated transport correctly refused that nonlocal identity, and
the controller closed the node. The incoming owner now validates the message,
then refuses peer ReadIndex at a non-leader before RawNode can relay it. Local
follower reads still create and forward their own authenticated context. The
transport identity check is unchanged. The new owner-boundary regression failed
on the preceding source and passed in both storage modes with the guard.

The public sharing/later-arrival/cancellation/term matrix passed its development
smoke in 28.46 seconds with all three ingress paths and both storage modes. The
schema-token check initially used the raw example seed and was corrected to the
actual registered namespace digest. Temporary shutdown diagnostics were removed.
The cleaned source passed 40 selected native cases: read/cohort, membership,
seed, all six public routing cases, and all five park/wake cases. Focused Clippy,
format/layout, native dependency isolation, documentation and all 42 plan tests
passed under the SDK CI test profile. A callback type alias resolved the preceding
Clippy-only failure; no runtime cases ran in that failed campaign and no lint was
suppressed. Source inputs are pinned by
`c2bbcdd1c342d60808910f76fd188f59f1b7ce93550a95bed877c126739406f9`.
Tested executables, operator CLI, source patches and logs are retained at
`qualification/read-relay-c2bbcdd1` under this checkout's workspace target;
its `record.json` SHA-256 is
`f5fa796524142d63894f8e70f77ef26b251c3bc2e28a0a42e477fa169c79e9e6`.
The public configuration change cut and replica preference ranking remain open;
Those partial or component results did not close RO-11 or OPT-03. The first
public configuration candidate held the leader's configuration fsync, but that
did not force the new configuration to overtake follower read selection. Both
reads could validly complete first. The failed development runs are retained at
`qualification/public-config-red-0f7dab94` and
`qualification/public-config-diagnosis-55096f6c`; they qualify no configuration
invalidation behavior. Its proposed production counter and second filesystem
provider were removed.

The replacement candidate holds actual follower persistence/apply, queues the
original two public reads, commits a rooted learner admission through the other
voters, and admits a later S3 mutation's data before releasing apply. It requires
one fresh shared barrier, refusal before callback entry, a changed local
configuration with the same term, independently verified fresh cell versions,
unchanged original quotas, and the exact pending movement debit. Its optimized
Rust 1.98.1 development smoke passed in 46.044 seconds, with both storage modes
and all three public ingress paths. Source inputs are pinned by
`7c741c89be9724f8b95236c36e8f4f2223e865c0cd780c75bac5f9374af5fbae`;
`qualification/public-config-apply-smoke-7c741c89` retains the executable, sources
and logs. This is a development result, not the combined regression campaign.
An initial diagnostic rebuild under the shell's Rust 1.99.1 default was stopped
before runtime and supplies no qualification. The complete request/evidence
ledger and replica preference ranking remain open. The expanded candidate adds
a both-completed shared cohort, explicit HTTP query identities, a later arrival
while a follower barrier remains pending, and a structured
request/result/cancellation/receipt and physical-owner ledger. Malformed,
duplicate and command-side query identity headers refuse before ReadIndex or
application callback entry. Its development smoke passed in 68.791 seconds.
The independent ledger audit passed 394 events: 76 valid queries, 50 verified
results, 20 generation refusals, six joined cancellations, 34 S3 receipt
observations, and 24 rejected malformed or misplaced identity requests.
Source inputs are pinned by
`8c4fb2f62d433c4a4e3d8afc57831eb299ce0a4035928cd619bc35cf69f558f7`;
`qualification/public-read-ledger-smoke-8c4fb2f6` retains that smoke.

The combined campaign passed focused Clippy, format/layout, dependency isolation,
documentation and all 42 plan tests. It passed 31 native cases, then
`election_repairs_leader_only` failed during initial group readiness with
`Runtime(DeadlineExceeded)`, before reaching its election oracle. The remaining
eight cases did not run. Replaying the exact election executable under the same
source passed in 26.816 seconds; the intermittent startup cause remains
unresolved. The failed campaign and replay are retained at
`qualification/public-read-ledger-election-startup-failed-8c4fb2f6`.
This is not a passing 40-case campaign or release qualification. The next
candidate retains startup-failure disks and records voter/group/resource state
before joined shutdown; it preserves the original deadline and assertions.

The diagnostic candidate preserves the original eager startup admission. On
failure it captures every voter's resource and performance counters before any
awaited inspection, then records later group observations, joins shutdown and
retains the disks. Its combined run passed 32 cases, including the election
case, before `metadata_outage_on_cached_path` failed at the next extents-cluster
startup. The materialized outage scenario had completed. Passive snapshots had
zero read barriers and pending persistence; sequential group observations later
showed a ready leader. Another checkout's formatter was observed after the
failure, but that sample does not establish overlap or causality. The failure,
executables and disks remain retained at
`qualification/public-read-ledger-startup-state-failed-64b27b95`.

The unchanged candidate subsequently passed all 40 selected native cases with
Rust 1.98.1 and the CI test profile. Each case began after a five-second sampled
quiet window and `sync`; the runner sampled competing compiler, formatter and
native fixtures every 100 ms. Three passing attempts overlapped with observed
external work and remain explicitly unqualified. Their later attempts passed
without observed overlap. An uninterfered failure would stop the campaign;
request deadlines, admission caps and assertions were unchanged. Sampling cannot
exclude short unobserved activity, and the intermittent startup cause remains
unresolved; this pass does not erase either earlier failure.

Focused Clippy, formatting, layout, native dependency isolation, documentation
and all 42 plan tests passed at those exact source inputs. The independent
public-read audit also passed all 394 events. The SDK, native HTTP and Axum
RO-11 oracle now covers two completed prequeued queries, cancelled waiters,
later arrivals during pending and completed barriers, term changes and a real
learner configuration change in both image modes. It verifies independent
image pins, exact versions and S3 receipts, generation refusals before callback
entry, unchanged two-slot admission, and joined resource drainage. The learner
cut retains its exact pending movement debit; it does not qualify completed
repair or reclaimed capacity.

The source-input digest is
`64b27b9500ee75f666fcd762077cc0e0dc18a5018f9eb7d0ffa14806700452aa`.
The executables, source overlay, all 43 attempts, checker logs, evidence and
auditor are retained at `qualification/public-read-ledger-isolated-64b27b95`
under the checkout's workspace target. Its `record.json` SHA-256 is
`84af9fd5f9afd698a5f679520441c484a6df3058722dfa92d2125dde07cf329b`.
This closes the selected public RO-11 functional oracle. RustFS verification
was pending at that native checkpoint and subsequently passed as recorded below.
Replica preference ranking, RO-10, broader OPT-03 acceptance and release
qualification remain open.

## Consistency-aware read candidate

Automatic linearizable reads prefer the local owner's observed leader, or
authenticated, configuration/boot/age-bound leader advice on an ingress-only
node. Other modes prefer local execution. A bounded list of cached alternatives
handles read unavailability, pressure or a ticket timeout before requiring route
repair. Selection reads one immutable transport view; tracking tried member IDs
prevents changed advice order from skipping an eligible peer. Every destination
still consumes the requested read authority and verifies the original minimum.
Explicit local and replica plans retain their ordering and bounds. Automatic
attempts divide the remaining original timeout among remaining tickets; no
deadline, attempt bank, controller quota, cache or worker is added.

The initial public lag cut denied metadata to every voter. The first local
attempt expired behind actual follower persistence, both remote candidates
returned `Unavailable` at the required destination catalog check, and the origin
then tried range repair. The diagnostic is retained at
`qualification/read-preferences-lag-diagnosis-f7cc3cd5`. A manifest-only
destination experiment was rejected by the regression below. Incoming reads
still resolve their destination against the current catalog and the actual read
owner still consumes the requested proof. The incoming session is clamped to
the receiver's captured RPC deadline, including time spent queued. Temporary
diagnostic probes were removed.

The unchanged production baseline first failed the new public leader-preference
oracle with member 1 serving a strong read that should prefer observed leader 2;
`qualification/read-preferences-leader-red-17f49fcc` retains that red seam. An
initial candidate failed enrollment before the read oracle because its handoff
setup waited for announcement alone. The setup now requires the actual leader's
readiness and current-term apply within the original five-second wait. The
failure remains at `qualification/read-preferences-setup-notready-faad90c7`.
The first healthy preference matrix passed at
`qualification/read-preferences-healthy-b4eb33c2`. The later metadata failure
and an interrupted attempt remain at
`qualification/read-preferences-lag-metadata-failed-7a84c42e` and
`qualification/read-preferences-interfered-4883d1c1`.

The manifest-only experiment's development matrix passed in 39.792 seconds at source inputs
`eae6b9ee05b3f1e81e32631321809875669a9fb3d10b21171129cca501e1a34b`.
Both image modes exercise SDK, native HTTP and Axum from voter and ingress-only
origins, all four read policies, exact S3 receipt minima, real follower fsync lag
under verified metadata denial, foreign-cell token refusal, and expiry followed
by caller cancellation while leader and follower SQL callbacks retain separate
image pins. The 204 events contain 60 verified results, six joined cancellations
and six foreign-minimum refusals. Each mode checks 36 physical query callbacks
and five command callbacks, unchanged two-slot/64-KiB controller caps, and joined
drainage. The in-memory conditional store supplies the archive backend for this
native matrix. That development pass did not exercise the superseded catalog
route and is not accepted evidence for removing the receiver check.

`qualification/read-preferences-fault-smoke-eae6b9ee` retains that development
checkpoint. The formatted candidate adds explicit metadata-denial counts and an
applied-index observation after the lagged query returns. Strict focused Clippy,
format/layout, dependency isolation, documentation and all 42 plan tests passed.
The broader source-bound campaign at input digest
`b59b054bf26ddf13e409ec0c03adfb65fbe3241788b4829f1a8081a5b17522be`
accepted 37 individual cases before the added
`catalog_routed_queries_preserve_invocation_and_authority` case failed at its
five-second SQL-entry observer, without sampled competing native/compiler work.
`qualification/read-preferences-regression-failed-b59b054b` retains the complete
failed campaign, source overlay and exact binaries. An isolated backtrace replay
reproduced that wait on both this candidate and the retained pre-change
`64b27b95` executable; those differential records remain at
`qualification/read-preferences-catalog-differential-b59b-64b2`. A targeted
precondition assertion confirmed that the held query selected replacement
group 203 at epoch 2 instead of the installed group 3 at epoch 1. That result
remains at `qualification/read-preferences-catalog-precondition-red`.
The fixture now checks cancellation and joins the physical callback before
publishing the replacement. It separately verifies that a subsequent query
with the obsolete hint selects the replacement, with no extra SQL callback.
The original deadlines and ownership assertions are unchanged. Both corrected
image modes passed in the next campaign before the subsequent failure.

The campaign at input digest
`62b5ed57cb29be81ff0812d4e3ef2f36571753a99846c7b258a21b270a0e112a`
accepted 41 individual cases before `shared_remote_queries_bind_cell_versions_and_gateway_owners`
rejected the manifest-only destination experiment. The exact catalog publication
replaced the old route while its physical manifest was still active. The gateway
did not return the required typed refusal. The complete failed campaign is
retained at `qualification/read-preferences-regression-ci2-failed-62b5ed57`
(`record.json` SHA-256 `3aecc7595bf55e4de45ba019e4d7572778b35fbd32f2be2a1fb144db910cdb76`).
The receiver's original catalog check is restored without weakening this test.
The new lag cut scopes metadata denial to the origin's adapter over the shared
store, checks actual origin denial and receiver access, and observes a receiver
range head read on the successful fallback. Both the original superseded-route
refusal and the revised public matrix passed without sampled interference at
`f21fdc4930f832c29a5e1b8c42d13848f34927ec82da9d026ebbfedb26ff8c8e`.
The matrix completed in 40.355 seconds and the independent ledger reconciled
all 204 events. `qualification/read-preferences-origin-smoke-f21fdc49` retains
the pre-format source and both binaries. The broader regression of the formatted
candidate, network partition, RecentSource expiry, full RO-10 and release
qualification remain open. The RustFS/Colima result below remains pinned to its
own source.

## Local container verification

The user explicitly requested local RustFS storage and Colima containers for
nodes. The verified dedicated profile is `cellar-multi-raft-6aff` under the
existing workspace-volume home `/Volumes/Workspace/crabbuild-target/c6aff`
(8 CPUs, 18 GiB, 100-GiB backing ceiling). Actual VZ paths and the backing file
were checked on that volume. Two initial owned profiles used the default home
because the requested custom directory did not exist; they and their initial
backend were stopped and retained. No other profile was modified or stopped,
and the active Docker context remains unchanged.

The fresh digest-pinned RustFS container on the verified workspace profile passed
all seven signed S3 protocol operations, including conditional create and ETag
compare-and-swap. The local harness's one-CPU/2-GiB/no-additional-swap envelope is
unchanged. Preflight positively matched the Docker and Colima SSH Engine IDs,
cgroup v2, the eight-CPU VM and its workspace backing file.
Credentials and generated runtime state stay outside the repository in the
checkout-private qualification directory.

The managed daemon runner previously hardcoded the default profile/context. Its
reviewed change binds Docker calls to the owned backend's exact profile/context,
accepts an explicit existing Colima home, binds the corresponding Lima home for
guest calls and records the Engine identity. Three configuration/routing tests
passed; the local multi-Raft Python suite passed 31 tests with its one opt-in
Docker configuration case skipped. Review found no change to voter/backend
limits, workload assertions, source-image checks, cleanup inventory or request
budgets. The plan checker initially caught the changed recovery anchor (41
passes, one preflight error). Source checkpoint `4a00fd96` was reviewed, and the
baseline's source revision, recovery digest/excerpt and review note were advanced
together. All 42 checker tests and direct source-anchor preflight now pass; no
checks were weakened. The full `plan.py preflight` CLI remains unavailable in this
fresh checkout because prior accepted tasks reference private ignored evidence
files that are not present; no synthetic replacement evidence was created.

The managed Linux daemon campaign passed at source
`a2bbabae57f1386e25370999c11e9b050903b760`, built with Rust 1.98.1 and the locked
dependency graph. The primary checkout remained clean at that revision throughout
the build and campaign. The runner checked the immutable image, source manifest
and deployed daemon/operator binary digests. The image is
`sha256:65e6ca2d92aa57a42294654e076a27fbbab1fefac191ff5ee1ed406f2714980a`;
its source manifest is
`519fc5131bee8e1fea9fb3a83eea896fe07ebf18a05633290cd4001299b02e82`.

| Image mode | Result | Fixture duration |
| --- | --- | ---: |
| Materialized | Passed | 74.977 seconds |
| Extents | Passed | 71.547 seconds |

Both modes used three separate Linux voter containers, each with its own disk,
one CPU, 3 GiB RAM and zero cgroup swap. The record includes observed cgroup-v2
limits and Docker inventory. The actual daemons acquired RustFS-backed node leases
and authenticated peers. Native and Axum ingress exercised Quorum/S3 receipts,
park/fresh-quorum wake, archive-write outage and exact S3 reconciliation,
leader SIGKILL and successor selection before retained-disk restart, maintenance
stop/return/resume, and quorum-loss read/write refusals. The intentionally killed
container exited 137 in each mode; the four other original/replacement containers
exited zero. Both fixture controllers exited zero. These results retain the
original request, resource and workload limits.

The private artifact root is
`/Volumes/Workspace/crabbuild-target/cellar-ios-6afff9f4-tikv/qualification`.
`linux-daemon-build.json` records the successful build and verification;
`managed-daemon-a2bbabae57f1/` retains the summary, mode reports and event ledgers.
RustFS objects, logs and voter disks remain available. Credentials stay outside
source. This campaign used one VM and did not select standby or failed-voter
repair. It does not close the failed native RO-09 run, public transition/fencing
cases, performance gates, independent host/zone resilience or release acceptance.

The current runtime/example source inputs were subsequently verified at committed
checkpoint `1ef5385e97d6fccdc544fb067394b993a3b0dcb1`. Rust 1.98.1 for ARM64
Linux built the daemon and operator CLI from a clean source archive with the
locked dependency graph. The selected Colima profile, Engine identity and
container limits remained the same. The runner independently matched the image's
source manifest and both deployed binary digests before launching voters. The
source-input digest is
`64b27b9500ee75f666fcd762077cc0e0dc18a5018f9eb7d0ffa14806700452aa`,
matching the 40-case native checkpoint above. The immutable image is
`sha256:1b4334a7d084b754182188267ef6c940c29813cf6ada15f2303027bddf047eb3`.

| Image mode | Current-source result | Fixture duration |
| --- | --- | ---: |
| Materialized | Passed | 79.509 seconds |
| Extents | Passed | 93.455 seconds |

Each mode passed all seven signed RustFS protocol operations, three live
RustFS-backed node leases, native/Axum Quorum and S3 SQL, fresh-quorum wake,
archive-write refusal and exact reconciliation, leader SIGKILL, retained-disk
restart, maintenance stop/return/resume and quorum-loss refusal. Each mode
retains five live voter/replacement cgroup samples and a separate joined
SIGKILL observation. Every live sample confirms one CPU, 3 GiB memory and zero
additional swap. The killed container exited 137; four other original/replacement
containers exited zero. Both controllers exited zero and all voter containers
are stopped. RustFS remains healthy and running; voter disks and remote objects
remain available.

`qualification/linux-daemon-1ef5385e97d6/` under the workspace target retains
the source archive, Linux compiler identity, tested binaries, immutable image
identity, build logs, mode reports, event ledgers, resource observations and
artifact checksums. Its `managed-daemon/record.json` SHA-256 is
`f8b757ba1610281ff247086e87780b65024e101c1caeb4f40a08742cf6ff53ab`.
The source archive SHA-256 is
`a294f50e957e01ed6327a09e602f06c781b7b59cf74fac8ef0538daaa433ea9e`.
This closes the selected current-source constrained RustFS/daemon campaign.
It preserves the one-VM limitation and does not select standby, failed-voter
repair, independent failure domains, sustained performance or release gates.
The intermittent host-native startup cause above remains unresolved.

## Validation

The table below records the **foundation checkpoint before the cache candidate**;
it does not automatically qualify newer work. The candidate table above records
58 selected Rust cases, separately from the 42 Python plan-checker cases. Local
logs are under `/tmp/cellar-tikv-routing-*`, `/tmp/cellar-tikv-election-*` and
`/tmp/cellar-tikv-cache-*`; final public results are in
`/tmp/cellar-tikv-routing-suite-final.log`.

Failed initial candidate runs remain diagnostic history: the first combined
public run had two 5-second operator-CLI fixture-startup timeouts; an isolated
election run found the test's incorrect historical-covering-version assertion;
an isolated outage run confirmed the publisher's independent catalog gate for
S3. The final suite passed with the original timeouts and the documented
receipt/authority contracts. Those failed attempts are not additional passes.

The SDK CI toolchain is Rust 1.98.1. Builds use a target directory unique to this
checkout on the Workspace volume. Current local results:

| Check | Result and scope |
| --- | --- |
| `cargo +1.98.1 test --locked -p cellar --lib raft::query_wire:: --no-default-features` | 24 passed, including three new outcome/advice cases, malformed frames and Quorum/S3 downgrade refusal. This checks the wire boundary; subsequent runtime deadline/reservation adjustments still require their own checks. |
| `python3 -m unittest discover -s docs/design/multi-raft/cell-groups -p 'test_*.py'` | 42 passed; plan checker coverage, not runtime acceptance |
| `python3 tools/check_native_dependencies.py` | Passed; 496 reachable native-only packages |
| `python3 tools/docs/check.py` | Passed; 117 documents and 2,451 local references |
| `native_cell_groups_execution` executable, `shared_remote_` and `shared_nonmember_`, one test thread | 14 passed, covering TLS grants, exact receipts, fresh versus previously uncertain rejection, cancelled/expired producers, follower read constraints and stale route replies. The fixture runs both image modes; these are selected runtime checks, not the proposed public RO target. |
| `cargo +1.98.1 clippy --locked -p cellar -p cellar-logic --all-targets -- -D warnings` | Failed on five unused fixture items in unchanged `cell_group_disaster_effects.rs` and `cell_group_rustfs.rs`, compiled by the transitions/archive-controller targets. This gate remains failed; no warning suppression was added. |
| `cargo +1.98.1 clippy --locked -p cellar -p cellar-logic --lib -- -D warnings`; `cargo +1.98.1 clippy --locked -p cellar --test native_cell_groups_execution -- -D warnings` | Both passed; this narrower scope does not replace the failed all-target gate |
| `cargo +1.98.1 format --check`; `cargo +1.98.1 check-layout` | Both passed for 2,134 Rust files |
| `native_cell_groups_archive_controller` executable, `s3_remote_slow_delivery_retains_proofs_and_bounds --exact --test-threads=1` | Passed; both image modes, command/receipt replies and drain/disconnect/revocation/timeout cuts retain physical output ownership and original quotas. Actual TLS/filesystem I/O with an in-memory conditional object store; no RustFS qualification. This build includes the final absolute-deadline constraint and membership scratch reservation. |

The initial default-toolchain compilation failed on the new codec's `GroupId`
representation; the codec now uses its actual 16-byte identity, and the CI-toolchain
wire run above passes. Failed/cancelled initial commands do not count as passes.
Local command logs are under `/tmp/cellar-tikv-*`; they are temporary diagnostic
artifacts, not durable release evidence or a source-pinned qualification bundle.

## Leader-discovery owner and combined qualification

The next OPT-01 candidate shares unknown-leader discovery by captured physical
route, verified configuration generation, logical epoch and birth digest. A
transport task owns the first finite candidate plan and deadline. Every caller
retains its own deadline; cancelling callers does not cancel the physical owner.
Metadata borrows the existing control bank with a quarter-bank sublimit, and
owners use the unchanged transport task limit. Full metadata or task admission
falls back to the original bounded inline discovery. Results leave the index
before delivery, retain their charge until consumed, and revalidate the captured
route/configuration before use. Source manifest `19402216` passed ten focused
ownership cases, two mutation retry cases, the existing leader-cache and refresh
cases, command codecs and performance observations: 52 selected Rust cases in
all. Focused strict Clippy, formatting, layout, native dependency and documentation
checks passed. The qualifier resumed its codec checks against the retained binary
after correcting a nonexistent test selector; the earlier completed results and
original harness error remain recorded. Public probe-storm qualification remains
open.

The combined published revision `6521122c`, source manifest `cebed8f9`, passed its
execution, routing, primitives and storage runtime suites, both public read-ledger
verifiers, and the real RustFS physical segment-credit case. Its scheduler suite
reported ten passing cases and a failure in `control_plane_fanout_bounded`:
after 32 cells formed four groups, enrollment of cell 32 returned a nonambiguous
`Execution(Refused(Unavailable))`. An isolated rerun of the identical binary,
with the original quotas and deadlines, reproduced the same failure at cell 32.
Both runs retained their failed disks. Those failed runs do not establish the full scheduler or repeated-split gate;
the later repair and composed rerun are recorded below. Evidence is
retained in the checkout-specific `integrated-runtime-6521122c` and
`fanout-reproduce-6521122c` qualification directories. Combined strict Clippy also found a duplicate
storage-fault test module. The routing fixtures now reuse the test-root module in
the repair checkout; its focused and composed checks are recorded below.

The fresh Linux Colima/RustFS campaign for `6521122c` passed materialized images
and failed extent images at `resume-maintenance` with an unknown management
outcome. The retained voter logged a lease-maintenance timeout and fenced its
session. An isolated extent rerun of the identical image, using a fresh owned
cluster and the original limits and deadlines, failed at acknowledgement of the
original receipt after archive restoration. Both failures and their process
ledgers remain retained; neither extent run is counted as qualified.


## Retired-parent retry repair

The repeated-split failure was a stale cached parent whose retired peers closed
preflight connections before returning a typed group refusal. All four original
attempts retried that unreachable placement. A repeated wholly unreachable probe
plan now checks its exact history through the existing joined discovery owner.
Only the trusted directory's fenced result requests interval repair; readiness,
busy responses and the first unreachable plan retain their ordinary retry scope.
No application mutation is replayed from an unknown response, and the original
tickets, deadline, control banks and engine slots remain in force.

Repair source manifest `82a17791`, component commit `a7bd8b0c`, passed the original
64-cell repeated-split test in both image modes in 335 seconds. Each mode formed
seven groups while preserving all existing receipt, SQL and reserve assertions.
The trace records the formerly stuck parent becoming typed `StalePlacement` on
its second probe plan, followed by successful enrollment. Focused strict Clippy
for the library, scheduler and routing targets, formatting, layout, native
dependency and documentation checks passed. The shared routing fixtures now reuse
the test-root storage-fault module instead of loading that file twice.

The separate coalesced-leader source manifest `e88191c7` passed 52 selected Rust
unit cases, all seven public routing cases, both public read-ledger verifiers and
focused strict Clippy/static checks with the shared fixture module. These are
component results. Public probe-storm and broader OPT-01 through OPT-08 gates
remain open.


## Composed runtime verification and daemon diagnostics

Published revision `a69acc34`, source manifest `9dafe562`, passed 60 selected
library cases and the complete nonignored execution, scheduler, routing,
primitives and storage suites in the selected campaign. Both public read-ledger
verifiers and the physical segment-credit case against local RustFS passed.
Strict Clippy for the library and six native test targets, formatting, layout,
native dependency and documentation checks passed. The qualifier retained all
seven executables and verified unchanged source and HEAD throughout the run.
Evidence is in the checkout-specific `combined-runtime-a69acc34` directory.
This closes the composed scheduler regression above; it does not establish all
feature modes, the Linux process campaign or OPT-01 through OPT-08 acceptance.
The daemon target was compiled in that campaign, without running it on macOS.

Diagnostic source manifest `703438f6`, Linux component revision `bb1d1965`, adds
an authenticated fixture endpoint for group observations, archive publisher
status and existing resource/performance counters. Only after the original
archive or maintenance assertion fails, the process harness records bounded
snapshots from the voters, then propagates the original error. Snapshots do not
retry a mutation, extend its reconciliation window or establish archive proof.
Strict library/daemon Clippy and static checks passed before committing this
source. Its immutable Linux image passed both materialized and extent modes twice in
fresh three-voter Colima/RustFS clusters. The image is
`sha256:51b8a98add4ff1071f2f8ff198f194501644cb31edf24e3267d7c3afe5232901`.
Both runs retained the original one-CPU/3-GiB voter caps, zero swap and request
and maintenance deadlines. The independently checked ledgers cover original S3
receipt acknowledgement after archive restoration, leader loss/restart,
maintenance resume, park/wake, SQL/receipts and both native/Axum ingress paths.
Evidence is in the Linux checkout's `linux-daemon-bb1d196501ed-diagnostics` and
`linux-daemon-bb1d196501ed-repeat` directories. The earlier failed source and its
disks remain retained; these passes do not establish the cause of those failures.
The scope is three Linux voters with separate cgroups and disks in one VM,
without independent failure-domain, performance or complete OPT acceptance.


## RPC base integration and public probe candidate

The optimization branch includes the RPC checkpoint base through `f1ad8b90` in
merge `c51c8475`. Composed source manifest `cdfb3219` passed strict library/six-target
Clippy and all 60 selected library cases. Its new public probe fixture passed
the first cancellation case but failed the shutdown case because two concurrent
commands used sequences one and two from the same client. Correct `SequenceGap`
refusal exposed that invalid fixture ordering. The candidate now uses two
independent clients at sequence one and preserves each exact ID for replay.
The public oracle and complete composed runtime campaign remain under validation.
No RO or OPT acceptance follows from the failed fixture run.


The corrected-ID candidate `e0e57e6e` again passed strict Clippy and the 60
selected library cases. Its first two public probe scenarios passed; the next
scenario returned typed `Execution(Busy)` during exact-ID replay. The fixture now
waits, within its existing five-second bound, for the actual voter SQL/controller
owners to drain before starting replay. Quiescence only synchronizes the fixture;
the subsequent replay must still establish the exact result and current authority.
That correction is under validation. Both failed candidates retain their binaries,
source manifests, logs and disks; neither is counted as full runtime acceptance.


The `339df88c` candidate passed strict Clippy and the 60 library cases. Its first
public scenario completed both exact-ID replays with two callbacks, but its final
counter assertion rejected two sequential discovery plans (one join and four
actual probes) during wake. That assertion confused cumulative starts with live
ownership. The candidate now records terminal discovery owners, including failure
and unpolled-drop paths. The held overlap still requires exactly one live owner,
one join and one probe. Completed plans and joins must fit the two callers' existing
four-attempt banks, and every plan retains its original three-member probe bound.
Final owner termination and joined shutdown remain required. The new counter and
revised oracle are under validation; the original failed records remain retained.

## Composed public probe result and remaining failures

Source manifest `ca1432ed`, based on merge `c51c8475`, passed strict library and
six-target Clippy, all 60 selected library cases and the public
`leader_probe_storm_is_bounded` oracle. All eight combinations of image mode,
native/Axum HTTP and cancellation/joined-close cuts passed. During the held
physical wake sync, SDK and HTTP callers shared exactly one authenticated probe
owner, one join and one actual probe. The original two-slot ingress bank refused
a third caller before admission. Both original command IDs reconciled with two
callbacks and exact results; all entered probe plans terminated within the
original attempt banks. This qualifies that selected oracle, not every RO-09 cut.

The same immutable source and retained binaries passed all 193 nonignored
execution cases, both primitive cases, all 21 storage cases, both public read
ledger verifiers and the real RustFS segment-credit case in both image modes.
Formatting, layout, native dependency and documentation checks passed. Evidence
is retained in `composed-probes-ca1432ed`, including the source manifest, working
patch, changed sources, seven executable hashes and complete logs.

The composed campaign failed two existing cases. Scheduler results were ten
passes and one failure: `periodic_raft_segment_settlement_retries_the_local_receipt`
returned an ambiguous `Runtime(DeadlineExceeded)` for seed command client `c8`,
sequence one, in materialized mode. The seven earlier routing cases had six passes
and one failure: `refresh_storm_is_bounded` returned HTTP 503 with an ambiguous
`Unavailable` outcome for client `a8`, sequence one, in the extent/native HTTP
cold-refresh pair. Neither failure is treated as successful qualification or
attributed to a particular change without further evidence. Limits, deadlines,
receipt assertions and command identities were unchanged. These failures and
matched-source Linux verification are the next integration priorities; live
range-publication and remaining RO/RL, crash and performance gates stay open.

## Failure isolation and live interval-repair candidate

The retained `ca1432ed` binaries passed both originally failed cases when run
individually in fresh clusters. The corresponding `a69acc34` control binaries
also passed both cases. These four serial runs are retained in
`isolated-failures-905fa882`; they do not erase the initial combined failures.

A second full scheduler/earlier-routing run of the same `ca1432ed` binaries is
retained in `broad-repeat-905fa882`. Each suite again had one failure, in different
cases. Scheduler periodic settlement passed, while extent-image physical segment
credit returned an ambiguous deadline for client `d4`, sequence nine. Routing
refresh passed, while election-case startup failed at initial group readiness in
materialized mode. The three startup observations had term one, no leader and no
SQL/controller work admitted. They do not establish the cause of the other
command deadlines. All original limits and deadlines remained unchanged.

The next test-source candidate records compact owner, SQL, transport, performance
and bounded actor observations only after a failure. It retains periodic-case
disks and propagates the original failure. It adds no mutation retries or success
criteria. The live `split_repairs_only_overlap` candidate uses six data nodes and
a zero-replica ingress. Two actual RF3 splits must preserve unrelated cached
ranges, original command receipts, old AtLeast tokens and all current replicas'
SQL values within the existing two-history/512-MiB node profiles. Both image
formats and SDK/native/Axum calls are required. The result follows below;
RO-06 and all complete OPT packages remain open.

## Live interval-repair result

Source manifest `fc2e5e15` passed the new public RO-05
`split_repairs_only_overlap` oracle in both image modes in 85 seconds. Six data
nodes hosted two actual RF3 splits, while ingress retained zero replicas. The
first split created two cached ranges; splitting one again preserved the other's
zero-directory-lookup path before and after overlap repair. Changed cells used
SDK and both HTTP adapters. Original IDs and S3 receipts replayed without extra
callbacks, old AtLeast tokens remained valid, and every current voter returned
the expected SQL value using the latest token. Per-node profiles remained at
two histories and 512 MiB, with 256 MiB per group.

The same source and retained binaries passed all eleven scheduler cases and all
eight earlier routing cases. Separate focused runs of periodic settlement,
physical segment credit, cold refresh and election also passed. Strict library
and six-target Clippy, formatting, layout, dependency and documentation checks
passed. The qualifier retained both executable hashes, changed sources and the
working patch in `live-ranges-fc2e5e15` and checked unchanged source and HEAD
throughout. This closes the selected public RO-05 functional oracle. The earlier
variable failures remain retained; a passing run of this test-only change does
not establish their cause. This campaign does not qualify the new source on
Linux/RustFS, the delayed-lookup RO-06 case, performance or a complete OPT package.

The next RO-06 candidate holds a real tree-changing directory CAS after ordinary
split preparation, then holds an ingress caller's actual old directory-head
response. A second public lookup must install the activated child generation before
the old GET completes. The old command must retain its ID and refuse the
superseded observation without an application RPC; exact-ID reconciliation must
use the already-cached child. Unrelated cached routes, old receipt tokens and
current replicas' SQL values remain required. Fault gates surround the actual
object-store operations and release on every error path. Once the old command
is constructed, all work keeps its original five-second call bound. This candidate
is unqualified until its own source-matched compile and runtime checks pass.

Candidate `3b06f2c1` passed strict Clippy and repeated RO-05 in both modes, but its
RO-06 setup found no physical immutable-page read: the ordinary directory client
already cached those bytes. The fault cut now retains the actual head GET result
after the backend selects its body and version. Cached immutable pages remain
enabled. The corrected physical cut has not yet been qualified.

Candidates `84dd66d8` and `1d301685` reached the controlled publication cut,
but the query assertion incorrectly required a transferred cell's data witness
to name its current group. The captured reply retained the original parent
commit, matching `CellManifestLeaf.witness`'s documented contract. The candidate
now uses a fresh Quorum command receipt to anchor the new child before releasing
the old lookup. Query checks retain the exact latest data witness across transfer;
local reads must report the expected voter identity, independently observed in
the current child. The original delayed command still must refuse a superseded
observation without an application RPC, then reconcile its exact ID. These
fixture corrections remain unqualified pending a new source-matched run.

## Delayed SDK lookup result and current Linux checkpoint

Source manifest `5e71d680` passed the selected public RO-06 SDK split oracle in
both image modes, using the same retained executables, in 75 seconds. The physical
old head response crossed a real split publication. A fresh Quorum command
anchored the activated child; releasing the old response produced the exact
superseded-observation refusal without an application RPC. Reconciliation kept
the original command ID and S3 requirement, used the already-cached child, and
left unrelated cached routes intact. Every current voter returned the expected
SQL value and reported its expected member/node identity. Query observations
retained each logical version's original commit witness across transfer.

Those binaries also passed RO-05 in both modes, periodic settlement, physical
segment credit and election. The cold-refresh case failed during materialized
initial group readiness, before its discovery cut: all three voters had no leader,
with idle SQL/controller admission. Broad suites were not entered. The prior
run's materialized RO-06 passed, but its extent startup failed before the race.
Both failures remain retained. Strict library/six-target Clippy and all static
checks passed for this source. Evidence is in `live-old-lookup-5e71d680`,
`live-old-lookup-repeat-5e71d680` and `live-old-lookup-static-5e71d680`. This
qualifies the selected SDK split oracle, not HTTP/recovery cuts, a complete
composed runtime campaign or an OPT package.

Published revision `dd0b1060`, source manifest `fc2e5e15`, independently passed
both materialized and extent Linux Colima/RustFS campaigns. Its immutable image
is `sha256:a6e80300d34457df9ef6b9f039ebcb5c7791d572c6016e1a76d2ff2f0fb791a1`.
Retained resource observations confirm one CPU/3 GiB and no additional swap for
each voter, with the original request, archive-restoration and maintenance
deadlines. The ledgers cover archive-outage refusal and original S3 receipt
replay, leader hard kill/restart, maintenance resume, park/wake and joined
shutdown in both modes. Evidence is in `linux-daemon-dd0b1060ea3a-ranges`.
Standby repair and failed-voter repair were disabled, so these are not full CG-13
results. The scope is separate Linux cgroups and disks in one VM; no independent
failure-domain or performance claim follows. This Linux image does not qualify
the later `5e71d680` test source.


## Public HTTP delayed-lookup checkpoint

Source manifest `2ed2a24c` passed the RO-06 live split oracle through SDK, native
HTTP and Axum in both image modes: six fresh RF3 scenarios in 186 seconds. Each
old command held the actual backend-selected directory head response across
publication. Before releasing it, a fresh Quorum command installed the activated
child's route. Stale completion refused within the original five-second call
window without another application RPC or callback. HTTP callers received the
exact `503 Unavailable` response; SDK callers retained their original ID and
nonambiguous superseded-observation error.

Reconciliation retained the original S3 command identity and fingerprint, reused
the newer covering child and returned the exact logical result. HTTP replay of
that same request added no callback or directory lookup. An unrelated interval
was warmed while the old response was held and remained usable after publication.
Current child voters retained the expected SQL values, member/node identities,
original receipts and historical cell data witnesses.

The same retained binaries passed RO-05 in both modes, all 11 native scheduler
cases and all eight earlier routing cases, including probe ownership, read
preferences and fresh cohorts. Focused periodic settlement, physical segment
credit, cold refresh and election cases also passed. Strict library/six-target
Clippy, formatting, layout, native dependency and documentation checks passed.
Evidence is in `live-old-lookup-http-2ed2a24c`, with source inputs, working patch,
CI profiles and retained executable hashes. The nine opt-in RustFS scheduler
cases were ignored in this native suite; the separately recorded constrained
Linux/RustFS checkpoint remains source `fc2e5e15`.

This accepts the selected delayed-lookup split surfaces, not recovery cutover,
complete routing authority/ambiguous-outcome qualification or a full OPT package.
Earlier startup/deadline failures remain retained; this passing run does not
identify their cause. PR #166 has since advanced to `02d71c81`; its native
activation/configuration integration and additional startup/tick observations
require their own composed-source qualification.

## Startup observations and current composed qualification

The branch includes PR #166 through `02d71c81` and adds bounded, fixed-vocabulary
observations at the actual shared cell-group owner: timer rounds, requested and
coalesced ticks, executed RawNode ticks, election-timeout work and log-admission
refusals. Managed readiness separately times archive authorization, durable
attachment and fresh quorum. Returned errors and dropped futures retain distinct
outcomes. These observations establish neither serving authority nor capacity
release, and do not establish measured fairness or an election-latency fix.

Source manifest `9ec7d32b` passed strict library/seven-target Clippy, five metric
owner tests, held-sync tick coalescing, both selected park/wake cases, and public
startup/quorum-loss checks. The readiness test explicitly preserves the operator
fixture's intentional missing-registry refusal before successful startup, then
checks exact counter deltas for the later quorum-loss call. The earlier
`65642ab1` assertion failure remains retained; it incorrectly omitted that
negative control, rather than demonstrating a production startup regression.

The same retained `9ec7d32b` executables passed all six public RO-06 split
scenarios, all 11 native scheduler cases and the remaining nine routing cases,
including both RO-05 modes. Native storage and activation-cache cases passed.
The ordinary JSON RPC case failed in extent mode during exact-ID replay of an
unpolled command: client `da` repeated 16 bytes, sequence one, returned ambiguous
`Execution(Busy)`. The expected transaction-panic controls are unrelated to this
failure. Its disks, source inputs, CI profiles and executable hashes remain in
`startup-phase-count-9ec7d32b`. This is an incomplete composed run.

Formatting, module layout, native dependencies, documentation links and all 317
performance-tool tests passed, with one opt-in Docker test skipped. The design
suite reported source drift in the host-options anchor. Review found exactly
15 added lines in `host.rs`: default-off periodic settlement, the required
existing cleanup owner and its matching 2-MiB evidence reservation within parent
control limits. All other anchor bytes, excerpts and lines matched committed
source `9b5d55ca`. The baseline now pins that reviewed source while retaining its
historical runtime revision and task acceptance. All 44 design tests pass after
that source review; the original failed run record is unchanged.

The RPC fixture waits for the fixture's actual RF3 SQL cell leases
inside its existing five-second replay window, then replays the original ID with
only the remaining time. It continues to reject unexpected `Busy` and preserves
every receipt/version/value/factory assertion. A visible receipt is not proof of
physical producer exit. The original failure log alone cannot identify which
Busy check fired. This is not a production retry-policy change or a claimed fix
for older startup failures.

Source manifest `e09efd7a` passed the new selected follow-up recorded in
`rpc-owner-replay-e09efd7a`: all three native primitive cases, strict
library/seven-target Clippy, formatting, layout, dependency and documentation
checks, and all 44 design tests. Two opt-in RustFS primitive cases were ignored.
Both image modes completed exact-ID RPC replay; their SQL-owner barriers observed
zero active leases immediately, so this run does not establish a causal fix for
the earlier intermittent `Busy`. Failure diagnostics now capture bounded owner
observations before joined shutdown.

All five CI-profile executable hashes were retained. The library, managed,
routing and scheduler executables are byte-identical to their `9ec7d32b`
counterparts; their selected passing results above remain applicable to those
executables. The earlier failed record is unchanged. This is selected follow-up
qualification, not a new full composed or Linux/RustFS campaign. Source-matched
release, performance and complete OPT-01 through OPT-08 acceptance remain open.

## Membership repair and RPC interleaving checkpoint

The branch now includes PR #166 through `d3c2621b`, including same-cell native
RPC interleaving between proven roots. Source manifest `20cb5342` passed the
selected native campaign in `membership-routing-20cb5342` at base `2fb0b365`.
Five CI-profile test executables and the exact Cargo operator CLI are retained
with hashes; the source manifest and both retained and executed artifact hashes
were checked after the campaign. The compiler profile retains optimization level
one, debug assertions and overflow checks without debug symbols.

The new `membership_refresh_keeps_range` case passed all six SDK, native HTTP
and Axum scenarios across materialized and extent images in 72.608 seconds.
Each fresh seven-node fixture replaces an RF3 voter with member five, transfers
leadership to that new voter and holds the ingress's actual birth-object read
until public demand refresh is observed. Directory root, cell range and range
epoch remain unchanged. Subsequent cached writes use the repaired membership;
removed local and explicitly selected replicas refuse without entering SQL.
The removed node's independent namespace grant still permits forwarding as an
ingress. Original IDs, stored results and S3 proof survive; replay selects the
current covering image version and witness. Current-voter local reads and old
version tokens remain checked. Eighteen structured warm, repair and fence
observations retain receipts, identities, counters and resource owners.

Two failed candidate records remain unchanged. Source `02e2fb23` failed after
movement settlement because the new fixture immediately requested physical
inventory at a newer Fleet root. Its retained binary reproduced the failure;
the `226d7cf3` probe measured required revision 35 with installed trust revision
34. The fixture now waits for the existing directory's exact node observation
and requests inventory with the remaining time inside one original five-second
window. Source `412efb53` then passed repair and removed-replica checks but
incorrectly expected the replay's image proof to predate later mutations.
The retained `e40e017f` probe confirmed unchanged ID, result and S3 durability
with the exact latest image version/witness. The oracle now checks that proof,
coverage of the original version and no additional callback, matching
`CommandReceipt` and `CellCommittedReceipt`. Both corrections are confined to
the new fixture; no production retry, authority, durability or capacity rule
changed. Temporary probe tags were removed from the qualified source.

The same `20cb5342` campaign passed strict library/seven-target Clippy, all four
native primitive cases in 281.267 seconds, all 11 scheduler cases in 508.478
seconds and the ten remaining routing cases in 631.160 seconds. The latter
includes all six delayed-lookup RO-06 scenarios and both RO-05 modes, making
11 routing cases overall. The primitive suite includes native activation,
ordinary JSON RPC, storage and the newly integrated proven-root interleaving
case. Its original-ID SQL-owner barrier actually waited once in materialized
mode; the extent scenario observed no active lease. This exercises the barrier
without identifying the precise cause of the earlier `9ec7d32b` extent Busy.
Three opt-in RustFS primitive cases and nine scheduler cases were ignored.

All five metric-owner tests, held-sync tick coalescing, both selected park
cases and startup/quorum-loss checks passed. Formatting, layout, native
dependencies, documentation, all 317 performance-tool tests and all 44 design
tests passed; one opt-in performance-tool Docker test was skipped. No request
deadline, command identity, profile capacity or archive assertion was relaxed.

This qualifies the selected combined native checkpoint. The prepared RustFS
routing entry points, constrained daemon source update, remaining authority and
recovery cuts, measured placement/fairness and complete primitive/performance
matrix still need their own evidence. The earlier constrained Linux/RustFS
campaign remains bound to `dd0b1060` / `fc2e5e15`; it does not qualify this newer
source. Full OPT-01 through OPT-08 acceptance remains open.

## Real RustFS routing harness and incomplete campaign

Revision `833c57d7`, source manifest `fb653fe1`, adds three opt-in real RustFS
routing cases. The `routing-range-repairs` runner requires exact test execution
and complete structured evidence for six membership, two overlap and six
delayed-lookup scenarios. Each caller/image combination uses its own object
prefix. The runner captures the CI compiler profile, source inputs, retained
test executable and exact Cargo operator CLI, and rejects missing or duplicate
evidence. Static/type/build preflight passed strict library/seven-target Clippy,
formatting, layout, native dependency and documentation checks, 91 multi-raft
tool tests and all 44 design tests; one opt-in Docker tooling test was skipped.
Evidence is retained in `rustfs-routing-preflight-fb653fe1`.

The actual campaign used the owned Colima RustFS container with one CPU,
2 GiB memory and no additional swap, pinned to image
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`.
Cellar nodes in this campaign were host-native TLS fixtures. Membership repair
passed all six SDK/native HTTP/Axum image combinations in 97.563 seconds, with
18 validated observations. Overlap repair passed both image modes in 115.439
seconds, with two validated observations.

The delayed-lookup case passed SDK/materialized, SDK/extents and native
HTTP/materialized, then failed in native HTTP/extents. The failing operation
was the fresh SDK Quorum anchor for `Counter:five`, client `cc` repeated
16 bytes and sequence two, returning ambiguous
`Execution(Refused(Unavailable))`. It was not the held HTTP S3 command for
`Counter:two`, client `c9` repeated 16 bytes and sequence two. The available
failure snapshot observed the original, already retired group rather than
the selected child, so it does not establish the cause. The campaign stopped;
the ordinary RustFS primitive suite did not run. The failed record, logs,
objects, fixture disks and executable hashes remain in
`rustfs-range-campaign-fb653fe1`.

An exact retained-binary repetition on a fresh bucket/prefix passed all six
delayed-lookup scenarios in 329.151 seconds. No source, command identity,
deadline or fixture capacity changed. Its diagnostic record is
`NOT_REPRODUCED` in `rustfs-old-lookup-reproduce-fb653fe1`; this does not fix or
supersede the failed campaign. The test executable hash is
`2edd498bd01b4bdaca73a23284d9cd586188877189eb2b540090bc7297151d4b` and the
operator CLI hash is
`76d725c285ab356575c387cd5cc0dcd92cd74be2ea6d6a281a3d2c796a53a0a3`.

Keep PR #167 draft. The immediate priorities are to capture the selected
child's state at the intermittent Quorum refusal, resolve its cause, repeat
the complete routing/primitive campaign and qualify the matching constrained
Linux source. The earlier Linux checkpoint remains `dd0b1060` / `fc2e5e15`.
Authority/recovery and ambiguous-outcome cuts, partition/RecentSource expiry,
held-owner reclamation and restart, repeated movement, measured placement and
scheduling fairness, and the full primitive/performance matrix remain open.

## Failure-boundary follow-up

The retained `fb653fe1` primitive executable subsequently ran against fresh
RustFS objects. Ordinary JSON RPC passed in 23.348 seconds and activation
fencing/cache/restart passed in 153.293 seconds. Interleaving between proven
roots failed in its extent/S3 variant with `Runtime(Overloaded)` after 113.160
seconds. The original error lacks operation context; it does not identify which
admission owner refused. The failed record and three fixture disks remain in
`rustfs-retained-primitives-fb653fe1`. This is another incomplete campaign.

Four fresh native-HTTP/extent delayed-lookup scenarios passed on diagnostic
source `df233673` in 235.806 seconds. A subsequent original six-scenario run
on diagnostic source `7fa80620` passed both SDK scenarios, then failed initial
RF3 readiness before the native-HTTP/materialized split. All three voters
observed term two, no leader and applied/committed index one. They executed
43--45 ticks without admission refusals. This is a separate startup failure;
it did not reproduce or explain the fresh Quorum anchor refusal. Its record
and disks remain in `ro06-authority-diagnostic-7fa80620`.

Diagnostic source `7afcdfcf` passed four fresh extent/S3 interleaving scenarios
in 138.711 seconds, then its exact retained executable passed the original four
Quorum/S3 and materialized/extent variants in 101.629 seconds. Those repetitions
did not reproduce the overload. Their records remain diagnostic; neither
supersedes the failed primitive campaign or establishes a causal fix. Command
identities, fixture capacities, deadlines and assertions were preserved.

Failure reporting now labels interleaving operations and captures bounded
group/resource observations before shutdown. A failed fresh Quorum anchor
records its original ID and request timing, then bounds an observation of the
directory-selected child to two seconds. That observation runs after the
request has failed; it neither retries the command nor extends its deadline.
Temporary diagnostic entry points and production debug probes were removed.
Current-source composed qualification and diagnosis of the retained failures
remain open, alongside the broader optimization and release gates above.

The resulting permanent-diagnostics source `8b529396` completed the selected
native campaign in `failure-boundary-native-8b529396`, with the overall record
retained as **failed**. Strict library/seven-target Clippy, all six membership
replacement scenarios, the five metric-owner cases, held-sync/park/wake and
startup/quorum-loss checks passed. All four primitive cases passed in 305.134
seconds and all 11 scheduler cases in 607.557 seconds. Nine of the ten remaining
routing cases passed, including all six delayed-lookup scenarios and both
overlap modes; eight range observations were retained alongside the 18 membership
observations. Formatting, layout, dependencies, documentation, all 317
performance-tool tests and all 44 design tests passed, with the opt-in Docker
tool test skipped. Five test executables and the operator CLI were retained
and checked against their executed hashes.

`leader_probe_storm_is_bounded` failed initial RF3 readiness before its routing
race. At the bounded failure observation, all three voters knew leader two in
term two. The leader had persisted its no-op at index two, but commitment and
application remained at index one; both followers still had an engine file sync
and persist operation in progress. There were no tick-admission refusals or
quarantined groups. Per-member/node election-seed derivation and the elected
leader do not support identical schedules as this failure's cause. This narrows
the observed boundary to completing the post-election durable quorum; it does
not establish a causal storage fix or explain every older startup failure.
Readiness correctly remained unconfirmed. The original deadline and required
durability proof were preserved. The latest fully passing selected native
checkpoint remains `20cb5342`; current-source backend and constrained Linux
qualification, the retained failures and complete OPT acceptance remain open.

## Cached advice and namespace origin revocation

Source `5231fdd2` passed `cached_advice_respects_origin_revocation` in 26.419
seconds: SDK, native HTTP and Axum in both image modes, using two fresh RF3
fixtures with a replica-free ingress. Each interaction warms a direct route,
revokes the ingress's write grant on every voter, and observes one cached
application RPC attempt with no probe, range or membership repair. Neither SQL staging
nor the application callback enters while permission is absent. An independent
read grant still serves the latest minimum-version token.

Stale grant revisions and an unverified process cannot restore permission.
A newer valid grant reuses the refused command's original ID and inputs with
explicit S3 durability. SDK and HTTP replay retain its exact result and current
covering version/witness, without another callback. Current local voters expose
the final value. Six `AUTHORITY_ROUTING_EVIDENCE` records retain identities,
routing/outcome counters, SQL entry observations, owners and receipts in
`public-origin-native-5231fdd2`. Strict library/seven-target Clippy and the
CI-profile executable/CLI build passed. All 94 multi-raft tooling tests passed,
with one opt-in Docker test skipped.

The earlier `e5387c52` run retained a new fixture expectation failure: it
expected plain HTTP `Unavailable` after transport revocation. The public
counter API instead preserves the ambiguous original command ID and typed
`Unavailable` cause. The corrected case explicitly requires that response and
SDK ambiguity; fixture observations cannot grant the client non-commit proof.
The no-SQL, no-callback, one-attempt and exact S3 replay assertions remain.
No production retry or authority behavior changed.

The `routing-authority` RustFS entry point passed the same six interactions
in 31.321 seconds at revision `52dcf964`, recorded in
`rustfs-origin-campaign-5231fdd2`. Its six-row ledger verifies complete
caller/image coverage, no entry during revocation, preserved ambiguity and
S3 proof. The retained test executable and operator CLI are byte-identical to
the native run. The backend observation confirms the owned pinned RustFS image
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`,
one CPU, 2 GiB memory and no additional swap. Cellar voters were host-native
TLS processes; constrained Linux voter verification still requires its own run.
This case covers current origin permission, rather than forged/lower/conflicting
term advice or restarted endpoint wire cuts. Full RO-03, current-source composed
qualification and the broader optimization/release gates remain open.

The same source passed the format check for 2,194 Rust files, module layout,
native dependencies, documentation and all 44 design tests, recorded in
`public-origin-static-5231fdd2`. These checks and the focused native case do not
qualify a real-backend or constrained-process run.

The subsequent broader `5231fdd2` RustFS campaign remains failed in
`rustfs-range-campaign-5231fdd2`. All six membership interactions passed in
124.665 seconds, with 18 audited observations. Overlap repair then failed in
materialized mode after 80.886 seconds: the native HTTP write for
`Counter:three`, client `ca` repeated 16 bytes and sequence three, returned an
ambiguous `Unavailable` while requesting S3 acknowledgement. That response
does not establish whether the mutation committed. The original log, object
prefix and 14 fixture/control disks remain retained. Its generic snapshot
observes the retired original group rather than the selected serving group,
so the failure cause remains open. Delayed lookup and the ordinary primitive
suite were not reached. No deadline, identity, capacity or assertion was relaxed.

## Retry horizons and protocol future boundary

A fresh RustFS diagnostic on source `d214e53b` captured a destination refusal
for `Counter:five`, client `cc` repeated 16 bytes and sequence three:
`Execution(Invalid("command retry window"))`. The selected child had an active,
ready leader. The fixture had captured one 60-second retry horizon before
initial commands and two split preparations, then reused it for new command
identities. This proves that fixture hazard for this observation. It does not
individually explain the earlier `ca`/sequence-three HTTP or `cc`/sequence-two
Quorum failures. The original records remain retained.

Source `95db97a5`, committed in `c27d04a5` and `d9792f5b`, captures the existing
60-second horizon when each new logical command is created. Original receipt
replays and pending delayed-lookup reconciliation retain their saved IDs,
inputs and horizons. Individual five-second calls, the remaining held-call
deadline, transition bounds, resource capacities and assertions are unchanged.
Failure context identifies the selected serving group after a split.

The composed library-test build exposed a `Send` type-evaluation overflow at
the connection task. One private `IncomingCall::execute` boundary now returns
a boxed future; its dispatch bodies and ownership transfer remain unchanged.
Strict library/seven-target Clippy passes with this boundary. The first
subsequent build retained a Rust 1.98.1 incremental-fingerprint compiler panic.
After retaining only this checkout's affected incremental directories, the
same source and compiler/profile built successfully. This comparison does not
establish a general compiler fix.

The native campaign `query-future-fresh-native-95db97a5` remains **failed**.
Ten of the 12 routing cases passed, including both overlap modes, all six
delayed-lookup interactions and all six cached-origin interactions. All five
primitive cases, all 11 scheduler cases, metric-owner/held-sync/park/wake and
public startup/quorum-loss checks passed. Library/seven-target Clippy,
CI-profile builds, formatting for 2,198 Rust files, layout, dependency and docs
checks passed, as did 317 performance-tool, 94 multi-raft-tool and 44 design
tests; each of the first two tool suites skipped one opt-in case. Five test
executables and the operator CLI were retained and verified against their
executed hashes.

`membership_refresh_keeps_range` failed before scenario evidence at the
five-second read-only `first-group standby` CLI call. Separately,
`leader_probe_storm_is_bounded` failed initial RF3 readiness in a later fresh
materialized fixture with `Transport(Timeout)`, after an earlier overlapping
probe interaction completed. Neither failure is a completed optimization
oracle or evidence of a causal fix. Their original logs and records remain
retained; no deadline, retry budget or assertion was relaxed.

The matching Linux build did not reach voter execution: `rustc` received
`SIGKILL` and BuildKit reported `ResourceExhausted: cannot allocate memory`
on the owned four-CPU/6-GiB parent VM. Its failed record remains
`linux-build-d9792f5b403e-95db97a5`. No current-source Linux image or daemon
qualification is claimed. The older `dd0b1060` / `fc2e5e15` constrained voter
checkpoint above remains separate.

Keep PR #167 draft. Next are diagnosis of the membership setup and initial probe-storm readiness
timeouts, current-source RustFS routing/primitive qualification, and matching constrained
Linux voters with adequate parent-VM memory and unchanged per-voter caps.
Endpoint-only repair remains an isolated, unqualified draft. The authority,
recovery, reclamation, placement, scheduling and final matrix gates listed
above remain open; none of OPT-01 through OPT-08 is accepted as complete.

## Current-source RustFS routing and primitive campaign

The seven-case `rustfs-current95-composed-20261111` campaign at revision
`920380f6`, source `95db97a5`, remains **failed**. It used the exact retained
native routing executable `d528f992`, primitive executable `e09a5d96` and
operator CLI `5717a275`; full hashes and source inputs are retained in its
record. Each independent case used fresh fixtures and separate object
subprefixes. Continuing the remaining cases did not retry failed mutations.

Both split-overlap image modes passed in 127.504 seconds, with two validated
observations. All four ordinary primitive cases passed against real RustFS:
JSON RPC in 63.012 seconds, activation/cache/restart in 160.206 seconds,
interleaving between proven roots in 164.111 seconds, and canonical SDK
source-proof dispatch in 71.749 seconds. These results do not retrospectively
explain the older intermittent Busy or Overloaded records.

Membership completed both SDK image-mode interactions, then failed initial
readiness in the next, native-HTTP/materialized fixture after 55.099 seconds.
All three groups were attached in term one, with no leader and applied,
committed and persisted index one. Their observed file syncs had completed.
Attachment consumed about 2.9 seconds of the original five-second startup
window, leaving about 2.1 seconds for fresh quorum; readiness correctly remained
unconfirmed. This differs from the native source-`95db97a5` probe-storm failure:
its three groups had not attached, with one retained preparation per node and
`NotFound` runtime observations. Neither snapshot establishes a causal fix.

Delayed lookup completed all four SDK/native-HTTP image-mode interactions,
then failed cell enrollment while preparing the Axum/materialized fixture,
before that fixture's lookup race. The case failed after 281.479 seconds with
ambiguous `Runtime(DeadlineExceeded)`. This is distinct from the earlier
native-HTTP/extents fresh Quorum-anchor refusal. Four observations are retained;
the complete six-scenario gate remains unsatisfied.

RustFS retained its pinned image, one CPU, 2 GiB and no additional swap. The
owned parent VM was restored to four CPUs/14 GiB before the campaign, without
changing voter or store caps. Host compilation and a later owned Linux build
ran concurrently; no performance or scheduling-causality claim follows from
these checks. Matching Linux execution is recorded below; the remaining
optimization gates above remain open.

## Current-source constrained Linux voters

Source `95db97a5`, Linux revision `d9792f5b`, passed the constrained daemon
campaign in both image modes using image
`sha256:4dcb170a3509f1a8efc52096041fa94771fd62710e507e9399a2410061ea2edf`.
The implementation bytes match the published source at `087df66a`; that later
revision adds documentation. Source manifests and both executable hashes were
verified from the immutable image before execution. The record is
`linux-daemon-d9792f5b403e-95db97a5`.

Materialized mode passed in 107.233 seconds with 137 ledger events; extents
passed in 105.494 seconds with 131 events. The existing verifier checked S3
outage refusal and exact-ID replay, continuing Quorum/linearizable operations,
retained-disk hard kill/restart, maintenance return/resume, park/wake,
no-quorum refusal and joined shutdown. Separate voter containers retained
separate disks. Captured effective cgroups show one CPU, 3 GiB and zero swap
per voter. RustFS retained its pinned image, one CPU, 2 GiB and no additional
swap. No test deadline or per-voter/store capacity was enlarged.

The failed six-GiB parent build remains retained. The identical source,
compiler, dev/debug-zero profile and one build job succeeded after restoring
the parent to four CPUs/14 GiB. Runtime then used six parent CPUs/14 GiB to
satisfy the daemon runner's existing preflight. The preflight was unchanged.
These are correctness results in one local VM, with host work running
concurrently; they establish no independent failure domains or measured
speedup. Standby and failed-voter replacement were disabled. Full CG-13,
CG-17 and OPT-01 through OPT-08 acceptance remain open.

## Startup phase diagnostic

Temporary source `795d66da` ran fresh materialized startups through the same
unarmed filesystem adapter as the probe-storm fixture. Twenty-two starts
passed, then iteration 22 failed readiness. All three blocking page-recovery
closures, preparation futures, manager handoffs and runtime attachments had
completed. Attachment took about 3.133 seconds, leaving about 1.864 seconds
of the original five-second startup window. Voters remained in term one with
no leader, with 18--19 ticks executed and no election timeout. Admission
correctly remained closed.

This observed failure is after attachment; it does not reproduce or explain
the earlier native `95db97a5` pre-attachment `NotFound` snapshot. It exposes
the budget consumed before fresh quorum, without establishing the cause of
that elapsed time. Original probe-storm and membership cases subsequently
passed on the diagnostic source. These repetitions are not causal fixes and
do not supersede their failed records. Exact source, phase logs, binaries and
failed disks remain in `startup-phase-diagnostic-795d66da`. After the process
finished, all seven temporary files were restored from their exact baseline;
no phase probes or diagnostic entry point are published.

Next are the remaining startup/enrollment causes, endpoint-only repair's
read and unrelated-follower negative paths, full authority/recovery and
ambiguous-outcome cuts, followed by the reclamation, placement, scheduling
and final combined-source gates listed above. PR #167 remains draft.


## Endpoint repair and current PR checkpoint

PR #167 is open and ready for review. This checkpoint supersedes the older
statements above that called the PR a draft and excluded endpoint repair.
The branch now integrates stacked base `447c35e7`, endpoint component
`35ebb664` and the physical-admission fixture correction `1bdb2c86`.
Temporary startup and maintenance probes remain excluded.

Endpoint discovery is keyed by the selected authenticated node boot, without
turning a physical endpoint change into range or membership discovery. A
healthy leader can continue serving while an unrelated follower's endpoint
metadata is unavailable. Reads repair their selected remote endpoint under
the original call deadline and attempt budget.

The controlled in-flight regression reproduced twice on source `74faf168`.
A real group-archive PUT held an admitted S3 command while another follower's
new NodeView was accepted. The selected leader and configuration stayed the
same, but whole-route pointer identity discarded the leader's completed reply
as ambiguous `TransportStalePeer`. Source `ccd8ab5b` captures a bounded cache
lifetime independently of immutable endpoint snapshots. It still checks the
selected peer, schema/configuration revision, current Fleet/NodeView trust,
physical membership and explicit remove/reinstall. Entered work pins the
refreshed cache entry until physical exit; leader-advice generation remains
independently fenced. No mutation outcome is reclassified or retried with a
new identity, horizon, deadline or capacity limit.

The component native campaign `endpoint-native08-ccd8ab5b` passed strict
library/routing Clippy, CI-profile build, lifetime/removal/configuration/boot
negative checks, four public endpoint cases, read cohort/preferences,
cached-origin authority and static checks. The endpoint ledgers cover six
caller/image interactions per write case and twelve read interactions
(SDK/native HTTP/Axum, materialized/extents, Strong/AtLeast). This component
predates the latest primitive base and does not qualify the combined source.

The actual RustFS follow-up `endpoint-rustfs08-ccd8-20261111` passed all
three available backend cases with the same retained component executable
and CLI: selected restart repair in 39.017 seconds (six observations), the
unrelated-follower negative case in 31.370 seconds (six), and read-first
repair in 34.158 seconds (twelve). Logs, complete ledgers and executable
hashes were retained. RustFS kept its pinned image, one CPU, 2 GiB and no
additional swap in the owned Colima VM; voters were host-native TLS
processes. The controlled in-flight case was native-only at that checkpoint; the
later RustFS entry point requires its own runtime evidence. This older
component result does not qualify
the latest combined primitive source or constrained Linux voters.

## Combined revision verification

Revision `6bff2a13`, source `191b46fc`, includes both endpoint repair and
the latest primitive/Storage base. The `pr167-combined-final-191b46fc`
record retained five test executables, the operator CLI, full source inputs
and executable hashes. Rust 1.98.1 used one build job and the CI test profile
(opt level one, debug information zero, assertions and overflow checks on).
**FAILED**: 28 checks passed; 3 failed.

| Check | Result | Seconds |
| --- | --- | ---: |
| `clippy` | Passed | 186.864 |
| `unit-router_refresh-tests-` | Passed | 0.054 |
| `unit-leader_cache-tests-` | Passed | 0.015 |
| `unit-leader_probe-tests-` | Passed | 1.864 |
| `unit-performance-tests-` | Passed | 0.012 |
| `unit-query_wire-tests-` | Passed | 0.053 |
| `unit-storage-erase-tests-` | Passed | 0.012 |
| `unit-raft-effects-outbox-tests-` | Passed | 0.015 |
| `unit-cell_query_lifetime_fences_removal_configuration_and_selected_endpoint` | Passed | 0.562 |
| `unit-process_refresh_is_atomic_across_routes_grants_and_capacity` | **Failed** | 0.108 |
| `inflight` | Passed | 27.675 |
| `read-first` | Passed | 40.120 |
| `endpoint-unrelated` | Passed | 32.896 |
| `endpoint-public` | **Failed** | 27.195 |
| `read-cohort` | Passed | 61.100 |
| `read-preferences` | Passed | 49.495 |
| `origin-authority` | Passed | 40.878 |
| `periodic_raft_segment_settlement_retries_the_local_receipt` | Passed | 42.562 |
| `raft_segment_settlement_credits_files_after_checkpoint` | Passed | 20.666 |
| `raft_segment_settlement_replays_after_root_cas_uncertainty` | Passed | 2.143 |
| `storage_reads_and_writes_exact_cell_versions` | Passed | 18.530 |
| `ordinary_cell_sdk_resolves_current_histories` | Passed | 31.850 |
| `native-storage` | Passed | 1.271 |
| `format` | Passed | 119.713 |
| `layout` | Passed | 0.391 |
| `dependencies` | Passed | 1.191 |
| `docs` | Passed | 0.575 |
| `performance-tools` | **Failed** | 17.339 |
| `multi-raft-tools` | Passed | 9.746 |
| `design-tools` | Passed | 3.240 |
| `patch` | Passed | 0.029 |

Two driver attempts selected physical file names as module paths after
successful build/library groups. Both failed harness records are retained.
The final continuation selects the actual Storage and outbox module paths
from the executable inventory and uses the same hashed binaries/source.
Those selection corrections fix no product behavior. The final run also
reproduces the CI transport task-count failure locally.

These focused checks do not replace the full failed native matrix or qualify
the combined revision against actual RustFS or constrained Linux voters.


The earlier combined source `06769419` remains failed: membership stopped at
the read-only standby CLI timeout; the busy-leader oracle proved its two
refusals before a later fresh standby setup timed out; probe storm reached an
actual RPC and failed ambiguous `Unavailable`; periodic segment settlement
refused command sequence 14 with non-ambiguous `RuntimeOverloaded`. Six
primitive cases, native Storage, the other ten scheduler cases, strict
Clippy/build and static checks passed. An unchanged retained-binary periodic
repeat passed both modes in 35.347 seconds; this is not a causal fix.

The periodic maximum-input loop now waits for actual ordinary-admission
release before issuing its next new command, matching the existing manual
purge workload. It keeps the original resource bank, command IDs, retry
horizons, call windows and Fleet settlement assertions. A lingering owner
still fails the bounded wait; response delivery alone returns no capacity.

Constrained Linux source `06769419` built successfully as image `ce68dd7e`,
but its materialized campaign failed after maintenance readiness with a Fleet
conflict of unknown CLI verb. The archive-outage and hard-kill verifiers
passed for the retained partial ledger; maintenance completion is unproved.
An independent extents campaign failed initial Node 4 inspection with
`Refused`. Diagnostic-only source `f27ac447`, image `cc38c4ff`, added bounded
read-only error context in one test helper, then failed initial Node 5
inspection before reaching maintenance. It does not identify or fix the
previous maintenance failure. Those temporary probes are not published.
The earlier source `95db97a5` passing Linux campaigns remain historical.

Existing GitHub CI at head `9c9448af` also retained a transport fixture task
count failure (9 versus the asserted 11) and design tests unable to read the
pinned historical source in their checkout. Neither is declared fixed by
these endpoint changes. Current combined-source RustFS/Linux acceptance,
startup and actual-RPC failure causes, complete authority/ambiguity cuts,
held-owner reclamation/restart, placement/fairness measurements and the full
primitive/performance/failure matrix remain open. No full CG-13, CG-17,
OPT-01 through OPT-08, independent failure-domain or speedup claim follows.

## Backend entry and verification-guard follow-up

The next source adds the ignored RustFS held-reply entry and includes it and
read-first repair in the named endpoint suite. Its parser requires all thirty
observations across four cases, with exact caller, image and read-consistency
coverage. Seven new refusal tests and the existing tool suite passed (105
tests, one opt-in skip). The parser also accepted retained native held-reply
and read ledgers and the actual RustFS read ledger. Those parser checks do not
qualify a new runtime source.

The local source-`191b46fc` task-count failure reproduced CI's nine-versus-eleven
assertion. Main's shared-group cutover `d0c2873f` removed
`snapshots::start_prepared` and its owner pair. The fixture now requires three
listener tasks, six peer tasks and two actual pools; all overload, atomicity,
physical-drain and capacity assertions remain. Its compiled regression still
requires verification on the new source.

A controlled depth-one fetch omitted the exact pinned historical source
needed by the design tests; fetching full ancestor history made that same
object readable. The SDK CI job now selects the documented
[`fetch-depth: 0` checkout option](https://github.com/actions/checkout/blob/v6/README.md#fetch-all-history-for-all-tags-and-branches).
The workflow parsed with the intended job setting. A GitHub rerun is still
required. The performance reader now includes all three endpoint owner
counters; all 317 performance-tool tests passed with one opt-in skip.

The source-`191b46fc` endpoint case completed all three materialized interactions,
then failed fresh extents voter-one readiness before the endpoint scenario.
That startup failure remains open. The full current-source RustFS endpoint
campaign, constrained Linux and broader optimization gates remain unproved.


## Current-source RustFS and constrained Linux checkpoint

Runtime artifacts below were built from clean revision `b9ff44ba` with source
manifest `46eca779`. Documentation and workflow follow-ups do not change that
runtime manifest. Temporary HTTP diagnostic probes are excluded from the
published source.

The current native campaign passed formatting (2,214 files), strict library and
routing Clippy, the CI-profile build, all eight directory tests, layout,
dependency, documentation and patch checks. The directory group includes the
corrected nine-worker baseline and lifetime/configuration fences. Multi-raft
tooling passed 105 tests and performance tooling passed 317 tests, each with
one opt-in skip. The first incremental compiler fingerprint panic is retained;
identical-source fresh-state compilation passed. No compiler fix is claimed.

The named four-case RustFS endpoint campaign remains **failed**. After one SDK
interaction, materialized native HTTP command E9/sequence 3 returned ambiguous
`Unavailable`. Its original identity and disks remain retained; a later local
SQLite result is not proof of the required S3 receipt. The three remaining
cases passed independently on the exact same retained executable and CLI:
unrelated follower (six observations), Strong/AtLeast read-first repair
(twelve), and held in-flight S3 replies (six). These 24 observations do not
convert the failed named campaign into a pass.

An isolated error-category probe completed the selected endpoint case's six
observations without reproducing that HTTP failure. It was removed before
returning to the published source; this passing diagnostic is no causal fix.
A subsequent independent repeat of the exact published binaries completed all
three materialized interactions, then failed fresh extents initial attachment
with `Transport(Timeout)`. All three groups were absent, with one retained
preparation per voter and approximately five seconds spent in attachment.
This pre-attachment failure differs from the earlier post-attachment quorum
wait and the original native HTTP command failure. None is declared fixed.

The current constrained Linux image `ef0024789e9f` passed the full existing
`daemon.py` campaign in both image modes. Materialized took 98.726 seconds
(143 ledger events); extents took 88.250 seconds (131 events). Existing
archive-outage/refusal/exact-replay, leader-loss/hard-kill/retained-restart,
maintenance-stop/return/resume, park/wake, no-quorum-refusal and joined-close
verifiers passed. Ten actual live lifecycle observations recorded one CPU,
3 GiB and zero swap per voter. These were three voters observed across their
lifecycles. The pinned RustFS container retained one CPU, 2 GiB and no extra
swap inside the six-CPU/14-GiB Colima VM. The image used dev opt-level zero,
debug zero and one build job; it is not the CI opt-level-one test artifact.
Standby/replacement, independent failure domains, performance and the full
CG-13/CG-17/OPT acceptance remain unqualified.

Retained qualification directories under the checkout's private target are
`endpoint-current-backend-fresh-46eca779`,
`endpoint-current-independent-46eca779`,
`endpoint-http-diagnostic-eb6e2eb1` and
`endpoint-selected46-baseline-repeat`. The separate Linux checkout retains
`linux-current-build-b9ff44ba0582-46eca779` and
`linux-daemon-b9ff44ba0582-46eca779`. Each record binds source, executable or
image hashes, outcomes and the limits of its evidence.

Current GitHub native CI subsequently failed five measurement-runner tests at
`checkout target directory`, before voter setup. Its native job did not export
`CARGO_TARGET_DIR`, which these evidence-retaining fixtures require. The job
now explicitly selects `${{ github.workspace }}/target`, matching Cargo's
existing output location. A fresh CI-profile executable reproduced the exact
error immediately with the variable absent. With the variable exported, the
same executable completed its serial campaign in 467.533 seconds: **19 passed,
two failed**. Warm-verifier-cache, rooted-image reclamation and occupancy-32
archive diagnostics passed. Both failures reached occupancy-128 enrollment
and consumed the original 60-second total window, with 66 and 59 cells present
respectively. The original enrollment identities and fixture disks remain
retained. Evidence is in `ci-perf-target-b9ff44ba`; its profile is opt-level one,
debug zero, with debug assertions and overflow checks enabled.

The first failed fixture's later observations had committed/applied index 68
and archive publication at 66. The second had a different cell count and
publication position. Those observations do not prove one coherent failure
cut or a fixed 64-cell ceiling. Aggregate SQL preparation, staging, publishing
and engine-sync timings identify boundaries for profiling, not a causal
performance fix. The campaign used host-native voters and an in-memory object
store; it does not qualify constrained Linux, RustFS or measured speedup.
This workflow change establishes the cause of the setup failure, not a startup
or protocol fix. A separate attempt selected an
older CLI executable instead of the test harness; it is retained as a harness
selector error and contributes no valid qualification evidence.


## Disposable extent initializer base and pinned-toolchain control

The initializer follow-up uses runtime source manifest `aeffb378`. An unenrolled extent initializer
previously staged and sealed a durable transaction-zero root solely to open its
disposable SQLite workspace. It now uses `ExtentWorkspace::empty`, which checks
the canonical transaction-zero binding without allocating an inventory root.
The private comparison marker remains metadata. Existing published bases retain
their actual sealed images, and initialized results still take the original LTX,
sealed-image, manifest and durability-proof path. Callback admission, workspace
ownership, command identities, deadlines and resource banks remain unchanged.

The regression observes actual inventory usage while an initializer is held.
The old production code reported one root and one admitted workspace; the new
code reports zero roots and one workspace. Both observations use the existing
concurrent enrollment/cancellation case, including its original callback,
reconciliation, conflict and joined-close assertions. The assertion runs after
cleanup so its failure cannot strand the held callback. New LTX public tests
exercise real SQLite writes, last-handle admission retention and invalid
binding/quota refusal before file creation.

A compiler-provenance audit found that the recent 19-passed/two-failed runner
campaign and its OS stack samples used ambient `stable`, which had become Rust
1.99.1. Their optimization/assertion flags matched CI, but they do not qualify
the pinned Rust 1.98.1 toolchain. Embedded compiler hashes independently confirm
that the earlier source-`46eca779` RustFS routing artifacts and temporary HTTP
probe used 1.98.1. The 1.99.1 candidate compilation panic and all diagnostics are
retained; no compiler fix is claimed.

The controlled before/after therefore explicitly selects Rust 1.98.1, with its
compiler hash checked in retained executables. The original production code,
plus the new inventory assertion, reproduced the one-root/one-workspace failure.
It also reproduced the original occupancy-128 enrollment deadline: the total
window ended at 60.000 seconds, with later observations of 54 cells in the main
group. The candidate passed all 19 native enrollment cases and 29 LTX cases
across workspace, source-backed workspace, admission and apply targets. Two LTX
entries are marked ignored: the isolated write-failure child is exercised by its
passing parent, while the retained decimal-10-GB campaign remains unrun.
Strict library/affected-test Clippy, formatting (2,214 files), layout,
dependencies, documentation and patch checks passed. Pinned LTX core-only and
all-feature builds also passed. Their first attempt returned zero from both
compiler commands but failed its source guard after a component README edit;
that attempt is retained as failed. Restoring the exact tested README and
repeating both feature checks passed with the guard intact.

The candidate's original occupancy-128 case still **failed**: enrollment used
60.003 seconds and later observations showed 77 cells in the main group.
The complete failed test, including observations and joined close, took 76.471
seconds; the before test took 67.962 seconds. Those separate observations and
single attempts establish neither a coherent failure cut nor a measured
speedup. The optimization removes the extra empty inventory root; it does not
close the scale gate. Stack samples from the earlier diagnostic exposed
manifest-page, archive-journal and engine persistence barriers for further
investigation. No persistence requirement is relaxed by that observation.

Records are retained under `enrollment128-profile-46eca779`,
`enrollment-empty-base-regression`, `enrollment-empty-base-candidate`,
`enrollment-empty-base-pinned-before` and
`enrollment-empty-base-pinned-after` in the checkout's private qualification
root. The pinned candidate campaign remains **failed** solely at `scale128`;
its other listed checks passed. Source inputs stayed frozen through each
actual job's completion. The before and candidate incremental caches are
retained separately, with the same compiler/profile settings.

Actual RustFS and constrained Linux have not yet qualified `aeffb378`.
The prior source-`46eca779` endpoint results and both passing daemon modes stay
bound to that earlier source. Occupancy-128 enrollment, the distinct endpoint
and startup failures, standby/replacement, authority/ambiguity cuts, held-owner
reclamation, placement/fairness and the complete primitive/backend/performance
matrix remain open. This checkpoint accepts no complete CG-13, CG-17 or
OPT-01 through OPT-08 package.

## Archive reservation checkpoint and recovery control

Runtime source manifest `c273e2b0be74`, committed as `dfc0d424`, removes one
intermediate local journal checkpoint from an ordinary archive upload. The
original reservation intent is already durable before its allocation CAS.
The persistent pending generation reconciles that exact intent to its original
ticket, even after unrelated registry revisions and partial immutable writes.
The journal therefore retains `Reserve` through upload instead of writing a
second `Upload` record. Publication intent remains durable before its CAS;
seed uploads retain their existing checkpoint path. Neither recovered tickets
nor partial objects establish archive coverage or serving authority.

The new regression holds the actual reservation CAS, reads the durable intent,
then loses an accepted immutable-create response and denies subsequent reads.
It joins the original controllers, SQL workers and Raft runtimes before opening
the registry in a new archive execution domain. An unrelated durable allocation
advances the registry, yet the persisted original intent reconstructs the same
ticket without another PUT. Collection preserves its partial generation after
the original local upload handle has exited. Recovery independently reopens the
selected local root, completes the upload and independently verifies the archive
before publication. Published coverage is then audited against the full SQLite
contents. Publication intent is persisted before commit; consumed
ticket replay refuses without another allocation, and all owners join cleanup.
The original ReplicaLog engines remain open: this archive-owner handoff is not
full node/process restart or power-loss qualification.

On unchanged old production code, every recovery, collection, SQLite and
joined-cleanup assertion passed before the final journal assertion reproduced
the extra `Upload` checkpoint. The candidate passes that assertion in both
image modes. Both controls explicitly use Rust 1.98.1; retained executable
compiler hashes were checked. Initial fixture attempts remain retained: one
missed the shared test-intent helper and another tried checkpoint capture after
the runtime had closed. The corrected fixture uses persisted-root recovery.
The before control's initial driver classification was corrected by inspecting
both retained output streams; it was not rerun under a new mutation identity.

The candidate's native campaign remains **failed**. Two archive-journal unit
tests passed. The controller target reported **16 passed, four failed, 23
ignored**, including a passing new recovery case. The archive-protocol target
reported **31 passed, one failed, seven ignored**. Its failure was
`quorum_continues_bounded_s3_outage`, at the fresh remote catalog-discovery
assertion. Original occupancy-128 enrollment also failed within its unchanged
60-second total window; the complete failed test and joined observations took
69.752 seconds. No scale acceptance or measured speedup follows from removing
this checkpoint. Strict library/affected-test/runner Clippy, formatting (2,215
files), layout, dependency, documentation and patch checks passed. The multi-raft
tool suite ran 105 tests with one opt-in skip and passed. Source inputs remained frozen
through actual completion.

A separate paired comparison uses the retained old-production and candidate
executables, with identical test code, profiles and Rust 1.98.1. In both,
`s3_enrollment_requires_the_original_archived_creation` fails at the duplicate
response ownership assertion and
`s3_remote_outputs_preserve_durability_and_creation` fails at the unbound remote
ambiguity assertion. The deadline and slow-delivery cases that failed in the
full controller campaign each pass independently on both executables. Those
repeats neither erase the full campaign failures nor prove their causes. The
archive-protocol outage failure has no paired before control in this campaign.
No assertions, request windows, identities or resource limits were changed.

The committed candidate then **passed** the named RustFS `archive-reservation`
suite: seven signed S3 capability probes and its exact ignored recovery case,
which exercises both materialized and extent modes. Its retained executable
hash matches the native candidate exactly, including the embedded Rust 1.98.1
compiler hash. Backend ownership, pinned image, loopback endpoint and live
one-CPU/2-GiB/no-extra-swap limits were checked before and after the campaign.
The backend runs in the owned Colima VM; these RF3 voters are host-native.
This pass qualifies the recovery case, not the default 23-case archive suite,
constrained container voters, full node restart or the broader S3 failures.

Records are retained under `archive-reservation-before`,
`archive-reservation-before-v2`, `archive-reservation-before-v3`,
`archive-reservation-candidate`, `archive-reservation-paired-regressions` and
`rustfs-archive-reservation-dfc0d424` in the checkout's private qualification
root. The latest constrained Linux evidence remains bound to source `46eca779`
and image `ef0024789e9f`; it does not qualify this candidate. Next priorities
remain occupancy-128 cost, the S3 ownership/ambiguity/outage failures and distinct
endpoint/startup failures, followed by latest-source constrained Linux,
standby/replacement, authority and reclamation cuts, placement/fairness and the
complete primitive/backend/performance matrix. Full CG-13, CG-17 and OPT-01
through OPT-08 remain open.

## S3 output contracts after routing and verification reuse

Qualification source `8eca02f3`, committed as `2010c5c5`, strengthens three
existing S3 scenarios. Production behavior is unchanged; the production Rust
diff corrects the public remote-command documentation. The previous failures
exposed test assumptions that predated admitted verification reuse, authenticated
rejection before admission and local destination-manifest routing.

The enrollment probe observed a successful duplicate with the same creation
record, S3 durability, two retained outputs and one physical verifier. The
scenario now denies new immutable writes while the duplicate shares that proof.
Dropping the first response must leave its final consumer's verifier charged.
An uncovered successor executes its callback but cannot receive S3 output until
that final consumer drops. Original initialization, creation-conflict, namespace
minimum, callback-count and joined-close checks remain in place.

The unbound remote probe observed authenticated `UnsupportedCapability`, no
callback or initializer, zero retained owners and `ambiguous=false`. The scenario
now checks this precise non-commit result and its opposite boundary. One original
invocation is rejected, retried through a bound gateway, then loses its response
waiter after SQL retires while S3 output is pending. After physical completion,
the same invocation receives another authenticated pre-admission rejection.
It must preserve its earlier uncertainty, original command identity, captured
inputs, 15-second deadline and four-attempt bank. Only one callback runs. The
fixture rebinds the gateway while retaining its existing TLS router; this is
client waiter cancellation, not process restart or a claimed network-loss cut.

The outage probe observed selected-destination Quorum success with zero receiver
catalog reads while the direct backend probe failed. The scenario now checks a
separate cold ingress: its actual catalog read must fail before transmission or
callback execution. The selected receiver still uses its active local manifest
and current execution authority. Its successful remote command counts within
the original six-callback load at the unchanged nine-entry ceiling. The local
client's next sequence is consequently six. S3 wait/refusal, stored minimum,
checkpoint and admission assertions remain, and replay retains the original
remote invocation instead of running another callback.

The full ordinary native campaign **passed**: controller **20 passed, zero
failed, 23 ignored**; archive protocol **32 passed, zero failed, seven ignored**.
Strict library/affected-test Clippy, formatting (2,215 files), layout, dependency,
documentation and patch checks passed. Source inputs stayed frozen. Rust 1.98.1
was explicitly selected and its embedded compiler hash checked in both retained
executables. The first build failed with an unstable incremental fingerprint
compiler panic before any runtime checks. Its record and twelve affected
checkout-private incremental directories remain retained. A fresh-state repeat
passed with the same compiler/profile settings; no compiler fix is claimed.
Earlier fixture attempts also remain retained: recreating an existing router
refused storage creation, and a new command at sequence two correctly refused
its sequence gap. The final counterexample uses the original invocation.

The default **23-case RustFS campaign failed** in its first selected case,
`rustfs_quorum_continues_bounded_s3_outage`. Seven signed capability probes and
inventory validation passed. Materialized mode reached an actual container stop,
restart and archive recovery, then command client `[188; 16]`, sequence **6**,
requested Quorum durability and returned ambiguous `Runtime(DeadlineExceeded)`.
This is the post-ceiling command after recovery, with its original 15-second
request window. The selected test process took 26.8 seconds. The runner restored
the same backend container; its pinned image and one-CPU/2-GiB/no-extra-swap
limits remained unchanged. Extents and the remaining default cases were not
reached. The original command outcome remains unknown; later observations or a
new fixture cannot turn this failed campaign into a pass.

Two independent exact RustFS cases then **passed** on the same retained native
executable and source, in a separate fresh bucket/prefix with seven signed probes:
`rustfs_s3_enrollment_requires_the_original_archived_creation` (7.003 seconds)
and `rustfs_s3_remote_outputs_preserve_durability_and_creation` (10.908 seconds).
Both exercise materialized and extent modes. These scoped passes verify the
stronger ownership and uncertainty checks through the actual S3 client; they do
not accept the failed default campaign. Voters remain host-native RF3 TLS tasks,
with RustFS inside the owned Colima VM. Earlier intermittent deadline and
slow-delivery failures did not recur in the passing native campaign, but their
causes are not established.

Records, executable/source hashes, logs and remote object scopes are retained in
`s3-output-contract-native`,
`s3-output-contract-native-fresh-incremental`,
`rustfs-s3-output-contract-8eca02f` and
`rustfs-s3-independent-ownership-8eca02f`. The targeted enrollment, unbound and
outage probes also remain under their `s3-*-probe` qualification directories.

The base branch advanced independently to `74591226` with parked read-cursor
image lifetime fixes. It has been fetched but is not integrated into this
checkpoint, which still builds on `447c35e7`. Integration and combined-source
qualification remain required. The new Quorum recovery timeout, occupancy-128
cost, distinct endpoint/startup failures, latest-source constrained voters,
standby/replacement, authority and held-owner reclamation cuts, placement/fairness
and the full primitive/backend/performance matrix remain open. The latest Linux
daemon evidence is still source `46eca779` / image `ef0024789e9f`. This checkpoint
accepts no complete CG-13, CG-17 or OPT-01 through OPT-08 package.

## Idle Raft connections after archive recovery

The retained Quorum recovery failure now has a reproduced transport cause.
The receiver closes idle TLS streams at the existing five-second I/O bound,
while the sender previously cached them without an expiry between exchanges.
An exact failed delivery reported peer EOF on the data lane to both followers.
The leader had local entry 11 while both followers and committed/applied progress
remained at 10. SQL had executed its callback, but no archive settlement owner
was blocking the command. This explains the reproduced failure; the original
failed invocation remains ambiguous and its campaign remains failed.

Runtime change `cbabde1d` retires cached data and control connections between
exchanges using the preceding batch/frame's original I/O deadline. Completion
of a late reply cannot renew that reuse window. A fresh connection uses the next
exchange's existing bound. Failed or uncertain frames are never replayed, and
active deadlines, route fences, physical reservations and application identities
remain unchanged. The rare delivery-feedback future has an opaque borrowed
boundary to avoid a compiler Send-proof depth overflow without changing compiler
limits or the healthy packet path.

Two new native RF3 TLS regressions wait for actual receiver-task exit before
the next Quorum command or control frame. Both fail against the previous runtime:
the command exhausts its original 15-second window, and the control frame reports
the same peer EOF. The candidate passes both tests in materialized and extent
modes (24.49 seconds in libtest). Failure paths retain fixture disks and join
network/runtime owners before returning; failed commands receive no fresh window.
The tests use in-memory object storage and disabled automatic Raft ticks, so
this selected pass does not qualify the complete RustFS outage campaign.

The controlled candidate source is `753b2a3e`; retained executable SHA-256 is
`98ed6a5ae58a539ebd76e7efbcb7782bb453d7f87f1ca51d09aa8c22a3d71951`.
Rust 1.98.1 is explicitly selected and its embedded compiler hash is verified,
with test opt-level 1, debug info 0, assertions and overflow checks enabled.
An ordinary Send-proof compilation failure and a subsequent unstable incremental
fingerprint compiler panic are retained separately. The fresh-state build passes
with the same compiler/profile; no compiler fix is claimed. Diagnostic traces
are removed from the committed source.

Base `74591226` is integrated without conflicts in `ba1c350e`. Combined source
`f1500526` is undergoing the full ordinary archive targets, actual transport unit
inventory, exact parked-cursor SDK case, original occupancy-128 diagnostic,
strict affected Clippy and repository checks. The complete default RustFS
campaign must then be rerun. These combined checks are pending at this checkpoint;
the selected pre-integration pass cannot substitute for them.

Evidence is retained under `quorum-recovery-exchange-series`, `peer-idle-before`,
`peer-idle-candidate`, `peer-idle-candidate-v2`,
`peer-idle-candidate-v2-fresh` and `peer-idle-native-combined`. Earlier failed
campaigns remain unchanged. Occupancy, distinct endpoint/startup failures,
current-source constrained Colima voters, standby/replacement and the remaining
authority, reclamation, fairness and full qualification matrix remain open.

## Shared archive fairness in both image modes

The archive campaign previously passed all 23 configured cases on source
`f1500526`, but its three-group fairness entry exercised materialized images
only. An audit of the retained log reproduced the missing evidence: a passing
libtest result alone cannot establish both image modes. The old campaign remains
a pass for its actual configured scope; it does not prove extent fairness.

Changes `27e1d706`, `abd76b1d` and `3bb0d605` run the fairness fixture separately
with materialized and extent images. All three extent groups reuse each node's
existing physical extent inventory and quota, and the fixture checks at least
14 roots in each shared inventory. It independently restores all 14 images and
compares their bytes and storage kinds, including the two initial images. The
existing single checkpoint slot, held publication, lost immutable-PUT response,
foreign-hold refusal, corrupt-tail refusal, healthy sibling cut and exact
repaired-tail checks remain. Original request windows and resource limits are
unchanged. Completed profiles are emitted only after runtime close and zero tail
operations. The RustFS runner requires exactly one complete record per image
mode and retains their hashed ledger; it rejects missing or duplicate profiles,
wrong resource values, missing inventory evidence and incomplete owner proofs.
Eight parser tests exercise these acceptance and refusal cases.

The shared archive-intent helper now belongs to its controller-archive fixture
module. This corrects an ordinary missing-import build failure in the execution
target; the first failed build is retained separately. Production Rust is
unchanged by this fairness follow-up.

Qualified source `1c757c7e` / commit `3bb0d605` passed:

- All 22 ordinary archive-controller tests, with zero failures and the 23
  RustFS-only entries ignored in this native invocation. The actual fairness
  output contains both completed image profiles and 14 shared roots for extents.
- Five exact execution regressions: mixed group roles, hot-group fairness,
  materialized and extent TLS apply/bounded retry, and distinct-history batching.
- Strict library/affected-test Clippy and 113 Python tool tests (one optional
  integration test skipped), plus formatting, layout, dependency, documentation
  and patch checks.

Rust 1.98.1 is explicitly selected. Both retained native executables contain
compiler hash `48a229ceaefd4985c50990b14116b6d856af0985`; test opt-level 1,
zero debug info, assertions and overflow checks remain enabled. The controller
executable SHA-256 is
`456d9b824e81c1de4f8d370fedae0a0417b79680412ad51ae830653b6da43308`.
Source inputs remained frozen through qualification. Records are retained under
`archive-fairness-before-coverage`, `archive-extent-fairness-native` and
`archive-extent-fairness-native-wiring`.

The full configured **23-case RustFS campaign passed** on that same source and
exact controller executable. Seven signed S3 probes, real backend stop/restart,
all case selections and the final backend/source guards passed. The fairness
ledger contains both completed image modes, verifies all 14 restored images in
each and records a shared-inventory minimum of 14 roots for extents. Its SHA-256
is `f3d41346572b6ab1d67233f9791401f1ca60d9a376918021f4452343b461cac5`.
The record is retained under `rustfs-extent-fairness-1c757c7e`. Voters in this
campaign are host-native RF3 TLS tasks; RustFS runs in the owned Colima VM with
the unchanged pinned image and one-CPU/2-GiB/no-extra-swap limits. This does not
qualify independent voter containers or the full scheduling/failure matrix.

## Latest performance and constrained-voter limits

On the preceding production source `f1500526`, all 71 transport unit tests,
22 ordinary controller tests, 32 ordinary archive-protocol tests and the exact
parked-cursor SDK case passed, together with strict Clippy and repository checks.
The combined native campaign remains failed at occupancy 128: enrollment reached
its original 60-second window with an ambiguous deadline error. Its snapshots
and later owner observations are distinct cuts. A separate pinned-source
reproduction with three passive stack samples again failed; the stacks show
manifest-page file synchronization and other persistence barriers. Sampling is
diagnostic evidence, without a measured speedup or causal attribution to one
barrier. Records remain under `peer-idle-native-combined-fresh` and
`enrollment128-f150-phase-profile-after-maintenance`.

The matching Rust 1.98.1 Linux image built as `a3c9d314`. The first constrained
runtime attempt was interrupted by a confirmed stopped Colima VM. After restoring
the same VM and exact RustFS container, a fresh materialized run failed at public
`resume-maintenance` with unknown outcome, before standby or extent mode. It
reached 95 events, including retained hard-kill/restart. Its original operation,
request and resume identities and 20-second command window remain authoritative;
a later fixture cannot resolve that mutation. Five redacted voter logs and
mount inventories are retained. The returning voter opened new ingress, while
its failure snapshot queried the old address; that snapshot cannot establish
that the voter was down. Mount inventories alone do not prove retained disk
contents, and forced cleanup is not joined SDK shutdown evidence. Records remain
under `linux-daemon-999a1fbf5c80-f1500526-standby`, its `-restored` sibling and
`f150-qualification-pipeline`.

Base PR #166 has advanced to fetched `9ccccad7`, adding named bindings, target
sequence sealing, leader-preferred archive publication and retry coalescing.
It is not integrated into this controlled checkpoint. Two fixture conflicts
remain in `cell_group_runtime.rs` and `cell_group_sql.rs`; integration and
combined-source qualification are the next prerequisite. Current-source
constrained voters, standby/replacement, occupancy 128, distinct endpoint/startup
failures, authority and held-owner reclamation cuts, measured placement/fairness
and the complete primitive/backend/performance matrix remain open. No full
CG-13, CG-17 or OPT-01 through OPT-08 package is accepted by these scoped results.

## Base-80 combined native and real-backend checkpoint

Merge `a5b5a8b7` integrates base `80bd02b1`, including named bindings, private
sequence retirement, leader-preferred publication and archive retry coalescing.
Its input digest is `06dbb5b1`. The six retained native executables were built
with Rust 1.98.1 (embedded compiler hash
`48a229ceaefd4985c50990b14116b6d856af0985`), test opt-level 1, zero debug info,
assertions and overflow checks enabled. Compilation took 1,682.002 seconds.
Inputs stayed frozen through the native and backend runs.

The selected native runtime checks passed:

- All 71 transport units, 23 ordinary archive-controller tests and 32 ordinary
  archive-protocol tests. The controller includes the new follower-prefix case
  and both image modes of archive fairness. Its 24 RustFS-only entries and the
  protocol target's seven RustFS-only entries are ignored in native invocations.
- Five execution regressions covering mixed roles, hot-group fairness, both
  image forms of TLS apply/bounded retry, and distinct-history batching.
- The exact parked-cursor SDK case and all four named-binding configurations:
  materialized/extents with Quorum/S3 source durability, under the original
  deadlines and with a Quorum target minimum. These do not qualify a target
  namespace with an S3 minimum or the complete HTTP/RPC primitive family.
- One private binding-seal unit, six manifest-index units and four effect-summary
  units. A supplemental run passed all ten actual `raft::receipts::tests` and
  both private class-contract tests. The initial broad receipt-module filter
  selected two fleet-finalization tests; its recorded scope remains those two.

The complete configured **24-case archive RustFS campaign passed**, including
actual backend stop/restart, follower-prefix verification and the two completed
fairness profiles. It used the same retained controller executable as the native
run and passed all seven signed capability probes and final backend/source
guards. A separate exact RustFS named-binding case passed all four configurations
in 75.596 seconds, with a fresh bucket/prefix and seven signed probes. Its
executable matches the retained native primitive executable. These are
host-native RF3 TLS voters with RustFS in the owned Colima VM. Host Clippy and
the separate Linux image build ran concurrently; these runs establish no timing
comparison or performance gain.

The combined native campaign remains **failed** at three checks. Occupancy 128
exhausted its original window at 60.004 seconds with ambiguous enrollment failure;
full process time was 66.442 seconds. Later serialized observations show 76–77
cells in the selected group, rather than one atomic deadline cut. Its disks,
performance snapshots, observations and executable binding remain retained.
SQL preparation consumed about 25.6/30.5/30.7 seconds of measured operation time
on the three nodes; engine file-sync times were about 12.5/12.6/14.1 seconds.
These durations overlap and cannot be summed as wall time or attributed to one
cause. No speedup is claimed.

Strict all-target Clippy found the archive-intent helper loaded twice in the
archive-protocol target. The source preflight also refused two stale reviewed
anchors. Formatting, layout, dependency, documentation, patch and 113 Python tool
tests passed (one optional integration test skipped). All failed records remain
unchanged under `base80-integration-native`; correct receipt-owner coverage is
retained separately under `base80-binding-contract-units`. Backend records are
`rustfs-base80-06dbb5b1` and `rustfs-named-bindings-base80-06dbb5b1`.

The helper correction gives the shared runtime fixture one archive-intent module
and reuses that function from both controller and protocol callers. The reviewed
successor baseline pins `acb84750` and changes only the effects and CI hashes:
private binding-seal contracts alone do not enable alarm/outbox scheduling;
CI fetches historical Git objects and exports its checkout-owned target directory.
The existing source, excerpt and line checks remain. All 44 design tests pass
after that review; the earlier preflight failure remains recorded. Strict
Clippy verification of the corrected helper is still pending at this checkpoint.

## Constrained Linux maintenance and standby checkpoint

Diagnostic follow-up `acb84750` changes three process-harness files while keeping
production code identical to `a5b5a8b7`. It durably records the original operation,
request, revision, arguments and attestation before the maintenance mutation.
Failure GETs sample the current process's ingress once, retain address provenance
and preserve the original mutation error. The 20-second command, 22-second outer
CLI and two-second diagnostic GET bounds remain unchanged.

Source `372b4822` built Linux image
`d76a4cc24afa48e472aa43f9d42170ec448272e72c329b3d173835d9fab00c8d`.
Both executables contain the same pinned Rust 1.98.1 compiler hash. This image
uses the existing dev/test opt-level 0 and zero-debug-info Docker profile; its
results are correctness evidence, rather than a native profile-1 comparison.

The complete existing daemon campaign with **standby passed in both image
modes**, taking 208.670 seconds overall. The materialized and extent fixture
processes completed in 95.977 and 87.159 seconds, with 130 and 100 ledger events.
Both modes include park/wake, archive outage and original S3 receipt replay,
retained hard-kill/restart, public maintenance stop/return/resume, returned-leader
SQL, and the fourth standby's capacity/binding, zero-group observer and joined
startup-cancellation checks. Each ledger preserves all four exact maintenance
requests, including the resume attestation. The failed-voter replacement gate
was not enabled.

Each mode retains six voter containers with the expected exit inventory
`[0, 0, 0, 0, 1, 137]`. Twelve whitelisted post-teardown inspections and redacted
logs retain one-CPU/3-GiB/no-extra-swap limits and no OOM kills. These inspections
are separate from the runner's live cgroup evidence; mount inventories alone do
not establish retained disk contents or graceful SDK shutdown. The run uses the
same owned six-CPU/14-GiB VM and one-CPU/2-GiB RustFS backend, with separate voter
disks and cgroups in one physical failure domain. Records are retained under
`linux-current-build-acb847506c44-372b4822` and
`linux-daemon-acb847506c44-372b4822-standby`.

The earlier interrupted Linux run and the source-`f1500526` maintenance-resume
failure remain unchanged. A passing fresh fixture does not resolve that original
unknown mutation. Replacement still requires the existing 15-GiB parent guard.
The upstream base has advanced again to fetched `d46ecc1d`, adding autonomous
binding recovery; that implementation is not integrated or qualified by these
records. Occupancy 128, remaining CI failures, authority and physical-owner cuts,
release-scale placement/fairness, full primitive/provider coverage and the final
single-source release matrix remain open. No complete CG-13, CG-17 or OPT package
is accepted by this checkpoint.

## Single archive-intent owner and connected binding recovery integration

Correction `f40c5d39`, input digest `ca0d4e85`, passes strict all-target Clippy
for `cellar` and `cellar-logic`, all 44 design tests, 113 tool tests (one optional
skip), formatting, layout, dependency, docs and patch checks. All 23 ordinary
controller tests, 32 archive-protocol tests and five selected execution tests
pass on three retained Rust 1.98.1/profile-1 executables. The intent helper is
loaded once by the shared runtime fixture and reused by its controller and root
protocol callers; its durable file/write/readback behavior is unchanged. The
first successor failed an ordinary unresolved parent import, retained under
`archive-helper-single-owner`. The corrected source and passing run are retained
under `archive-helper-single-owner-import`. These passes resolve the helper and
reviewed-anchor checks; they do not resolve the earlier occupancy-128 failure.

Merge `521fb92c` integrates upstream `b0356fea`, including the connected
source-claim, authenticated target-retirement and source-lane settlement owner.
The routing integration preserves the existing typed selection errors: terminal
refusals stop selection, stale physical placement refreshes only the selected
route within the original session, and bounded retries retain the exact target
history and request. The receiver keeps its existing boxed future boundary while
adding the private authenticated retirement purpose. No compiler limit, request
window or resource ceiling is enlarged.

The standard runner now has a `named-bindings` suite for the ordinary and
interrupted/restarted binding cases. Each case must emit four distinct completed
image/source-durability profiles with its own recovery flag. The runner retains
hashed ledgers and refuses missing, duplicated, foreign, legacy or failed profile
output. Process-isolated keep-going can collect the other case while retaining
the campaign failure. This covers a Quorum target minimum, rather than the whole
HTTP/RPC family or target-S3, cross-class, concurrent-creation and disaster gates.

The reviewed successor baseline pins the merge and updates only the host anchor
and its scheduler comment. Binding classes require bounded shared recovery even
without optional user alarm/outbox scheduling; startup validates the existing
bounds. Source metadata discovery excludes stored business arguments. The new
class-schema seed and committed call due time require fresh matching namespaces
and binaries; markers remain 1 and no legacy reader is introduced. This records
source review and integration. Runtime qualification of this combined recovery
source is still pending, and all complete CG/OPT acceptance remains open.

## Binding allocator and joined-close follow-up

The published transport repair `87d42d77` sends private target retirement through
`RaftTransport`'s shared state, as command and name calls already do. The first
combined pre-publication check at `8272ead9` emitted E0599 for the missing method.
That owned compiler was stopped after the deterministic diagnostic; its terminal
return 101 and interruption reason remain under `pr167-prepublish-8272ead9`.
This was not a compiler panic. Fresh strict checks of the repaired source use the
same pinned toolchain/profile under `pr167-prepublish-87d42d77`; they remain in
progress at this checkpoint.

Reviewed merge `7192455f` integrates base `90e19b34`. Binding maintenance now reads
only its fixed-size system allocator through the existing SQL query owner and a
linearizable exact route/history check. It takes no command-byte reservation or
pending-effect payload; transient contention repeats only that read within its
original maintenance deadline. Retirement and allocator errors preserve their
causes. Shutdown seals admission and joins controller/SQL/runtime owners before
sampling producer quiescence. Failed joins, poisoned admission, retained responses
and unresolved owners still withhold the proof. This neither resolves a lost
business response nor authorizes deletion of old disks.

All 44 design tests pass on the merge with every reviewed anchor unchanged.
New allocator tests deny pending-payload reads and refuse malformed/exhausted
counters without resetting them. Native execution and real-backend recovery on
this combined source remain pending. The next native selection covers query-wire
separately from transport, chooses the actual Raft receipt owner by a unique test
name and requires four profile records for each ordinary/recovery binding case.
The original occupancy-128 failure, source-bound RustFS/Linux checkpoints and all
remaining CG/OPT gates retain their recorded scopes.

Hosted predecessor CI logs confirm both JavaScript worker adapters fail at
`alice/bounds` with an ambiguous outcome (14 other worker tests pass), while the
Typed RPC 3.14 full job reports incomplete host shutdown in the Rust/Rust ABA
peer-cycle case. These failures need their own reproductions; current-source
native Raft tests alone cannot qualify them.

## Typed SQL cursor admission follow-up

On clean `ee8e23e3`, the unchanged Node Workers storage/recovery case reproduces
hosted CI's `alice/bounds` ambiguous failure with Node 24.22.2 and Rust 1.98.1.
Its retained executable uses the CI test opt-level 0, zero debug info, assertions
and overflow checks enabled. The build completes in 753.507 seconds and the selected
case exits 101 in 5.109 seconds. The same binary also fails a stronger fixture-only
negative control at native cursor admission provenance in 3.605 seconds.
Records are `baseline-workers-cursor` and `fixture-negative-control` under the
isolated checkout's `cellar-tikv-cursor-refusal-6aff/qualification` target.

The fixture assumed 128 retained cursors while the native policy permits 64 live
statements. The guest checks its own 128-item allowance; the native SDK's string
conversion discarded the distinction between refusing before SQL execution and
failing after a statement or prefix ran. The candidate preserves a typed native
`SqlCursorRefusal` for the initial count and SQL-text byte checks only. The guest
attaches pre-effect validation only at that direct operation boundary. Failures
inside preparation, binding, prefix execution, row/result copying or buffer
accounting remain unproved outcomes. The public low-level string-returning cursor
entry point retains its signature. No cursor count, byte cap or request budget
is increased, and a refusal says nothing about earlier enclosing operations.

The fixture saturates the effective policy and reports its actual capacity,
checked against the configured native limit by the Rust host. A separate smaller
adapter capacity checks the other owner and repeated release. Shared native/Raft
SDK assertions retain quota text checks, require exact typed initial refusals,
prove rejected prefixes did not execute and require an executed prefix to survive
a later quota failure without acquiring the refusal type. The guest buffer oracle
also checks a real written prefix before cleanup.

On `e114b360`, frozen source `32ffe248`, all 18 combined Node/V8 worker cases
pass, as do all nine Node-only cases; the Node-only dependency graph excludes
V8. The native storage case and all eight ordinary Raft primitive cases pass
(eight RustFS cases are ignored), including independently checked four-profile
ordinary and interrupted-recovery binding ledgers. Strict all-target Clippy for
the affected core, logic, guest and JavaScript adapters, formatting, layout,
dependency, documentation, design and fixture checks pass. Records are
`typed-cursor-workers-with-bundles`, `typed-cursor-workers-node-only` and
`typed-cursor-sdk-and-clippy-with-bundles` under the same qualification target.
These checks use Rust 1.98.1, Node 24.22.2 and CI test opt-level 0; they do not
replace the profile-1 performance and backend matrix. The two failed controls
remain failed. This compatibility fix grants no full CG/OPT acceptance or
performance claim.

The next integration includes base `d290719c` and its bounded completed archive
verification handoff. Both test-helper exports retain the idle-peer regression
and the late-receipt regression. Qualification of the combined source remains
separate from the completed cursor-fix checks above.

## Immutable manifest pack candidate

The native profile-1 negative control at `3a8ed804`, source `0c4e8c34`, builds
in 576.019 seconds and fails the changed-path test's new one-file-barrier oracle
in 0.336 seconds. The original independently sealed 64-image and authenticated
old/new-root assertions remain intact. The observed second insertion completes
two file barriers instead of one; this is evidence of persistence amplification,
not a measured explanation of the entire occupancy-128 failure.

The [manifest pack candidate](manifest-page-packs.md) streams each immutable AVL
update into one bounded file, retains logical page hashes and leases, accounts
for resident encoded containers and compacts within existing page/byte quotas.
Actual source unlink and directory barriers precede credit. Node maintenance
counts physical file deletions and shares a 96-record relocation budget. The
local marker-1 layout is a hard cutover; old page directories are refused.

The core compile check passes. A fixture check initially rejects the new test's
`GroupLog` generic scope; correcting it to the existing native engine fixture's
scope makes the storage, snapshot and execution fixture checks pass. New owning
tests and the full native profile-1 storage/runtime/backend/container gates still
need execution. Earlier occupancy-128 and original CI/compiler failures retain
their recorded outcomes. No performance or full CG/OPT acceptance follows from
these compile checks.

The first complete all-target check at `f1dd6a1a`, source `d7b1fbae`, rejects
the archive/disaster futures: retaining a Send-only host writer made the page
store lose its existing `Sync` contract. The follow-up contains that writer in a
mutex accessed only through exclusive ownership on blocking workers. It also
corrects the independent fixture's logical-page hash to use the established
domain prefix. A retained original occupancy-128 page independently confirms
that domain-prefixed SHA-256 matches its filename and plain SHA-256 does not.
Production page hashes remain unchanged. The first candidate's native runtime
and remaining checks retain their own frozen source; corrected-source runtime
and full all-target validation remain pending.
The corrected-source default RustFS archive suite also requires the new
late-receipt verification-handoff regression, bringing its fixed case set to 25.
The preceding source's 24-case backend evidence retains its original scope.

The retained first native candidate at `f1dd6a1a`, source `d7b1fbae`, builds in
669.528 seconds with verified Rust 1.98.1/profile-1 executable hashes. Its full
owning storage run completes with 19 passed, three failed and two harness-child
cases ignored. The changed-path 64-image test now passes the single-file-barrier
oracle, as do every preceding storage, quota, held-reader, engine-publication and
process-crash case and the new malformed-framing case. The three new compaction
cases fail at the independent helper's plain-SHA condition before reaching their
compaction oracles. This candidate remains failed; corrected-source evidence
must establish the actual physical-copy and held-root contracts.

The follow-up also preserves the existing page-scan unit and ceiling. A bounded
per-pack cursor resumes record inspection across maintenance calls; a new pin
marks its page live if it arrives after an earlier scan step. Physical unlinks
retain their separate file budget. The owning compaction test now requires two
one-record turns without page reads, barriers or credit. It admits a previously
unpinned reader, then
requires the final one-record turn to preserve both real roots during relocation.

At `04e70637`, frozen source `f4bf00d7`, the complete native profile-1 storage
suite passes: 22 cases, zero failures and two harness children ignored by direct
selection (the parent crash cases launch them). Build time is 592.065 seconds;
runtime is 11.443 seconds. Compiler and executable hashes and unchanged inputs
are verified. This includes all four new compaction/framing contracts, the late
pin between bounded scan turns, all original storage/crash assertions and the
single-file-barrier oracle. Strict native core/logic all-target Clippy also passes
in 924.883 seconds. Other repository checks pass except formatting of the new
compaction assertions; that campaign remains failed for formatting, with the
runtime and Clippy results retained separately. The follow-up formats the file.

The follow-up expands the existing process-kill matrix from 30
to 52 cuts: 26 boundaries in each image mode. It retains all original cuts and
adds full-frame writes and both sides of pack rename, plus eight compaction
copy/publication/unlink boundaries. The parent independently authenticates the
replacement container or its written prefix, requires both source and copy
before unlink and requires selected-root recovery from the copy after unlink.
Every cut still verifies exact recovered SQL/receipts and one durable callback
execution. Per-boundary mode/cut evidence is emitted only after those checks.
This expanded matrix needs its own runtime evidence; no broader lifecycle,
backend, occupancy-128 or full CG/OPT acceptance is inherited.

Sealed pack indexes additionally retain only their actual record count in boxed
slices. The writer's maximum 96-record allocation is temporary, including for
one-page checkpoint imports; the unchanged parent admission now charges the
bounded writer/sealed-index overlap. Current-source runtime and all-target checks
must include this residency correction as well as the expanded crash matrix.

At `b92843bd`, frozen source `ed7a05fe`, the full native profile-1 storage suite
again passes all 22 cases (two directly ignored harness children). The retained
driver independently verifies all 52 distinct mode/boundary evidence rows, 26 per
image mode, including the actual replacement/source container checks. Build time
is 393.879 seconds and runtime 20.486 seconds, with compiler/executable hashes
and unchanged inputs verified. The all-target campaign fails only at its new
`println!` evidence call, which violates the repository's forbidden-macro policy;
format, layout, dependencies, documentation, design, tooling and patch checks
pass. The follow-up uses the existing `cellar::note!` output helper. This failed
Clippy record remains failed, and the queued broader native campaign records
`NOT_RUN_PARENT_FAILED`; it performs no runtime tests. The corrected output needs
its own full storage/52-row and all-target checks before that broader campaign.

## Packed manifest native qualification and backend follow-up

At `6b883dd0`, frozen source `3c86d6fa`, the complete owning storage suite
passes all 22 cases and its retained driver verifies all 52 distinct process-kill
boundaries, 26 per image mode. The two directly ignored harness children are
launched by their parent cases. Runtime is 20.839 seconds after a 10.029-second
build. Strict core/logic all-target Clippy, format, layout, native dependency
boundaries, documentation, 44 design tests, 120 tooling tests (one optional skip)
and patch checks pass on the same frozen inputs. No lint or assertion is weakened.

The broader native profile-1 campaign builds and retains seven executables in
954.663 seconds, independently verifying executable hashes, the pinned Rust
1.98.1 compiler, original assertion/overflow settings and unchanged source.
All 389 selected cases pass: 126 component cases, five ordinary snapshot cases,
193 ordinary execution cases, 24 ordinary archive-controller cases, 32 ordinary
archive-protocol cases, eight ordinary primitive cases and the original
occupancy-128 diagnostic. Both named-binding cases retain all four required
image/source-durability profiles; archive fairness retains both image profiles.
Ignored real-backend entries do not become native coverage.

The original occupancy-128 case completes in 92.055 seconds, including its
commands, follower reads, exact receipts, archives and joined close. Enrollment
of the 129-cell population completes in 27.445 seconds within its unchanged
60-second budget. Its observations and post-close encoded pack inventory are
retained with the actual live process, source and executable binding. Earlier
failed occupancy-128 runs remain failed. Another checkout's compiler is active
at the start of this run; neither this single functional pass nor its difference
from an earlier failure establishes a controlled speedup. Repeated equivalent
workload measurements at occupancies 1/8/32/64/128 remain required.

The first current-source real RustFS campaign fails after six passing archive
cases. Its remote-deadline case returns before the required enrollment S3-wait
state is observed, with the original 800 ms invocation deadline. The remaining
18 archive cases are not run. The separate two-case binding campaign passes
interrupted recovery with all four profiles, but ordinary bindings fail in the
materialized/S3-source profile at command sequence three with an ambiguous
`Runtime(DeadlineExceeded)`; its other three profiles pass. These are failures,
not a complete 25-plus-two backend pass.

Three independent fresh-namespace reproductions of the remote-deadline case
then pass on the exact same retained executable and original limits, in 31.318,
34.122 and 37.092 seconds, while the actual matching Linux builder remains live.
They do not reproduce the failure or establish its cause. The full standard
25-case archive and two-case binding suites are queued to repeat after the
owned Linux build and focused reproductions actually finish, with original
failures preserved and external host compiler activity observed separately.
Matching constrained Linux execution requires those complete backend suites to
pass. A successful image build alone supplies no runtime evidence.

The repeat after the owned build finishes passes the previously failing remote
deadline case, then fails in `rustfs_s3_remote_slow_delivery_retains_proofs_and_bounds`
with an unlocalized elapsed deadline after 41.21 seconds. Seven archive cases
pass; 17 are not run. That full archive campaign remains failed. Both named-binding
cases pass on this repeat in 160.546 seconds, each with all four required profiles
and the exact retained native executable. This does not erase the first ordinary
binding failure. Matching Linux image
`sha256:8ce943291bdcdc8e21bd8b3ec8c99f5b440dedcc324c516ed2713a314d8f6bcb`
builds under the original parent limits; both queued runtime pipelines record
`NOT_RUN_PREREQUISITE_FAILED` because their full archive prerequisite fails.

Three fresh-namespace slow-delivery reproductions, with Rust/anyhow backtraces
enabled and the owned build finished, pass in 68.859, 61.793 and 57.878 seconds.
They use the unchanged source and executable, original wire deadlines and S3
envelope, and the standard runner's signed backend checks. The failure remains
unlocalized; these focused passes do not replace the failed full archive suite.

A subsequent public test candidate adds
`recent_source_expiry_keeps_physical_image` through SDK, native HTTP and Axum in
both image modes. It configures a 500 ms source-observation lifetime within the
original five-second invocation and callback bounds. One selected follower's
actual SQL callback holds its immutable image; the invocation must refuse with
`VersionUnavailable` before its outer deadline, retain its read pin and ordinary
admission after refusal, and release them only after physical SQL exits. A new
S3 command and fresh source read verify the subsequent image under the same term,
without a ReadIndex barrier or historical fallback. This candidate still needs
compilation and execution; no new runtime acceptance is claimed for it.

Public wire-authority and ambiguous-response cuts, physical-owner and repeated
movement/settlement qualification, broader fairness and placement measurements,
failed-voter replacement/all-disk-loss, full SDK/provider compatibility and the
single-source release matrix remain open. These checkpoints accept no complete
CG-13, CG-17 or OPT-01 through OPT-08 package.

## Public source expiry and matching packed runtime

At test-only follow-up `3261b2fe`, source `d572b657`, the retained native
profile-1 build completes in 1056.624 seconds. The new public expiry case passes
in 25.678 seconds, with all six SDK/native HTTP/Axum and image-mode profiles
independently checked. Each returns `VersionUnavailable` in 308–464 ms within
its original five-second request budget, retains one physical image pin and one
ordinary admission while SQL remains held, then releases both after physical
completion. The next S3 command and fresh query preserve the exact cell result,
version and receipt without a ReadIndex barrier or historical fallback. The
existing preference case passes in 39.328 seconds. Strict core/logic all-target
Clippy passes in 1121.486 seconds; all eight source/static checks pass.

That first campaign remains failed: the cohort case times out in its prerequisite
`first-group standby` CLI call after 5.971 seconds, before any cohort assertion.
Its original five-second CLI limit and retained operator directory remain intact.
A fresh-process follow-up explicitly retains and selects the matching CLI,
verifies both compiler/executable identities and reuses only those unchanged-input
static checks. Expiry and preferences pass in 23.211 and 42.237 seconds; three
complete independent cohort attempts pass in 42.088, 42.163 and 51.250 seconds.
All six expiry profiles are checked again. The failed CLI attempt is not relabeled
or attributed to the executable path. Hashing/copying observes executable file
data; these passes are not cold-start or controlled speedup qualification.

The complete standard RustFS campaign at packed-runtime checkpoint `6b883dd0`,
source `3c86d6fa`, then passes all 25 archive cases in 215.534 seconds and both
four-profile binding cases in 147.798 seconds. It retains the exact native
executables, signed backend probes, original deadlines, original backend caps
and frozen inputs. Rust/anyhow backtraces are enabled for diagnosis. The first
two failed campaigns and all focused repetitions retain their original outcomes;
their transient deadline causes remain unresolved.

Matching Linux image
`sha256:8ce943291bdcdc8e21bd8b3ec8c99f5b440dedcc324c516ed2713a314d8f6bcb`
passes the complete existing daemon campaign with standby, native/Axum ingress
and real RustFS in both image modes in 203.688 seconds. Both daemon and CLI ELF
binaries have the pinned Rust 1.98.1 compiler identity. Materialized and extent
fixtures exit zero in 86.198 and 95.288 seconds. The original six-CPU/14-GiB VM,
one-CPU/3-GiB/no-extra-swap voters and one-CPU/2-GiB backend remain enforced.
Independent inspection verifies all 12 retained voter-log hashes, run/image
identities, voter caps and no OOM. Post-teardown exits include intentional fault
stops; this inventory is not proof of graceful close for every file or container.

These are distinct source checkpoints: the new read qualification is a test-only
follow-up, while the full native/storage/backend/Linux packing evidence is bound
to `6b883dd0`. The subsequently fetched base `7c893506` adds grouped SQL bindings
and RPC error compatibility changes. Their integration needs its own checks and
cannot inherit these broader combined-source results. Failed-voter replacement,
independent domains, physical hard-storage bounds, repeated performance, full
primitive/provider compatibility and all remaining CG/OPT acceptance stay open.

## Grouped SQL integration and operator certificate I/O

Combined source at `ca5f92df`, digest `296e79b9`, integrates base `7c893506`
with packed manifests and the public source-expiry regression. Its native
profile-1 campaign passes 51 selected cases: two SQL, 15 Raft host, 22 storage,
nine ordinary primitives, two RPC reply-envelope and one public cohort case.
An independent audit verifies all 52 storage crash cuts on its retained
executable. All eight static/source checks pass, including strict core/logic
all-target Clippy. The build and checks preserve the pinned compiler, original
assertions, limits and frozen inputs.

That campaign remains failed. Expiry and preferences stop in their prerequisite
five-second standby CLI call, before their proof oracles, in 37.384 and 7.422
seconds overall. Cohorts pass in 47.234 seconds. A later diagnostic repeats the
exact preference executable and matching retained CLI in three fresh processes:
all pass in 32.993, 34.823 and 34.939 seconds. External monitoring observes no CLI
child long enough to trigger its 2.5-second sampling threshold. These repetitions
do not identify a cause or replace the failed campaign.

The actual compiled dependency graph enables both native and bundled certificate
roots in reqwest. Five alternating isolated construction probes observe native
root loading at 139–176 ms versus 43–48 microseconds with only native loading
disabled. These contextual component measurements are not a workload speedup or
cold-start qualification.

A separate induced control runs the actual retained operator CLI against an
independent loopback HTTP standby service. An empty regular certificate file
allows all three requests in 10–12 ms. Replacing only that file with an owned
unwritten FIFO blocks all three invocations until their original five-second
deadline; none reaches the HTTP service. Process samples locate the wait in
reqwest client construction, native certificate loading and file open. This
establishes an unnecessary certificate-store I/O dependency for HTTP. It does
not establish why the original, uninstrumented standby calls timed out.

The follow-up disables built-in certificate acquisition for HTTP management
URLs and retains it for HTTPS. Redirect refusal remains in force. Three actual
CLI regressions cover the blocked certificate source, explicit HTTPS native-root
trust and rejection of an untrusted HTTPS server. The separate test-only source
`35cc0f95`, digest
`76ec66fe`, passes its native faulty-wire advice case in 87.586 seconds. Its
independently checked 60 rows cover ten advice/authentication faults across SDK,
native HTTP and Axum in both image forms, with no SQL entry during refusal and
once-only S3 replay under the original command identity. Its strict all-target
Clippy check fails because the new responder uses the forbidden Tokio select
macro. The follow-up switches both new fixtures to Cellar's fair polling macro;
the failed lint record remains failed.

The combined checkpoint `89ad604c`, source digest `ec6f482f`, passes its complete
selected native campaign with frozen inputs. All six CLI cases pass in 17.576
seconds, including the three certificate controls. Faulty-wire advice passes in
80.740 seconds, with 60 independently checked observations covering all ten
faults, three ingress paths and both image forms. Entered-source expiry passes in
14.331 seconds with all six profiles independently checked; preferences and the
fresh-barrier cohort pass in 27.787 and 28.276 seconds. These are ten test cases,
not 60 separate test functions. Strict core/logic all-target Clippy and all seven
other static/source checks pass. Retained executable hashes, compiler identity,
native profile-1 settings, source inputs and evidence ledgers are recorded under
`qualification/cli-http-roots-fair-advice` in this checkout's target directory.
The original five-second deadlines, assertions and resource limits are retained.
This scoped pass does not explain or replace the earlier startup failures and
does not qualify a later base integration, current-source RustFS/Linux or
controlled performance.

Removed-member, untrusted-certificate and superseded-boot
coverage remains open. The preceding RustFS/Linux results remain tied to
their exact revisions; no full RO-03, CG-13, CG-17 or OPT package is accepted.

The subsequent PR integration includes base `ab06b48a`, which adds stateless SQL
ingress and reuse of locally applied binding histories. The counter registry
retains both branches' counters, and the baseline retains both review histories;
all anchored file hashes still match the reviewed `89ad604c` checkpoint. Runtime
results above remain bound to their stated revisions.

Combined checkpoint `8d578fba`, source digest `837c0127`, passes all 18 selected
native cases: five performance-registry, two native SQL, one public SQL-binding,
six CLI and four public routing/read cases. SQL bindings cover both image modes
in 113.549 seconds; CLI cases pass in 18.004 seconds. Faulty-wire advice passes
in 86.093 seconds with all 60 observations independently checked. Entered-source
expiry passes in 15.086 seconds with all six profiles independently checked;
preferences and the cohort pass in 27.932 and 26.506 seconds. Strict core/logic
all-target Clippy passes in 1208.402 seconds, and all seven other static/source
checks pass. The build preserves native profile-1 settings, the pinned compiler,
original deadlines and assertions. Seven executable identities, their original
Cargo artifacts, source inputs and both evidence ledgers are retained under
`qualification/pr167-sql-ingress-base-ab06b48a`; its source/HEAD/clean-tree guard
remains unchanged through completion.

This qualifies the selected native integration. Earlier failed records remain
failed, and the original uninstrumented standby timeouts remain unexplained.
Current-source RustFS/Linux, broader native/storage/primitive coverage, measured
performance and full CG/OPT acceptance remain open. The base's recorded RustFS
interrupted-binding failures are not resolved by these native results.

## Certificate and actual-boot wire authority

Checkpoint `0a923923`, source `f43eecff`, passes all twelve selected native cases
and all eight static/source checks. The actual untrusted-certificate case passes
in 12.960 seconds with all six SDK/native HTTP/Axum image profiles independently
checked. Each cached trusted connection is joined before changing certificates;
the real client sends `UnknownCA`, no RPC or SQL producer enters, and restored
trust and permission reuse the original command once with S3 proof. The wire
case passes in 83.081 seconds with all sixty captured request/reply observations
checked against the stricter framing, correlation and authenticator ledger.
Native profile-1 settings, original five-second windows, executable identities
and frozen inputs are retained at `qualification/untrusted-tls-advice-native`.
These runs use in-memory object storage, not a current RustFS/Linux deployment.

Checkpoint `26b045da`, source `3f38fcd6`, passes all thirteen selected native
cases, fresh strict core/logic all-target Clippy, and all seven same-source
repository checks. The latter are reused only after verifying the preflight's
exact HEAD, complete source manifest, clean tree and retained record hash. The
actual issuer-restart case passes in 39.709 seconds across six fresh caller/image
fixtures. Its controlled call/boot changes take 314–601 ms within the original
five-second budget. Public inspection verifies a successor physical boot on the
same engine; all old tails are written to their owned TLS streams after current
trust and a genuine owner query are installed. This records local writes, not a
claim that the client decoded the old reply. No SQL/callback or non-commit proof
is admitted, ambiguity remains, and both the first recovered S3 command and exact
replay use one cached RPC. The matching wire, certificate, origin-revocation,
read-expiry, preference, cohort and CLI controls pass on the same retained
executables. Records are under `qualification/superseded-boot-advice-native`.

The first boot-source preflight fails at the evidence JSON macro's recursion
limit; the next fails at an invalid SDK query variant. Both remain failed with
their seven passing static checks. The corrected checkpoint uses smaller evidence
objects and the existing `Linearizable` API; no request bound or assertion is
relaxed. These are scoped native authority checkpoints, not full RO-03, CG or
OPT acceptance.

The subsequent integration includes base `55fff63a`, with bounded stateless SQL
identities and nonce-bearing RPC envelopes. Its changed production source needs
new combined validation; it cannot inherit the preceding checkpoints. Actual
combined authority coverage, current-source RustFS/Linux, broader native and
primitive/provider coverage, physical-owner accounting and performance gates
remain open.

## Removed-member wire authority checkpoint

Checkpoint `543634b8`, source `2df204aa`, passes the focused native
`removed_member_advice_cannot_grant_authority` case in 62.656 seconds. The
independent ledger checks all six SDK/native HTTP/Axum image profiles. Each
fixture actually replaces and retires an original voter, moves leadership to
member 5, verifies physical inventory exclusion and establishes a successful
linearizable query through the new voter before injecting authenticated advice
that names the removed member. The command enters no SQL producer; its complete
pre-admission refusal preserves a known non-commit outcome. Restored permission
reuses the original command identity once with S3 proof, with unchanged current
membership, term and logical range.

The first candidate `0ce7cad1`, source `aa899b54`, passes its source preflight but
fails its final replay assertion; an unchanged repetition reproduces that
failure. A focused probe shows that both tokens authenticate the same canonical
version and witness, all receipt/callback invariants hold, and only token bytes
differ. Tokens include a fresh issue time on each reply. The corrected oracle
authenticates both tokens and checks their decoded version and witness exactly,
while retaining command identity, result, durability, coverage and callback
assertions. Two diagnostic attempts fail earlier with `StalePeer`; those remain
separate setup failures. The corrected fixture uses the existing current-trust
inspection barrier before membership inspection, without claiming that this
establishes the cause of those earlier failures.

The frozen source, executable identities and six captured wire observations are
retained under `qualification/removed-member-authenticated-replay`; the evidence
file SHA-256 is
`09adeb1943ca02cfd3db3fa0acd91e7030369f6ccfdb3a3fa09d9f1b8900acb6`.
This uses in-memory object storage and the original request/resource bounds.
It does not qualify current-base integration, strict Clippy, RustFS/Linux,
full RO-03, CG-13, CG-17 or OPT-01–08.

## Physical archive reader cancellation candidate

The next RL-02 component regression holds an actual immutable image/extent read
inside checkpoint upload, then drops its asynchronous caller. It advances every
populated cell through the existing Raft/apply fixture and attempts bounded
physical collection. Source and archive reservations must remain charged, and
the old manifest page and physical image bytes must remain exactly unchanged,
until the physical file reader exits. Only then may collection remove the old
closure. A fresh upload and independent receiver must verify the current cut.

The ordinary `archive_cancelled_waiter_keeps_physical_checkpoint` case and its
ignored RustFS counterpart cover both image modes. The new fixed
`archive-owners` suite requires both hash/owner ledgers and retains their actual
runtime provenance. The existing checkpoint fixture accepts an explicit image
filesystem for this cut; its ordinary callers preserve their existing adapter.
No request or parent resource ceiling changes.

This candidate still requires compilation and runtime validation. Synthetic
verifier controls prove only evidence refusal. Other reader/snapshot/transfer
cuts, Fleet settlement, allocated-block accounting, constrained voters and full
RL-02/CG/OPT acceptance remain open.
