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
| OPT-02 | Interval-proof/generation candidate below; coalesced bounded demand refresh and public transition/version qualification for RO-05/06/09 remain open |
| OPT-03 | Automatic consistency-aware selection and bounded fallback candidate below; public network partition/RecentSource expiry and full RO-10 acceptance remain open; the public RO-11 cohort oracle passes at its source checkpoint below |
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
