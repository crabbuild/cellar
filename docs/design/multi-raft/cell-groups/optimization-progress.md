<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# TiKV-informed optimization implementation

Purpose: Track implementation and validation against the
[optimization design](tikv-informed-optimization.md), independently of historical
CG acceptance and release qualification.

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
| OPT-03 | Consistency-aware replica ranking and fresh bounded ReadIndex cohorts; RO-10/11 |
| OPT-04 | Physical shared-file/segment release proof and once-only accounting qualification; RL-01 through RL-05 |
| OPT-05 | Load telemetry, reserve-aware placement and repeated release-scale movement; RL-06 |
| OPT-06 | Measured scheduling fairness, batching and queue isolation; RL-07 |
| OPT-07 | Optimized wake/transition/recovery routing; RL-08 and retained CG-13 gates |
| OPT-08 | Combined-source public primitive, RustFS, constrained-process, fault and performance qualification |

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
cgroup v2, the eight-CPU VM and its workspace backing file. This is backend and
environment evidence; no Cellar node container qualification is claimed yet.
Credentials and generated runtime state stay outside the repository in the
checkout-private qualification directory.

The managed daemon runner previously hardcoded the default profile/context. Its
reviewed change binds Docker calls to the owned backend's exact profile/context,
accepts an explicit existing Colima home, binds the corresponding Lima home for
guest calls and records the Engine identity. Three configuration/routing tests
passed; the local multi-Raft Python suite passed 31 tests with its one opt-in
Docker configuration case skipped. Review found no change to voter/backend
limits, workload assertions, source-image checks, cleanup inventory or request
budgets. The plan checker's pinned recovery anchor correctly reports this source
drift (41 passes, one preflight error). After the source checkpoint its reviewed
revision, digest/excerpt and review note must be advanced together; the guard is
not being weakened. The Rust 1.98.1 Linux builder is present in the verified VM.
A matching source image and three constrained voters remain pending. Local
container runs do not establish independent host/zone resilience or release
acceptance.

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
