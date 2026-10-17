# AGENTS.md

Repository guidance for Cellar. Check for a more specific `AGENTS.md` before
working in a subtree. Maintain this file when architecture or verification
commands change; `CLAUDE.md` links here so both tools use the same guidance.

## Start here

Read [Rust quality and module layout](#rust-quality-and-module-layout), the
[repository map](#repository-map), and [ownership boundaries](#ownership-boundaries)
before editing Rust. Use [build and validation](#build-and-validation) for the
checkout-specific target directory and required commands. Read the affected
component, its callers and tests before deciding where a change belongs.
The detailed runtime and recovery contracts below apply when touching those
surfaces; file organization must preserve their behavior and public Rust paths.

## Rust quality and module layout

These are Cellar's repository conventions. Optimize for cohesive responsibilities,
explicit ownership, small reviewable changes, and code whose invariants can be
understood without following unrelated modules.

### Choose one module shape

| Responsibility | Layout | Test placement |
| --- | --- | --- |
| Leaf module | `feature.rs` | Small private `#[cfg(test)] mod tests { ... }` in the same file |
| Leaf with substantial private tests | `feature.rs` and sibling `feature_tests.rs` | Declare `#[cfg(test)] #[path = "feature_tests.rs"] mod tests;` in `feature.rs` |
| Component with production submodules | `feature/mod.rs` plus named implementation files | Private `tests.rs` declared by `mod.rs`, or tests beside the child they exercise |
| Public contract spanning modules | Crate `tests/<behavior>.rs` | Use public APIs; share fixtures under `tests/support/` |

Never combine `feature.rs` with a `feature/` directory containing Rust files.
In particular, do not create `feature/tests.rs` beside `feature.rs` just to move
tests out of sight. Substantial tests alone do not justify a component directory.
For example, node driver tests live in `node/driver_tests.rs`; tail transport
implementation and progress reporting live in `durability/log/tail_stream/`
with `mod.rs`, `progress.rs`, and `tests.rs`.

`cargo check-layout` enforces the file/directory rule across tracked and unignored
new Rust sources, including examples and tooling. CI runs it. Small test modules
can remain inline; do not extract tests merely to satisfy a line-count target.

### Keep components cohesive

- Choose boundaries by responsibility and invariants, rather than arbitrary file
  size. Split a large module when a distinct parser, policy, store, transport, or
  lifecycle can have a clear contract. Do not create catch-all `utils`, `helpers`,
  `common`, or `types` modules for unrelated features.
- A component entry point declares its children and presents its cohesive API.
  Existing implementation may remain there; extract named responsibilities as
  they change rather than replacing one large file with arbitrary fragments.
- Keep implementation modules private by default. Use `pub(super)` or
  `pub(crate)` only for the callers that need access; explicitly re-export the
  intended API. Do not expose implementation details just so tests can use them.
- Use ordinary `mod child;` resolution within components. Existing crate-root
  `#[path]` mappings preserve Cellar's public API; update them when moving files.
  Sibling private test files may use `#[path]`. Do not add path indirection where
  the normal module tree suffices or introduce another flattened public API.
- Keep private unit tests in their owner's logical module with `use super::*`
  when useful. Production imports and re-exports should be explicit; preserve
  existing public re-exports during layout-only changes.
- Keep fixtures and schemas beside their owner. On a move, check `include_str!`,
  `include_bytes!`, Cargo targets, conditional modules, macros, and documentation
  links. Physical moves must preserve logical paths, visibility and `cfg` gates.

### Write idiomatic, explicit Rust

- Model meaningful states with enums and domain types. Prefer exhaustive matching
  and constructors that establish invariants to boolean combinations, sentinel
  values, or repeated validation scattered across callers.
- Borrow when ownership is unnecessary. Clone when a real independent owner is
  required; avoid cloning buffers, allocating strings, or introducing `Arc<Mutex<_>>`
  merely to work around a poorly chosen interface. Keep lock scopes short and
  synchronous guards out of `.await` points.
- Return `Result` for recoverable failures and `Option` for genuine absence.
  Preserve typed errors when callers need to distinguish refusal, fencing,
  ambiguity or cancellation; add useful context at boundaries without losing
  the source. Never turn malformed or unavailable state into success via defaults.
- `unwrap`, `expect`, indexing and assertions in production require a local,
  established invariant. Validate untrusted input and use checked conversions
  and arithmetic for sizes, offsets and counts. Assertions and `unwrap` are fine
  in tests when failure is the intended test failure.
- Make async cancellation, task ownership and cleanup explicit. Use injected
  execution-domain services and bounded work; keep blocking I/O off async workers.
  Preserve resource cleanup and joins on errors as well as success.
- Add abstractions for an actual contract or repeated responsibility. Prefer
  straightforward concrete code over speculative traits, generics, wrappers and
  configuration. Reuse existing types and dependencies before adding new ones.
- Document public contracts and non-obvious invariants, including error,
  cancellation and durability semantics when relevant. Comments explain why.
  Every new unsafe block needs a local `SAFETY:` explanation of its obligations.
- Run `cargo format`, `cargo check-layout`, relevant behavioral tests and Clippy
  for affected targets. Fix warnings; lint exceptions must be narrow and explain
  the boundary they serve. Never silence a failure or weaken a test to pass.

### Test behavior at its owning boundary

Cover normal behavior and meaningful failure/cancellation boundaries. Use unit
tests for private algorithms and state transitions, crate integration tests for
public contracts, and the existing native/fleet harnesses for ownership, recovery
and process behavior. Keep test names specific about the condition and outcome.
Use deterministic synchronization and injected clocks/services where available;
do not replace phase observations with sleeps or invent durability evidence.
Extend the nearest relevant suite instead of duplicating it in another layer.
For file-only moves, run the existing suites and verify their test discovery;
additional tests should cover new behavior, such as layout enforcement itself.

## Framework

Cellar is a Rust framework for applications composed of durable business cells.
Each cell has its own SQLite state. A conditional object-store write assigns
ownership to one node; nodes sharing a bucket form a fleet. The framework routes
calls, captures SQLite writes as LTX, proves durability, and manages alarms,
durable wakes, replication, recovery, and fleet shutdown.

Applications implement contracts from `cellar::sdk` and compile into native
executables with their application factories embedded. The `cellar` CLI builds,
develops, publishes, installs, and operates those applications. `cellar` is the
single application CLI executable; the optional `cellar-dashboard` service serves
the management console. A generic CLI binary cannot serve an application whose
factories were not compiled into it.

Deployment `stream_bindings` use `BindingConfig` selectors. Their `resource.id`
may be omitted for catalog name lookup; runtime `Binding`/outbox lanes always
retain resolved UUIDs. A private namespace/method-bound lookup reads an existing
active resource and validates current grants; it cannot provision or mutate.
The declaration travels in a bounded 128 KiB RPC body with a 64 KiB JSON cap,
not in the authority header. Catalog and partition identity/grants are checked
again before generation-local bindings pin once and admission opens. Name reuse
requires a new generation; old handles cannot emit or seal against successors.
All owned markers remain 1.

Event RPC host outcomes use `node/event_failure.rs` and the invocation RPC gate.
Pre-submission refusal/never-connected transport is `Unavailable`; fenced or
exhausted stale ownership is `OwnershipLost`; lost submitted results and unproven
post-execution output are `Ambiguous`. Authenticated peer host verdicts carry a
bounded header, with unknown/missing verdicts failing conservatively. Existing
DeliveryError and activation transport tags remain in the error chain. Typed
outcomes do not authorize ordinary RPC replay or prove a prior commit absent.
The tunnel pool uses Hyper try_send_request and retries only a returned unsent
request. Buffered bytes alone cannot authorize retry after response loss. Reuse
that exact returned request/body/headers; serialized failures surface as ambiguous.

Event delivery is under implementation; track the complete remaining scope in
`plans/outbox-implementation-status.md`. Native `streams` declarations and
`stream_bindings` are validated in `deployment/event_resources.rs` and require
`event-delivery-v1`. `LoadedDeployment::event_streams` and
`Generation::event_streams` carry desired configuration, not activation proof.
Node startup and adoption reconcile them through `node/event_resources.rs`,
verify current catalog revision/digest and independently resolve every binding
before event admission. Failed adoption preserves the serving generation, but
does not roll back resource changes already committed. Deployed event bindings
require a bucket-backed node or fleet with replication and durable wake discovery;
SQLite-only embedded nodes reject activation until a local recovery inventory
is implemented and qualified.
When continuing outbox/stream/subscription work, reassess critical bugs, missing
behavior and unqualified correctness boundaries at the start of each turn.
Prioritize loss, duplicate effects, fencing and recovery failures before API
polish or additional metrics. State the selected risk and its acceptance gate;
report what changed, exact verification and the highest remaining blocker.
Retained Storage handles must check the installed I/O context's domain as well
as activation identity. Ordinary access and commit recheck open-domain admission
inside an already-entered poll; forced closure cannot rely only on the next turn
to refuse new work. Preserve original-connection rollback/retirement cleanup
after closure. storage_authority_tests.rs covers wrong-domain access and closure
between business/outbox writes and commit. These local SDK regressions do not
qualify an old process still executing across full fleet takeover.
event_running_owner.rs exercises two embedded-node takeover cuts with a callback
still inside its native poll: before starting a transaction and after business/
outbox writes but before commit. Isolate only the old store client, observe
domain closure and a higher owner epoch, and audit original state before releasing
the callback. Its caught storage refusal must not become HTTP success. Preserve
the original producer revision/envelope and complete both node joins. These
independent-domain/executor cases are not separate-process or physical partition
qualification; do not relax their deadline or repair the prefix by command replay.
`live_owner.py` carries those source cuts into four independent processes. Its
opt-in owner_fault client refuses actual ordinary/CAS/list operations after a
private marker; CAS keeps the original no-retry client. It supplies no proof.
The private listing adapter caps its materialized inventory at 1000 objects;
this is not a general pagination implementation or capacity fixture. The native
callback publishes entered/attempted phase observations and remains in its poll
until a measured release, with the ordinary 10s handler policy unchanged.
Require lease expiry, live/non-stopped old PID, a higher-epoch successor and a
saved original-prefix audit before release. Require closed-domain refusal,
interrupted response, joined expiry fencing, immutable old DB/WAL inspection
and one consumer effect before complete survivor/store cleanup. Never use an
ordinary source replay or a signal to repair this boundary. Core runtime and
all owned markers remain unchanged; physical fault qualification is separate.
`consumer_owner.py` carries the inbox/effect cuts across four independent
processes using the same private store-outage control and actual SDK deliveries.
Keep the original effect audit before release/replay, a live/non-stopped old PID,
real expiry/higher epoch, separate acceptance/commit observations, interrupted
output and joined expiry fencing. The old joined SQLite/WAL archive must retain
only the first inbox digest/effect. Replaying the saved fetch ID must recover the
exact pending receipt/batch, apply once and skip repetition before ack. After
survivor joins, inspect the successor archive for both exact inbox digests/effects;
business totals alone cannot prove retained deduplication. Audit both archives
through disposable copies. Keep the 10s handler and 30s delivery policies, private
1000-object listing cap, Bucket proof scope and complete process/store cleanup.
The SQLite/RustFS process cuts are not physical or follower-only qualification.
`consumer_recovery.py` qualifies consumer inbox/effect/saved-receipt recovery
from actual Fleet proof after SIGKILL and complete consumer directory removal.
Keep consumer per-cell/session bucket inventory unchanged under the real replica
publication refusal; peers use Bucket proof. Audit the original committed prefix
before fetch/ack replay. Retain both event records and probe inbox acceptance
through the SDK in an always-rolled-back transaction; require both false so a
probe cannot repair a missing receipt. Require zero new effects on fetch replay
and archive exact inbox digests and receipt hashes after joins. Cold mode kills all original PIDs, retains exact
CLB1/CLE1 witness batches and restarts every peer with a fresh generation/retained
lineage. Its selective loopback gate refuses only the old consumer session's
seal requests. Require signed framework `Unavailable` before the original 10s
operation budget, unchanged consumer owner and an unsealed recovering log.
Returning real witness service must recover on the same PID/generation's first
fresh read. Never treat a generic application 500 or timeout as that verdict.
Reinspect all archives through copies and join every node/gate/private store.
Do not turn these retained-disk/client-service cuts into physical-loss claims.
Follower append acknowledgments require both the committed batch barrier and an
independent state.json base/end barrier. Batch-only end recovery cannot detect
deletion of a previously acknowledged suffix. Cold load checks contiguous batch
coverage of that retained end; missing coverage refuses duplicate/probe append
credit as well as a complete seal certificate. Do not replace that obligation
with a maximum over remaining files or advance the cache before state persistence.
log_follower_loss.rs covers all/last missing batches, a corrupt last batch,
duplicate/probe refusal and exact original-byte return preserving the seal floor.
consumer_recovery.py --restart-followers --witness-data missing|corrupt removes
or damages real batches only after every original PID joins. Require two explicit
Unavailable results and actual incomplete seal replies, including after a loss
diagnostic exists. Join both damaged peers before restoring only original batch
bytes; never roll back state.json. Propagate proxy frontend EOF at the actual
upstream join, then cold-reload fresh peer generations with the same recoverer.
Audit original inbox/effects before replay. Recreate older development state;
all markers remain 1. Added state barriers require fresh capacity measurement.
A warm tail/seal scan that finds acknowledged coverage missing must also mark
its cached range unreadable. Returning an incomplete certificate alone does not
stop duplicate/probe/new-suffix append credit from the old cache. Indexed payload
reread failures affecting that acknowledged range have the same refusal. Preserve
the durable end/seal and leave original files untouched; only cold verification,
a genuinely covered base or verified epoch adoption reestablishes append credit.
A directory observation error alone is retryable, not evidence of missing bytes.
The warm-cache unit matrix and log_follower_loss.rs native case exercise these
boundaries; the native case uses previously proven LTX writes and original-byte
return while retaining the seal, not a fabricated successful proof.

Follower state.json is a format-1 record binding leader session, follower node,
fragment epoch/base/end/seal floor with SHA-256 over canonical binary fields.
Reject unknown/old shapes, unsupported markers, invalid ranges, identity/digest
mismatch and read errors; never turn decode failure into an empty fragment.
A genuinely absent state is fresh only when no retained batch evidence exists.
Retain invalid metadata and batches unchanged, including across repeated failed
tail/seal/probe calls. The native log_follower_metadata.rs regressions cover those
boundaries and original-byte return. Python follower_state.py independently
validates archived records. consumer_recovery.py --witness-data state-missing|
state-corrupt requires real peer errors (not proxy-generated refusals), unchanged
files before return, joined peers and the original inbox/effects before replay.
Recreate old unchecksummed development state; no legacy reader or migration.
Read follower state through one open HostFile: fstat before requesting at most
4096 bytes, require an exact read and unchanged handle length, then decode.
Whole-file read followed by a decoder bound allocates before rejecting oversized
metadata. Only NotFound while opening means optional state; stat/read NotFound,
short reads and changing lengths are errors and cannot cache an empty fragment.
follower_state_tests.rs measures requested bytes at the actual cold-store seam;
the oversized input is guarded by the test filesystem to avoid a huge test
allocation. consumer_recovery.py --witness-data state-oversize pads the original
valid record to 64 KiB without changing fields/checksum. Require actual size
refusals, untouched batches and exact original-state return after peer joins.
Follower cold scans enumerate through FileSystem::read_dir_iter and release each
verified batch payload before reading another. follower_index.rs keeps at most
128 metadata records per run/buffer, spilling larger inventories into anonymous
injected scratch files with checked lengths and per-record SHA-256. Scratch is
derived, never authority. Read/sort/write failures refuse without caching a
shorter inventory. Seal checks verified range coverage without building a tail;
subsequent payload reads must match the indexed digest/state/length/range.
Canonical numeric batch filenames are required; noncanonical names remain unknown
gap evidence. Custom filesystems must supply streaming directories and scratch
for large histories; no eager fallback. All owned markers remain 1. The isolated
cold-load/seal RSS fixtures qualify only their 16-by-8-MiB profile. Production
tail transfer uses `durability/log/tail_stream/mod.rs` through the existing
authenticated tail upgrade, including the recovery-startup HTTP surface. No
buffered POST fallback exists; rebuild all participants on this format-1 cutover.
The buffered tail helper is
for diagnostic/fixture inspection, not production recovery. Stage an immutable
snapshot under the original leader guard in owned blocking work; release the
guard before network waiting. Retain at most two active snapshots/transfers per
follower and split raw data into at most 1 MiB frames, including within one legal
maximum writer entry. A format-1 manifest binds leader digest, epoch/base/end,
completeness, u64 counts/bytes and payload SHA-256. Each receiver stages its peer
independently and verifies full sequence coverage, counts, digest, completion
frame and EOF before exposing the snapshot. Progress frames supply no proof.
During preparation, progress.rs wraps the same captured filesystem, reporting
completed chunk/metadata work; an idle timer alone cannot keep a stalled attempt
alive. Monitor client EOF during preparation and propagate cancellation through
cold scans, index/row merging and entered workers. Domain shutdown still joins
uninterruptible entered I/O. A late snapshot reread/merge failure aborts the whole
gather: never keep its partial rows and continue on another witness. Source and
receiver RSS fixtures qualify their measured profiles only. Large bundle bodies,
live-fold bundle helpers, provider/progress inventories and total recovery memory
remain separate gates. Dead-session recovery and live-cell folding stage rows through
recovery_rows.rs. Payloads spill into anonymous files on the replica's injected
filesystem under its data root, never ambient /tmp. Every nonempty gather requires
anonymous scratch support, including histories small enough for an in-memory
metadata run. Variable-length key runs hold
at most 128 rows/1 MiB and merge through bounded caches without a run inventory.
Verify scratch lengths/counts/checksums, original descriptor identity tags beside
payloads and selected payload digests. Check every descriptor before key dedup or
batch-budget decisions, including ignored duplicates and lookahead. Strict full
metadata order includes priority/rank/original offset; an exact copied descriptor
must refuse before returning a batch that could raise coverage past a lost row.
A failure noticed only after that watermark can already make a retry skip lost
data. Scratch identity cannot be inferred from a valid per-record checksum alone.
Any failure must reach the caller before sealing. Recovery keeps last-follower precedence;
live fold keeps first-follower precedence with bucket copies preferred. Sort the
final bucket window and follower rows together numerically before any watermark
can skip older rows. Upload batches cap rows and accounted bytes, allowing one
legal oversized row. Owned blocking work beats the claim and cooperatively stops
on caller cancellation; domain shutdown still joins entered work. Full tail/HTTP,
live-fold bundle reads, oversized bundles and provider/progress inventories remain
separate implementation gates before total recovery bounds can be claimed.

Node-log append/seal HTTP replies and ordered reply frames contain scalar
metadata; collect at most 64 KiB and reject an excess frame header before its
payload allocation/read. Keep the distinct 64 MiB writer-data frame allowance
and bounded raw recovery-tail protocol. Signed HTTP checks status and response
authentication before bounded collection, retains supplied pools/deadlines and
never treats size refusal as durability credit or protocol-incapable status.
reply_reader.rs reserves finite mailbox capacity before reading another frame;
receiver drop must release both blocked reservation and blocked read, and retire
must join the actual tracked reader before redial. Keep unanswered wire appends
at or below that same mailbox capacity. Larger configured submission windows
wait for responses rather than being refused; a writer that keeps taking queued
jobs can deadlock both socket directions after the finite reply queue fills.
The actual 128-job/128-byte-duplex lane regression checks exact FIFO replies and
normal peer/lane joins, separately from the pump-only queue tests. Admission uses try_spawn so
closure within an entered poll returns DomainClosed without panic. The real
signed length/chunked/pending-body and ordered-reader regressions cover these
seams; they do not establish a total recovery memory ceiling or explain the
independent native-actor lease-expiry failures.

Consumer-recovery failure cleanup must run every node/gate/store/ledger action
through cleanup_all, preserving the primary error even when a stopped node was
fenced. WitnessGate.close disconnects persistent frontends before server_close
joins handlers; otherwise a failed driver can remain in Python finalization.
The Python regressions exercise the actual consumer finally branch and a real
idle keepalive connection. PeerProxy and WitnessGate share relay_upgrade for
buffered binary upgrades. The outer /peer/tunnel upgrade carries HTTP RPC inside;
PeerProxy must continue inspecting that HTTP to hold/drop the selected exact
reply. Treating every 101 as opaque bypasses the fault gate and cannot qualify
reply loss. Log/application upgrades are opaque and join both relay directions
on EOF. PeerProxy close retires both TCP endpoints before joining handlers;
ACK cleanup also uses cleanup_all so a proxy failure cannot mask readiness or
skip the private store/ledger. --trace captures selected actor/lease-store events;
unexpected-refusal files retain the exact verdict before further observation.
Keep the original one-second fixture lease, typed Unavailable requirement and
operation budgets. Cleanup repairs do not explain an observed NodeLeaseExpired.

Cold destination ack/fetch fixtures refuse only the consumer's selected stream
ownership write, preserving other writes and real proof paths. A returning
consumer may otherwise validly own the stream and replay locally, bypassing the
required network measurement. Retain exact forwarded receipt/reply assertions,
assert a different recovered stream owner and audit claim-config/marker custody.
The failed RustFS ack run returned AlreadyAcknowledged with unchanged effects/
checkpoint but no captured replay; keep it as a fixture failure, not a pass.
The cold RustFS expired-fetch run also exposed a separate startup/shutdown
failure: returning peers exited at __catalogResolve while the destination kept
the predecessor log recovering, then missed readiness and drain budgets. Passing
traced/staggered/untraced replays alone did not establish its cause. The controlled
recovery-first case and startup retry below reproduce and address this race. Activation
readiness and debug diagnostics preserve the full error chain. Fixture cleanup
uses cleanup_all to attempt every owned node/proxy/store/ledger after a stop
failure without masking the original error; already-exited processes close their
output handles. Cleanup of a failed run is not successful graceful-drain proof.
Cold startup can fail one recovery pass before a retained witness publishes its
new route. Waiting a full node TTL can outlast that witness's event activation
deadline and lose its serving process before the retry. Missing-witness errors
retain the exact member lease observations; startup_retry.rs polls those roots
during the existing backoff and wakes only for a live current-protocol return
or changed lineage/generation/address. Ordinary heartbeats, missing/invalid reads
and untagged failures retain backoff. Both embedded and CLI boots use this wait;
attempt counts, deadlines, fencing and actual witness proof remain unchanged.
fetch_faults.py --recovery-first requires expired cold follower mode and observes
the real first missing-witness refusal before returning peers. Audit its bounded
log prefix, unchanged predecessor/owner and old peer leases, then require the
original recovered prefix before replay and full joins. Trace mode includes
recovery info and startup warnings. This does not explain other startup failures.
Node-log recovery cannot seal possible fleet acknowledgments without a complete
witness, even after every member is long expired/unreachable or the ensemble is
empty. Durable loss diagnostics do not authorize replacement admission. Retained
bundle corruption also refuses the uncovered-row fold on every scan, including
when the diagnostic is cached. Preserve the recovering predecessor and require
valid evidence before retry can succeed. Local SQLite state alone is not a
complete node-log witness; do not add a fallback that silently resets accepted
outbox, stream, inbox or checkpoint state.
A missing node lease root is unproven, not an absent folded log. The core must
invoke RecoverNodeLog for a named foreign owner whose lease is missing; the log
reader must refuse that root in explicit/takeover recovery. Only own-node first
boot reads an optional root before lease installation. Retired leases remain
present tombstones. Never fabricate a no-log record to unblock recovery. Persist a canonical 128-bit node lineage in both owned cell records and node
leases before actor/log construction. Retained roots preserve that lineage;
a missing root starts a fresh lineage and cannot certify existing owned cells.
Own-name claim/reconciliation, output/release/GC gates and remote lease routing
must compare lineage as well as node name. Recovery checks the expected lineage
against the same root read used for its folded proof decision. Identity capture
also retains whether a root was present. Startup cannot treat a later absence
as first boot: require that retained root before predecessor GC or lease install.
Enforce that provenance at the bucket lease CAS too: after successful recovery,
a stale Match rejection and absent readback cannot authorize Absent creation
under a retained lineage. Refuse before transport and applied-log notification;
true first boot can still create its root. The post-recovery read cut requires a
real sealed predecessor and payload, preserves cut-time evidence on refusal,
then returns those actual sealed bytes before recovery and receipt reuse.
Startup does not collect other generations from its cached lease observation.
A no-log root can race a real writer opening and crediting a bundle. Historical
GC runs in both postures during maintenance, with an independent 512-object
cursor and two-second deadline. Every bundle row and validated checkpoint hint
requires fresh contiguous per-cell coverage or a persisted epoch-retirement
certificate. Never use another writer's cached zero, resident watermark or a
maximum past a recovery hole as deletion authority. Corrupt/unavailable objects,
current-session data, other lineages and loss diagnostics stay retained.
Bucket bundle CLB1 envelopes checksum payload, footer identities/ranges, footer
length and magic with CRC64-ISO. Fresh decode rejects missing integrity, empty/
duplicate identities, zero epoch/TXID, incomplete/overlapping ranges and footer
TXIDs differing from L0 header min/max. Verified metadata precedes any coverage
or recovery decision. Row extraction from a verified envelope/trusted writer index
checks bounds and L0 identity without rehashing the whole object for every row.
This is a hard development cutover: rebuild all participants and recreate older
unchecksummed bundle state. CLB1 and all Cellar-owned markers remain 1; no legacy
reader or migration is supported.
log_startup_gc.rs holds a real no-log response while a live generation obtains
Bucket proof, excludes follower/per-cell credit, then force-joins the writer and
removes its local directory. Audit original SQL before replay. Require collection
only after independent recovery uploads data; its paged fixture does not qualify
provider memory bounds or the full capacity/fault matrix.
Before-recovery and post-seal root cuts hold actual response bytes/token,
erase or replace the root, and require refusal with owner, payload and follower
bytes unchanged; returning the original metadata must recover before receipt
reuse. Claim-phase cuts select the actual recovering process with scoped peer
root-CAS refusals, then hold its ordinary-bucket GET of a real Recovering root.
A stale claimed response can legitimately let recovery seal followers and add
per-cell copies before its final root read refuses. Require the original cell
owner, acknowledged .batch bytes and existing bucket objects to remain, and no
fabricated missing root or overwritten replacement lineage. Return the actual
claimed metadata after failed startup joins, then audit original clock/event/
source/inbox/checkpoints before receipt reuse. State and batch preservation are
distinct at this phase; do not relax the older cuts' exact-byte assertions. Keep
metadata rollback and physical loss qualification separate from retained-root evidence.
Foreign claim-phase cuts start under a different node's own lease and trigger the
predecessor interlock through a signed cold read. Require explicit Unavailable
before the unchanged routing deadline; a timeout is not the expected refusal.
Restarted cuts join the failed recoverer before returning real claimed metadata.
Live-resumption cuts instead await two explicit routing refusals, keep the same
Node/domain, and require the first fresh audit after real metadata returns to
succeed within the unchanged budget, without a read retry. Assert identical host
services and persisted generation/lineage before receipt reuse. Both variants
audit the original prefix. Successful own-node startup is independent of
admission to a cell last owned by the missing/replaced predecessor.
Delayed-reply fixtures retain the exact removed root atomically in their private
SQLite store, assert NodeLeaseMissing and full join, then restore its proof fields
with only expiry shortened before expecting retained-proof recovery.
Returning-witness coverage belongs in tests/support/log_recovery.rs, included by
native_log.rs. It must use cold follower stores and exact retained bytes, remove
leader-local data and exclude bucket data before recovery, and check SQL state
before any replay. A prior blocked diagnostic is history; current valid evidence
can permit recovery without rewriting that history. This native-domain fixture
does not substitute for the independent-process destination failure matrix.
Follower-only native log fixtures must exclude node-session bundle publication
as well as per-cell uploads. The bundle loop's first tick can otherwise win
with legitimate Bucket proof before Fleet credit; a 1ms fixture-only flush probe
exposes that route. Preserve real metadata CAS, reads and loss diagnostics;
BundleFault excludes only the selected session's bundle PUTs, never supplies proof.
Clock-refusal follower fixtures must observe a real blocked bucket upload within
the original operation budget before auditing the unchanged replica prefix. Fleet
proof can release a response before the independent bucket sync runs; an immediate
zero attempt count does not prove that publication is unguarded. Do not extend
the original budget or substitute a fake acknowledgment.
Clock-refusal recovery fixtures select the next automatic append at the real
successor peer proxy before fetching a fresh lease and acknowledging retained
history. Select that successor with the existing owner-key CAS-only peer fault
in every cut, including cold restarts, and assert its name. Otherwise a source
can legitimately claim the stream and dispatch locally without visiting the proxy.
Start both cold followers and the selected successor concurrently. Inside the
successor's start future, issue its signed read-only recovery audit while the
peers resolve bindings: refusing their owner CAS writes cannot itself request
ownership on the permitted successor. Retain the independent post-startup clock,
history, identity and receipt audits. Keep all startup futures heap-pinned and
retain the original activation deadlines. Unexpected original startup errors
name the node role and retain the fixture directory; inspect it after its test
process exits. Passing replays do not diagnose an earlier deadline failure.
Exclude producer ownership on that successor
with a producer-owner-key CAS-only fault and assert distinct owners: cold
recovery can otherwise validly co-locate the outbox and stream and bypass the
peer boundary. Required-history backpressure can save a retry up to 60s away;
ack does not notify a remote producer to advance that deadline. Bound phase selection by
that policy plus the original call budget, compare the entire original request,
release within the ordinary 3s held interval and keep the 5s settlement check.
Do not use operator retry, row repair or mutation replay to select this phase.
Keep implemented behavior separate from release qualification. Passing reruns
do not close unexplained failures or substitute for missing fault cases.
Outbox dispatch uses immutable stored principal/grant identity after source
proof; current application bindings govern new emission, and receivers repeat
authorization against durable resource grants. Outbox and inbox schemas have
version-1 markers; activation rejects unsupported markers, inconsistent retained
accounting and outbox events exceeding the captured host policy. Cellar is in internal development: all Cellar-owned protocol, format and
schema markers stay at version 1. Hard cutovers are permitted. Do not add legacy
readers, layout migrations, version negotiation or fleet upgrade commands.
The log-tail request frame uses tail_format 1; streamed manifests and preparation
progress use CTS1/CTP1. CLT1 is only the buffered diagnostic/fixture codec, never
a network fallback. Missing/unsupported markers and entries-only request shapes
are refused. Native
follower reads/writes require node/lineage/generation identities, and discovery visits
only that current directory layout. Rebuild every participant on wire cutover;
former flat-layout development state requires recreation, not a migration.
Follower group commits admit at most 64 frames and 64 MiB of total encoded
CLE1 entry bytes. Validate count/u16 cell/u32 payload representation before
persistence; an invalid first frame receives refusal with unknown epoch. A frame
that would exceed a burst's budget is returned untouched and carried into the
next ordered commit. The maximum CLB1 file adds its 72-byte header/digest to
that entry budget. follower_file.rs opens one handle, checks its length before
reading, and requires an exact read and stable length for both metadata/batches.
Oversized/disappeared/failed batch reads remain unreadable-range evidence; never
lower the independently acknowledged end or credit a duplicate/probe. Native
regressions exercise an already-delivered full-budget ordered burst, cold reload,
zero payload reads above the bound, and post-open faults. The process oversize
fault extends each original batch with zeros to 64 MiB + 73, preserves metadata,
requires two real incomplete seals/refusals, and returns only original batch bytes
after peer joins while preserving their seal floors. The archive auditor checks
original prefixes and zero extension with bounded reads. All markers remain 1;
rebuild all participants and recreate development state containing larger batches.
This bounds individual batches, not total retained tails or fleet capacity.
Only checksum-protected committed .batch files supply follower entries. Former
.entry/.entries files are rejected without decoding or migrating their payloads;
current cleanup reclaims only covered committed batches.
Recreate development state and rebuild all participants when layouts change;
mixed builds are unsupported. External codec versions (SQLite WAL and upstream
LTX), package versions and runtime revision/sequence counters keep their meaning.
Telemetry exports use schema marker `1`; KV large-value references and object
prefixes use `v1` with ownership epochs. Recreate earlier development blobs on
cutover; readers and collectors accept only the current layout.
Initial catalog/partition creation requires a host provisioning claim from an
exact captured deployment declaration; proposed grants cannot authorize their
own creation. Ordinary SDK configuration can only administer existing resources.
Keep the remaining qualification gates explicit before release.

Source diagnostics use `Storage::outbox` under the owning activation, with
bounded metadata pages and source-incarnation continuations. Lane retry requires
the captured producer revision and advances its dispatch generation to fence
old replies; the dependency-free core owns that decision. `cellar outbox`
inspect/retry/status/audit uses fleet-HMAC ingress, a distinct source RPC
capability and the original owner/output/discovery gates. Retry operation
receipts and bounded audit pages live in an independent version-1 source
control schema and survive business reset.

`event_late_reply.rs` qualifies operator retry while a real append acceptance or
Forbidden reply is held. Preserve the source's ordinary operation budget and
prove its old RPC is still pending at the control cut. After releasing that reply,
require a blocked newer submission before inspecting pending state; socket-write
completion alone does not prove the old callback finished. Current generation,
rows, status and error must survive, and operation-receipt replay cannot rearm
again. Host counters only establish phase boundaries; signed source inspection,
stream history, original request/envelope bytes and inbox effects supply state
evidence. A targeted negative control disabled only the generation check and
exposed stale settlement and stale blocking; any such probe must restore
production bytes before qualification. These are embedded-node/domain cuts,
not process/power proof.

`event_settlement_race.rs` holds original signed ack/extend submissions at the
real peer proxy before the destination owner sees them. Exercise reset at the
same offset, actual lease expiry and replacement, and acknowledged progress with
a next live lease. Audit the replacement before releasing the old requests and
compare its full lease/checkpoint afterward. Preserve the ordinary 30s lease and
3s operation budget; wait outside the calls until near expiry, then require both
requests captured while the original lease is still live. A timeout cannot stand
in for typed receipt refusal. Inbox deduplication is scoped to one stable consumer
database; separate worker databases do not share it. These are native domain/
executor cuts, not independent-process or clock/retention qualification.

`event_consumer_overlap.rs` pauses an actual old delivery callback across
natural expiry, then applies the replacement through the same stable consumer
database and inbox scope. Hold outside transactions/input blocks. Preserve the
ordinary 30s lease and 3s call budget; capture the old callback while its lease is
live, commit/sync the replacement effect before releasing it, then compare full
lease/checkpoint/history and effect state. Cover stale ack before replacement
ack and obsolete ack after progress with the next batch leased. Separate worker
databases do not share deduplication. These embedded-domain/executor cases do
not qualify independent-process faults or external effects.

`event_consumer_takeover.rs` holds a real consumer callback inside one native
poll before inbox acceptance or after receipt/effect writes before commit.
Isolate only the old client's store, observe lease-expiry domain closure and a
higher consumer owner epoch, then audit the successor's original prefix before
release/replay. Record acceptance separately from transaction failure so later
write refusal cannot hide illegal inbox admission. Require interrupted output,
joined expiry fencing, unchanged destination receipt/checkpoint/history and
copied old SQLite/WAL evidence of one original receipt/effect after rollback.
Only after those audits may the successor probe receipt identity and process the
pending event. Keep normal operation/handler/lease budgets. These independent
embedded domains/executors do not establish separate-process or physical faults.

`cellar stream` describe/read/sources/status/audit and `cellar subscription`
list/describe/pause/resume/reset/delete use signed
`/streams/` ingress and delegated durable grants. Operator authority is limited
to stream control methods, with optional provenance omitted from ordinary event
packets. Subscription receipts retain the operator label and enforce it during
replay/status; ordinary SDK receipts carry no operator. Reads exclude bodies and
encoded envelopes unless explicitly requested. `cellar stream retire` and
`retirement-status` route to the catalog; operator authority also permits only
the matching catalog retirement and partition preflight/closure methods.
Catalog and stream schemas use version 1 with nullable operator provenance.
Current markers require those columns before base schema mutation.
Terminal retirement receipts retain the affected payload range after
bounded cleanup. Subscription inventory uses a bounded primary-key range and includes retained
deleted incarnations without lease tokens or payloads. Its `subs1` cursor is
bound to the stream UUID; every page checks current administration authority.
Pages are live observations, not a snapshot. Destination source inventory uses
`Stream::sources`/`cellar stream sources`, a bounded producer primary-key range,
and a `sources1` cursor bound to tenant/application/stream identity. It shows
accepted watermarks and retained seals, including after payload expiry, without
payloads or tokens. It is not a census of undispatched sources or an all-producer
drain proof. Each page repeats current administration checks. Whole-fleet
resource/source inventories and complete event metrics remain unfinished.
Fixed-cardinality event observations belong to each host execution domain.
`HostServices::event_metrics()` and fleet-authenticated internal `/event-metrics`
export operation counters, histograms and live reservation gauges without
resource identities or payload labels. Timers retain their creating domain.
These observations are not a receipt ledger or cold-cell state census;
resident host aggregate gauges remain unfinished. Fleet root-state collection is
available through `cellar event metrics` and signed `POST /event-state/SCOPE`.
Subscription status and bounded inventory carry owner-sampled lag/lease
observations. Logical unread offsets include lost history; retained unread age
is null when no unread payload remains. A read clamps to the durable clock but
does not advance it or expire leases. Each returned row adds at most one primary-
key timestamp lookup; SDK/operator acceptance checks observation consistency.
This diagnostic coverage does not supply aggregate metric gauges.
Outbox inspection uses transactional per-lane row/byte counters and seven
fixed permanent failure counters, including blocked seal delivery. Explicit
covering age indexes bound source/lane timestamp reads; pages do not aggregate
event ranges. Current blocked reasons clear on retry or accepted delivery.
Restore validates accounting and requires its indexes/triggers. The schema
marker stays 1; prior development layouts require recreated state. SDK/operator
page acceptance checks scope, lane identity, totals, reason set and ages.
`Stream::stats`/stream describe expose current offsets, payload bytes, lease/age
and exact protected/age-blocked retention bytes under administration authority.
Byte-prefix positions are internal ring bookkeeping, not delivery identity;
a retained window stays smaller than its modulus and offsets never wrap.
Restore audits contiguous offsets, sizes, timestamp order and byte positions
before base-schema mutation. SQLite allocation observations use a fixed framework
helper after activation checks; public pragma permissions stay unchanged. Full
WAL/LTX/bucket physical usage and aggregate export remain unfinished.
Stream maintenance only schedules age reclamation for an eligible first record;
required checkpoint protection can quiesce age work entirely. Live/expired lease
cleanup retains its expiry wake. Checkpoint/lifecycle/configuration transitions
recompute deadlines in their transaction; configuration cannot leave newly
eligible age work dormant. Indexed minimum checkpoint, at most 129 lease probes
and one oldest record bound scheduling independently of retained payload size.
Use durable logical time, preserve overflow refusal, and keep bounded continuation
intervals plus independent application alarms/work reasons.
Catalog inventory uses `cellar stream list` with a separate
fleet-administration namespace capability and `/catalogs/` ingress; resource
grants cannot mint it. Owner pages select at most 101 scalar identity/lifecycle
rows, including tombstones. `catalog1` continuations bind the namespace and tail.
Catalog identity and accepted/desired revision columns commit with configuration
state; the schema marker stays 1 and older layouts require recreated state.
`cellar stream retirement-proof --resource RESOURCE.json` uses an exact-resource,
read-only platform capability through the same catalog owner path. It requires
a terminal identity and matching committed retirement receipt, returning scalar
identity metadata without grants or event bodies. Resource grant revocation does
not prevent platform verification; this capability cannot mutate resources.
Source destruction uses a durable source lifecycle and captured lane/work
snapshot under the source control schema, still marker 1. Private source RPC and
outbox work may continue after business admission closes; ordinary contexts,
retained storage/cursor/inbox/facet handles and suspended turns keep their captured
source generation. Restore and instance reset reconstruct the durable fence.
`cellar outbox destroy/recreate` require exact incarnation, operation, reason and
explicit CLI intent; destruction also requires a loss acknowledgment. Retirement
evidence commits before deletion; stale verification and dispatch replies cannot
advance a replacement. Event/inbox/lane cleanup is batched, business reset uses its
existing transaction, receipts/tombstones survive and recreation mints fresh
producer identity. Source roots register top-level physical facet names durably before opening.
Destruction retires child activations and reclaims registered subtrees in at most
100 replica objects and, after remote completion, at most 100 local files or
directories per host batch, keeping each registry row until completion. Local
cleanup uses `FileSystem::remove_dir_batch` on the owning blocking executor;
custom filesystems must implement that resumable, no-symlink-following contract.
Runtime-only facet fixtures must supply their per-domain host-gate adapter because
registration writes parent storage. Synthetic fixture responders test API/policy
contracts and are not fleet durability evidence.
Recreated sources qualify facet storage names by producer incarnation; logical
names and startup identity remain unchanged. Native opens carry a captured
admission check across host waits and restore, and serialize with subtree cleanup.
An authenticated unfinished destruction replay re-arms durable cleanup before proof;
terminal replays leave successors untouched. Injected lost replica-delete replies
and complete owner handoff preserve closed admission and exact discard receipts.
Large local-directory/SQL reset capacity and the complete source fault/lease/policy
matrix remain unfinished. Do not infer full release qualification from the base-database tests.

Cold source recovery can use a separate framework-only worker pool when its
application class is absent. It accepts only restored epochs with retained
source identity, repeats schema/host-limit validation and preserves the actor's
restore, VFS, ownership and proof path. It has no application bindings or
handlers. Internal work and source controls execute before rejecting business
calls; unavailable application alarms back off without consuming abandonment
budget. The pool participates in generation retirement, census and shutdown.

Library applications start `sdk::Node` with explicit `NodeConfig`,
`NodeServices`, a `DeploymentGraph`, and `CompiledApplications`. The caller
supplies listeners, executors, clients, authentication, and any fleet services.
Axum hosts use `Node::start_external` with `ExternalNodeServices` and mount
`cellar_axum::CellarService` on their already-bound public listener. Application
handlers and body producers retain SDK event authority; only the node driver
polls admitted ingress in its owning domain. Keep the private peer listener.
Each node owns an isolated execution domain; library defaults do not inspect
process environment. The CLI captures environment settings at startup.
NodeServices/ExternalNodeServices capture an optional `wall_clock` before
bootstrap; use None for system UTC. Standalone domains use
ExecutionDomain::with_wall_clock. The provider belongs to ProductionDomain and
follows scoped tasks, blocking work and cleanup; it is never a process override.
Negative samples clamp to zero. Monotonic deadlines/timers and peer signature
timestamps retain their separate host clocks. Keep fleet UTC sources consistent.
`event_clock_retention.rs` moves only the destination clock: live leases must pin
aged capacity, required checkpoints must keep pinning after expiry, and rollback
plus joined owner handoff must retain observed expiry before any mutation can
repair state. It uses explicit 120s node TTL/recovery backoff to avoid turning a
31s wall step into an unrelated authority expiry, preserving ordinary 30s stream
leases and 3s operation budgets. Audit source obligations and accepted bytes
before reclaim/replay; bounded-history loss requires an explicit reset. These
native domain cuts do not qualify physical clock faults or all rejected-operation
time observations.
Stream deadline mutations capture `event_stream::store::clock` after authorization
and before their rollback transaction. Refusal/deduplication cannot forget expiry;
scheduling reuses persisted time rather than sampling UTC again. Keep denied
requests and read-only diagnostics from advancing it. Exact configuration receipt
replay preserves the recorded caller's result, including its own grant rotation,
without authorizing a new change. `native/stream_clock_tests.rs` uses real broker
RPC/native contexts and SQLite close/reopen, with no background maintenance;
these regressions are not fleet proof of clock-only commits on refused RPCs.
`event_refusal_clock.rs` tests bucket and follower recovery of a proven
StreamFull clock-only update. Its shared publication fault covers stream replica
and old-session bundle writes while keeping CAS/lease clients unchanged. Keep
the original lease and payload present at capture, block shutdown repair, discard
the joined owner's disk, and audit a higher-epoch owner before receipt reuse.
All participating nodes use explicit 120s TTL/backoff so the 31s wall step does
not expire live witnesses; stream leases remain 30s and operation budgets 3s.
The post-join expiry-only CAS is a controlled liveness fault, preserving every
folded proof field. Readback may see legitimate recovery state/certificate
progress but cannot change proof identity or authority. Do not delete the folded
record to emulate ordinary expiry. Read-only audits can repeat a closed-owner
Ambiguous transport result; ordinary mutations and receipts cannot.
`event_unproven_clock.rs` holds actual publication with fleet proof disabled:
local SQLite clock evidence cannot expose a typed refusal. Require Ambiguous,
then independently audit the retained proven prefix after failed publication and
disk loss. This selected absence result does not mean ambiguity proves rollback.
These are native domain cuts, not independent processes, natural node expiry,
or physical store-loss proof. The cold-follower variant joins both original
follower domains, drops their service references and restarts both from retained
directories before readiness can await predecessor recovery. Heap-pin their
startup futures. No original follower domain supplies the recovery tail; graceful
follower close is not SIGKILL or power-loss persistence qualification.
Queue leases and actor settlement share the host's captured operation deadline.
Embedded bootstrap/adoption generation builds and metrics sampling use fallible
blocking admission. Closing a domain can race a later effect in an already-entered
poll; return `DomainClosed` without invoking the refused factory/sampler. Started
blocking work still joins before releasing the node directory. Metrics maintenance
ends on closed admission. Keep startup/directory deadlines and assertions intact;
cancellation diagnostics independently distinguish retained locks from pending
replacement bootstrap. A fixed admission panic does not by itself explain an
observed bootstrap deadline failure.
The cancellation fixture must observe factory entry without consuming the host
blocking pool it measures. Use an async entry notification with the actual
factory-entry Instant and preserve its five-second cutoff even when receiver
polling is delayed. A one-blocking-worker variant retains the original entry,
restart, directory-retention and join assertions. Queued notification coverage
rejects late entry; it does not reject timely entry merely observed later. This
fixes reproduced observer starvation, not the historical startup/lease failures
whose causes remain unestablished.
Lower-level library hosts pin a 15-second default on first use; configure actor
policy before constructing generations to choose another budget. CLI startup
captures its environment override before any generation can sample that budget.
SDK HTTP uses `NodeConfig::http` and an optional `NodeServices::http_client`
builder factory. Lower-level hosts configure HTTP through `HostServices` before
the first fetch. Keep required redirect behavior and invocation subrequest
accounting in Cellar; factories choose DNS, TLS, proxies, credentials and pools.
Library HTTP defaults ignore ambient proxies. Domain closure drops cached pools
and prevents their reconstruction.
Asset loading receives an explicit `AssetCache` with directory, cleanup target,
file-size policy and optional caller filesystem. Generations, cohosted scripts
and CLI reloads retain that capability. Asset streams, fallback refreshes and
maintenance belong to their serving execution domain; closure joins begun
blocking file work. CLI cache options are captured once under the node data
folder unless an explicit environment override chooses another directory.

The workspace must remain free of JavaScript engines, including with all
features enabled. Source JavaScript, Node.js modules, and the former Python/Wasm
wrappers are not executable application formats. Cellar starts with fresh
configuration and state; Celld configuration aliases and state migration are
unsupported. Framework SQLite tables reserve the `__sys_` prefix.

## Repository map

```text
crates/
  cellar/                 Production effect adapters, host, and application SDK
    cli/                  Entry points, commands, packaging, dev, environment parsing
    application/          SDK bindings, storage, transactions, and application APIs
    actor/                Serialized core driver and effect execution
    execution/            Execution domains, injected services, and task ownership
    node/                 Startup, supervision, managed integration, and shutdown
      network/            Node WebSocket ingress and authenticated peer tunnels
    runtime/              Generations and application execution
      cells/              Cell activation, dispatch, and facet lifetimes
      workers/            Compiled workers, pools, loading, and configuration
      invocation/         Event context, gates, turns, channels, and service calls
    transport/            HTTP, TCP/TLS, and WebSocket adapters and registries
    storage/              SQLite, object-store clients, and Blob storage
      sqlite/             Activation-bound state, cursors, transactions, alarms
      object/             Bucket clients and local object-store implementation
      blob/               Shared Blob backend, multipart, and streaming
    durability/           Replication, fleet logs, recovery, and proof task ownership
      replication/        LTX backend, handles, storage clients, and policy
      log/                Authenticated node logs, follower proof, and recovery
    fleet/                Leases, peer authentication, handoff, and durable wakes
    deployment/           Shared manifests, artifact protocol, deployment loading
    containers/           Cell-owned container service and Docker adapter
    observability/        Telemetry export and OTLP encoding
    platform/             Machine and process-memory adapters
    tests/                Native SDK and host integration tests
  cellar-axum/            Axum handlers, external ingress service, shared builder, OpenAPI
  cellar-admin/           Browser management API, authentication and operation audit
  cellar-logic/           Dependency-free decision core
  cellar-ltx/             SQLite WAL capture, LTX codecs, restore, and compaction
  cellar-native/          Owned values, structured clone, crypto, and web primitives
  cellar-fleetcare/        Field-service app, embedded/daemon hosts and open-loop client
examples/                 Compiled Rust applications and project configurations
dashboard/                Vite, React and shadcn/ui browser management console
docs/                     Builder/team guides, evidence, decisions, and SVG explorer
tools/                    Formatting, dependency checks, documentation, and fleet tooling
.github/workflows/        SDK checks and release builds
```

Cellar groups its implementation into component directories next to
`Cargo.toml`; `lib.rs` declares the existing Rust module names. The logic crate
retains a flat crate root; native groups structured clone, SQL and web primitives
under component `mod.rs` entry points. `cellar-ltx` uses `src/`. Cargo
declares the CLI entry point as `crates/cellar/cli/main.rs`.

CLI files retain their existing Rust module paths through `#[path]` declarations
in `crates/cellar/lib.rs`. For example, `cellar::native_deploy` is implemented in
`crates/cellar/cli/native_deploy/mod.rs`. Shared package configuration and
publication helpers also serve the SDK and host. Keep those callers in view
when editing CLI tooling. Shared bucket, fleet, protocol, and runtime adapters
remain outside the CLI directory.

Physical folders describe component responsibilities, while Rust paths retain
their existing names through `#[path]` declarations. For example,
`cellar::daemon` is implemented in `node/mod.rs`, `cellar::runtime` in
`runtime/mod.rs`, and `cellar::asyncrt` in `execution/domain.rs`. Keep module
visibility, conditional-build selection, and exported macro paths stable when
moving files. Node ingress modules under `node/network/` remain logical
children of `daemon`.

Only `lib.rs`, `sdk.rs`, and Cargo's `build.rs` remain as Rust files at the
Cellar crate root. Place new implementation helpers beside their owning
component and use private child modules where appropriate. Name shared helpers
for their contract and owner rather than collecting unrelated code in `utils/`.

SQLite alarm records stay with `storage/sqlite/`; fleet wake discovery and
response gates live under `fleet/wake/`. KV large-value and Queue policy/batching
helpers sit beside their built-in cells under `application/kv/` and
`application/queue/`, retaining their original crate-root module names.

Workflow batch/configuration, compensation, history, and byte-stream adapters
live under `application/workflow/`. Their journals and chunks share the owning
instance's SQLite state and output gate. Workflow subscriptions and stream
readers retain their opening invocation; continuation reads do not consume a
new subrequest slot. Workflow binding definitions are bounded, non-executable
diagrams. Create/restart snapshots and content digests share the instance transaction and retention;
history events retain their execution generation. Inspection inputs are explicit
`StepConfig::input` values, redacted with sensitive outputs. Definition versioning
does not pin executable versions or change replay policy.
Native workflow packages retain the `workflows-v1` feature while Cellar is in
internal development.
Use `native_workflow`, `native_workflow_cell`, `native_cron`, and
`native_artifacts` when changing these contracts.
Dashboard cron overrides live under `config/cron/`, keyed by deployment prefix and
declared expressions. Conditional writes guard the observed schedule revision.
The loader and standalone watcher share resolution; embedded hosts explicitly
adopt a loaded graph. `LoadedDeployment::cron_revision` records an adopted override,
including an empty schedule that must retire an existing alarm. Do not infer a
successful scheduled occurrence from generation adoption. Bounded execution observations
live in `application/cron/history.rs` in the scheduling cell's own SQLite/output
gate; retain started attempts without terminal evidence after interruption. Only
the primary application owns that cell. The cron class remains inspectable after
schedule removal, while `Generation::cron_cell` determines active adoption.
Cron inspection uses the authenticated reserved-cell route, a fixed read-only
`__cronHistory` RPC, and `data_read`;
anonymous `/do/` requests must not expose its journal.

The layout preserves three mixed modules for later focused refactors:
`node/mod.rs` combines shared node composition with process startup;
`node/managed.rs` combines managed services with connection/credential commands;
`deployment/fleet.rs` combines deployment loading with operator orchestration.
Shared package configuration and publication helpers still live under `cli/`
and serve the SDK and managed host. Separate those responsibilities before
changing their public interfaces. `platform/machine.rs` also retains its
existing identity, default-policy, and explicit environment adapters.

## Ownership boundaries

- `cellar-logic::on_event` advances behavioral state and returns effects. The
  core has no async work, I/O, ambient clocks, randomness, locks, or dependencies.
  Put lifecycle, routing, ownership, scheduling, and durability decisions here.
- The serial actor owns the core. Adapter futures execute effects and return
  versioned completion events through its mailbox; they do not mutate core
  state or implement competing policy.
- The execution boundary uses `cellar::asyncrt` and injected host services for
  tasks, timers, clocks, randomness, and node filesystem operations. Read
  `clippy.toml` before touching these paths. Direct host operations in CLI/build
  tooling need an explicit, justified boundary exception.
- Node policy and services belong to their execution domain. Keep actor,
  runtime, cell API limits, loader, replication, log, and shutdown configuration
  explicit in library APIs; environment resolution belongs in CLI adapters. Capabilities
  and spawned work must retain the domain that created them.
  `CellLimits` is pinned before generation construction or SQLite activation;
  preserve host isolation and receiver-side budgets when extending API limits.
- Transport registries and binding adapters retain their execution-domain and
  event ownership. Their location under `transport/` does not make sockets,
  HTTP streams, or upgrade capabilities independent of invocation gates.
- `cellar::application` implements application contracts and binding adapters;
  `cellar::sdk` is the application-facing namespace. Storage and context handles
  carry event/activation authority. Retiring an activation must invalidate its
  handles and wake or cancel outstanding work.
- `cellar-native` owns engine-independent values and codecs. `cellar-ltx` owns
  SQLite replication mechanics; fleet ownership and output-gate policy belong
  to the framework, not to the LTX codec.
- `durability/` groups framework replication and node-log adapters. Keep one
  `DurabilityOwner` for the coupled replication, log, and follower tasks;
  shared storage clients do not acquire lifecycle or ownership authority.
- Operator SQL, KV, and Queue commands use the authenticated fleet route to
  reach the owning cell. Never mutate their persisted SQLite state behind the
  owner. Blob commands reuse the binding's `blob_store` implementation.

## Contracts to preserve

1. A cell has one authoritative owner and epoch. Stale completions, leases, and
   storage handles cannot authorize work for a later activation.
2. Externally visible writes and egress pass through the output gate. Prove the
   required write position and ownership before acknowledging success; a
   durability or fencing failure must fail the operation.
3. Recovery must account for the prior owner's replicated log before takeover.
   Eviction, handoff, and shutdown cannot discard unproven writes or required
   wake/alarm coverage.
4. Transactions, cursors, sockets, reservations, and background tasks retain
   their owning lifetime and release resources on cancellation and failure.
5. Persisted protocol fields, cell identities, bucket keys, LTX formats, and
   feature gates are shared contracts. Search readers, writers, and consumers
   before changing them. Read `crates/cellar-ltx/README.md` and
   `crates/cellar-ltx/reference/ltx-format.md` for format changes.
6. Native packages validate their execution format, host protocol, target,
   executable digest, and size. Installation verifies downloaded bytes and
   refuses to overwrite an existing executable.
7. CLI stdout carries command data; progress and diagnostics belong on stderr.
   Use `cellar::cli_output::Output` and `note!`. Preserve JSON output, bounded
   listings, continuation information, and successful closed-pipe handling.
8. Recover predecessor logs before replacing a node lease. Keep the node's
   directory lock and caller-owned executors alive until owned work joins.
   Both stop modes drain complete event drivers, including post-response
   background work and queued SDK calls, before changing cell lifecycle.
   Progress may extend the stall window, never the absolute stop deadline.
   Handoff progress counts terminal adoption completions, including failed
   acknowledgements after ownership release; retry attempts are not progress.
   Successful adoption telemetry remains separate from that completion count.
   Forced cancellation supplies no graceful durability proof; report incomplete
   drain phases through `ShutdownReport` and return `NodeOutcome` to library
   callers. Process exits belong to the CLI boundary.
   Domain closure can race a task's current poll. Production actor selectors
   use `try_start`/`try_step` and fallible effect admission; cancellation must
   not become an invariant panic or a fabricated completion event.
   Reserve tracked work before releasing the task registry lock to dispatch
   it to an executor. Executor rejection can synchronously destroy work;
   never dispatch while holding that lock. Rejected async/blocking captures
   retain their original domain, and a racing close must retain cancellation
   and join coverage until every reserved task has released its resources.
   Preserve the core's typed fencing reason in `FenceReport` and
   `NodeOutcome::Fenced`. A closed reporting channel is a supervision failure,
   never evidence of an authority loss.
9. Idle, unowned cell routing history uses the actor's captured
   `max_cached_routes` limit. Reclamation cannot discard ownership, alarms or
   unfinished work. Every cell-table replacement maintains the recency index;
   direct retirement paths must refresh it. Stale completions and queued timers
   cannot affect a new request after its old routing record was reclaimed.

## Build and validation

The manifests use Rust 2021 and declare Rust 1.94.1 as the minimum. CI selects
its toolchain in `.github/workflows/native-sdk.yml`; inspect that file rather
than assuming the local toolchain matches CI. Dependencies are centralized in
the root `Cargo.toml`; member crates use workspace dependencies. Keep builds
locked and review any intentional `Cargo.lock` change.

On the development machine, place compilation artifacts on the mounted
workspace volume. Assign a separate, stable target directory to each checkout
or worktree, set it for each shell invocation, and confirm the volume is
writable. Do not reuse or clean another checkout's artifacts. CI may use its
runner's default target directory.

Run these commands from the repository root, with `<checkout-name>` replaced
by the current checkout's identifier:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
cargo format --check
cargo check-layout
python3 tools/check_native_dependencies.py
python3 tools/docs/check.py
cargo check --workspace --all-targets --all-features --locked
cargo test -p cellar --test native_cli --test native_artifacts --locked
```

`cargo format` (alias for `cargo xtask fmt`) applies stable rustfmt plus the
repository's declaration spacing. `cargo format --check` checks the same result
without writing source files; CI runs it before tests. The tool lives under
`tools/xtask` and covers tracked and unignored new Rust files, including examples.
Run `cargo test -p cellar-xtask --locked` when changing formatting or layout checks.
Plain `cargo fmt --all` remains available for rustfmt alone.

For Axum adapter changes also run both feature modes and the shared example:

```sh
cargo test -p cellar-axum --no-default-features --locked
cargo test -p cellar-axum --all-features --locked
cargo test -p cellar-axum-counter --all-features --locked
cargo check -p cellar-axum-counter --no-default-features --bins --locked
```

Read `crates/cellar-axum/README.md` for middleware, body and WebSocket lifetime
constraints. Cellar WebSocket handlers use `CellWebSocketUpgrade`; ambient
Axum upgrade tasks do not carry SDK authority. Each acknowledging body chunk
uses the existing output gate. OpenAPI is an optional immutable declaration
owned by the same application generation; export must not start a node.

Choose additional existing integration tests for the changed bindings, host
behavior, or storage paths. The full SDK CI gates are:

```sh
cargo test --workspace --all-features --lib --tests --locked
cargo test --workspace --all-features --doc --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

`cellar_internal_tests` enables externally supplied simulation/conformance
sources through `CELLAR_INTERNAL_*` and `CELLAR_CONFORMANCE_*` paths. Those
sources are not all checked into this repository. Do not enable the flag
without the corresponding harness, or claim those tests ran in a normal build.

Exercise user-facing changes through the CLI or a compiled application when
practical. The local counter smoke test is:

```sh
cargo run -p cellar --bin cellar --locked -- dev examples/native-counter/project.json
# In another terminal:
curl -X POST http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/counters/alpha
```

`tools/perf/README.md` describes isolated Docker/Colima validation of the
embedded `sdk::Node` path, actual RustFS recovery, rolling handoffs and returned
shutdown proofs. `python3 tools/perf/fleetcare.py` builds the separate
`cellar-fleetcare` application and runs its 10,000-cell read/write matrix in
embedded and daemon modes with Bucket and Fleet durability. Use
`--profile smoke --suite smoke` for a short functional check; the default
standard suite additionally verifies crash, store outage and cold recovery.
For a persistent local FleetCare console, use
`python3 tools/perf/fleetcare_console.py up`; it publishes the native package
with `fleetcare-publish`, starts three 1-vCPU/2-GiB nodes with private 4-GiB
disks, and serves the dashboard on a free loopback port, starting at 4174.
Its checkout-specific Colima profile and ignored `.cellar/fleetcare-console/`
state are independent of the benchmark. See `crates/cellar-fleetcare/README.md` for login, audit,
stop and resume commands.
Its separate `faults.py` driver exercises process failures
during writes with an application receipt ledger. Verify acknowledged receipts
before replaying ambiguous requests; otherwise retry can hide lost writes.
The driver's final audit rechecks saved ledgers from each verification stage.
Its isolation scenario disconnects a node from peers and its bucket; distinguish
the SDK's typed lease-expiry fence from a replication failure, and require joined
owned work with both caller executors alive before restarting that directory.
The peer-partition scenario changes only the owner's private network namespace:
peer paths fail in both directions, bucket paths and other peers stay reachable,
and the owner renews its lease without changing generation or cell epoch.
Keep image tags and compiled target caches private to the
checkout. Run the harness proof guards with
`python3 -m unittest discover -s tools/perf -p 'test_*.py'`.

The dev supervisor retains project state under `.cellar/<project-hash>` beside
the configuration, keyed by its canonical path. It does not migrate earlier
development-state layouts.
`--clean` intentionally resets that project's local state. Container examples
require Docker or Podman. See `examples/README.md` for application packaging and
other runnable examples.

`docs/README.md` indexes builder, platform, operations, and team documentation.
Keep source-backed behavior distinct from proposed practices and release-specific
qualification gates. `python3 tools/docs/build.py` regenerates the catalog,
offline explorer, and SVG sources; `python3 tools/docs/check.py` validates their
local references and coverage. Browser-only documentation scripts are independent
of the Rust application execution format. Use the optional preview renderer in
`tools/docs/render.py` only when generating PNG previews; it requires an existing
`agent-browser` installation and is not an application or CI dependency.

`cellar-native-examples` uses `examples/main.rs` to compile the focused modules
into registered applications; `examples/project.json` configures the combined
cookbook. Standalone native hosts keep `main.rs`, `project.json`, and their assets
in their recipe directories. Each recipe has a project config;
`examples/catalog.json` records sources, routes and prerequisites. Runtime smoke
suites use private temporary project state:

```sh
cargo build -p cellar --bin cellar --bin cellar-native-examples \
  --bin cellar-example-embedded --locked
python3 examples/focused_smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" --with-cron
python3 examples/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar"
python3 examples/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --project examples/team-services/project.json --suite team
python3 examples/platform_smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar" \
  --embedded "$CARGO_TARGET_DIR/debug/cellar-example-embedded"
```

`cellar-example-embedded` runs `sdk::Node` with local SQLite and checks returned
drain phases. `cellar-example-container-peer` validates the dependency-free Rust
peer built inside the example container. Native pattern equivalents do not imply
third-party SDK compatibility; keep gaps in `examples/ROADMAP.md` explicit.

`cellar-example-streams` is a separate bucket-backed `sdk::Node` host. It provisions
required stream retention, commits order state with `tx.emit`, composes projection
effects with inbox receipts and syncs before remote acknowledgment. Its explicit
retry endpoint plus preserving process restart demonstrate redelivery without
duplicate local effects. The private bucket also retains the randomly generated
fleet secret. Verify the real binary with `python3 examples/durable-streams/smoke.py
--binary "$CARGO_TARGET_DIR/debug/cellar-example-streams"`; this is a local demo,
not production crash/response-loss or multi-node capacity qualification.
Its separate `examples/durable-streams/faults.py` opt-in harness records an
independent fsynced ledger, drops a real source HTTP response and observes
SIGKILL with a pending source obligation and proven consumer effects before
ack. It audits before replay, preserves lease expiry, archives accepted envelope
digests/effect receipts and re-audits saved stages. Run its oracle guards with
`python3 -m unittest discover -s examples/durable-streams -p 'test_*.py'`.
This is single-node SQLite-bucket process evidence; it does not complete the
destination/follower/network/power/cut-point/capacity release matrix.
The serial Node suite also places destination, producer and consumer in distinct
execution domains. A real loopback peer tunnel proxy captures a proven append
reply, holds it through an independent source/destination audit and closes both
sockets. Retry bytes and acceptance match exactly; one destination offset and
one inbox-protected effect survive. The same proxy can lose a proven acknowledgment
reply: the consumer stores the exact batch receipt atomically alongside its
retained inbox effect before proof, then replays unchanged receipt bytes after
independently auditing checkpoint 1 and no live lease. Replay returns
`AlreadyAcknowledged` without another effect or checkpoint change. This is actual
network reply loss between embedded nodes, not separate-process/host, SIGKILL or
power qualification.
`examples/durable-streams/before_send.py` holds the source's actual input gate,
proves its command/obligation and SIGKILLs before the first dispatch attempt.
Copy the killed SQLite family and audit zero attempts before restarting, then
audit recovery before replay. Audit-only mode checks saved database/WAL hashes,
envelope identity and replay ordering; SHM read marks are mutable bookkeeping.
Keep the original handler/gate deadlines and complete final shutdown assertions.
`examples/durable-streams/fleet_faults.py` uses opt-in bounded loopback fleet
configuration to start separate destination/producer/consumer processes sharing
a private SQLite bucket. The proxy holds a proven append reply through audit,
SIGKILLs the destination and blocks automatic retries until recovered acceptance
is audited. Consumer transport ambiguity is audited before retry and preserves
original lease expiry. Saved evidence validates exact peer byte digests, process
roles/IDs and complete surviving/replacement shutdown. This is separate-process
single-machine evidence, not physical-host/follower/power/capacity qualification.
The same driver accepts `--rustfs-image` to create a fresh private Docker S3
server and bucket. It captures image/backend identity, retries only startup
CreateBucket 503s, injects separate no-retry conditional-write clients and removes
its own server after complete node shutdown. Credentials stay outside saved
configuration and ledgers. This adds network-store destination recovery evidence;
it does not prove follower-only acknowledgment, store loss or physical-host loss.
`fetch_faults.py` extends that process proxy to proven __streamNext response
loss. Runtime prefix audits must validate original state before permitting each
retry; audit-only mode requires complete evidence. Live mode explicitly records
a 1000ms fixture node lease, while expired mode retains 30000ms; both keep the
original 30000ms stream lease. Live replay requires exact request/response bytes;
post-expiry replay requires a fresh receipt and stable subsequent reply. Two
fetches apply one inbox effect, and only the saved receipt advances checkpoint.
Run both modes on SQLite and private RustFS when changing this boundary, plus
the Python guard suite. They do not qualify follower-only, power/store loss or
concurrently executing old owners. The optional fixture authority setting must
not change existing harness/default runtime policy.
`resumed_owner.py` verifies real SIGSTOP/authority expiry/successor claim/SIGCONT
with bucket proofs on SQLite and private RustFS. Before resume, the successor
must settle the original source under a higher epoch. The old process must
report NodeLeaseExpired before the original handler budget, fail its held
response, and retain zero old dispatch attempts/settlement. Audit before every
command/consumer replay, with complete survivor shutdown. Inspect archived
SQLite families only through disposable copies; read-only WAL opens can create
empty sidecars. These selected process cuts do not establish every stale-write,
physical partition, power/store-loss or follower-only proof requirement.
`follower_faults.py` selects real fleet durability, backpressures a source behind
required retention and refuses only producer replica uploads through an opt-in
ObjectStore decorator. Both put and multipart initiation must fail; this adapter
never supplies proof or changes lease/catalog/wake clients. Before producer
SIGKILL, independently inventory exact source bucket LTX bytes across the command
and observe a refused source upload plus original receipt/pending obligation.
Archive the killed source database/WAL through the shared immutable inspector;
the recovered accepted envelope must match its pending envelope digest.
Delete only the killed leader's private node data, observe actual authority
expiry and require a distinct successor/higher epoch to recover the original
receipt/obligation before any consumer effect or acknowledgment. Never replay a
source POST to repair this test. Runtime prefix and complete offline audits plus
survivor joins are mandatory. This selected source cut does not qualify follower
disk loss, destination append/fetch/ack follower-only cuts or physical host/power
loss. Existing fixtures keep their default proof/lease policies.
`--restart-followers --peer-proof bucket` kills every original process and keeps
destination/consumer leader writes bucket-backed while the source uses real fleet
proof. Recheck source bucket absence with all original memory gone; archive the
original ensemble's committed batches before any restart. Inspect current CLB1/
CLE1 headers, ranges and SHA-256 footers, not only the possibly zero state.json
seed. Require unchanged files, actual old authority expiry, distinct PIDs/new node
generations and recovery audits before effects/ack. Bucket observations include
the producer session's node-log bundle/recovered objects as well as source LTX;
either publication invalidates a follower-only claim. Recheck both archives at
completion. The all-fleet `--peer-proof fleet` variant remains unqualified: a
private S3 case deleted the destination's sole follower witness with the producer
disk and refused startup. Preserve that failed trace and do not treat the
source-only proof case as its fix or general disk/power qualification.
`fleet_faults.py --follower-only` instead isolates destination append proof with
bucket-backed producer/consumer leaders and the destination replica refusal.
Require unchanged destination LTX and original-session log objects, an actual
refused upload and installed fleet-ack marker. Delete destination-local data
after SIGKILL and observe old authority expiry. Keep the original append reply
held until the recovery prefix is independently audited: dropping it earlier
allows retries to acquire another route and repair that prefix. Then drop the
reply, release identical retries and verify one envelope/offset, effect and
checkpoint plus full joins. destination_proof.py enforces the extra proof gates
before dependent work and in saved evidence. `ack_faults.py --follower-only` and
`fetch_faults.py --follower-only` share those gates with their own method, restart
and recovery-audit identities. Arm after the accepted-event audit; refuse source
command replay after that point. Fetch keeps its original live/expired timing
policy and must preserve the exact live lease or obtain higher authority only
after expiry. Ack must recover checkpoint one and the exact saved inbox receipt
before AlreadyAcknowledged replay. Every consumer effect stays unique. These
cases retain live follower disks; other commit/proof cuts, follower disk and
physical host/power/store loss remain separate qualification.
For ack/fetch, `--follower-only --restart-followers` additionally kills every
original process, archives the destination ensemble's current CLB1/CLE1 batches
and checks unchanged destination LTX/session-log bucket data with all memory gone.
Delete only destination-local data; keep the original witness disks unchanged.
Start all replacements before awaiting readiness, require expired old leases,
new PIDs/generations and a saved recovered prefix before any request replay.
`destination_disks.py` checks custody during execution and rechecks archived bytes
offline. Returning witnesses may acquire stream ownership, so their advertised
routes also need paused exact-byte reply proxies until the recovery audit passes.
Require exact live receipt/reply or higher post-expiry lease authority, original
stored ack replay, unique effects and complete process/proxy/store joins.
This qualifies selected cold retained-disk ack/fetch cuts, not witness-disk loss,
cold pending-append recovery, consumer-local replay, or physical host/power faults.
`fleet_faults.py --follower-only --restart-followers` adds a selected cold append
cut. Returning peers keep their follower disks but enable a private exact-scope
`blocked_claim` client: only puts to that stream's `cells/SCOPE/own.json` fail
while `claim-block` exists. Preserve the original no-retry CAS store, reads,
other ownership/lease writes and actual follower replies. Observe at least one
real refused ownership write before starting the replacement destination.
Its ordinary claim/recovery must retain the accepted event. Require a blocked
identical append after both peers are ready and a saved original-prefix audit
before releasing the held reply or automatic retry. Sources may move between
returning peers; neither may own the selected stream during measurement.
Offline audit checks the exact private configurations/markers as well as witness
archives. This placement gate supplies no proof and does not qualify unrestricted
placement, missing disks or physical failures. A reset source socket can already
be disconnected when the measuring proxy drops its held reply: accept only
ENOTCONN at that shutdown call, retain other errors and join all remaining proxy
listeners before propagating cleanup failure.
`capacity.py` fills one opt-in Backlog source/lane to its captured row quota while
append traffic is paused. Overflow must roll back business state and sequence.
It audits before producer SIGKILL and before resumed retry, without source POST
replay, then uses resumable reads and a slow required consumer to verify every
acceptance, inbox effect and checkpoint. Unarmed peer errors must be forwarded
unchanged so advertised batch limits reach the dispatcher. Saved evidence must
pass the independent oracle and include complete final shutdown. RSS/residency
and live local file sizes are observations, not aggregate/bucket physical metrics,
memory SLOs, many-cold-source or long-soak qualification.
`many_sources.py` exercises independent Order sources beyond one host's resident
limit. Required retention, rather than only a network proxy, backpressures local
and remote appends even when source ownership moves. It archives the protected
prefix, audits remaining obligations, SIGKILLs every fleet process and starts all
replacements before waiting for readiness. No source requests occur before full
consumption. Its saved oracle and cleanup-failure guard are mandatory. Activation
retries only tagged network failures for its idempotent configuration or read-only
resolution/status calls, within the original activation deadline. It encodes the
authority/body once and uses a new dispatch identity per attempt to avoid prior
abort tombstones. General RPC still refuses ambiguous replay; protocol/authority/
codec refusals are not tagged for this loop. Preserve failed and passing evidence
in the implementation status: a passing replay alone cannot establish an earlier
failure's cause or justify relaxing readiness/durability/deadline/shutdown checks.
Startup boot/periodic wake discovery now waits for the captured generation's
event-resource activation before doing store reads or submitting cold hints.
Both embedded and CLI paths use `node/wake_discovery.rs`; predecessor recovery,
node authority and authenticated internal serving retain their existing order.
Failed/cancelled activation cannot start discovery; hosts without event activation
do not wait. Durable publications remain intact and normal discovery resumes
after readiness, including removed-class delivery. Gated reply task admission
uses try_spawn: refusal drops reply/value/gates in their owner and returns a closed
completion to the driver, without unproven output or a closure panic. The trace
identified startup source-wake pressure and the concrete alarm reply spawn caller;
untraced 144/192-source RustFS cases pass after these fixes. Wider maintenance
fairness and failed-startup/shutdown cut points retain their separate audits.
`whole_fleet.py` explicitly selects bucket proofs, audits one accepted event with
a proven unacked inbox effect plus one capacity-blocked source obligation, then
SIGKILLs all three processes. It inventories/discards only its private node data
after every kill is observed and audits bucket-restored receipts/state before
consumer replay. No source POST repairs recovery. Replay adds zero old effects;
ack releases capacity for the pending event and final shutdown is complete.
SQLite/RustFS results qualify this boundary, not follower-only/power/store loss.
`ack_faults.py` captures a proven `__streamAck` reply, audits checkpoint/effect/
receipt before destination SIGKILL and reply loss, then audits recovery before
exact stored-receipt replay. Opt-in consumer routes atomically save the receipt
with its effect and sync before ack; replay never fetches a new batch. A stale
pooled transport requires another saved audit before retry. Exact bytes and
`AlreadyAcknowledged` plus unchanged effect/checkpoint qualify this boundary;
consumer/follower/power/store loss and the full lease/ack matrix remain open.

`cellar outbox census [CLASS]` reads one bounded provider cell-directory page,
then performs up to four concurrent signed source-owner inspections, including
producers unseen by destinations. `census1` continuations bind fleet/prefix/class;
filtered pages still advance without an unbounded listing. Catalog/stream cells
are excluded, other framework roots stay candidates. Rows distinguish current
source, empty and unavailable; partial failure prints its cursor and returns a
failed exit. Metadata excludes lanes/payloads/tokens. Cold inspection can restore
and progress durable work. This is live root-cell census, not a snapshot/drain
proof, facet/historical-source inventory or aggregate gauges. The RustFS CLI
smoke is `census_smoke.py`; keep census scope distinct from destination watermarks.


## Change discipline

Use a single active PR against `main` for ongoing outbox, stream and subscription
work. Reuse that PR's head branch for follow-ups; do not create stacked or
successor PRs while it is open. After it merges, start any next PR from updated
`main`. Do not base PRs on deletable feature branches. When consolidating,
preserve all reviewed changes, close superseded PRs only after the surviving
PR contains them, and record the consolidation in its description.

The dashboard is an optional management application, separate from native cell
execution. Browser JavaScript does not authorize adding a JavaScript engine to
the Rust workspace. `fleet/management/` and `fleet/operator.rs` supply shared
metadata observations and owner-routed requests. The console must not open cell
SQLite files or expose the private peer listener. `cellar-admin` owns its separate
audit database; authentication, permissions and fleet configuration are explicit.
Node stop acceptance is not shutdown proof. Signed stop requests bind the node,
generation, requested mode and operation identity. Final receipts in the fleet
bucket require the returned drain report and an explicit execution-domain join;
CLI hosts also join their dedicated core thread. A lost reply or failed receipt
publication remains unknown. Audit reconciliation must preserve a terminal proof
even when an earlier acceptance response arrives later.
KV console inspection and conditional edits live under `application/kv/inspection.rs`
and `fleet/management/kv.rs`. Every namespace put assigns a persisted revision;
compare it inside the owner transaction, including expiry after blob publication.
Conditional console requests also pin the inspected binding identity across deployments.
Console edits preserve exact expiry and storage tags, and distinguish absent
metadata from JSON null. Definite revision conflicts are rejected; lost write
replies remain unknown. Keep the existing unconditional SDK/CLI signatures.
See `plans/003-fleet-dashboard.md`, `plans/004-sql-kv-workspaces.md`,
`plans/005-resource-inspectors.md`, and `plans/006-event-console.md` for
implementation progress and release gates.

For dashboard changes, also run:

```sh
cargo test -p cellar-admin --locked
npm --prefix dashboard ci
npm --prefix dashboard run build
npm --prefix dashboard run lint
npm --prefix dashboard run test:ui
```

Browser fixture tests do not prove live-fleet behavior. Operator and lifecycle
changes additionally need the corresponding SDK and isolated fleet tests.

- Read the changed module, its callers, adjacent implementations, and relevant
  tests before choosing a fix. Verify dependency contracts from their source or
  official documentation when the behavior depends on them.
- Make one clear implementation of the requested behavior. Keep unrelated
  cleanup out of the change, and avoid speculative options or extra adapters.
- Preserve public Rust APIs during file-only reorganizations. Update Cargo
  entry points, module paths, embedded-file paths, and documentation together.
- Follow surrounding Rust style. Propagate recoverable failures with `Result`
  and useful context; do not introduce input-triggered panics. Avoid blocking
  the actor or holding synchronous locks across `.await`.
- Separate import blocks, types, implementations, and functions (including
  methods) with a blank line. Keep documentation and attributes attached to
  their declaration. Expand dense configuration, closures, and macro payloads
  into readable lines. Use `cargo format` to apply standard layout and declaration
  spacing together.
- Use `asyncrt::select!` for fair polling. A biased select needs a reason tied
  to lifecycle ordering. Never bypass the forbidden macros in `clippy.toml`.
- Tests should exercise behavior and regressions. Do not weaken checks, alter
  expected results, or suppress lints merely to hide a failure.
- Keep credentials, deploy tokens, `.dev.vars`, and live fleet data out of
  commits and tool output. Use local test stores for destructive checks unless
  the task explicitly authorizes an external fleet operation.
- Preserve license and attribution notices when moving or adapting source.
- Before handoff, inspect the diff, run checks suited to the changed surface,
  and report the outcome plus any unverified behavior. Commit or push when
  requested, keeping the user's unrelated work intact.

This guide follows the organizational approach of
[Crab's AGENTS.md](https://raw.githubusercontent.com/crabbuild/crab/refs/heads/main/AGENTS.md),
with Cellar's architecture and commands derived from this repository.


Fleet root metrics use a separate scope/method-bound platform StateAuthority and
`runtime/event_state.rs`, under ordinary owner/output/discovery gates. Native
sampling bypasses application invocation, including removed-class recovery, and
reads indexed source counters and at most 129 live subscription metadata rows.
Deleted stream partitions remain observable and have a separate retired count.
Source/stream gauges and root SQLite allocation are canonical decimals; aggregate
sums use checked u128 and ages take maxima. All owned markers remain 1.
`cellar event metrics` scans at most 100 provider prefixes per page and probes four
owners concurrently, within its max-roots/page bound (default 10,000; cap 100,000).
Duplicate/invalid prefixes, failed probes, failed/repeated listing tokens or an
exhausted bound make collection incomplete. Incomplete Prometheus emits coverage
only; JSON puts partial values in observed_gauges with gauges=null and a failed
exit. Complete means a successful live root-directory traversal, never a snapshot
or drain proof. IDs are absent from global labels. It does not include facet
SQLite/WAL/LTX/bucket physical bytes, historical producer incarnations, or resident
host gauges. `census_smoke.py` also exercises actual RustFS CLI state collection,
protected/undispatched state, caps and unavailable owners. Keep these scopes explicit.


`cellar event inventory` discovers one bounded provider page of root descriptors,
with four signed owner probes and an inventory1 cursor bound to fleet bucket/
prefix/endpoint/region. Invalid, duplicate or unavailable roots remain explicit;
partial pages print their continuation and return failure. Catalog descriptors
recover namespace identity from scalar retained rows and validate its hashed scope;
empty catalogs cannot invent a namespace. Stream descriptors recover only resource
identity; ordinary roots expose current incarnation and receipt-history availability.
`event inventory SCOPE --page resources|producers|history` reads at most 101 scalar
rows for 100 returned records. A distinct exact-scope/method InventoryAuthority and
native framework handler precede application invocation and retain owner/output/
discovery gates. Resource/producer pages share existing validated range readers.
Source history projects retained operation identities, source incarnations,
recreation predecessor, lifecycle and loss counts without full receipts/operator
reasons/credentials. History1 binds root scope and exact sequence tail and remains
valid across supported recreation because the receipt ledger is retained.
Stream retirement deliberately purges producer metadata after terminal tombstones;
producer replies carry retired/cleanup_complete so an empty terminal set is not
mistaken for an active producer census. Pages are live, not snapshots/drain proofs.
Facet inventory, every historical restore/backend/ownership/capacity cut and
complete release qualification remain separate work. All owned markers stay 1.


Subscription fetch acceptance compares the receipt with the identity resolved for
that fetch, including subscription UUID and incarnation. Raw and leased reads
share the bounded envelope decoder: remaining caller bytes bound base64 allocation,
and family/source/incarnation/lane/event identities are independently checked.
Leased replies must match the exact contiguous receipt range and a nonnegative,
positive lease interval within captured lifetime limits. Unknown application event
schemas remain readable; processing authority never depends on application decoding.
Negative extension deadlines and retry delays outside host policy fail before RPC.
All owned markers remain 1; full codec, cancellation, lifecycle/fault qualification
remains tracked in plans/outbox-implementation-status.md.


Wake projection admission rejects previous alarm-only __sys_cell_wake_* triggers
through wake_record::check_schema before base-schema DDL, journal pragmas or writer
epoch updates. It never drops/replaces those triggers as an upgrade. Recreate that
development layout; all markers remain 1. Current projections preserve same-epoch
publication identity and consumed tombstones; a new owner advances the epoch while
retaining the monotonic sequence and independent alarm/work deadlines.


SDK event administration independently validates response metadata. Configuration
acceptance binds resource/operation and active definition revision/digest; pending
creation requires revision 1, while status may report later operations. Lifecycle
receipts bind action/reason/operator and identities, validate state/incarnation/
checkpoint/loss-range consistency, and retain exact decimal positions. Audit pages
allow monotonic sequence gaps and multiple authorized principals but enforce
bounded counts/receipt bytes, scope, unique operations and exact continuation.
Terminal retirement replies validate retained range/count/byte accounting; pending
intent can precede captured loss metadata. Retry responses require canonical,
nonnegative deadlines consistent with delay. All owned markers remain 1. These
checks do not establish full transport allocation/cancellation or failure-matrix
qualification; track remaining evidence in plans/outbox-implementation-status.md.


Shared event reply acceptance consumes exactly one ok/error branch; contradictory
or undecodable submitted results are Ambiguous. Wire bytes are capped at 64 MiB
before JSON/structured decode; encoded error metadata is capped at 16 KiB and
message text at 4096 bytes. Valid typed errors retain their exact metadata.
Do not clone a whole success/error object or reflect malformed body text in an
error. Peer event/control body collection uses that same wire ceiling and the
original route operation deadline. Coalesce frames into one bounded buffer, drop
unused trailers and yield bounded frame/byte work so ready frames cannot starve
cancellation or deadlines. Size/timeout refusals are Ambiguous without
RemoteRpcTransportFailure; genuine body I/O errors retain the activation-only
network tag. Cancellation wins undelivered body/deadline ties and releases the
body. The proven response-head abort disarm remains intact; collection failure
cannot kill the handler's later waitUntil work. Full request-phase allocation and
cancellation qualification remain in the implementation status.


Entered event/control RPC dispatch uses `node/event_wait.rs`: one captured absolute
budget covers routing, retry/invalidation, local execution/output gates, submission,
response head and body. Cancellation wins delivery/deadline ties. Preserve explicit
never-connected/stale outcomes; an unclassified submitted timeout is Ambiguous,
without an activation network retry tag or any rollback claim. Local activity is
owned before constructing its wait future. Proven-head abort disarm is unchanged.
SDK source calls capture their host budget before the egress gate and retain it
through queue/result waiting. Expiry before submission is Unavailable; after
submission it is Ambiguous and drops the exact request cancellation guard. The
full cut matrix remains separately tracked; do not call this complete end-to-end
deadline qualification.
Post-submission/refusal deadline fixtures must establish their named state before
installing the wait. A 10 ms budget may expire before an unpolled fixture begins;
Unavailable with zero effects is then correct, not a lost committed effect.
Poll an owned pinned operation to its pending checkpoint and preserve its drop
guard; never extend the deadline or change the expected ambiguity to hide setup
jitter. Keep both expired-before-submission/no-effect and submitted-effect cases,
including wait installation after the original deadline. These synthetic wait
tests do not replace the storage/process proof cuts.


Event RPC deadline budgets include the canonical, nonnegative `__streamNext`
wait when it is within the captured host polling policy, plus the host transport
budget. That combined absolute deadline is captured once per source/entered-node
operation and is never reset at response headers or a retry. This preserves
empty-poll results when the default 30-second wait exceeds the 15-second transport
budget. Budget inspection accepts at most 16 KiB and bounded structured objects/
values/depth, reads only the array/body/wait field without alias expansion, and
never grants extra time to malformed or over-policy waits. Preserve saturating
addition for captured maximum budgets. All Cellar-owned markers remain 1.


`docs/26-event-delivery.md` is the SDK event-delivery contract and developer entry
point. Update it, the API map, runnable durable-streams example and generated
asynchronous delivery flow when changing this surface. ErrorCode::as_str/Display
use canonical wire names. DeliveryError::is_permanent describes automatic outbox
lane blocking; it never substitutes for an operation-specific replay contract.


Use event_stream::error::submitted_result only around validation of an already
submitted mutation/fetch success body. Invalid acceptance metadata is Ambiguous
and must not become a definite refusal or reflect the invalid body's context.
Local preflight and call_bound/call_authorized broker error branches bypass this
boundary so valid typed refusal codes and metadata remain intact.


Event activation joins a heap-pinned dispatch_rpc_call future with its reply
receiver in node/event_resources.rs. Retain that boundary: embedding the whole
routing/execution/deadline state inline propagates a large poll frame through
activation and startup in unoptimized builds. The normal-stack catalog inventory
Node integration covers this path; do not compensate by enlarging worker stacks.


Embedded startup must observe both generation event readiness and its supervisor.
prepare() marks activation pending before the driver first polls activation; its
cancellation guard therefore cannot cover every early driver exit. Keep
wait_for_event_activation on the production Node::start path, reject completed
supervisors even when readiness is simultaneously ready, and preserve close/join
on activation failure. If readiness fails first, retain the supervisor's joined
outcome alongside the activation error; preserve typed supervisor/join error chains.
A generic activation refusal cannot replace a fence reason or component failure.
Focused embedded tests force that branch by keeping supervision pending until
closure, without wall-clock timeouts. This diagnostic preservation does not explain
the separate intermittent bootstrap/recovery cases.


A stopped remote attempt queues invalidation of its observed route without
waiting for actor cleanup. Its Ambiguous verdict never authorizes replay; the
next independent call can resolve a replacement endpoint. Ready retries still
await invalidation under their captured deadline. Keep the regression that polls
the stopped future once, checks exactly one invalidation and no queued replay.

The SQLite development object store keeps HEAD metadata body-free. Ordinary GETs
capture bodies of at most 128 KiB in the same SQLite statement as their metadata,
so lease renewal cannot invalidate an already returned small response. SQL CASE
must exclude large BLOBs before extraction, not truncate them after allocation.
Large streamed/ranged reads use at most 128 KiB per incremental BLOB operation,
checking the captured ETag and length in that operation's read transaction; a
replacement refuses the large stream rather than mixing versions. Captured small
bytes and native body work belong to the response's original execution domain even
when another domain consumes it. Closed admission and undelivered completion
preserve typed DomainClosed, and shutdown joins entered SQLite I/O. Entered-body
fixtures use a body above the atomic cap to retain their real SQLite blocking seam;
keep original context, refusal, reservation and join assertions. No connection or
transaction is held while a body is idle. Isolated HEAD/range/full-read RSS fixtures populate
their 80 MiB object in a parent process and keep the fixed 16 MiB growth budgets.
Reopening the BLOB for each chunk has a measured large-read throughput cost;
these memory passes do not qualify throughput, eager copy/multipart operations,
provider inventories or total retained-bundle recovery memory.

Production graceful close and live-cell folding use gather_uncovered_bundles,
with at most 16 concurrent object transfers per window into anonymous scratch
under the replica's captured filesystem/data root. Only file handles remain
between transfer and decoding. Verify the complete CLB1 CRC, exact payload
partition, L0 header identities and unique row identities before exposing a
bundle's rows. Scratch parsing/copying uses 128 KiB chunks; a second payload copy
must match its first digest, and the reread footer must match what was parsed.
The bounded external gather index checks original descriptor tags and payload
digests again, including after its uniqueness scan rewinds. Keep bundle-over-
follower precedence and fail the whole gather on late scratch failure. A cached
loss diagnostic never authorizes an empty scan or seal; native_log exercises the
actual close caller and requires the original open folded log unchanged.
Native log fixture Result errors and failed setup join source/recovery/follower
domains before directory cleanup. Preserve five-second proof/recovery guards,
captured publication policies and original crash/proof assertions; failure-only
stage context is not a runtime timeout repair. Live folds and graceful close share one node-local scan reservation. Acquire it
before follower staging and retain it through the derived Rows/SortedRows spool,
including uniqueness rewinds and asynchronous upload consumption. Entered scratch
file work, returned scratch handles and SQLite metadata/body workers retain that
reservation after caller cancellation; an async-future-only permit is insufficient.
The opaque object-store ReadGuard is capacity retention, never execution or proof
authority; the original domain admission/result checks still apply. The indexed
two-cell memory regression verifies each original mixed-bundle row before its
unchanged RSS budget; native blocking cancellation cases require both admission
and shutdown to remain pending until real entered I/O joins. Qualification of this
candidate is tracked in the status ledger. Diagnostic collectors, dead-session
bundle reads, GC/overlay helpers, metadata inventories and one legal oversized
upload row retain separate memory/capacity gates. All owned markers remain 1.
