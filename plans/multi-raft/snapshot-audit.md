# Physical snapshot boundary

MR-10 is in progress. The [native progress evidence](evidence/MR-10-native-progress.json)
pins source `08b2bca3da32982795994d0240844324437ff0c7`: 304 Rust cases pass across
17 binaries, including 26 snapshot, 18 read and all 88 public Node cases. All 21
commands pass, with executable/log digests. Clippy covers the library and all
affected native/integration targets; cellar-ltx passes both backend feature modes.
Format checks 631 Rust files. Layout, dependency policy, docs, 17 plan tests and
the reviewed 68-anchor preflight pass. Seven ignored process children are invoked
by owning parents and excluded from counts.

The [cancellation record](evidence/MR-10-abort-progress.json),
[peer-admission record](evidence/MR-10-inbound-progress.json) and
[earlier physical record](evidence/MR-10-physical-progress.json) preserve their
separate historical source qualifications. Native existing-replica catch-up is
implemented; fresh-member authorization, S3 seeding and the complete native
recovery crash matrix remain. All 17 delivery packages remain in scope.

## Identity and bytes

`SnapshotPackage::capture` selects an exact synced applied cut while its immutable
published generation is pinned. Its marker-1 descriptor binds cluster, namespace,
cell, group, incarnation, immutable birth, application schema, included index/term,
canonical ConfState, image name/digest/length/LTX state and the encoded full-LTX
payload length/digest. Canonical JSON and complete protobuf comparison refuse
unknown fields, alternate encodings and mismatched metadata. Source reads verify
the full image SHA-256 before encoding.

A package pins its source generation through transfer/checkpoint work. Its
`checkpoint_material` carries only bounded metadata and the verified retention
capability into `RaftRuntime::checkpoint`. The runtime refuses busy Ready/apply
state immediately rather than queueing behind an apply the caller might block.
A dedicated phase persists compaction through the same batched node log owner as
Ready writes. Admission is not durability; `Checkpointed` follows sync and reference
handoff. The mechanical `checkpoint` write remains restricted to quiesced fixtures.
The native host schedules periodic checkpoints through the SQL owner and runtime;
a busy checkpoint retries without claiming compaction progress.

`SnapshotDownload` verifies the descriptor before buffering, accepts only ordered,
nonempty chunks within the declared length, and admits at most 128 MiB encoded,
64 MiB materialized and 1 MiB per chunk. Individual buffer growth uses fallible
exact reservations. Physical LTX checks bind the full image state. Generation
publication syncs bytes, renames and syncs the directory. Repeated downloads adopt
an exactly matching existing image; a colliding name never overwrites other bytes.
Native source and receiver banks each admit one body owner per node. Source
reservation precedes encoding; the receiver charge survives sealing and publication.
Transfer deadlines and joined shutdown bound their lifetimes.

`SnapshotPackage::retained` verifies and pins the exact current durable checkpoint,
including when a newer committed image has become applied. Re-encoding must match
the descriptor's length and digest. A replaced descriptor is refused rather than
silently serving a different cut. A captured package remains usable through later
checkpoint replacement and reclamation; an empty checkpoint has no image or bytes.

The materialized image cap stays 64 MiB. `ltx::full_snapshot_bound` includes worst
case block compression, page framing, index varints and file framing. Snapshot
and native image-restore defaults allow 128 MiB encoded; archive defaults allow
128 MiB per object and 256 MiB per recovery set. Configuration refuses a snapshot
budget below that conservative bound. Native host admission checks the image
restore and archive budgets before opening member storage.

## Publication and retention

`recovery.rs` reads coherent engine snapshot/applied references and refuses mixed
images at the same cut before reclamation. A strict empty descriptor carries no
image or payload and preserves the exact configuration/index/term. It installs
without inventing a SQLite file; the first committed LTX command can create the
initial image afterward. Empty capture refuses an existing committed image, and
installation cannot erase committed data.

A prepared image produces `SnapshotCompletion` only for the exact local runtime
ticket, snapshot protobuf digest and included index/term. The existing runtime
persists the snapshot, applied marker, configuration and required HardState before
emitting `SnapshotInstalled` or releasing dependent messages. Physical completion also
supplies an opaque verified reference bound to the exact ticket/digest/image;
mechanical fixtures explicitly use no such capability. `PreparedSnapshot`
checks the durable engine snapshot and exact applied image before publication;
sealing bytes or merely submitting a completion does not make state queryable.

`ImageApplier` separately retains the durable snapshot generation and the latest
applied generation. Recovery validates/adopts both before retiring other inventory.
At an equal included/applied index, the image markers must agree; valid individual
files cannot excuse an incoherent engine checkpoint. Normal data publication
refreshes the snapshot reference before retiring the previous head. A newer synced
checkpoint replaces the old snapshot reference while old reader/transfer pins
continue to protect physical bytes. Retained references survive process-local pin
release and restart.

## Cancellation before install authorization

`RaftRuntime::abort_snapshot` accepts only the exact pending local ticket before
install authorization. It uses reserved physical-completion admission. Admission
revokes late install completion immediately; repeated cancellation remains
idempotent while pending. It proves no durable progress by itself. If an older
physical apply already owns a ticket, cancellation waits for that same owner and
persists its completion rather than discarding or reissuing the effect. Consuming
that completion into the abort write also prevents duplicate completion during sync.

The serialized abort write preserves the old durable commit, snapshot and applied
configuration, while persisting RawNode's required current term/vote and the joined
older apply marker. RawNode's unstable snapshot commit must never be copied into
this write. Only after sync does the runtime emit the older `Applied`, then
`SnapshotAborted`, and rebuild RawNode from the coherent stable state. It drops
snapshot-dependent messages and unstable state; durable committed entries can
replay normally. Operation capabilities remain monotonically allocated, so the
same snapshot bytes need a fresh ticket on retry. Once `complete_snapshot` is
accepted, cancellation is refused and the caller must join the install outcome.
A stale-completion error does not establish that an install failed.

Three real SQLite/engine tests cover same/higher-term voting across cancellation,
old checkpoint/image retention and restart, late completion refusal, joining a
held older physical apply, duplicate completion refusal during held abort sync,
retry installation and refusal to cancel an authorized install. Failed abort sync
releases neither completion nor dependent acknowledgement; after joined shutdown,
recovery retains the old committed prefix and a coherent old/new HardState. These
are component checks. Native drills extend cancellation to held partial transfers,
retry and joined shutdown; the complete native process-crash matrix remains.

## Peer admission during pending image work

The transport now separates authenticated frame admission from group execution.
The serialized worker checks membership/message bounds before queueing and again
at execution. Each queued body retains its original node charge plus a group
reservation. Per-group defaults allow 32 requests and 20 MiB plus 4 KiB; the
runtime options document their relationship to node/message limits. A stalled
group's frame can be admitted or refused promptly while other groups continue on
the same pooled control connection. Public embedding `step` retains its original
execution-waiting semantics.

Sender unreachable and snapshot-status hints have separate admission of 256 slots
and 1 MiB, independent of ordinary work and physical completions. The worker
coalesces hints to one small record per current member, preserves failure until
execution, retains the latest ordered snapshot result and prunes retired peers.
Member validation occurs before coalescing and at execution. Queue contention
retries on the owning clock within transport cancellation/deadline; it never waits
for a group's image work. Hints run before deferred bodies when the group becomes
ready. Native immediate send refusals also retain snapshot-failure hints. Neither
hint admission nor execution invents a Raft response or install acknowledgement.

Physical apply/snapshot completions have eight reserved queue slots and 32 KiB
independent of ordinary requests. Full ordinary admission cannot prevent owned
physical work from completing and refunding deferred-message reservations.
Frame admission still proves neither execution nor snapshot installation.

The transport fixtures hold a mechanical snapshot completion while another group
advances over actual TLS. They cover group item and byte saturation, execution
refunds, joined shutdown preserving the old durable prefix, and reserved snapshot
completion after eight groups fill all 256 ordinary request slots. A bidirectional
pending-snapshot fixture also checks sender Finish/Unreachable hints and healthy
second-group progress. These fixtures
qualify scheduling/engine barriers; physical SQLite installation is covered by
the separate snapshot target. They do not implement native snapshot RPCs.

## Verification and remaining work

The owning target is `native_raft_snapshot`. Its tests cover actual runtime-ticket
installation and duplicate-completion refusal, premature publication refusal,
wrong history/schema/configuration/position, corrupted or incomplete transfers,
name collisions, old readers, compacted boundary terms, committed replay with an
uncommitted suffix, independent snapshot retention through restart and mixed
snapshot/applied-marker refusal.

The process-kill matrix covers seven cuts: before image sync, after rename, after
image-directory sync, before engine sync, after engine sync, after the engine
reply and after image publication. Recovery must select the coherent previous or
installed state and reclaim interrupted unreferenced images. The before-engine-
sync case permits either coherent state because process kill does not simulate
power loss; a completed sync requires the installed state.

The generation owner's bounded monotonic references are independent of ordinary
retirement. Before storage submission, a prospective reference protects the
candidate while the old durable reference remains selected. Sync establishes the
durable reference and releases the prospective one before completion or dependent
messages. Failed/uncertain I/O preserves the candidate through producer shutdown;
recovery selects the engine's coherent old or new checkpoint. Closing an in-flight
runtime batch settles reference handoff even when its event consumer is gone.

The owning target now additionally exercises delayed checkpoint/newer apply/late
retirement/restart, lost completion, failed sync with joined producers, stale engine
material, busy-apply refusal, empty configuration installation/first LTX/restart,
and refusal to erase committed data. The pinned peer-admission evidence qualifies those component and owning regressions.

The boundary fixture adds an exact 64-MiB real SQLite image with seeded random
overflow pages and a passing integrity check. Its encoded LTX exceeds 64 MiB.
It retains the older checkpoint after a newer LTX update, transfers that exact
cut, and restores the newer snapshot-plus-tail archive after both node directories
are deleted. This is a component test over the in-memory object store, not native
member repair, physical S3 or evidence of a 64-MiB command envelope. The per-command
envelope limit is unchanged. Additional cases cover replaced checkpoints, capture
lifetime through GC, empty retained checkpoints and insufficient encoded budgets.

## Native recovery path

The host captures checkpoints after the configured committed-entry threshold,
reserves capture capacity before encoding and retains the exact source generation
before sending MsgSnapshot metadata. Separate member-only TLS pulls carry ordered
chunks with route, request, descriptor, transfer, offset and body-digest bindings.
Each transfer has a fixed deadline; its body connection cannot consume control or
query capacity. Routes and current membership are checked for every chunk.

The receiving SQL owner seals the verified image before returning the exact
SnapshotCompletion. Runtime installation syncs image/configuration/index/term
selection before the host publishes the image and releases waiting queries. A
snapshot overlapping an older physical apply aborts, joins that owner and retries
with a fresh ticket. Private command producers retain their ordinary uncertain
outcome handling. Empty configuration snapshots need neither a body lease nor a
socket. Delivery failure releases only its matching source lease.

Three native drills use real public Nodes, three voters and two groups. They
compact away an isolated follower's missing prefix, heal the connection, install
a checkpoint through 257-byte authenticated chunks and serve the retained AtLeast
token with no follower business callbacks. A response held after initial chunks
proves incomplete transfer cannot satisfy that read. Transfer timeout/retry and
shutdown join occur while a second group keeps writing. Startup separately refuses
insufficient encoded, generation-slot and image-copy budgets before creating any
catalog or member state. These drills use small SQLite images; the 64-MiB boundary
remains qualified by the separate component fixture described above.

Broader qualification found a dropped ReadIndex reply could strand a linearizable
read under unchanged leadership. A direction-specific fault test reproduced it.
The host now retries after four poll intervals clamped to 100–1000 ms, keeping the
original invocation/deadline and requiring a real safe ReadIndex result. The
forced-loss test checks unchanged term/leader; all 18 read cases pass with the
snapshot feature enabled. No cached status substitutes for quorum proof.

Still required for MR-10:

- Authorized fresh-member admission and S3/peer seeding through a surviving live
  group. Preserve committed member/node/disk bindings across compaction and never
  reuse a lost voter's identity or restore its votes from an archive.
- Explicit native quorum-loss restore refusal with available archived cell bytes.
- Complete native process-crash, recovery/readiness and image-pressure fault
  qualification, followed by pinned package completion evidence.

Existing-member recovery does not create members, reset stable votes or provide
disaster restore. Blob/effect capabilities remain unsupported; their external
dependency closure must join snapshot installation before those capabilities can
be enabled. Local TLS and in-memory object-store fixtures do not qualify physical
S3, power loss, zones, production throughput or larger cluster capacity.
