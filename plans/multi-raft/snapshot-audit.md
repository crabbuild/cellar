# Physical snapshot boundary

MR-10 is in progress.

The [native S3-seed qualification](evidence/MR-10-s3-progress.json) pins source
`9fac03a5b3771d51bb081bbc52b97645454174ec`: 210 Rust cases across 12 binaries and all 21 commands pass.
This includes 40 snapshot cases, 29 archive, 18 read, six protocol contract,
22 library cases, owning Raft regressions and the affected public Node lifecycle.
Clippy, both cellar-ltx backend modes, format (638 Rust files), layout, dependency
policy, docs, 17 plan tests, all 30 required snapshot names and 68-anchor preflight
pass. Seven ignored child entry points run through owning parents and are excluded.

A fresh safe ReadIndex authorizes an exact immutable S3 manifest. Metadata
selection avoids payload downloads and proceeds while a private writer is busy.
The target verifies and physically replays the full image and committed LTX tails,
seals a local image, syncs group/configuration/bindings/applied state with vote zero,
then reconciles its permanent claim. A missing selected payload refuses startup
even when a newer complete head exists. S3 bytes alone cannot authorize a seed.

Public Node restart now starts the joined learner before voters. Verified causal
reads remain available; strong reads refuse until quorum returns. Three claim
failure cuts cover a lost successful response, failure before the PUT, and a lost
confirmation after a successful PUT. Restart reconciles exact synced state and
claims without resetting votes, reseeding or rerunning command callbacks; later
replication resumes.

The internal identity is `cellar-raft-ltx-1/members-1/seeds-1/s3-1`, with numeric
format markers still 1. Earlier checksum-valid roots are refused; use a fresh
experimental namespace/prefix and local state. MR-10 remains IN_PROGRESS for the
remaining fresh-seed image/engine process-kill matrix. MR-11 through MR-17 remain
open. Object storage in these fixtures is in-memory; no physical S3, power-loss,
production-scale, federation or complete release qualification is claimed.

The [member-binding progress](evidence/MR-10-bindings-progress.json) pins source
`88b05883acaba7881fcf30fc0d06e0dc2474725c`: 198 Rust cases and all 21 commands pass,
including 30 snapshot, 27 archive, 18 read, six protocol contract and 22 library
cases, owning Raft regressions and the affected public Node Raft lifecycle.
Clippy, both cellar-ltx backend feature modes, format (633 Rust files), layout,
dependency policy, docs, 17 plan tests and 68-anchor preflight pass. The snapshot
inventory includes all 20 required cases. Joined node/disk bindings and retired-ID
high-water now survive journal sync, snapshot transfer, compaction and restart;
archives verify their immutable birth IDs. Stale/unbound/rebound proposals, direct
fresh voters, empty-learner promotion and mixed member/snapshot markers are refused.

That earlier cut changed the internal namespace protocol identity to
`cellar-raft-ltx-1/members-1` while keeping format markers at 1. Earlier namespace
roots are refused; use a fresh experimental namespace/prefix and local Raft state.
There is no compatibility reader or migration. Binding observations do not grant
fresh-member admission. At that cut, native live-quorum authorization, peer/S3
seeding, claim reconciliation and the native crash matrix were still outstanding.
MR-11 through MR-17 remain open.

The earlier [native progress evidence](evidence/MR-10-native-progress.json)
pins source `08b2bca3da32982795994d0240844324437ff0c7`: 304 Rust cases pass across
17 binaries, including 26 snapshot, 18 read and all 88 public Node cases. All 21
commands pass, with executable/log digests. Clippy covers the library and all
affected native/integration targets; cellar-ltx passes both backend feature modes.
Format checks 631 Rust files. Layout, dependency policy, docs, 17 plan tests and
the reviewed 68-anchor preflight pass. Seven ignored process children are invoked
by owning parents and excluded from counts.

The [quorum-loss follow-up](evidence/MR-10-quorum-progress.json) pins
`ad5818a1ba4d68f0cb1ca36890e34f819eadd5a1`: all 27 snapshot cases and 12 commands pass,
including all 17 required inventory names. Only the snapshot integration test
changed since the separate 304-case qualification. A three-voter group archives
an acknowledged command, then loses two consensus owners. Available archive bytes
cannot establish a command acknowledgement or strong-read proof. Physical restore
into three blank replacement directories still refuses an old claimed identity,
a different disk and an unassigned node; every failed attempt leaves no local
group/voting state. Explicit disconnected historical reads remain coherent.

The [cancellation record](evidence/MR-10-abort-progress.json),
[peer-admission record](evidence/MR-10-inbound-progress.json) and
[earlier physical record](evidence/MR-10-physical-progress.json) preserve their
separate historical source qualifications. Native existing-replica catch-up is
implemented. The current cut also implements authorized peer/S3 seeding and
public joined-node restart; the fresh-seed image/engine crash matrix remains. All 17 delivery packages remain in scope.

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

Physical apply/snapshot completions have eight reserved queue slots and 128 KiB
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

## Committed member bindings

`member_state.rs` binds fresh IDs to node/disk identities at committed learner
admission. Configuration intent is canonical, bounded and tied to the last applied
configuration index. Stale requests, unbound new IDs, direct new voters, rebinding
and empty-learner promotion are refused before append. Member IDs increase beyond
a retained high-water; retiring a binding never makes its ID reusable. Joint
outgoing bindings survive until the group leaves the joint configuration.

The group journal, peer descriptor and S3 manifest all retain this state. Image
completion carries the exact ledger into the runtime's install batch; recovery
refuses mismatched snapshot/member markers at an equal cut. Retained older
checkpoints still describe their own configuration while newer applied bindings
remain independent. Archive reads and publication compare the immutable birth ID
set, and data/query/snapshot transport validate joined node/disk identity.

Physical completion capacity is now eight slots/128 KiB; each snapshot completion
reserves 16 KiB for the bounded ledger and existing references. Group/capture
accounting includes spare binding allocation retained during retirement. Format
markers remain 1. The protocol identity changes to
`cellar-raft-ltx-1/members-1`, and a checksum-valid root from the earlier protocol
is refused. This requires a fresh experimental namespace/prefix and local state;
no compatibility reader or migration is supplied.

That earlier record is an admission/recovery foundation. The ledger itself
supplies no live-quorum seeding capability. The current source adds authenticated
peer/S3 authorization, permanent-claim reconciliation and public joined-node
restart; fresh-seed image/engine process-kill qualification remains.

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

- Complete the fresh-seed image/seal/directory/atomic-engine process-kill matrix
  through public Node startup and restart. Existing snapshot crash/pressure and
  joined-node claim/restart cases retain their own recorded evidence.
- Publish pinned package completion evidence only after those cuts pass.

Existing-member recovery does not create members, reset stable votes or provide
disaster restore. Blob/effect capabilities remain unsupported; their external
dependency closure must join snapshot installation before those capabilities can
be enabled. Local TLS and in-memory object-store fixtures do not qualify physical
S3, power loss, zones, production throughput or larger cluster capacity.


## Native fresh-learner peer seeding

Explicit admission commits an exact fresh member/node/disk binding against a
membership revision. Endpoints come from bounded trusted peer configuration;
retries reuse the same identity and revision to reconcile an ambiguous result.
Routes follow committed membership. Admission receipts and snapshots remain data,
not live-quorum authorization.

A purpose-separated TLS seed RPC obtains a fresh safe ReadIndex through the
native host. The source must publish a cut covering that barrier and still
recognize the target as that exact learner. The response binds the request
nonce/tag, physical route, snapshot digest and lease. The opaque target capability
also binds the local engine owner, process and original deadline. Missing quorum
refuses a blank seed even when an acknowledged S3 archive is available.

The target verifies the descriptor before body allocation, validates ordered
bounded chunks, seals its image on the SQL owner and creates all initial engine
records in one synced batch. Original birth configuration remains distinct from
the current included configuration. HardState has vote zero and imports no source
leadership or process state. Prospective references survive uncertain sync. The
permanent claim is conditionally created/read back only after local sync; an
existing claim without stable group state refuses blank initialization. Only the
matching engine owner may attach the member afterward.

Native drills seed from a follower, serve a linearizable query, replicate a later
command, reopen and recover the physical image and binding with vote zero, and
refuse a claimed blank replacement. An interrupted partial body retries with a
fresh grant under the original deadline. New requests cancel an older source
lease to that same learner; old active streams retain their pins through bounded
exit. The missing-quorum drill verifies both absent local group and absent new
member claim.

This changes the internal identity to `cellar-raft-ltx-1/members-1/seeds-1`, with
format markers still 1 and no compatibility reader. That peer-seed cut left S3
seeding and public restart open. The current S3 cut below covers both; fresh-seed
image/engine process-kill qualification still precedes MR-10 completion. MR-11
through MR-17 also remain open.


## Native fresh-learner S3 seeding

`JoiningFromS3` uses the same exact committed learner binding, fresh safe ReadIndex,
TLS exporter/request binding, local log owner and original deadline as peer seeding.
The source downloads only bounded head/manifest metadata and selects an immutable
`ArchiveReference`; it does not capture a speculative callback image. Selection
retries against the frozen barrier while the archive catches up. The returned cut
must cover that barrier, remain no newer than locally published state and name the
exact target as a learner. Neither cached archive availability nor an old grant
permits a quorumless seed.

The target downloads only that reference, under one receiver reservation shared
with peer snapshot work. It never falls back to a new mutable head. Hash/decode
workers retain the reservation across cancellation. A metadata reservation holds
4 MiB for the configured maximum 1-MiB manifest and verification copies; the body
reservation covers configured snapshot working memory plus `max_set_bytes` (at
most 256 MiB). The default receiver bank is one slot/640 MiB. These are accounting
bounds, without eager buffer allocation.

Physical S3 replay runs on the owning SQL worker. It checks immutable birth,
schema, cut, full configuration and joined member/node/disk binding, then seals a
local generation and constructs a canonical snapshot descriptor for that actual
image. The shared atomic seed path persists complete Raft state with vote zero;
only subsequent permanent-claim reconciliation permits attachment. Failure cannot
turn an archive reference into standalone activation authority.

The native tests cover exact tail replay, a blocked private writer, an unavailable
selected payload despite a newer complete head, and missing live quorum. Public
Node restart starts the learner first with new process generations. It serves
verified retained causal state, refuses strong reads until voters return, and
then resumes replication. Claim faults cover failure before PUT, lost response
after PUT, and lost confirming read; verified local state and exact claims are
reconciled without a blank reset. The fresh-seed image/engine process-kill matrix
remains open; existing seven physical cuts cover installed-replica snapshots.
