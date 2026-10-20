# Physical snapshot boundary

MR-10 is in progress. The [runtime progress evidence](evidence/MR-10-runtime-progress.json)
pins source `0260abb94c15ee0375bea0f2508ac40ce1274f65`: 147 Rust cases pass across
ten targets, including 15 snapshot, 17 LTX and the affected public Node Raft
lifecycle case. All 17 commands pass, with executable/log digests. Clippy and
checks cover owning targets and cellar-ltx without backend defaults and with all
features; format checks 629 Rust files. Layout, dependency policy, docs, 17 plan
tests and the reviewed 68-anchor preflight pass. Seven ignored process children
are invoked by owning parents and excluded from counts.

The [earlier physical progress](evidence/MR-10-physical-progress.json) retains its
separate 218-case/88-Node qualification at source
`24410fb754b055078255ccf5d595c0893f0d1b0e`; it is historical, not the current
runtime qualification. Native automatic scheduling, snapshot RPCs and authorized
member recovery remain unfinished. All 17 delivery packages remain in scope.

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
Native automatic checkpoint scheduling is still unfinished.

`SnapshotDownload` verifies the descriptor before buffering, accepts only ordered,
nonempty chunks within the declared length, and admits at most 128 MiB encoded,
64 MiB materialized and 1 MiB per chunk. Individual buffer growth uses fallible
exact reservations. Physical LTX checks bind the full image state. Generation
publication syncs bytes, renames and syncs the directory. Repeated downloads adopt
an exactly matching existing image; a colliding name never overwrites other bytes.
These per-transfer bounds still need node-wide admission and owned cancellation
before use in the native host.

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
and refusal to erase committed data. The pinned runtime evidence qualifies those component and owning regressions.

The boundary fixture adds an exact 64-MiB real SQLite image with seeded random
overflow pages and a passing integrity check. Its encoded LTX exceeds 64 MiB.
It retains the older checkpoint after a newer LTX update, transfers that exact
cut, and restores the newer snapshot-plus-tail archive after both node directories
are deleted. This is a component test over the in-memory object store, not native
member repair, physical S3 or evidence of a 64-MiB command envelope. The per-command
envelope limit is unchanged. Additional cases cover replaced checkpoints, capture
lifetime through GC, empty retained checkpoints and insufficient encoded budgets.

Still required for MR-10:

- Native automatic checkpoint creation/compaction and source transfer scheduling
  using the runtime API and the SQL owner's serialized state.
- Authenticated bounded peer snapshot RPCs with reserved control capacity, source
  pins, transfer admission, deadlines, cancellation and joined shutdown.
- Native host handling of incoming snapshots and atomic readiness transitions,
  including pending commands/queries and image-capacity pressure. Minimum image
  admission must account for the independent snapshot reference and private
  command/apply generations, and transfer limits must cover configured images.
- Authorized fresh-member admission and S3/peer seeding through a surviving live
  group, with explicit refusal of S3-only normal activation after quorum loss.
- Full native lifecycle/fault qualification and pinned completion evidence.

The production host still refuses unsupported incoming snapshot work. The current
component does not create members, reset stable votes or provide disaster restore.
Blob/effect capabilities remain unsupported; their complete external dependency
closure must join snapshot installation before those capabilities can be enabled.
