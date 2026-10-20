# Physical snapshot boundary

MR-10 is in progress. This component supplies physical snapshots and durable
reference recovery; it does not yet connect the native host to snapshot RPCs or
authorize replacement members. All 17 delivery packages remain in scope. The
[physical progress evidence](evidence/MR-10-physical-progress.json) pins source
`24410fb754b055078255ccf5d595c0893f0d1b0e`, nine executable digests and all 13
successful commands: 218 Rust cases pass, including nine snapshot and 88 Node
cases. Six ignored process entry points are invoked by owning parents and are
excluded from counts. Formatting checks 628 Rust files; layout, Clippy, dependency
policy, docs, 17 plan tests and the reviewed 68-anchor preflight pass.

## Identity and bytes

`SnapshotPackage::capture` selects an exact synced applied cut while its immutable
published generation is pinned. Its marker-1 descriptor binds cluster, namespace,
cell, group, incarnation, immutable birth, application schema, included index/term,
canonical ConfState, image name/digest/length/LTX state and the encoded full-LTX
payload length/digest. Canonical JSON and complete protobuf comparison refuse
unknown fields, alternate encodings and mismatched metadata. Source reads verify
the full image SHA-256 before encoding.

A package retains its source generation until transfer/checkpoint work is joined.
Its checkpoint write must remain serialized with the runtime's storage protocol;
current fixtures persist source checkpoints before starting RawNodes. Node-owned
checkpoint scheduling remains to be implemented, rather than permitting a direct
log write to race an outstanding Ready.

`SnapshotDownload` verifies the descriptor before buffering, accepts only ordered,
nonempty chunks within the declared length, and admits at most 64 MiB encoded,
64 MiB materialized and 1 MiB per chunk. Individual buffer growth uses fallible
exact reservations. Physical LTX checks bind the full image state. Generation
publication syncs bytes, renames and syncs the directory. Repeated downloads adopt
an exactly matching existing image; a colliding name never overwrites other bytes.
These per-transfer bounds still need node-wide admission and owned cancellation
before use in the native host.

## Publication and retention

A prepared image produces `SnapshotCompletion` only for the exact local runtime
ticket, snapshot protobuf digest and included index/term. The existing runtime
persists the snapshot, applied marker, configuration and required HardState before
emitting `SnapshotInstalled` or releasing dependent messages. `PreparedSnapshot`
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

Still required for MR-10:

- Node-owned checkpoint creation/compaction serialized with Ready persistence,
  including empty state and configuration-only snapshots before the first image.
  The retained reference must be established before dropping the last capture
  pin if newer applies retired that generation while the checkpoint waited.
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
