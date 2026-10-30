<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Shared cell groups: architecture decision

Status: selected replacement architecture; runtime conversion and qualification
are pending. This document does not claim that Cellar supports shared groups yet.
It supersedes one-cell-per-group as the target architecture, while the existing
technical design and qualification records describe the implemented baseline.

The [executable delivery package](cell-groups/README.md) expands this decision into
the complete replacement, including dynamic split/merge and coordinated hibernation.
Its detailed protocols supersede the initial assignment sketch below: range-based
ownership and replicated enrollment replace per-cell reservation as the single
assignment authority, and stable cell history is separate from group placement.
Fixed assignment is an intermediate delivery milestone, not the final scope.

## Decision

Make a **cell group** the unit of Raft consensus, placement, membership, snapshot
and disaster recovery. A group contains multiple cells. Each cell retains its own
SQLite database, LTX chain, command receipts, query versions and application
identity. Every replica of a group owns the group's complete committed state.

Use one implementation for every group size. A large or hot cell can occupy a
group alone; it does not select a second consensus architecture. Remove the
assumption that creating a cell creates a Raft instance. Keep the shared node log
engine, worker pools, transport and resource limits already implemented.

Groups are independent of physical nodes. A three-voter group spans three
placement domains; one node can lead several groups and follow others. Grouping
all cells currently on a node would bind ownership to a host and produce an
unbounded failure and scheduling unit. It is not the intended design.

The decision targets the fixed consensus overhead of many small cells. It does
not promise lower latency for every workload, remove per-cell storage work, or
provide cross-cell transactions. Busy cells in the same group compete for a
leader, log bandwidth and the contiguous apply prefix.

Grouping also does not split a hot cell's SQLite transaction stream across leaders.
The application retains its existing per-cell atomicity boundary.

## TiKV verification

Inspected the user-provided TiKV checkout at commit
`b3995dab28308f09cbb7c125bf32f789dbc203fe` (2026-10-22). No TiKV files were changed
and no TiKV benchmark was run. The paths below are relative to that checkout.

| Source | Verified behavior | Cellar consequence |
| --- | --- | --- |
| `components/raftstore/src/store/peer.rs`, `Peer` | A Region peer owns a `RawNode<PeerStorage<...>>`. | One Raft machine per group replica, rather than per application object. |
| `components/raftstore-v2/src/raft/peer.rs`, `Peer` | The tablet path also owns one `RawNode<Storage<...>>` per peer. | The consensus boundary survives a change of storage layout. |
| `components/raftstore/src/store/fsm/store.rs`, `StoreMeta` | Maps Region IDs and key-range boundaries to Region metadata. | A node hosts many groups and routes to the selected group. |
| `components/raftstore/src/store/util.rs`, `check_key_in_region`; `store/fsm/apply.rs`, `handle_put` | Writes validate their key against the Region's half-open key range. Many distinct keys can use the same Region. | Every Cellar entry must identify and validate its target cell inside its group. |
| `components/raftstore/src/store/fsm/apply.rs`, `exec_batch_split`, `exec_prepare_merge`, `exec_commit_merge` | Splits and merges change Region boundaries and epochs through replicated administrative operations. | Moving a cell between groups requires a protocol and routing fencing, not a local map edit. |
| `components/raftstore/src/store/async_io/write.rs`, `WriteTaskBatch::add_write_task` | Batches log writes while retaining their Region IDs. | Shared physical persistence and shared consensus groups are separate optimizations. Cellar already has the former. |
| `components/raftstore/src/store/fsm/peer.rs`, `quorum_agree_to_hibernate`, `on_hibernate_request`, tick handling | Coordinates hibernation votes and checks pending history before stopping ticks. | Group hibernation is a separate future protocol; an idle cell cannot stop a busy group's Raft ticks. |

TiKV's [multi-Raft explanation](https://tikv.org/deep-dive/scalability/multi-raft/)
and [current Region glossary](https://docs.pingcap.com/tidb/stable/glossary/#regionpeerraft-group)
confirm that a Region holds a range of keys and its peers form one Raft group.
Cellar's analogous partition contains whole cells, not individual SQLite keys or
pages. Keep the name **cell group** to avoid confusing it with a geographic
region. Do not copy TiKV's byte thresholds or historical event-loop defaults:
Cellar has different image, transaction and recovery costs.

## Evidence and expected savings

Cellar source inspected: `1be27d234d4674d4d014785c60cecbfe394bdab9`.
The local idle experiment at that source measured 32 and 128 three-voter groups:
1.437 and 5.794 MB/s outgoing control traffic, respectively, or approximately
44 KiB/s per logical group across the three embedded nodes. Settled ten-second
windows recorded no engine writes or object-store reads/puts. These are debug
build diagnostics on one M2 Max with TLS loopback proxies and an in-memory object
store, not production costs, RustFS results or a shared-group benchmark.
Probe sources, exact environment and run metadata remain in the local ignored
`.cellar/cell-cost-report.md` and `.cellar/cell-cost-metadata.json`.

For C cells with approximately K cells per group, G is approximately ceil(C/K).
With R voters, no learners and heartbeat interval H, baseline leader-to-follower
logical deliveries are G*(R-1)/H per second, before replies and batching.

| Cells | Target cells/group | Groups at ideal packing | Group replicas with three voters |
| ---: | ---: | ---: | ---: |
| 1,000,000 | 1 | 1,000,000 | 3,000,000 |
| 1,000,000 | 32 | 31,250 | 93,750 |
| 1,000,000 | 64 | 15,625 | 46,875 |
| 1,000,000 | 128 | 7,813 | 23,439 |

The number of cell database replicas remains 3,000,000 in every row. Grouping
reduces consensus state, timers, election and membership bookkeeping. It does not
divide cell data, receipts, LTX bytes, SQLite apply work or archive payload by K.
These counts are arithmetic, not a supported fleet size or a measured speedup.
Partially filled groups, dedicated cells and placement constraints increase G.

Benchmark occupancies 1, 8, 32, 64 and 128 before choosing a default. Limit group
size by total bytes, write rate, apply latency and recovery budget as well as cell
count. Cell count alone cannot distinguish 64 tiny counters from 64 large cells.

## Identity, assignment and admission

Separate physical consensus history from the target cell:

| Contract | Scope |
| --- | --- |
| `GroupScope` | Cluster, namespace, group ID, group incarnation; no cell name |
| `CellScope` | Group scope, cell identity and immutable assignment identity |
| Group birth / membership | Voter identities, disk claims, placement and initial protocol/schema binding |
| Cell enrollment | Cell scope, schema/application digest and verified initial image |
| Command / receipt / served version | Cell scope plus the exact group term/index and cell image identity |

The names above describe the replacement contracts, not types already delivered.
Start with cells from one namespace and its immutable application/schema contract
in a group. Do not combine tenants or incompatible durability/placement policies
merely because they currently reside on the same node.

Initially, assignment is immutable for an enrolled cell. A directory selects a
group with capacity for a new cell; adding nodes or groups does not rehash existing
cells. Concurrent creation uses a single conditional cell-assignment reservation.
Only its confirmed winner may enroll the cell, and the replicated enrollment binds
that reservation and the group incarnation. An uncertain reservation is reconciled
before proceeding. Route publication follows committed, durable enrollment; a
stale route cannot grant authority. Reservations and retries are bounded and
recoverable across controller/leader crashes.

New groups can take new cells without moving existing ones. Group capacity is
rechecked at enrollment, so concurrent reservations cannot exceed its hard bound.
A refused reservation cannot be silently reassigned after it might have enrolled.
Birth records and permanent voter-disk claims occur per group. Cell-directory and
enrollment metadata still scale with the cell population.

Online cell transfer, split and merge are separate work. They require source
quiescence committed through its quorum, an exact transfer cut, destination
installation and a fenced routing transition. No destination becomes writable
while the source might still accept commands. Receipts and effect identities must
survive transfer without replaying acknowledged work. Until that protocol passes
fault tests, operations move whole groups or create new groups for new cells.

## Write, apply and read invariants

1. Admit a command under current group leadership and the exact cell assignment.
   Prepare it in that cell's private SQLite transaction, including its receipt.
   Preserve one unresolved data proposal per cell in the first implementation.
2. Propose an envelope containing the group history, cell identity/assignment,
   proposal identity, application digest and that cell's base/result LTX images.
   Never propose an unscoped LTX update or replay application callbacks on followers.
3. Different cells may prepare concurrently through bounded shared SQL workers.
   Fair cell queues feed group and node byte budgets. A shared log still orders
   all entries; initial apply publication remains ordered by group index.
4. Apply an entry only to its addressed cell. Fsync immutable image data before
   atomically advancing the durable group manifest and contiguous applied prefix.
   Publish the cell view and settle output only after that marker is durable.
5. Quorum and S3 acknowledgements retain their existing meanings. Timeouts remain
   ambiguous after proposal; duplicate receipt lookup remains scoped to the cell.

If entries 40 and 41 update A and B, group applied=41 means both entries are
applied. A's latest image still corresponds to entry 40. B's progress cannot satisfy
an A receipt, replace A's base image or manufacture a newer A version. Advancing a
global applied index past an unfinished earlier entry is forbidden.

Linearizable reads obtain a current-term group barrier, wait for the group apply
prefix to cover it, then pin the target cell's image from a coherent published
manifest. The cell need not have a mutation at the barrier index. AtLeast compares
the target cell's verified version within the same assignment and incarnation.
Stale reads remain explicitly stale. No read treats directory ownership or another
cell's newer image as a quorum proof.

## Durable group manifest and crash recovery

Replace the single-image `DurableApply` with a bounded committed manifest root.
The root authenticates the group history, applied position, enrollment state and
each cell's image commitment, LTX position and latest mutation position. Include
deletions/tombstones if deletion is exposed; absence must not resurrect a cell.
Use incremental immutable manifest storage so updating one cell does not copy
every SQLite image or serialize an unbounded group map on each command.

Recover only a complete manifest selected by the durable engine marker. A crash
before the marker may leave orphan image/manifest objects; it cannot publish them.
A crash after the marker must recover every referenced cell without repeating its
callback. Missing, corrupt or mismatched referenced state refuses group admission.
Retention must protect the previous recoverable root until the new root is durable,
plus all snapshot, archive, reader and in-flight apply pins.

Raft hard state, membership and apply state remain group scoped. Preserve the
existing shared-engine failure quarantine and permanent voter identity checks.
Closing one idle execution cache leaves its committed image and group membership
intact. It does not stop elections, change ownership or erase its cell manifest.

## Snapshots, S3 and reclamation

A group checkpoint at index K contains the complete cell manifest as of K,
membership/enrollment state and all referenced image data or extent roots. Cell
images may have different last mutation indexes, each no greater than K. Pin that
consistent set before streaming. Install it atomically at the group boundary;
receiving only a subset cannot make the group queryable or writable.

S3 recovery sets contain a complete checkpoint plus an optional contiguous,
cell-addressed committed log tail through a declared group cut. Verify every
dependency, checksum, base/result chain and membership transition before publishing
coverage. Subsequent checkpoints can reuse unchanged immutable cell objects; they
need not upload every database again. A hot cell's archive failure can delay the
group's archive prefix, so track group lag and bound retained bytes.

An S3 acknowledgement for entry N requires a retained complete group recovery set
covering N. Archive coverage for one cell cannot cover unrelated earlier entries
in the group. Keep quorum-only acknowledgements distinct: a zero-quorum disaster
can lose their unarchived suffix even though the live quorum had acknowledged it.

Local log truncation requires a valid covering group snapshot and the existing
recovery/retention conditions. Where reclamation relies on S3, require confirmed
complete group coverage too. Uploaded bytes alone establish neither condition.
Physical shared-engine file reclamation must respect every group's live log ranges.
Reclaiming entries never deletes current cell images or dependencies pinned by a
retained recovery set. Preserve snapshot availability for lagging peers.

Zero-quorum recovery fences every old voter of the group using the existing
fencing-authority contract, then creates one new group incarnation with fresh voter
identities. Restore every cell from the selected group recovery set, preserve its
receipts and require a fresh quorum before admission. Old tokens/assignments cannot
be used to write in the new incarnation. Old-origin outbox ambiguity still refuses
admission unless explicitly reconciled; grouping does not solve external effects
or make different groups' recovery cuts a fleet-wide consistent snapshot.

## Replacement and delivery

Replace the current one-cell/group path instead of adding another production mode.
Keep format markers at 1, with distinct protocol/domain bindings that reject old
layouts and mismatched peers. Use a fresh development namespace/storage root for
the hard cutover. Do not reinterpret a one-cell image as a multi-cell manifest,
silently migrate existing state, or delete existing disks/archives. A group with
one enrolled cell uses the exact same replacement code as a larger group.

| Step | Owning code | Required evidence before completion |
| --- | --- | --- |
| 1. Separate identities and enrollment | `cellar-logic/consensus.rs`; `application/consensus/protocol.rs`; Raft catalog, routing, version and receipt adapters | Cross-cell/namespace/assignment tokens and LTX updates refuse; concurrent enrollment has one winner; old wire/storage bindings refuse. |
| 2. Group manifest and ordered apply | Raft cache/storage, apply, extent roots, host and SQL owners | Three distinct SQLite cells in one actual `RawNode` group; interleaved writes; durable marker/image crash cuts; duplicate receipts; missing manifest dependencies refuse. |
| 3. Group snapshots and peer rebuild | Snapshot/checkpoint transfer, membership and startup | Follower catch-up and lost-disk replacement restore every cell at one cut; no partial group admission; bounded streaming and pins. |
| 4. Group S3 archive and reclamation | Archive codec/publisher/recovery, extent objects, log purge | Real RustFS checkpoint/tail restore; delayed cell dependency blocks coverage; acknowledged history survives actual file reclamation and restart. |
| 5. Fenced group disaster recovery | Catalog disaster API, fencing authority and Docker fixture | Delete all voter disks; restore all cells into one new incarnation; reject old tokens; preserve receipts and new writes; quorum-only unarchived suffix reported separately. |
| 6. Fleet and application integration | Public SDK, native/Axum ingress, fleet planner, alarms, outbox and operator views | One group leader/placement for many cells; per-cell authorization, schema, effects and fairness; whole-group repair/drain; all owners join on shutdown. |
| 7. Remove baseline assumptions and qualify | CLI/examples/docs, all native Raft harnesses, capacity profiles | No production fallback allocating a group for each ordinary cell; occupancy-one uses shared code; equal-workload performance and fault evidence below. |

These are dependent implementation slices of one replacement, not separate
supported architectures. They are not marked complete by adding types, changing
an ID, grouping rows in one SQLite database, or counting fewer catalog records.
Existing MR-16 physical-tier release gates remain open.

## Qualification matrix

Compare the existing revision against the replacement at equal logical cell
population, per-cell data sizes, offered arrival rate, active fraction, request
mix, durability and hardware. Within the replacement, compare occupancies
1/8/32/64/128. Use independent SQLite files and LTX chains in every cohort.

Measure idle CPU/RSS/FD/control bytes, real Raft instance count, fsyncs, log and
image write amplification, archive requests/bytes/lag, p50/p95/p99, refusal/error
counts and recovery time to successful business reads/writes. Include a hot cell
beside cold cells, concurrent cells in one group, separate groups, large images,
slow apply, a slow minority, leader loss, object-store outage and mixed durability.
Record shared-group head-of-line delay rather than averaging it away.

Fault tests must cover interleaved cell history across every new durable boundary,
stale assignment/term completions, enrollment crash/retry, snapshots while another
cell writes, corrupt/missing cell dependencies, reclamation with lagging peers,
retained-disk restart and the destructive real RustFS drill above. Strong-read and
receipt assertions must use the individual cell's history. Preserve failure and
ambiguous outcomes in the workload ledger.

Group hibernation follows as separate work with safe wake/read authority, retained
state, peer-triggered wake, pending work, alarms/outbox and membership coverage.
No full-group wake latency or production density claim follows from this decision.
