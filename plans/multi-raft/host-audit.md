# MR-07 host integration audit

This audit covers the registered command/query path delivered by MR-07. It does
not close the later read-consistency, archive, snapshot, membership, effects or
release work packages. The task manifest retains their original requirements.

## Mode and target identity

A Raft target is the complete cluster/namespace/cell/group/incarnation identity,
not a class name alone. `Node::start_raft_cells` checks the selected application
definition, namespace mode, class/schema/command-manifest digest and canonical cell scope before
opening the log service. Catalog root creation is conditional and reconciled by
reading the exact immutable record. A conflicting root cannot be replaced by a
lease expiry, a different process generation or a changed request mode.

The application digest binds the sorted required command names and contract
digests as well as the class/schema base. Matching a name/version alone cannot
admit an implementation missing a required command. Query contracts are checked
per request, preserving voting compatibility for a replica with different query
code. The command-manifest case checks omission, changed contracts/names and
order independence; live startup rejects an incompatible manifest before any
catalog or log state is created.

The current Fleet namespace facade cannot resolve one of those Raft bindings.
It derives its own namespace key from script/class and returns ordinary Fleet
stubs. `Registry::validate_configuration` refuses a Raft definition in Fleet's
`do_classes`; native worker validation independently requires a mutable factory
for an ordinary class. `Node::start_raft_cells` refuses a selected mutable
factory or workflow before catalog creation or private execution. The Axum
definition builder delegates the same restricted class lookup and preserves
the registration exclusion.

No conversion or fallback connects these two facades. A class with the same
name in a separate application's Fleet namespace is not the Raft group identified
by an opaque namespace/group/incarnation. `RaftCellsServices::application` selects
the immutable Raft definitions explicitly. Changing the ordinary application
generation does not convert a Raft handle into a Fleet stub or replace its
registered group. In-place Fleet-to-Raft namespace conversion remains excluded
by the technical design.

The storage trees are also disjoint:

| Owner | Storage selection |
| --- | --- |
| Fleet ownership and recovery | `cells/<cell>/own.json`, `cells/<cell>/ltx/e<epoch>/` |
| Raft catalog root | `namespaces/<namespace>/root`, with immutable selected storage prefix |
| Raft remote history prefix | `raft/<cluster>/<namespace>/` below the configured catalog prefix |
| Raft local engine | The owning node's exact `raft` directory |
| Raft SQLite/images | `raft-cells/<namespace>/<group>/<incarnation>/`, using hex identities |

Fleet GC, lease refresh and LTX upload loops enumerate their Fleet paths and are
never installed on the Raft private executor. The Raft path has no Fleet owner
CAS or bucket-fallback effect. Available catalog storage cannot authorize a
minority to commit commands. Existing Fleet namespaces keep their current
protocol; they do not gain a new Raft-root lookup or reinterpret Raft files.

The live admission case verifies that Fleet node startup refuses a Raft class,
Raft startup refuses a mutable factory without calling it or creating catalog/log
state, and an existing Fleet-mode root prevents Raft initialization. The catalog
cases cover the reverse root conflict, simultaneous creators, schema/history
mismatch and permanent member claims. `fleet_gate_unchanged` exercises both Fleet
proof sources, including bucket ownership verification and insufficient proofs.
The existing actor/storage/replication/log-protocol targets provide owning
regressions for the unchanged Fleet paths.

## Command ownership and output

`host.rs` retains `CommandState` and each accepted producer in the node's
execution domain. The fixed blocking pool in `host_sql.rs` owns SQLite, private
executors, generation pins and directories. Public futures own waiters. They do
not own a producer's submitted RPC or private state.

Before staging, submission and successful output, sequenced runtime observations
check the exact replica and term. A revoked term cannot be reactivated by a late
status completion. Protocol Ready work can defer a check under the original
deadline while the producer retains its frozen private result. It cannot rerun
the callback or create a second command slot. Deadlines are checked by the
producer and SQL owner even if the returned waiter future is never polled.

The output gate binds complete command authority, operation/proposal identity,
exact entry index/term/digest and a verified receipt from the published image.
Neither local persistence, proposal acceptance nor a Fleet durability position
can release a fresh command result. Duplicate lookup validates operation,
contract and normalized input before returning its stored result. A reconstructed
receipt may name a later verified covering cut, but cannot invent an earlier cut
or execute an expired/conflicting request as a new command.

`CellStatus` watermarks are diagnostic observations only. In particular, a
persisted suffix may be uncommitted. They never enter the output gate as proof.
The partition fixture uses those observations to verify that an ambiguous command
was actually persisted, rather than treating a caller timeout as evidence of
submission.

## Partition and retirement histories

The TCP fixture proxies the real authenticated TLS transport. Fault transitions
close existing streams and reject new streams; the healthy pair stays connected.
Both histories leave catalog storage available and keep the original command ID:

1. Cut both directions of both lanes. The isolated leader persists a command
   without committing it. Its waiter times out ambiguously, another command is
   overloaded, and queries continue to see the prior committed value. The healthy
   majority elects a new leader. Healing installs a newer committed barrier,
   overwrites the uncommitted suffix and retires the old private shadow. Retrying
   the original ID through the majority applies the mutation once.
2. Keep the old leader's outgoing data lane while cutting control and incoming
   traffic. Both followers persist the entry without acknowledging it to the
   old leader. The healthy majority elects a leader and commits that retained
   entry through its current-term barrier. Healing publishes the committed image
   on the old follower and retires its producer. Retrying the original ID returns
   the stored result without another business callback.

In both cases the producer remains counted until authoritative reconciliation
and private cleanup complete. The old node then follows the new leader and has
no command-ready authority. Callback counts and business values distinguish a
discarded speculative attempt from an already committed receipt. Clean service
closure reports no unfinished producers after these histories.

Captured ordinary `Context`/`Storage` handles also retain their existing
invocation/activation checks. The blocking pool installs its execution domain,
not a native invocation I/O context. Such handles cannot authorize work there;
Raft handles additionally refuse calls from the synchronous callback guard.
Native applications remain trusted code rather than an OS sandbox.

The separate shutdown/cancellation cases verify that a blocked callback retains
the command slot and node directory lock until joined. A retained old handle
cannot reopen its executor after shutdown. These are three embedded nodes with
separate directories/domains in one process, not independent-machine or physical
power-loss qualification. The later release/failure matrix remains required.

## Integration and remaining packages

The Cargo target `cellar-raft-counter` runs two three-voter groups. Native HTTP,
Axum and direct SDK commands use the same normalization and dispatcher. Tests
cover authentication, contract mismatch, conflicting input, duplicate receipts
and actual follower image publication. The process drill stops and restarts all
three embedded nodes, verifies retained receipts and then performs new writes.

MR-07 provides Quorum command serving and stale committed-image queries. Stronger
reads and S3-durable commands are refused until MR-08/MR-09 supply their proofs.
Snapshot installation, automatic membership repair, committed effects, operational
limits/metrics and complete release/scaling qualification remain their planned
work packages. A SQLite image or a successful local close is not an S3 recovery
proof and never authorizes discarding replica disks.

The [MR-07 evidence](evidence/MR-07.json) pins the implementation revision,
executable digests, exact commands, repeated histories and qualification limits.
All four MR-07 implementation steps and six required case names are covered.
