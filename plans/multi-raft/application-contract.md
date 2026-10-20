# Cellar Raft application contract

This is the SDK contract implemented by MR-06 through MR-12. Restricted
`RaftClass`, `CommandContext`, `QueryContext` and operation registration are now
exported through `cellar::sdk`. `Node::start_raft_cells`, public command/query
handles and receipts connect Quorum/S3 commands and committed-image reads
to node-owned producers. The counter example in `examples/raft-counter/README.md`
uses both native HTTP and Axum. Local Stale, AtLeast and safe ReadIndex paths
are implemented and qualified in MR-08, including bounded replica routing and
explicit same-consistency leader fallback. MR-09 connects S3 output gates,
stronger receipt retries and archive image restoration; final qualification is
pending in the [archive audit](archive-audit.md). Atomic member snapshot install
and effect adapters remain their respective later work packages.

## Commands and queries

Register operations by `(class, operation, kind, contract_digest)`. Ingress binds
tenant and cell identity from authorized routing, not an unchecked client field.
The destination resolves that registration and rejects a kind/digest mismatch
before calling business code. Both native and Axum ingress use the same dispatcher.

| Surface | Required contract |
| --- | --- |
| `RaftClass` registration | Explicit schema version, command/query definitions and supported capabilities |
| `CommandContext` | Scoped cell transaction, request identity, captured inputs and outbox intent writer |
| `QueryContext` | Pinned read-only generation, authorized identity, served version and deadline |
| `CommandReceipt<T>` | Result, opaque committed version, durability achieved and command identity |
| `QueryResult<T>` | Result and exact served version |
| `CommandOptions` | Stable client/command ID, request digest, requested durability and deadline |
| `QueryOptions` | Stale, AtLeast or Linearizable; explicit fallback and deadline |

Command callbacks are synchronous within one transaction in the first release.
They cannot await networking, send HTTP, call another cell, spawn background work,
or retain writable storage. Expose captured clock/random values only through the
host; resulting state and receipts are replicated as LTX. Native application code
is trusted code, so a Rust type is not a sandbox against deliberate direct OS I/O.
Document and test the supported contract; applications that bypass it are outside
the durability/effect guarantee.

The internal MR-06 transaction facade supplies a captured Unix-millisecond value
and 32 captured random bytes per attempt. Its SQL authorizer refuses ambient
clock/random functions; use these captured values as SQL parameters. Additional
input facilities require explicit host adapters before becoming SDK capabilities.

`RaftClass::schema_digest()` first binds the class name, explicit schema version
and supplied schema-definition digest under the `cellar-raft-schema-1` SHA-256
domain. It then binds that base digest and the sorted required command names and
contract digests under `cellar-raft-class-1`, with explicit count/name lengths.
Read this identity after registering commands and use it for the namespace root
and physical update application digest. Omitting, renaming or changing a command
cannot preserve voting compatibility by repeating the schema version/digest.
Declaration order does not affect the identity. Query contracts remain separate:
a replica with incompatible/missing query code can still vote with the matching
required commands, but refuses that query before executing it. Changing required
command contracts requires a fresh immutable namespace identity; this is a
marker-1 hard cutover without a compatibility reader or in-place conversion.
Commands and queries have separate operation contract digests. The shared
`consensus::command_request_digest` helper binds the exact normalized input bytes
under its own marker-1 domain; the registered execution bridge validates it even
when a request would return a duplicate receipt. Ingress normalization must
produce the same bytes for native and HTTP calls.

Queries receive no general `Context`, writable `Storage`, normal cell constructor,
alarm setter or outbound-network capability. SQLite authorizer/read-only flags
must reject writes, schema changes, ATTACH, mutating PRAGMA and writable extension
paths even if they arrive through raw SQL. Query functions use stateless services
or immutable configuration; application instance fields cannot substitute for
replicated state.

Application flow with an explicit read consistency:

```text
register class Counter, schema 1
  command Increment(delta):
    read current value through transaction
    validate delta and update value
    return new value
  query Get:
    read current value from pinned snapshot

receipt = counter.command(Increment(1), command_id, durability=Quorum)
answer  = counter.query(Get, consistency=AtLeast(receipt.version_token))
```

The framework wraps the command with its receipt transaction. Applications must
not independently commit early. Errors and panics roll back private changes. The
encoded result is bounded and stored with the mutation before LTX capture.

## Receipt semantics

For v1, use `(authenticated_client_id, sequence)` as the command ID, scoped to
the cell and incarnation. Each client identity is non-reusable within that
incarnation. The caller allocates increasing per-cell sequences and persists its
pending command before sending; `CommandOptions` requires that explicit ID. Different client identities can submit concurrently;
one identity submits its commands in sequence.

Persist operation name, contract digest, normalized request digest, result bytes
and minimum requested durability in a reserved receipt table. A retry with the
same identity and content returns the same committed result while its receipt is
retained. A conflicting digest for a retained receipt returns `CommandConflict`,
including after leader change or snapshot installation. An expired sequence
returns `CommandExpired` without claiming to remember its original content.

Associate a newly applied receipt with its Raft position in framework recovery
metadata in the same crash-consistent publication step as the image. It is not
necessary to know the eventual Raft index while the private SQLite transaction
executes. If a response is reconstructed later from a committed image, its token
may name that image's covering index; it must not claim an earlier unverified cut.

Maintain a durable per-client `last_sequence` and `expired_through` floor. The next
new command must be `last_sequence + 1`; a higher value returns `SequenceGap`.
A lower/equal value returns its retained receipt or `CommandExpired` when at/below
the floor. Sequences never wrap. Receipt pruning is a replicated command that
advances the floor only over a contiguous expired prefix. Keep the per-client
floor after pruning and cap the admitted client-identity count; exceeding that
budget refuses a new client instead of deleting a floor.

Persist `retry_until`/receipt retention policy. Capacity pressure cannot silently
shorten the promised retry window. Incarnation-changing disaster restore
may lack receipts after the archive cut; return a reconciliation outcome, not an
automatic retry into the new history.

## Result and error behavior

| Outcome | Meaning and retry behavior |
| --- | --- |
| `NotLeader` | Redirect hint; server still validates authority after reroute |
| `Unavailable` / `Overloaded` | Refused before proposal submission; retry within policy |
| `Ambiguous(command_id)` | Proposal may have committed; reconcile using the same ID |
| `CommandConflict` | ID reused with different content; do not retry with altered content |
| `SequenceGap` / `CommandExpired` | Reconcile the client's sequence or expired retry; never execute it as a fresh command |
| `VersionUnavailable` | Replica cannot cover the requested committed version before deadline |
| `WrongIncarnation` | Token or retry belongs to a different history; reconcile explicitly |
| `UnsupportedCapability` | Operation requires an SDK capability not qualified for this mode |
| `UnsupportedConsistency` | No silent fallback to a weaker read contract |
| `ArchiveUnavailable` | S3 requirement not yet proven; a submitted command remains ambiguous |
| `CorruptState` | Stop that replica's serving/proof role and recover through the protocol |

`RaftCellHandle::command`, `query` and `status` admit synchronously before
returning a future. Dropping the future cancels its waiter. An unpolled waiter
does not suspend its deadline: the node checks expiry before starting queued
private work and before submission. A readiness observation is a hint; every
command still checks live authority and can be refused if it changed.

Cancellation before submission rolls back private work. Cancellation after
submission cancels the waiter, not the consensus decision. Never convert a lost
reply into a fresh mutation ID. Read retries retain their original deadline and
consistency requirement. Cross-cell workflows remain application coordination.

## Effects and built-in services

MR-12 implements committed alarms and an outbox adapter. An alarm record and its
next occurrence are replicated; the current leader rebuilds scheduling on startup
and after election without waiting for user traffic. Each firing uses a stable
operation ID and writes its progress through the same command path.

Outbox payload and stable delivery ID commit with business state. The leader
dispatches only after that intent is committed and its required durability is
satisfied. Receiver deduplication handles replay after a lost response. Delivery
completion is another command. An old leader can have an external request in
flight after losing its term, so receiver receipts remain required.

Blob references must name immutable durable content before their referencing
command is acknowledged. Such commands can incur object-store latency even in
Quorum mode. Archive manifests retain those dependencies. Multi-cell service calls,
WebSockets, mutable containers, existing Workflow and Queue callbacks are refused
for Raft classes until their capability adapters receive equivalent qualification;
classes in Fleet mode continue to support their existing behavior.

## Deployment and authentication

Keep separate command and query contract digests. A follower can hold the correct
physical data while lacking compatible query code; it then remains a voter but
does not serve that query. Leadership eligibility requires a compatible command
implementation and applied schema. Do not silently migrate on query opening.

Perform current authentication and authorization at ingress and destination.
Authorization derived from a stale database image is itself stale; operations
requiring latest revocation checks must use a fresh authoritative check. Version
tokens prove requested history, not access permission.

The current SDK exposes a diagnostic `CommittedVersion` in `version` and an
opaque authenticated observation in `version_token`. `AtLeast` accepts the
opaque token and validates it on the destination before any index comparison.
Tokens bind cluster, namespace, cell, group, incarnation, application digest,
index, term, issuance and expiry. They survive ordinary leadership changes;
cross-history comparisons fail. The returned position names the cut captured
with the pinned image, including verified protocol-only coverage of that image.

`QueryOptions::routing` defaults to local execution. `QueryRouting::Replicas`
accepts up to eight distinct member IDs in priority order, a per-attempt timeout
between one millisecond and sixty seconds, and an explicit leader-fallback flag.
The total query deadline is fixed on initial admission. Each attempt preserves
its contract/input, consistency/token and stale policy. A fallback uses an
untried leader hint and obtains the same read proof at its destination.
`QueryResult::served_by` reports the responding member. A forwarding origin need
not have compatible query code; the destination must check it before execution.

Each `RaftCellRegistration` explicitly selects `RaftCellPlacement::Replica` or
`QueryRouter { leader_hint }`. A routing-only binding requires an existing
namespace/birth and cannot create a member or SQLite image. It rejects command,
local-query and local-status calls; choose explicit replica routing. Its optional
leader hint is discovery metadata and never supplies a read barrier.
`RaftCellsConfig::query_origins` installs trusted namespace read grants for exact
cluster/namespace/node/disk/process endpoints. These grants do not authorize
Raft messages or change membership. Live revocation cancels old query waits;
revision tombstones prevent stale updates/regrants from reviving those requests.
The embedding supplies current trusted grants on restart; durable distribution
and dynamic placement remain controller work. See the [read audit](read-audit.md)
for scope and [counter HTTP contract](https://github.com/crabbuild/cellar/blob/8f18c29a0f28977eb8a4cae51848982ad7d43739/examples/raft-counter/README.md) for
the native/Axum consistency, token and bounded-routing headers.

`RaftCellsServices::versions` supplies explicit managed Ed25519 signing and
verification keys. Configured token lifetime is between one millisecond and
seven days. At most 64 verification keys are retained. A token cannot outlive
its signing key's verification deadline. Future-issued or expired tokens are
refused; clock skew/rollback can affect availability and supplies no strong-read
proof. The development counter uses a fixed test key; production callers must
inject their own stable managed material.

Rotate with a verifier-first rolling deployment: distribute the new public key
while the old signer remains active, then switch signers while retaining all
unexpired verification keys. The namespace catalog records bounded canonical
public-key retention metadata under conditional updates. Startup reconciles an
ambiguous write by exact reads and refuses a configuration that drops or changes
an unexpired registered verifier, including after restart. Expired keys may be
removed. Private signing material is never written to this registry, and registry
contents cannot add a key to a server's configured trust roots.

Public constructors remain centralized in the application builder. Registration
validation rejects an ordinary mutable `Cell` factory for a Raft class. Background
workers and maintenance must use the same mode-aware dispatch boundary so they
cannot bypass this restriction.
