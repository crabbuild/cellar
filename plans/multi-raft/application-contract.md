# Cellar Raft application contract

This is the SDK contract implemented by MR-06 through MR-12. Restricted
`RaftClass`, `CommandContext`, `QueryContext` and operation registration are now
exported through `cellar::sdk`; public command/query handles and receipts below
remain design contracts. MR-07 still must connect the node-owned dispatcher,
ship a compiling example and exercise it through native and Axum ingress.

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

`RaftClass::schema_digest()` binds the class name, explicit schema version and
supplied schema-definition digest under the `cellar-raft-schema-1` SHA-256 domain.
Use that value for the namespace root and physical update application digest.
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

Illustrative application flow, to become a compilable example in MR-07:

```text
register class Counter, schema 1
  command Increment(delta):
    read current value through transaction
    validate delta and update value
    return new value
  query Get:
    read current value from pinned snapshot

receipt = counter.command(Increment(1), command_id, durability=Quorum)
answer  = counter.query(Get, consistency=AtLeast(receipt.version))
```

The framework wraps the command with its receipt transaction. Applications must
not independently commit early. Errors and panics roll back private changes. The
encoded result is bounded and stored with the mutation before LTX capture.

## Receipt semantics

For v1, use `(authenticated_client_id, sequence)` as the command ID, scoped to
the cell and incarnation. Each client identity is non-reusable within that
incarnation. The SDK allocates increasing per-cell sequences and persists its
pending command before sending. Different client identities can submit concurrently;
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

Public constructors remain centralized in the application builder. Registration
validation rejects an ordinary mutable `Cell` factory for a Raft class. Background
workers and maintenance must use the same mode-aware dispatch boundary so they
cannot bypass this restriction.
