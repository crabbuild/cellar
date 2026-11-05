<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cellar Raft application contract

Purpose: Define registered Raft commands and queries, receipts, read versions, effects, and application compatibility.

[Design library](../README.md) · [Topic contract](README.md)

| Implementation | Validation or example |
| --- | --- |
| [Code and API](../../../crates/cellar/durability/raft/README.md) | [Checks and usage](../../../examples/raft-counter/README.md) |

The map summarizes this document’s scope; the recorded gates determine completion.

```mermaid
flowchart LR
    A["Registered command"] --> B["Committed history and receipt"]
    B --> C["Explicit-version query"]
```

- This is the SDK contract delivered across MR-06 through MR-12.
- Restricted `RaftClass`, `CommandContext`, `QueryContext` and operation registration are now exported through `cellar::sdk`.
- `Node::start_raft_cells`, public command/query handles and receipts connect Quorum/S3 commands and committed-image reads to node-owned producers.
- The counter example in `examples/raft-counter/README.md` uses both native HTTP and Axum.
- Local Stale, AtLeast and safe ReadIndex paths are implemented and qualified in MR-08, including bounded replica routing and explicit same-consistency leader fallback.
- MR-09 connects S3 output gates, stronger receipt retries and archive image restoration, qualified in the [archive audit](archive-audit.md).
- MR-10 qualifies native snapshot/member recovery; MR-11 qualifies automatic repair and leadership balancing.
- MR-12 qualifies bounded immutable Blobs, committed alarms, signed outbox/inbox delivery, capability gates and a producer/receiver restart example at the [recorded native boundary](evidence/MR-12.json).
- Operating, release and scale work remains MR-13 through MR-17.

## Commands and queries

Register operations by `(class, operation, kind, contract_digest)`. Ingress binds tenant and cell identity from authorized routing, not an unchecked client field. The destination resolves that registration and rejects a kind/digest mismatch before calling business code. Both native and Axum ingress use the same dispatcher.

| Surface | Required contract |
| --- | --- |
| `RaftClass` registration | Explicit schema version, command/query definitions and supported capabilities |
| `CommandContext` | Scoped cell transaction, request identity, captured inputs and outbox intent writer |
| `QueryContext` | Pinned read-only generation, authorized identity, served version and deadline |
| `CommandReceipt` | Result, opaque committed version, durability achieved and command identity |
| `QueryResult` | Result and exact served version |
| `CommandOptions` | Stable client/command ID, requested durability, retry window and timeout; framework hashes input |
| `QueryOptions` | Stale, AtLeast or Linearizable; explicit fallback and deadline |

- Command callbacks are synchronous within one transaction in the first release.
- They cannot await networking, send HTTP, call another cell, spawn background work, or retain writable storage.
- Expose captured clock/random values only through the host; resulting state and receipts are replicated as LTX.
- Native application code is trusted code, so a Rust type is not a sandbox against deliberate direct OS I/O.
- Document and test the supported contract; applications that bypass it are outside the durability/effect guarantee.

The internal MR-06 transaction facade supplies a captured Unix-millisecond value and 32 captured random bytes per attempt. Its SQL authorizer refuses ambient clock/random functions; use these captured values as SQL parameters. Additional input facilities require explicit host adapters before becoming SDK capabilities.

- `RaftClass::schema_digest()` first binds the class name, explicit schema version and supplied schema-definition digest under the `cellar-raft-schema-1` SHA-256 domain.
- It then binds that base digest and the sorted required command names and contract digests under `cellar-cell-class-1`, with explicit count/name lengths.
- Read this identity after registering commands and use it for the namespace root and physical update application digest.
- Omitting, renaming or changing a command cannot preserve voting compatibility by repeating the schema version/digest.
- Delivery targets use `RaftClass::definition_digest()` plus an exact operation contract.
- That declaration identity binds the class name, schema version and supplied definition digest before adding bindings, allowing self-delivery and cyclic class declarations without recursive manifest hashes.
- It does not replace the complete `schema_digest()` check at namespace admission.
- Full logical target history (`CellHistory`) selects the immutable namespace and its required compiled manifest. Physical group placement is absent from command and effect identity; see the [shared command boundary](../../../crates/cellar/durability/raft/README.md#shared-group-registered-command-boundary).
- Registered shared queries expose their exact `CellVersionObservation` through `QueryVersion::Cell`; `QueryContext::identity()` is logical cell history. The temporary old host uses a distinct `GroupCut` observation with no mutation sequence. CG-16 removes that staging branch; see the [registered query boundary](../../../crates/cellar/durability/raft/README.md#shared-group-registered-query-boundary).
- Declaration order does not affect the identity.
- Query contracts remain separate: a replica with incompatible/missing query code can still vote with the matching required commands, but refuses that query before executing it.
- Changing required command contracts requires a fresh immutable namespace identity; this is a marker-1 hard cutover without a compatibility reader or in-place conversion.
- Commands and queries have separate operation contract digests.
- The shared `consensus::command_request_digest` helper binds the exact normalized input bytes under its own marker-1 domain; the registered execution bridge validates it even when a request would return a duplicate receipt.
- Ingress normalization must produce the same bytes for native and HTTP calls.

Queries receive no general `Context`, writable `Storage`, normal cell constructor, alarm setter or outbound-network capability. SQLite authorizer/read-only flags must reject writes, schema changes, ATTACH, mutating PRAGMA and writable extension paths even if they arrive through raw SQL. Query functions use stateless services or immutable configuration; application instance fields cannot substitute for replicated state.

Shared-group `QueryOptions::default()` requests a linearizable read. It requires
a fresh quorum, so a group that has lost its majority refuses the request.
Use `AtLeast` with a receipt's version token to require a specific committed
version, or explicitly select `Stale` to permit an older image.

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

The framework wraps the command with its receipt transaction. Applications must not independently commit early. Errors and panics roll back private changes. The encoded result is bounded and stored with the mutation before LTX capture.

## Namespace and archive setup

`RaftNamespaceConfig` declares a compiled namespace capability; physical
`RaftGroupAssignment` values declare complete group placement. Logical handles
bind `CellHistory`, and `enroll` replicates the registered initializer under an
explicit immutable creation ID. A handle does not create a Raft machine or infer
a cell incarnation from a physical group.

`RaftNamespaceProvisioning` is the public operator setup path. It runs catalog
work on the supplied execution domain and shares the host's immutable range
initializer. `initialize: Some(plan)` selects an explicit, retry-stable initial
range plan; `None` only opens existing namespace/directory metadata. Managed
namespaces still require their separate authority protocol and refuse this path.
This setup creates no local member or serving capability.

`prepare_archive` returns a one-use `RaftGroupArchiveInitialization`. Persist and
sync its exact `encoded_intent()` before `commit`; uncertain completion requires
retaining that intent and recovering rather than allocating another initializer.
`open_archive` verifies only the catalog-bound identity and existing registry
counters. Missing binding, identity or counters refuse; startup never replaces
them. Neither a prepared intent nor an opened archive supplies a learner seed,
fresh quorum, local reclamation proof or disaster fencing authority.

`RaftCells::identity` exposes the immutable node/disk/process tuple as metadata.
For example, an ingress can combine its complete process generation with a
checked sequence to allocate query invocation IDs shared by native and Axum
handlers. Identity metadata does not authorize a stale process to serve or restart.

## Managed shared startup ownership

Managed startup checks the actual namespace declarations against the compiled
classes, rooted host capabilities and complete declared namespace registry. It
opens existing namespace metadata; group birth and range publication remain
separate operator protocols. An enrolled standby can boot before its first range
is published, without inventing an empty directory or serving a cell. This is not
capacity readiness, complete inventory or group activation.

Cold operators can publish an explicit standby directory genesis before capacity
admission. Its pin, journal and operation index prove zero selected groups; a
missing head or pin refuses. The exact standby observation is then the CAS base
for the first complete range map, after the fleet has reserved every birth.
Standby has no cell route and cannot replace an existing partition. Persist and
retry the same encoded intent after an uncertain publication. Import freezing
also journals its head transition, including for a real zero-group inventory.
Initial birth admission revalidates each voter's opaque capacity receipt at the
exact fleet cut. An ordinary retained boot invalidates the previous receipt even
if its descriptor remains Active. Existing groups retain ownership; new births
require fresh capacity qualification and activation for that boot.

The host pins its physical engine before binding a boot. The ordinary boot CAS
or retained maintenance rejoin protocol must settle, and the exact lifecycle
guard must sync before endpoints are published or groups attach. Retained routes
use durable current membership. Joined members still require their authenticated
install receipt, permanent engine claim and current catalog/boot checks. Initial
members use the managed initial-member permit. No metadata read grants a vote or
current-term quorum authority.

The log owner and immutable rooted node descriptor must match before a storage
pin or boot mutation. Advertised control/data/query endpoints remain explicit
configuration and may differ from listener bind addresses, including wildcard
listeners. Endpoint discovery cannot substitute for those owner checks.

One bounded directory service owns periodic rooted trust and endpoint refresh.
Its independent catalog read turn cannot occupy the foreground mutation turn.
Each batch retains one original deadline and at most eight observations; its
metadata reservation remains owned until the worker exits. Missing observations
preserve accepted routes. A superseded local boot or rooted conflict closes Raft
admission. Shared shutdown signals this service and joins entered work before
consensus and storage close. Internal observers do not retain the last external
SDK lifetime.

Node installs its existing stop reporter from that same bound authorization,
synced guard and actual probe signer. A dropped stop waiter cannot cancel an
admitted lifecycle update. Stop acceptance, metadata discovery and local producer
quiescence remain distinct from joined-owner proof and safe disruption accounting.
The complete managed lifecycle and cold capacity/inventory bootstrap require their
own public qualification; these ownership contracts do not mark CG-07 complete.

## Receipt semantics

For v1, use `(authenticated_client_id, sequence)` as the command ID, scoped to the cell and incarnation. Each client identity is non-reusable within that incarnation. The caller allocates increasing per-cell sequences and persists its pending command before sending; `CommandOptions` requires that explicit ID. Different client identities can submit concurrently; one identity submits its commands in sequence.

- Persist operation name, contract digest, normalized request digest, result bytes and minimum requested durability in a reserved receipt table.
- A retry with the same identity and content returns the same committed result while its receipt is retained.
- A conflicting digest for a retained receipt returns `CommandConflict`, including after leader change or snapshot installation.
- An expired sequence returns `CommandExpired` without claiming to remember its original content.

- Associate a newly applied receipt with its Raft position in framework recovery metadata in the same crash-consistent publication step as the image.
- It is not necessary to know the eventual Raft index while the private SQLite transaction executes.
- If a response is reconstructed later from a committed image, its token may name that image's covering index; it must not claim an earlier unverified cut.

- Maintain a durable per-client `last_sequence` and `expired_through` floor.
- The next new command must be `last_sequence + 1`; a higher value returns `SequenceGap`.
- A lower/equal value returns its retained receipt or `CommandExpired` when at/below the floor.
- Sequences never wrap.
- Receipt pruning is a replicated command that advances the floor only over a contiguous expired prefix.
- Keep the per-client floor after pruning and cap the admitted client-identity count; exceeding that budget refuses a new client instead of deleting a floor.

Persist `retry_until`/receipt retention policy. Capacity pressure cannot silently shorten the promised retry window. Incarnation-changing disaster restore may lack receipts after the archive cut; return a reconciliation outcome, not an automatic retry into the new history.

## Result and error behavior

| Outcome | Meaning and retry behavior |
| --- | --- |
| `NotLeader` | Redirect hint; server still validates authority after reroute |
| `Unavailable` / `Overloaded` | Availability refusal; inspect the command error's ambiguity flag before deciding whether submission may have occurred. Preserve the original ID and input. |
| `StorageFull` | Private SQLite execution ran out of storage before proposal submission; free capacity before retrying the same identity. The private image is discarded and rebuilt; a failed write cannot establish a reusable transaction. |
| `Ambiguous(command_id)` | Proposal may have committed; reconcile using the same ID |
| `CommandConflict` | ID reused with different content; do not retry with altered content |
| `SequenceGap` / `CommandExpired` | Reconcile the client's sequence or expired retry; never execute it as a fresh command |
| `VersionUnavailable` | Replica cannot cover the requested committed version before deadline |
| `WrongIncarnation` | Token or retry belongs to a different history; reconcile explicitly |
| `UnsupportedCapability` | Operation requires an SDK capability not qualified for this mode |
| `UnsupportedConsistency` | No silent fallback to a weaker read contract |
| `ArchiveUnavailable` | S3 requirement not yet proven; a submitted command remains ambiguous |
| `CorruptState` | Stop that replica's serving/proof role and recover through the protocol |

`RaftCellHandle::command`, `query` and `receipt` admit synchronously before returning a future. Dropping the future cancels its waiter. An unpolled waiter does not suspend its deadline: the node checks expiry before starting queued private work and before submission. A readiness observation is a hint; every command still checks live authority and can be refused if it changed.

`CellCommandError` preserves the original command ID, a conservative ambiguity
flag and its typed cause. `CellDispatchError::refusal()` exposes a local or
authenticated remote application refusal without importing verification interfaces.
A refusal does not clear the enclosing command's ambiguity. HTTP projection must
retain both the command identity/ambiguity and the cause; it cannot infer rollback
from an availability or archive error.

Cancellation before submission rolls back private work. Cancellation after submission cancels the waiter, not the consensus decision. Never convert a lost reply into a fresh mutation ID. Read retries retain their original deadline and consistency requirement. Cross-cell workflows remain application coordination.

## Effects and built-in services

- MR-12's alarm adapter declares synchronous callbacks through `RaftClass::alarm` and requires explicit host scheduling options.
- Commands set/cancel named generations in their transaction.
- The current leader discovers committed rows without user traffic and fires through private system commands.
- Firing consumes the exact generation together with its callback's SQL changes; failure rolls back.
- Normal callers cannot invoke these commands or consume the reserved system client.
- The [alarm contract](effects-design.md#committed-alarms) describes bounds, clocks, retention and the separate Node semantic-drain limitation.
- `RaftHostOptions::effects` and `RaftEffectOptions` configure the shared owned scheduler.

- `RaftClass::outbox` declares signed logical destinations and explicit bounds.
- `CommandContext::emit` persists actual request bytes and a monotonic lane sequence with business state.
- The current leader commits an attempt and its retry deadline before sending, respecting the intent's durability policy.
- `RaftInbox` couples an authenticated source request, receiver SQL mutation and signed receipt in one ordinary command transaction; its configured fixed durability gates receipt output.
- `RaftNativeEffectSink` provides the native adapter.
- Source cleanup verifies the exact request/receipt and attempt generation, then commits the settled prefix.
- `QueryContext::outbox_progress` reads only its pinned generation's progress.

- An old request can still commit after its sender loses authority.
- Retries retain identical bytes and logical IDs, with one receiver client per source lane.
- Normal operations cannot consume that client in an inbox class.
- A missing sink response remains ambiguous even when the adapter reports unavailability; it cannot justify a fresh ID.
- Signed native receipts authenticate a trusted receiver's assertion; they are not Byzantine quorum certificates or arbitrary external exactly-once proof.
- Existing receipt limits apply until MR-13 adds safe operational maintenance.
- The [outbox contract](effects-design.md#committed-outbox-and-receiver) records bounds, key/definition identity, retained failures and the recovery example.

- The implemented `RaftClass::immutable_blobs` adapter stores actual bounded bytes in the command transaction.
- `CommandContext::put_blob` returns a digest/length reference; `blob` reads transaction bytes or, on `QueryContext`, the pinned committed generation.
- Quorum durability includes the bytes in LTX; S3 durability also verifies complete dependency objects and the recoverable archive.
- A reference alone cannot register missing content.
- The initial adapter retains all values, has no GC API, and duplicates their bytes in the SQL image and dependency objects.
- See the [business-feature contract](effects-design.md) for bounds and remaining work.
- Any future external-reference-only Blob adapter must separately prove complete durable content before acknowledgment.
- Multi-cell service calls, WebSockets, mutable containers, existing Workflow and Queue callbacks are refused for Raft classes until their capability adapters receive equivalent qualification; classes in Fleet mode continue to support their existing behavior.

## Deployment and authentication

Keep separate command and query contract digests. A follower can hold the correct physical data while lacking compatible query code; it then remains a voter but does not serve that query. Leadership eligibility requires a compatible command implementation and applied schema. Do not silently migrate on query opening.

Perform current authentication and authorization at ingress and destination. Authorization derived from a stale database image is itself stale; operations requiring latest revocation checks must use a fresh authoritative check. Version tokens prove requested history, not access permission.

The canonical shared SDK query result carries CellVersion, CommitWitness,
CellVersionToken and the serving NodeIdentity. The logical version remains
separate from physical group term/index. Tokens authenticate the complete logical
history, schema, version, witness, issuance and expiry. They prove no access
permission. AtLeast validates the token before comparing the selected cell's
version; another history or schema refuses. Linearizable needs a fresh safe
ReadIndex and the corresponding applied root. Stale supplies neither a quorum
barrier nor a wall-clock age promise.

QueryOptions supplies consistency, timeout, bounded max_attempts and QueryRouting.
Local uses the same shared controller. Replicas captures one to eight distinct
member preferences, a positive attempt_timeout no larger than the original timeout,
and an optional leader_fallback. Candidates run in order, at most once; one final
known leader may be tried only if it was not already a candidate. An absent leader
hint does not authorize a guessed destination. Each attempted read consumes the
same global attempt bank. Every ticket deadline can only shorten the original
invocation deadline, which starts before the waiter returns.

Retries preserve the contract, input, token, logical history, routing fingerprint
and node admission. Expired physical SQL and transport work retain their permits
until they join, even while a later attempt runs. Results retain the actual local
or authenticated remote replica and response reservations. Wrong-history/schema,
registration and permission failures do not become replica fallback opportunities.
Incoming query RPCs execute locally and cannot recursively forward.

Physical group discovery and leader hints are observations; destination admission
still checks current namespace/process grants, route, membership and the selected
cell read proof. The retired per-cell QueryRouter registration path is not used.
The shared routing component passes its recorded native gates; complete public
SDK, ingress and managed lifecycle qualification remains tracked by the
[cell-group implementation record](cell-groups/progress.md).

RaftCellsConfig::query_origins installs trusted namespace read grants for exact
cluster/namespace/node/disk/process endpoints. Every authenticated cell route
binds a physical group and a separate logical cell. A grant does not authorize
Raft messages, commands, enrollment or receipt mutations. Revocation cancels old
query waits; revision tombstones prevent stale updates or regrants from reviving
those requests. The embedding supplies trusted grants on restart. The
[read audit](read-audit.md) retains historical qualification scope; its old SDK
fixtures must be converted before they can qualify the shared runtime.

- `RaftCellsServices::versions` supplies explicit managed Ed25519 signing and verification keys.
- Configured token lifetime is between one millisecond and seven days.
- At most 64 verification keys are retained.
- A token cannot outlive its signing key's verification deadline.
- Future-issued or expired tokens are refused; clock skew/rollback can affect availability and supplies no strong-read proof.
- The development counter uses a fixed test key; production callers must inject their own stable managed material.

- Rotate with a verifier-first rolling deployment: distribute the new public key while the old signer remains active, then switch signers while retaining all unexpired verification keys.
- The namespace catalog records bounded canonical public-key retention metadata under conditional updates.
- Startup reconciles an ambiguous write by exact reads and refuses a configuration that drops or changes an unexpired registered verifier, including after restart.
- Expired keys may be removed.
- Private signing material is never written to this registry, and registry contents cannot add a key to a server's configured trust roots.

Public constructors remain centralized in the application builder. Registration validation rejects an ordinary mutable `Cell` factory for a Raft class. Background workers and maintenance must use the same mode-aware dispatch boundary so they cannot bypass this restriction.
