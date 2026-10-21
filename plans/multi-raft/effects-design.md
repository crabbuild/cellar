# Committed business features

Status: MR-12 is IN_PROGRESS. The immutable Blob adapter is
[qualified at its native boundary](evidence/MR-12-blob-progress.json).
The alarm implementation is
[qualified at its native boundary](evidence/MR-12-alarm-progress.json). Outbox dispatch, durable
destination receipts, leadership races and capability gates are implemented and
undergoing combined qualification. Enabling Blobs alone starts no business timer.

## Immutable Blob adapter

`RaftClass::immutable_blobs(RaftBlobLimits)` explicitly enables immutable values
and binds their limits into the compiled class identity. Classes without this
capability preserve their existing identity and refuse these calls. Changing
limits requires a fresh compatible namespace; there is no migration reader.
Before catalog or member creation, Node checks the declared limits against its
image, result, archive dependency, capture and recovery-set budgets. The direct
host entry point applies the same checks.

`CommandContext::put_blob(bytes)` hashes and retains the actual bytes in a private
framework table in the same transaction as the command receipt and business
changes. An aborted command rolls back its Blob as well. Equal content reuses the
same immutable value. Conflicting stored bytes, digest/length mismatch and bounds
violations fail explicitly. This is a bounded cell-state adapter: default limits
are 64 KiB per value, 128 values and 1 MiB total per history. Values remain retained;
there is no deletion or reachability-GC API. SQL state and image admission still
bound the complete cell.

`RaftBlobReference` encodes a SHA-256 digest and a length into 40 bytes. It is a
content identity, not access, existence or durability proof. A reference supplied
by another caller cannot register absent content. `CommandContext::blob` reads
this transaction's bytes; `QueryContext::blob` reads only the query's pinned
committed generation. Application SQL cannot manipulate the framework table.
This adapter does not grant the existing Fleet Blob bucket capability or perform
network I/O inside synchronous command/query code.

The bytes themselves travel in committed SQLite/LTX state, so Quorum durability
retains the actual immutable value. Archive capture opens the exact published
generation, checks its complete bounded inventory and verifies every digest
before producing dependencies. It reserves the entire capture allowance before
opening that generation or allocating the closure, and retains the reservation
through archive publication. Entry selection accounts for dependency and durable
membership memory inside the same allowance.

S3 publication writes each content-addressed dependency, checks the immutable
manifest and physically replays the recovery set before acknowledging. Missing
dependency uploads leave the old head intact. A lost S3 wait can be ambiguous;
retry uses the original command ID and input and cannot rerun a committed callback.
An exact selected recovery set refuses a missing dependency instead of choosing
a newer head. This initial adapter duplicates the Blob bytes in the SQL image
and its dependency object; eliminating that duplication requires a separately
proved external-reference retention protocol.

## Committed alarms

`RaftClass::alarm(name, digest, callback)` declares a synchronous transaction
callback; optional `alarms(RaftAlarmLimits)` sets its explicit bounds first.
Defaults permit 16 named alarms and 64 KiB per payload. `CommandContext::set_alarm`
and `cancel_alarm` accept only declared names. Setting an alarm records its due
time, payload, durability and a generation bound to the full history, setting
command ID and name. A rearm replaces that generation; cancellation removes it.
These changes share the business transaction and receipt.

The host requires explicit `RaftHostOptions::effects` (`RaftEffectOptions`). Before catalog creation,
it checks capability and image capacity. Its one owned timer examines at most
four registered groups per discovery turn, reserves metadata before observation,
checks actual current-term leader readiness and reads only a published image
through the existing SQL owner. Follower and learner roles cannot invoke an
alarm callback. S3 intents wait for archive proof covering the selected cut;
Quorum intents retain ordinary backlog admission. Defaults use a 100 ms interval,
5-second status/firing deadlines, a 60-second receipt retry window and a 512 KiB
shared metadata reservation. These are configured bounds, not a deployment latency SLO.
Owned physical SQL work can delay this single scheduler; it is joined on closure.

Alarm firing uses a private system-command registry. Public contract discovery,
normal command validation and HTTP commands cannot invoke those operations or
consume their reserved per-history command client. The private path retains the
ordinary term/process/configuration checks, private execution, LTX proposal,
commit, publication and durability output gates. The next internal sequence comes
from the selected committed image after the host has no older producer and the
runtime has current-term readiness. An unresolved submitted producer remains
owned by the host; a late waiter result cannot rebind its command identity.

Firing rechecks the exact generation and captured wall-clock due time, deletes
that row, and invokes the callback in the same transaction. Failure rolls back
the deletion. A successful callback may arm the next occurrence atomically.
Replaced/cancelled generations cannot invoke the callback. Callbacks have the
same private/speculative execution contract as other commands; this supplies no
external exactly-once guarantee. Use committed outbox delivery for external work.
Operation deadlines use the execution-domain monotonic clock; wall-clock changes
can delay due-time scheduling and grant no consensus authority.

One stable internal client per history bounds client identities. Existing receipt
retention limits still apply; this component adds no automatic pruning. Admission
can refuse further work at those configured limits. Operational retention and
the complete drain/reporting contract remain later work. Public Node closure
joins the timer's domain; its existing Fleet report does not supply a graceful
Raft close receipt or permission to discard unarchived disks.

## Committed outbox and receiver

`RaftClass::outbox(RaftOutbox)` explicitly declares bounded lanes. Each lane binds
an immutable logical target: full destination history, stable class definition and
exact command contract, receipt verification key and a fixed minimum durability
policy. Signing
keys, grant identities, lane definitions and limits enter class compatibility.
Physical endpoint hints do not enter that identity. Changing a target or key is
an incompatible definition, rather than a silent redirect of retained intents.

`RaftClass::definition_digest()` binds the class name, explicit schema version and
supplied definition digest before adding delivery bindings. Pairing that stable
identity with an exact operation digest permits self-delivery and mutually
referencing class declarations. A digest of the entire target manifest would
create recursive hashes. Namespace admission still checks `schema_digest()`,
which binds the complete required command/capability manifest, including grants,
limits, keys and delivery protocol identity. The target's full immutable history
selects that namespace; the stable definition alone grants no compatibility or
mutation proof. Native tests exercise actual S3-gated self-delivery, rather than
only comparing declaration hashes. Wire markers remain 1; incompatible capability
protocols require fresh experimental namespaces, without a migration reader.

`CommandContext::emit(lane, bytes)` persists the actual signed canonical request in
the business transaction. Full origin history, origin schema, binding identity,
monotonic lane sequence, target and payload digest are immutable. Rollback removes
the request and sequence increment. Repeated attempts and elections send identical
bytes: attempt generations, terms, process identities and clocks stay outside the
receiver input. Payload bytes are separate from logical identity, so reusing an ID
with changed bytes cannot create a new delivery. A new incarnation cannot silently
redispatch an old history's retained bytes; disaster cutover needs an explicit
reconciliation protocol.

The shared owned scheduler rotates groups and alternates alarm/outbox priority
per history. A node-wide parity counter would unfairly pin priority when the
registered group count is even. Both adapters use the same committed internal
client allocator and one reserved metadata permit. It skips older producers,
configuration changes and unpublished state, then observes actual current-term
leader readiness and reads only the published image. S3 intents wait for verified
archive coverage of that selected cut.

Before each send, a private command conditionally claims the head of one lane,
committing an attempt generation and retry deadline. Its ordinary command receipt
must pass the intent's durability gate. The dispatcher rechecks term, local process
and engine authority immediately before calling the sink, and at bounded intervals
while awaiting its result. Leadership loss or the operation deadline drops the
waiter; an already sent receiver command can still commit. A committed claim whose
response was lost becomes retryable from its persisted deadline. Cancellation is
never evidence that the destination did nothing.

`RaftInbox` wraps an explicitly declared normal destination command. Its source
grants bind cluster, namespace, source class, binding digest and sender public key.
The signed request also binds the exact destination history/definition/operation. Strict
marker-1 canonical decoding, size limits, signature and payload digest checks run
before command receipt lookup and again at transaction execution. Signatures supply
configured native-receiver authentication; they are not Byzantine quorum certificates
or an OS sandbox. Native code and provisioned keys remain trusted.

A source lane uses one reserved client namespace and an increasing command sequence,
rather than a new client per message. Normal application operations cannot consume
that client in a receiver class. Generic command receipts enforce exact input and
contract identity; the receiver additionally commits its business change, full lane
digest, request digest and signed result atomically. It retains the latest receipt
per authorized source lane and refuses expired sequences rather than executing them
again. Receiver callbacks are private/speculative until commit, so arbitrary network
side effects inside a callback do not acquire this contract.

A receipt signs a purpose-separated digest of the exact canonical request, full
logical lane identity, target, fixed durability policy and bounded business result.
The receiver requires that policy before invoking its callback, and releases the
signed bytes only through its ordinary Quorum/S3 output gate. A receipt from a
different request, history, payload or key cannot settle the source intent. Source
cleanup is another private replicated command: it verifies the proof and exact
attempt generation, deletes that intent and advances only the contiguous settled
prefix. Stale-generation completions are harmless conditional refusals. Lost
cleanup responses are reconciled from the committed source image.

`RaftNativeEffectSink` is the concrete adapter for up to eight independently
replicated native receiver handles. Endpoint replacement remains a routing concern;
logical identity and receiver proofs stay fixed. Custom adapters must honor their
memory/cancellation bounds and must not detach tasks. A Rust success result or an
arbitrary HTTP 200 is insufficient for settlement. Endpoint routing preserves
terminal identity/contract/expired-receipt refusals, rather than replacing them
with a later follower routing error. A permanently refused delivery stays retained
and blocks its lane; this component provides no implicit payload rewrite or
dead-letter/skip operation. There is no generic external
exactly-once promise. The [producer/receiver example](../../examples/raft-effects/README.md)
runs a primary replacement and then resumes the same retained disks in a second
process invocation.

| Bound | Default / maximum |
| --- | --- |
| Declared outbox lanes | 16 / 32 |
| Pending signed requests per cell | 16 / 128 |
| Payload bytes per request | 32 KiB / 32 KiB |
| Encoded signed request | 60 KiB |
| Retained pending request bytes | 1 MiB / 16 MiB |
| Business result / signed receipt | 4 KiB / 16 KiB |
| Receiver grants / source lanes | at most 32 / 128, explicitly configured |
| Retry delay | 1 second / configured 100 ms–60 seconds |
| Scheduler metadata | 512 KiB default; required for outbox activation |

Node and direct-host preflight check capability, input/result/receipt/client/image
capacity before creating catalog or engine state. SQL observations check lengths
and types before allocating retained fields. Settled intent rows are deleted;
generic command receipts have their existing separate retention limits. There is
no automatic pruning yet: a long outage or enough attempts can fill those limits
and cause controlled refusal. MR-13 must supply an operational retention contract,
including protection of the latest receiver receipt while its source can retry.
No indefinite uptime or unconstrained recurrence is claimed here.

## Mutable SDK capability gates

Raft registration cannot also own a Fleet `Cell` or Workflow factory. Node checks
that boundary independently of `Application::resolve_raft_class`, before catalog
or engine creation, so a custom application cannot bypass it. Native tests use
actual Workflow registrations and mutable factories that request the existing
Queue, container and websocket APIs, and assert no factory invocation or catalog
write. Fleet cell/Workflow bindings cannot reinterpret a registered Raft class;
unrelated Fleet resources can coexist in the native application.

`CommandContext` has no Workflow, Queue, container or websocket methods; compile-fail
documentation checks those type boundaries. Deployment bindings available to a
Fleet `Context` are not inherited by a Raft transaction. The explicit immutable
Blob, committed alarm and outbox adapters are the supported feature surface.

Combined qualification must execute all six original MR-12 cases, owning Raft
regressions, existing Fleet outbox/event-stream cases and the finite native restart
example before this package is marked DONE. MR-13 still owns unified Node semantic
drain and archive-obligation reporting. Release, physical scale and federation
retain their separate gates in the implementation plan.
