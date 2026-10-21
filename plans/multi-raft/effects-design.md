# Committed business features

Status: MR-12 is IN_PROGRESS. The immutable Blob adapter is
[qualified at its native boundary](evidence/MR-12-blob-progress.json). Alarm/outbox scheduling, durable destination receipts,
leadership races and the full unsupported-binding gate are still required.
No effect callback or timer is enabled by this Blob component.

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

## Remaining MR-12 integration

1. Persist bounded alarm/outbox intent identity, destination binding, payload,
   sequence, retry deadline and generation in the replicated transaction.
2. Add a bounded owned scheduler that rotates registered histories, verifies
   current-term leader readiness and reads only published images. Followers
   never run business timer or delivery callbacks.
3. Commit attempts before sending. Recheck authority, cancel obsolete waiters,
   and reconcile completion through a conditional replicated command. Cancellation
   cannot establish that an external request did not take effect.
4. Supply a concrete receiver adapter whose business change and stable receipt
   commit together. Retrying equal identity/payload returns that receipt; changed
   payload conflicts. Retention cannot silently permit an old effect to run again.
5. Validate unsupported Workflow/Queue/container/socket capabilities before
   activation. Do not route them through Fleet ownership for a Raft history.
6. Run all six original MR-12 cases through native producer/receiver examples,
   including no-traffic alarm recovery, private-intent suppression, an old request
   racing failover, and lost delivery completion. Run existing outbox/event-stream
   regressions as well as owning Raft tests before marking this package DONE.

Raft alone supplies no external exactly-once contract. MR-13 still owns unified
Node semantic drain and archive-obligation reporting. Release, scale and
federation retain their separate gates in the implementation plan.
