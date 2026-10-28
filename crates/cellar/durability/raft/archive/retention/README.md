<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Remote-root ownership registry

This is an unstable verification component for persistent ownership records.
It does not yet publish extent data, run garbage collection, select the existing
archive head, or permit native activation before complete hydration.

## Ownership and retries

Create a registry in an extent archive namespace and persist its exact
`RootRegistryId` in the authoritative owner. Creation allocates a random namespace
and exclusively creates an immutable identity object. Opening an existing ID
requires that identity and a valid conditional registry record; it never resets
a missing record or its revision counter. Do not delete these control objects.
Their loss or rollback requires authority recovery, not automatic recreation.

Each allocating operation has two steps:

1. Prepare a `RootIntent` and durably record the complete intent in the owner.
   Preparation reads metadata only and protects no data.
2. Commit that exact intent. After a lost response or restart, repeat it to
   recover the same still-live reservation/reference. Do not prepare another
   intent merely because a response was lost.

An intent binds one next revision, a random nonce and its complete operation.
Different callers cannot accidentally share a reference by submitting identical
parameters. A competing committed mutation expires the intent unless its exact
result is still present. Released/aborted results cannot be recreated by retries.
The revision counter replaces an unbounded collection of retired-token tombstones.
Every live reference counts independently, even when it names the same root.

Reserve before uploading. The committed reservation protects its upload
generation and, optionally, one exact live parent root. Independently verify the
new immutable descriptor and its complete dependency closure before binding it.
Binding consumes the reservation and creates an explicit reference; it may also
atomically select the new root at a non-regressing native log position. Selection
retains its root independently of explicit references. A child reservation keeps
its parent retained even if the original reference is released meanwhile.

Only a live explicit reference can be cloned, or the exact selected root acquired.
Dropping a Rust handle has no remote effect. Release references only after the
authoritative owner has durably retired/fenced all uses. Abort permanently ends
an upload reservation; an already-issued late upload may leave an orphan but
cannot authorize binding that generation. Do not expire unknown owners by time
alone. Interrupted intents and confirmed references must be reconciled from the
owner's persisted records. A replacement process must acquire and persist its
own independent hold before serving; merely reusing its predecessor's serialized
reference would remain vulnerable to that predecessor's delayed release. If a
concurrent release wins, acquiring the replacement hold must fail before admission.
Joined readers and writers must retain their owner's hold until physical exit.

Storage changes use conditional updates and independent bounded read-back.
Only definite CAS conflicts may repeat a transition automatically. A successful
or uncertain write followed by a different record returns `Unconfirmed`; an
allocating caller retains its intent and reconciles explicitly. No second
allocation is silently substituted. Reads and writes use the archive execution
domain, cancellation and per-operation deadlines.

The record is limited to 64 KiB, 64 concurrent explicit references and eight
concurrent reservations. Releasing slots permits continued lifetime churn.
Revision exhaustion refuses further changes. These are metadata/work bounds,
not limits on logical cell bytes or the number of lifetime updates.

## Required collector integration

`RootProtection` is a consistent mark input, not a deletion permit. A future
collector must verify every protected descriptor and expand its exact physical
dependency keys before deleting anything. It must protect complete pending
upload generations and exclude generations newer than the mark revision.
Missing/corrupt protected metadata must refuse collection.

Physical object keys must include never-reused generations. Reusing a deleted
content-addressed key would let a delayed DELETE destroy a new owner's bytes.
New roots may inherit old objects only from a parent protected atomically by
their reservation; they cannot resurrect an arbitrary unprotected descriptor.
Mark exact object keys after binding: retaining entire historical generations
indefinitely would accumulate unrelated dead bytes.

The registry does not implement those descriptor/key/collector operations yet.
It cannot be used to collect the existing `root-extents`, dependency, manifest or
archive-head keys. Native markers, source grants and owner reconciliation must
persist registry references and establish the new installed-root proof before
this component can support partial-hydration activation. Registry success alone
proves neither byte integrity, log authority, voting eligibility nor permission
to acknowledge an application result.
