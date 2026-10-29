<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Zero-quorum disaster recovery

This path is separate from managed fleet repair. Managed namespaces refuse
disaster admission. An unmanaged namespace must atomically claim permanent
exclusion from fleet import before publishing any disaster intent or invoking
the fencing authority. That claim shares the birth-inventory CAS with freezing:
if freeze wins, disaster admission refuses; if disaster wins, later freeze/import
refuses, including after an interrupted recovery. Existing initial-name intents
remain immutable; fenced successors are decoded through their predecessor proof.
Use matching binaries and fresh experimental catalogs for this format-1 cutover.

The operator API restores a cell from a selected S3 recovery set into a new Raft
history after permanent loss of its voters. It is distinct from learner seeding,
which still requires a fresh ReadIndex from a surviving quorum.

This recovery path supports `RaftImageStorage::Materialized`. Extent-backed
namespaces refuse before intent publication or infrastructure fencing; their
disaster seeding path is not implemented.

## Authority and durable decisions

`S3DisasterRecovery::open` takes explicit object storage, catalog/namespace
configuration, LTX host services and archive limits. `archive_reference` observes
an address for review. The operator supplies `DisasterRecoveryRequest` with the
old scope, that exact reference, and a replacement birth. `fence_request` produces
the canonical digest for an infrastructure controller; it does not mutate storage
or authorize a replica. `recover` validates the referenced material, reserves the
new group, confirms an immutable intent, obtains and verifies infrastructure
evidence, and confirms a separate completion record before returning the birth.

An intent closes new admissions for the old history, including old routers and
retained-disk startup. It cannot stop processes already running. The configured
`DisasterFenceAuthority` must use authoritative deployment inventory, stop all
old execution and effects, revoke restart/replacement-launch capability, and
verify that the fence took effect. Inventory must include members added after
the selected archive, standby hosts, routers and effect workers. Unreachable
machines, elapsed time, caller booleans and an S3 flag are insufficient evidence.
There is no default authority implementation. The controller is a trusted adapter;
catalog integrity and trusted host/controller code are assumed, as with ordinary
bootstrap and storage. This is not Byzantine consensus or protection against a
malicious holder of infrastructure administration privileges.

The complete decision binds the intent digest and bounded evidence. Concurrent
and ambiguous attempts reconcile exact object keys through conditional creation
and readback. A different target or cut cannot replace an intent. Retrying a
completed request independently verifies its winning evidence. A failure after
intent publication is not a rollback: keep the old history fenced and retry the
same request. Retain all referenced catalog records and archive objects. There is
no archive garbage-collection policy in this change.

Each successor has new group, incarnation and initial physical node identities;
member numbers may repeat within the new scope. No ancestor history/node identity
can be reused. Discovery follows a bounded chain of at most 64 births. Original
births and reservations remain intact, so old archive keys retain their exact
meaning. New initial-member claims are stored under the new incarnation.

## Restore and admission

Register the returned birth as `RaftCellPlacement::Replica`, with fresh node,
disk and process identities and an empty local directory. The host requires a
confirmed current recovery birth and absent permanent claim before downloading
the pinned reference. Missing or corrupt selected objects fail startup even if a
newer head exists. Archive reads and replay use the existing bounded snapshot
receive budget, scratch ownership, generation limits and blocking SQL pool.

The engine atomically installs a materialized checkpoint at index 2, term 1,
vote 0 with the new initial configuration. Index 1 remains reserved for empty
genesis. Old terms, votes, membership changes and leadership do not transfer.
The snapshot/LTX replay preserves application bytes, receipts, client sequence
floors and Blob values without executing callbacks. The initial member claim is
confirmed before runtime attachment; the fresh quorum must elect a leader before
command admission. Incarnation-bound old version tokens refuse.

New stable state resumes through ordinary startup without reseeding or resetting
votes. A permanent new-member claim with missing local state still refuses.
Crashes before the engine checkpoint can replay the same pinned recovery set;
crashes after it reconcile the same local state/claim, using the existing seed
storage protocol. This change does not claim a new exhaustive process-kill matrix.

Pending old-origin outbox deliveries refuse admission. They may have reached a
receiver after the selected archive cut; rewriting their origin would create
duplicates. This change provides no bypass or reconciliation command. Choose a
reconciled recovery point/application procedure before disaster publication.
Other pending application work retains the semantics of the selected image;
cross-cell and external-effect consistency remains the operator/application's
responsibility.

## Recovery-point limits

Raft supplies live quorum durability. Quorum acknowledgements newer than the
selected archived cut may be lost after all voter disks are lost. S3-durable
acknowledgements are protected only by a complete, retained recovery set covering
them; deliberately selecting an older cut deliberately rolls back later state.
Per-cell latest heads are not a globally consistent fleet snapshot.

## Reproduction

Build `tools/perf/raft.Dockerfile` from the checkout, using a target cache owned by
that checkout. Run the immutable image with:

```sh
python3 tools/perf/raft_disaster.py \
  --context YOUR_ISOLATED_DOCKER_CONTEXT \
  --image sha256:YOUR_SOURCE_MATCHING_IMAGE \
  --output .cellar/disaster-qualification \
  --cells 24 --port-base 19800
```

The fixture uses three native voter processes and digest-pinned real RustFS. It
issues concurrent S3-durable commands, records exact plans, refuses missing and
forged controller certificates, then disables restart, kills/removes every owned
old voter container and deletes all three owned voter volumes. The controller
checks exact IDs and labels before destructive operations and confirms absence
before signing the plan. Its ephemeral private key is outside every container
mount; the Rust adapter verifies Ed25519 signatures with an explicitly provisioned
public key. Docker container-ID removal plus the catalog admission fence supplies
this fixture's fence. A production scheduler needs its own stronger platform
integration and launch revocation, not this local helper.

Fresh voter volumes recover solely from RustFS and trusted deployment metadata.
The fixture verifies all replicas, old-token rejection, original receipt retries,
new S3 writes, stale old-configuration recreation refusal, and ordinary new-host
restart. Owned surviving containers stop at the end; new volumes and private
logs remain for inspection. Only the invocation's original voter disks are
deleted. Unit tests separately exercise changed ownership and unconfirmed stops.

## Qualification on 2026-10-28

The final destructive run passed at source
`a86490bb4869b7f71f8d5133f6ad915094233670`, after merging `main` at `5998b82c`.
The immutable native image was
`sha256:ea1674877860b4071d62bf456158e5f928a4237ab53a67521b10fbf7aeca66d4`.
It used actual RustFS pinned to
`sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858`,
three voter processes and 24 groups in one eight-CPU Colima VM.

| Check | Verified result |
| --- | --- |
| Before loss | 216 S3-durable acknowledgements; independent state model |
| Selected archive cuts | Snapshot index 3 through committed index 11 in every group |
| Infrastructure fence | All three original container IDs removed and all three voter volumes deleted; absence confirmed before signing |
| Recovery | Fresh group, incarnation, node and disk identities; all 72 replica states matched |
| Authority failures | Missing and forged certificates refused; identical completed recovery retried successfully |
| Client continuity | 72 old-version-token refusals, 24 original receipt retries, 24 new S3-durable writes |
| Post-recovery checks | 72 matching states after new writes; stale old-configuration startup failed specifically with `Fenced` |
| Restart | 72 matching states after ordinary retained-disk restart; another independent restart checked both `served_by` and state on all 72 replicas |
| Shutdown | All surviving owned processes stopped, no OOM; normal processes exited 0 and the refused old host exited 1 |

Recovery publication through initial state/token verification took 7.71 seconds
in this run. This excludes fencing and disk deletion, includes the identical
operator retry, and is an observed interval rather than a recovery-time target.
Other host workloads were present. The earlier successful run on merged source
`26e8c9dd` observed 9.92 seconds at the same measurement boundary.

At the final source, 195 affected integration cases and the outbox refusal unit
test passed with the default thread stack (four concurrent integration tests).
Four ignored process helpers run through their parent matrices; an opt-in
extent/RustFS case remains unrun. Clippy with warnings denied passed for the
library, five integration targets and the measurement binary. Formatting/layout
covered 1,358 Rust files; dependency isolation and documentation checks passed.
The Python harness suite passed 257 cases with one existing opt-in skip.

Validation found and fixed a fixture retry-window error and a native startup
stack regression. The latter reproduced on this branch, passed on unmodified
`main`, and passed with the default stack after boxing catalog history traversal.
An archive coalescing assertion and a partial-body injection assertion initially
missed their timing conditions under concurrent load; both passed unchanged in
the final complete suite. The initial VM loss happened before fixture creation.
Machine-readable results and raw-evidence hashes for these attempts and the
successful final run are retained locally outside version control, together with
private configuration, ledgers and stopped new volumes.

MR-13 remains open. This qualifies the materialized-image component and Docker
controller boundary. It does not qualify independent failure domains, Amazon S3,
production infrastructure fencing, extent-backed disaster seeding, pending-outbox
reconciliation, fleet-wide atomic restore, capacity, an exhaustive new crash
matrix, or an RTO service-level objective.
