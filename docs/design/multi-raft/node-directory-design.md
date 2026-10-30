<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Trusted node registration and process views

Purpose: Define trusted engine registration, process-view ordering, routing refresh, and physical storage verification.

[Design library](../README.md) · [Topic contract](README.md)

| Implementation | Validation or example |
| --- | --- |
| [Code and API](../../../crates/cellar/durability/raft/README.md) | [Checks and usage](../../../examples/raft-counter/README.md) |

The map summarizes this document’s scope; the recorded gates determine completion.

```mermaid
flowchart LR
    A["Stable engine registration"] --> B["Monotonic process view"]
    B --> C["Authenticated route refresh"]
```

- Status: MR-11 implementation contract.
- The [local storage foundation](evidence/MR-11-node-storage-progress.json) qualifies stable engine identity, root-fence allocation and physical close ownership.
- The S3 pin/process-view adapter and mandatory physical storage checks are [qualified locally](evidence/MR-11-directory-progress.json): nine directory cases and a public standby identity regression pass with owning Raft checks.
- Opt-in host publication/distribution, atomic routing refresh and independently bounded host control admission are [qualified at their component boundary](evidence/MR-11-control-progress.json).
- Local registered-history capture is [qualified](evidence/MR-11-inspection-progress.json).
- Authenticated peer collection is [qualified](evidence/MR-11-peer-progress.json), including separate namespace permissions, conservative ages, physical routes and joined cancellation.
- Complete bounded node-wide load collection is [qualified](evidence/MR-11-load-progress.json).
- The integrated automatic controller is [qualified at the native boundary](evidence/MR-11.json).
- The [membership contract](membership-design.md) remains authoritative for learner initialization and configuration changes.
- A directory observation never changes voters, initializes storage or supplies a quorum proof.

## Stable storage registration

- Each newly created Raft engine generates a nonzero 128-bit storage instance ID from its execution-domain RNG.
- Both its synced node marker and engine root carry that ID.
- Resume reads and compares both records with configured node/disk identities.
- The ID survives ordinary restart; recreating an empty engine produces a different ID, even if an operator repeats the configured disk lineage.

Before production member bootstrap or learner initialization, conditionally create and read back the immutable cluster/node storage pin:

~~~text
<catalog-prefix>/raft/<cluster>/nodes/<node>/pin
{ cluster, node, disk, storage_instance }
~~~

- The exact pin must match the open engine.
- A different disk or instance refuses admission and requires a fresh node identity.
- A missing local group still uses the existing permanent member-claim checks.
- Neither clearing files nor choosing another member ID authorizes an old node to masquerade as intact storage.
- The pin is cluster-wide, shared by namespaces, and grants no group membership.

This changes required marker-1 storage fields and namespace protocol identity. Use a hard cutover with fresh experimental namespace/prefix/local state, without compatibility readers or backfilling older records.

A stored instance ID is not proof against copying live vote state to another disk. Normal recovery retains exclusive storage ownership. Disaster restore fences the old deployment and creates a fresh history.

## Durable process-view ordering

Reserve a process-view revision by syncing a nonempty node-root fence through the existing engine worker. Its monotonic global sync sequence is separate from every group's configuration revision. A typed node-fence reply cannot settle a Raft persistence/apply operation. Resume preserves the sequence. No additional per-command directory write is required.

Publish a canonical bounded marker-1 record at:

~~~text
<catalog-prefix>/raft/<cluster>/nodes/<node>/view
{ cluster, node, disk, storage_instance, revision, process, boot_attempt,
  protocol, label, server_name, control_address, data_address, query_address }
~~~

- A process-view publisher is bound to the exact open engine owner.
- The configured local endpoint must match its node, disk and process and the host's authenticated label.
- A fresh boot uses a fresh process generation.
- Retrying an ambiguous PUT keeps the boot attempt and payload identity; exact readback reconciles success.
- Conditional updates use the observed object version.
- An older publication that wins before this owner's CAS is retried with the exact same fenced payload.
- An old closed owner cannot reserve or publish another revision.
- A late old PUT cannot overwrite a newer conditional update.

If the view object is missing, a current owner can publish a newly fenced revision above every revision it could previously have published. The immutable storage pin still has to match. Missing/corrupt local storage cannot recreate the ordering state from S3. A missing pin or view is not a membership change.

- Readers look up exact configured trusted node keys, without scanning a bucket to grant trust.
- Validate node, disk, storage pin, protocol, authenticated label, server name, address bounds and revision.
- Address/process refresh cannot change the trusted physical disk or add an unconfigured node.
- All reads/writes have bounded bytes, attempts and original deadlines and remain owned during close.

## Atomic routing refresh

Store a separate last-observed process-view revision per physical node. On a newer valid view, update that node across group routes, query routes, inspection permissions and pooled connections under one short transport-registry lock. Membership IDs and group configuration revisions remain unchanged. Refuse equal revision with different contents, revision regression, disk rebinding or conflicting node identities.

Replace route objects and cancel old pools/permissions so old in-flight messages, queries, snapshot leases and inspection replies fail their existing route/owner checks. Reserve replacement task capacity before changing the registry. Keep old physical owners counted until joined. The host also updates its peer map so the next committed membership route uses the accepted process view.

If a host observes its own registration superseded by another process, close local Raft admission and join its owned service. A directory response still cannot override a live group quorum or synthesize a vote, image or configuration.

## Inspection and controller integration

Purpose-separated TLS-exporter/HMAC inspection requests bind full history, schema, both physical node/disk/process routes, nonce and original deadline. Inspection permission is explicit and separate from query-only origin grants. Reserve a bounded metadata budget and control admission independently of business queries and snapshot bodies. Only configured registered histories are inspected.

Return bounded role/occupancy, software compatibility, declared capacity, draining state, applied/configuration/repair observations and node leader load. Sampling time is receiver-local monotonic time. A reply is a hint; fresh seeding uses its existing quorum grant, and promotion/transfer recheck actual progress at the serialized Raft owner. Failed/stale inspection cannot invent commitment.

- The owned controller timer bounds group scans, direct probes, retries, active repairs and transfers.
- Standbys pull committed plan hints, then use local `activate_learner` inside their existing host.
- Leaders reconcile the persisted plan before selecting another repair and use fresh compatible capacity samples.
- Existing scope occupancy makes a node ineligible for a different member ID.
- Process views are refreshed without a metadata write per business command.

## Required verification

- Stable storage IDs survive restart; fresh empty engines cannot match an old
  pin, including with repeated configured node/disk values.
- Fenced view revisions survive process kill and missing view objects; stale
  owners/conditional writes cannot replace a newer boot.
- Wrong disk, instance, label, protocol, scope or process is refused. Corrupt and
  oversized records do not grant admission.
- Process refresh cancels old routes/queries/leases across multiple groups while
  preserving configuration revisions and existing committed data.
- Six native nodes replace a failed member without business requests, repeat
  controller/process restart and lost responses, refuse quorumless activation,
  and bound leadership movement while a voter lags.
- Inspection saturation, S3 outage and controller loss preserve existing data
  plane authority; shutdown joins metadata, initialization and transport work.

These local protocol cases do not qualify physical zones, S3, power-loss recovery or larger capacity tiers. Those remain separate release/scale gates.