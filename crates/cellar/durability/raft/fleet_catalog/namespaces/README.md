# Fleet namespace registry and inventory

Declare every managed namespace before a managed host boots or a history enters
the fleet. `declare_namespace` roots an exact namespace/class descriptor through
the fleet CAS. The descriptor binds namespace identity, schema and the immutable
namespace-root digest, including its data prefix and birth owner. Changing a
class, schema, owner or data prefix under the same namespace ID conflicts.

The registry is append-only and uses a separate immutable radix index. Adding a
namespace preserves every prior coverage obligation. At most 64 namespaces are
accepted; this is an initial implementation bound, not a capacity qualification.
Descriptors and the registry head are bounded at 4 KiB; index pages at 64 KiB.
The fleet head permanently retains the namespace count; deletion or count
regression refuses, and a missing registry/index cannot reinitialize an empty set.
No authority path lists objects or infers the namespace set from reachable nodes.

Persist each request identity. A lost declaration reply can be retried through
a recreated catalog after later declarations: the original immutable candidate
and journal determine its original applied revision. A changed request payload
conflicts. An unadmitted stale candidate cannot overwrite a newer root. A
different request cannot replace an earlier declaration.

## Compiled host and birth admission

Managed startup checks every actual prepared namespace and compiled class against
the current rooted registry, after physical identity checks and before pinning
the engine or publishing its endpoint. A caller-provided capability digest does
not bypass this check. Nodes can declare different subsets of the fleet's compiled
namespaces, and import hosts can boot before import is complete.

History enrollment, import start and managed birth admission require the exact
declaration. Each history's class prefix must match that namespace's declared
class. Reconciliation of already-admitted work retains its original references.
Existing business write acknowledgement performs no registry or inventory read.

## Full inventory check

`check_inventory` starts with the complete rooted registry. Caller-supplied
catalog handles select storage adapters, not the coverage set. Omitted or duplicate
namespaces refuse success; foreign data roots or classes conflict.

For each imported namespace, the check confirms its exact adopted marker and
rereads retained coverage witnesses. These witnesses prove its original admission
cut entered managed enrollment; they do not assert current quorum health. Permanent
node tombstones and engine/member claims remain valid historical references after
later movement. Handoff itself still refuses evacuated/removed current voters.

For every namespace, the check captures and streams its current managed birth
index, including later births after import. Every canonical intent must match a
rooted history. A second streamed pass checks every fleet history against its
declaration, permanent name/group indexes, enrolled initial-node references,
group-identity claim and actual immutable birth decision. Root reservations before
intent publication and pending root confirmations retain their references. Counts,
including the root's total history count, are checked exactly. Missing pages or
decisions refuse success; they never become empty sets.

The whole read runs in the catalog execution domain under one admission and
deadline, with bounded pages and no detached work. A concurrent later birth can
make the selected root too old; retry at a new root rather than accepting a
partial scan. Adding another namespace invalidates a former caller-selected
subset, even if that new namespace has no serving cells.

`FleetInventory` reports one checked revision and its counts. It is not a capacity,
archive, current-membership, empty-node or removal receipt. Capacity and maintenance
must still revalidate the exact root in their own admitted operations, establish
their separate live proofs and join physical work. Those runtime workflows remain
in the [delivery plan](../../../../../../plans/multi-raft/fleet-operations/progress.md).
Automatic fleet management remains default-off.

The experimental protocol hard-cuts to `/catalog-trust-1`. Use fresh fleet and
data prefixes, namespaces and matching binaries. Numeric format markers remain 1;
there is no compatibility reader or state migration.
