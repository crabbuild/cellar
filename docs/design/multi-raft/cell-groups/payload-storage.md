<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Immutable cell payload storage

Purpose: Define external payload publication and retention before connecting ordinary KV bindings.

`NamespaceCatalog::cell_payloads` binds a `CellGroupPayloadStore` to the actual
Raft namespace backend and prefix. Nodes share one `CellGroupPayloadPool` across
their namespaces. This is a data capability: it does not enroll a cell, grant
execution authority, acknowledge a Raft write or prove a complete archive cut.

The object key contains a domain-separated hash of cluster, namespace and exact
logical cell ID, the writer's cell incarnation, the byte length and SHA-256.
Physical groups, leader terms and routing epochs are absent. Movement does not
change a row's locator. A new incarnation writes a separate subtree and can read
an old locator belonging to the same logical cell after a separately authorized
recovery restores that row. Another cell, cluster or namespace is refused.
References retain format marker 1 and reject unknown fields and invalid bounds.

Publication first checks an existing object, then conditionally creates a missing
object and reads it back. Full length, response range, bounded chunk count,
actual EOF and digest must match. A lost PUT reply is not proof of failure or
success: only the exact read-back confirms publication. A corrupt existing object
is refused rather than overwritten. The entire operation has one monotonic
deadline, including hashing, every request, streamed bodies and read-back. It
cannot renew the original caller deadline between these stages.

One operation/byte bank accounts active I/O and returned bytes. Admission reserves
bounded transport/hash/body workspace before reading; a retained body releases
its operation slot while keeping its byte charge until dropped. Cancellation and
errors release their reservations without detached tasks. The default maximum
value is the ordinary KV ceiling of 25 MiB. This does not increase an RPC envelope,
private-image budget or result quota; deployment admission must intersect them.

The API has no list or delete operation. A list of current KV rows cannot protect
old images, retained archive cuts, readers, pending uploads or interrupted turns.
All payload objects are retained until a separate collector can enumerate and
prove retirement of the complete root/pin closure. This preserves recovery bytes
at the cost of growing remote storage; reclamation remains required work.

The owning protocol tests are in `raft_cell_group_payloads`. They exercise native
storage and an explicitly configured real RustFS backend, including an actual
conditional write whose reply is lost. They also check cold reopening, old/new
incarnation references, corruption and ranged-response refusal, original body
deadlines, cancellation and shared admission. These are payload protocol gates,
not RF3 or ordinary binding qualification.

Remaining integration: install one node-owned pool with explicit admission;
connect the runtime KV service's prepare/put/get to the current Raft invocation,
its original deadline and namespace; commit a row only after confirmed external
publication; deliver revision/TTL/list/bulk/stream and durable expiry adapters;
verify actual payload bytes after retained restart, movement, wake and fenced
disaster recovery; then implement and qualify root-aware reclamation. Fleet's
epoch-based blob collector must never collect these immutable payload keys.
