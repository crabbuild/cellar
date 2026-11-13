<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Immutable cell payload storage

Purpose: Define immutable payload publication, ordinary KV transport and recovery boundaries.

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
Ordinary KV bindings use a separate immutable wire protocol for arguments or
replies larger than the actual Raft execution limits. The native request/reply
purpose and canonical named-cell identity are verified after decoding. Source
binding intents and receipts retain compact descriptors. A public wrapper cannot
tunnel the private operator contract. The caller's one original deadline covers
preparation, publication, the actual child call and reply expansion; no phase
receives a fresh budget.

A prepared descriptor is a pending upload intent, not a confirmed dependency.
Preparation performs no object I/O. An already applied target in the node's
bounded namespace route cache supplies a pending locator locally, avoiding an
otherwise redundant preparation command, receipt and source binding intent.
This observation supplies data keys only: source-proof admission still precedes
publication, and the actual write resolves/fences its history independently.
For stateless ingress without applied metadata, the same bounded, cancellable
routing owner resolves or durably enrolls the target and then constructs the
pending locator before consuming any command-stream sequence. This preserves
enrollment durability, competing-creator reconciliation, identity checks and the
original deadline. Stateful cold callers keep their source-journaled enrollment
and preparation-command path. Errors do not become cache misses. Fixed counters
`cell_kv_local_payload_preparations` and `cell_kv_resolved_payload_preparations`
observe these paths without per-cell labels. Publication checks the prepared writer, logical
owner, digest and length and confirms the exact bytes before marking the
descriptor published. Target execution verifies
the uploaded input and commits an external-value row only after publication.
Oversized non-value envelopes receive the same immutable treatment. Ordinary
values above the existing 1-MiB storage threshold use direct value references:
the SDK publishes bytes before the small child call, and that quorum command
independently verifies actual complete bytes before installing the row. A read
commits the exact row reference; the SDK verifies its body before exposing a
value. The private operator/CLI uses the same path. Value bytes are not repeatedly
read, encoded and re-published inside a long child RPC. Aggregate value admission
runs before fetching bodies, preserving the native result ceiling.

Small ordinary Quorum calls remain independent of asynchronous archive PUTs;
large transfers additionally require the payload provider. Caller-supplied
publication flags and well-formed locators alone never authorize a row.

Managed KV registration requires an explicit node-wide pool admitting the full
64-MiB native wire ceiling plus framing, decoder workspace and a concurrent
publication. The default standalone payload limits remain 25 MiB. Returned
body leases keep decoder workspace charged through operation/reply consumption.
This is lazy capacity, not memory allocated for every idle cell. Node usage and
Prometheus output include active payload operations and retained/capacity bytes.

KV conditional revision tokens include the current cell incarnation and the
monotonic SQLite counter. A restored counter in a new incarnation cannot match
a pre-disaster token. Retained restart and group movement preserve tokens.
Raft TTL evaluation uses the receiving service's clock, including relative TTLs;
ordinary alarm work reclaims expired rows without another caller request.

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

The ordinary SDK, stateless native/Axum ingress, private authenticated operator
and CLI adapters are implemented. Their owning tests reuse the Fleet KV API
contract and add full-size wire transport, conditional edits, autonomous expiry,
retained restart, archive outage and quorum refusal. Implementation and passing
qualification are distinct: consult the source-pinned progress record for actual
results. A compiled adapter does not establish a complete primitive family.

Remaining work includes interrupted large calls, movement and wake, and the
complete payload dependency closure before fenced disaster-recovery admission.
Recovery must verify actual external-value and wire bytes, including descriptors
retained by source intents and receipts. It must distinguish prepared/pending
uploads from confirmed dependencies. Then implement and qualify root-aware
reclamation across every retained image, reader and recovery cut. Fleet's
epoch-based blob collector must never collect these immutable payload keys.

Format markers remain 1. The value-reference service changes its compiled KV
schema identity, and the integrated callback kernel changes its private work
contract. Internal development therefore requires fresh compatible namespaces
and matching voters. Existing Fleet data is not reinterpreted as Raft data.
