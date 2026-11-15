<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Immutable cell payload storage

Purpose: Define immutable payload publication, ordinary KV transport and recovery boundaries.

`NamespaceCatalog::cell_payloads` binds a `CellGroupPayloadStore` to the actual
Raft namespace backend and prefix. Nodes share one `CellGroupPayloadPool` across
their namespaces. This is a data capability: it does not enroll a cell, grant
execution authority, acknowledge a Raft write or prove a complete archive cut.

The object key contains a domain-separated hash of cluster, namespace and exact
logical cell ID, the writer's cell incarnation, a fresh random allocation ID,
the byte length and SHA-256.
Physical groups, leader terms and routing epochs are absent. Movement does not
change a row's locator. A new incarnation writes a separate subtree and can read
an old locator belonging to the same logical cell after a separately authorized
recovery restores that row. Another cell, cluster or namespace is refused.
References retain format marker 1 and reject unknown fields and invalid bounds.

Publication durably reserves the exact upload allocation before checking an
existing object, conditionally creating a missing object and reading it back. Full length, response range, bounded chunk count,
actual EOF and digest must match. A lost PUT reply is not proof of failure or
success: only the exact read-back confirms publication. A corrupt existing object
is refused rather than overwritten. The entire operation has one monotonic
deadline, including hashing, every request, streamed bodies and read-back. It
cannot renew the original caller deadline between these stages.

One operation/byte bank accounts active I/O and returned bytes. Admission reserves
bounded transport/hash/body workspace before reading; a retained body releases
its operation slot while keeping its byte charge until dropped. Cancellation
keeps its original admission until domain-owned reader-pin cleanup joins. A
failed cleanup leaves its persistent pin protected for explicit reconciliation.
The default maximum
value is the ordinary KV ceiling of 25 MiB. This does not increase an RPC envelope,
private-image budget or result quota; deployment admission must intersect them.
Ordinary KV bindings use a separate immutable wire protocol for arguments or
replies larger than the actual Raft execution limits. The native request/reply
purpose and canonical named-cell identity are verified after decoding. Source
binding intents and receipts retain compact descriptors. A public wrapper cannot
tunnel the private operator contract. The caller's one original deadline covers
preparation, publication, the actual child call and reply expansion; no phase
receives a fresh budget.

A prepared descriptor is a pending upload locator, not a confirmed dependency.
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
observe these paths without per-cell labels. Publication checks the prepared
writer, logical owner, digest and length and confirms the exact bytes before
marking the descriptor published. Target execution verifies
the uploaded input and commits an external-value row only after publication.
Oversized non-value envelopes receive the same immutable treatment. Ordinary
values above the existing 1-MiB storage threshold use direct value references:
the SDK publishes bytes before the small child call, and that quorum command
independently verifies actual complete bytes before installing the row. A read
commits the exact row reference; the SDK verifies its body before exposing a
value. The private operator/CLI stages the authenticated JSON body before native
RPC encoding and releases its base64 field before publication. Only the compact
reference enters the native codec. Value bytes are not repeatedly read, encoded
and re-published inside a long child RPC. Aggregate value admission
runs before fetching bodies, preserving the native result ceiling.

Small ordinary Quorum calls remain independent of asynchronous archive PUTs;
large transfers additionally require the payload provider. Caller-supplied
publication flags and well-formed locators alone never authorize a row.

Payload transfers use an internal I/O-only work kind. It becomes eligible for
polling without SQLite only after the ordered source checkpoint has its required
proof and the writer has parked. Stream readiness then polls that owned transfer
without reopening the image for each body chunk. Ordinary Storage access and
application continuations still require a resumed writer. Ordinary binding tasks
also poll their owned remote I/O while parked, then explicitly resume the writer
before source intent selection, completion or refusal; see the
[binding contract](named-bindings.md#source-intent-and-target-proof).
Every transfer poll retains invocation,
activation, route, term and original-deadline checks. Callback and queue readiness
unpark the existing SQL worker directly, with no additional queue slots or tasks.
Cancellation continues dropping the same owned futures and byte reservations.

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

The framework records pending dependencies before source-proven payload I/O
and upgrades them to confirmed only after full publication/read verification and
writer resumption. Caller-supplied publication flags never supply this proof.
A durable command handoff pin is installed before a remote reader releases its
transient pin or a publisher can expose its locator. Only a selected source
archive that pins the same dependency may adopt this owner. This protects the
interval between Quorum source work and asynchronous archive publication,
including cross-cell reads after the target deletes a value. The bounded SQLite
journal persists the handoff identity and covers input and reply descriptors; committed KV
rows independently retain live value references after receipt pruning. Partial
native turns keep their dependencies until the scheduler excludes all live or
parked same-cell frames and unresolved child calls. Wall-clock expiry alone
cannot retire their bytes.

Archive replay scans the actual recovered SQLite images and verifies every
confirmed external dependency with actual complete bytes before coverage,
movement or fenced recovery admission. It rechecks these bytes before copying a
prepared recovery cut. A missing, corrupt, ranged or incomplete object refuses
admission. Pending descriptors remain protected without requiring an upload to
have finished. `CellGroupArchivePool::with_payloads` supplies the same explicit
node-wide pool used by foreground transfers; an archive with external references
refuses when that admission is absent.

Archive publication installs scoped pins before selecting its root. A selected,
confirmed archive belonging to the logical payload owner may adopt its upload
pin; foreign source references cannot retire a target's pending publication.
The registry's append-only namespace index discovers these pins. Collection
marks selected roots, retained recovery cuts and pending parents, then retires
only that registry's older unselected pins. Concurrent newer or pending
generations and other registries' pins survive. Dropping a coverage handle
never releases its durable hold.

`CellGroupArchiveRegistry::collect_payloads` explicitly runs this retirement and
`CellGroupPayloadStore::collect` reclaims unprotected bytes. Persistent readers
protect entered GETs until the complete body is local; returned bytes continue
charging memory without requiring a remote pin. Collection validates the full
bounded ownership inventory and hashes confirmed protected objects before any
DELETE in that namespace. A namespace exceeding `max_scan_objects` refuses the
pass before deletion; the caller must provide sufficient inventory admission.
It never treats an unordered truncated first page as a complete mark. Each pass
caps actual object deletion with `max_delete_objects`.

An irreversible CAS tombstone precedes DELETE. Allocation IDs prevent delayed
DELETEs from touching a later publication of identical content. Tombstones stay
and later passes remove any delayed PUT body at the retired allocation. New
reads, roots or publishers cannot resurrect an unrooted allocation. Generic
owners use explicit `retain`/`release`; `abort_upload` requires the publisher and
all possible reference commits to have joined or been permanently fenced.
Unknown uploads/readers never expire by a clock or missing response. A lost
process may therefore leave conservative pins requiring owner reconciliation;
this API does not infer a permanent node fence or promise automatic reclamation
of unknown owners. The fleet epoch collector never touches these keys.

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

The external-byte receiver/collection gate seeds an archived KV row directly and
checks missing/corrupt bodies, retained cuts and an entered reader during actual
object deletion. It does not establish the full ordinary KV lifecycle/disaster
matrix, automatic collection scheduling, unknown-owner fencing or independent
host durability. Interrupted large SDK calls, movement and wake remain separate
qualification gates.

Format markers remain 1. The value-reference service changes its compiled KV
schema identity, and the integrated callback kernel changes its private work
contract. Internal development therefore requires fresh compatible namespaces
and matching voters. Existing Fleet data is not reinterpreted as Raft data.
