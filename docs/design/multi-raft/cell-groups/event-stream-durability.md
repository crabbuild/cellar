<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# EventStream on shared groups

Purpose: Define the source journal, callback ownership and remaining public EventStream adapters.

This is part of CG17-04 through CG17-08. Public shared-group EventStream
registration remains refused until authenticated target RPC, service registration
and resource activation are connected. The source implementation alone does not
establish end-to-end publication or subscription.

## Source state and identity

Business changes, event bytes, sequence allocation and delivery discovery share
one SQLite transaction. A source command proves a recoverable obligation at its
Quorum/S3 minimum; destination acceptance and consumer processing have separate
proofs. Savepoints retain the command's SQL authorizer.

The source owner binds cluster, namespace and logical cell, excluding physical
group and incarnation. A separate domain-separated digest binds that owner and
cell incarnation to a 128-bit event producer incarnation. Caller-chosen equal
incarnation bytes in different namespaces cannot borrow an event identity.
History comes from the executing command, not application arguments. The private
SQLite scope alias never enters an event envelope.

Each lane persists its producer incarnation. Fresh-history recovery retains old
event bytes, IDs, sequences and pending seals; new writes use a new lane.
Retained restart and group movement preserve lanes. A foreign owner refuses
before changing source metadata. Capacity/SQL failures roll back identity changes
with the business operation.

Outbox pages bind pagination to the current producer incarnation but retain each
lane's original incarnation in its revision. Retrying an old pending lane uses
that exact revision; seal dispatch also selects the lane's incarnation. An old
cursor cannot cross a producer incarnation. Required source-owner and per-lane
columns retain marker 1 and refuse earlier development layouts before mutation;
there is no legacy reader, identity reset or automatic migration.

## Shared durable work

`__sys_work` retains the ordinary installation generation and deadline. Its
`__sys_raft_native_work` row projects discovery into the manifest and carries the
callback generation, attempt and claimed lease. Both change in one transaction.
An old completion cannot remove replacement work at the same deadline;
generation overflow refuses without partial discovery changes.

Outbox, stream-maintenance and resource-reconciliation use the bounded shared
callback owner. The exact claim commits at the required durability before
callback dispatch. Completion matches that claimed lease, and rescheduled work
survives it. Retained state supplies discovery without client requests or a task
per idle cell.

Publication permissions use a host-installed immutable script/binding/readiness
view, retaining no provider, application or node execution owner. Arguments do
not install bindings. Shared contexts refuse the Fleet event RPC path until the
shared adapter exists; pending source obligations remain recoverable.

## Private destination admission candidate

The destination candidate carries a typed Event/Name/Catalog authority, method
and canonical native request in the retained command input. Its private `CEVT`
frame keeps marker 1 and cannot decode as ordinary RPC. Normal command discovery
and validation cannot select its operation. Transport uses a separate
authenticated `EventCommand` purpose and body marker, retaining the independent
namespace write grant, current boot/membership fences and exact route binding.
Application bytes cannot select Event admission. Maintenance keeps its separate
derived-client identity check and local ownership.

Source calls reuse the binding intent, original child identity/deadline,
checkpoint proof and independently configured target minimum. Interrupted source
recovery can retire the original private child sequence without invoking business
work. The receiver validates the global resource scope and canonical name before
receipt lookup, then installs authority only in that invocation's context. The
existing receiver still checks its current resource grants. Long polling proves
and releases its root before waiting so another append can enter.

Stream and catalog service definitions are independent of producer aliases and
variables. Private routing requires one admitted global service namespace per
class and cluster; missing or conflicting owners refuse. This does not provision
resource definitions or activate application bindings.

The candidate requires its own compilation, owning tests and RF3/public-path
qualification. It does not establish public EventStream support.

## Remaining public adapters

Use a private authenticated query-route purpose, following binding retirement.
Its origin, target, purpose and body must be covered by existing transport
authentication and independent namespace write grants. Normal command/RPC input
cannot select it or supply trusted event authority. Preserve boot/membership
fences and the receiver's current stream grants.

Retain the host-minted authority and canonical RPC in the request fingerprint.
Populate `Context.event_authority` only from private admission; validate the
reserved global stream/catalog scope and canonical resource name. Public command
discovery and validation cannot expose private EventStream operations. Keep
source child IDs, original deadlines, intents, uncertainty and independent target
proofs. Existing maintenance retains its derived system-client checks; do not
relax the generic system-command check. Local and remote event requests require
the same private admission and receipt binding.

Normal deployment definitions must register unambiguous resource-owned stream
and catalog namespaces; a producer binding does not own a foreign resource.
Provisioning/reconciliation must reach destination Quorum/S3 proof before catalog
configuration becomes active. Named bindings need actual catalog resolution and
generation-local readiness; handles cannot silently retarget recreated resources.

The public activation candidate reuses the same configuration/status and catalog/
stream resolution protocol for Fleet and shared-group targets. The shared path
retains a nonce-bearing private command, one original deadline and the existing
bounded ingress/receipt owner. It pins both generation and shared-cell factory
bindings after proof. Superseded activation completions cannot reopen or cancel a
newer source latch; an already pinned generation cannot adopt a recreated ID.

Embedded shared-event nodes return a control-capable handle with public ingress
closed. After installing Raft and provisioning the declared stream/catalog groups,
the owner calls `Node::activate_event_resources`. The daemon activates after its
Raft startup hook. A directly embedded `RaftCells` owner can activate a captured
generation with `activate_event_generation`. Event services require explicit
scheduler, command/result and source image capacity; a SQL-only profile refuses
instead of receiving larger limits. This candidate still requires its public RF3
and RustFS qualification; these API paths are not family acceptance evidence.

Reuse existing stream, subscription and catalog state machines. Qualify
publication/read, lease fetch, inbox/sync/ack, retry/extend, configuration,
retirement and operators. Long polling releases the writer between proven roots.
Test interruption, leader replacement, retention, clocks, movement/wake, archive
outage and fenced fresh-incarnation recovery with original event/receipt IDs.
External effects still require receiver idempotency or reconciliation.

The [remaining priorities](remaining-work.md#primitive-delivery-priority) retain
all primitive and CG-13/15/16 gates. This source checkpoint accepts no complete
family or hard cutover.
