<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Ordinary named-cell binding delivery

Purpose: Specify the ordinary binding adapter and its remaining qualification boundary.

This CG-17 implementation is in progress. Full public primitive compatibility at
Quorum remains the required product contract. A focused binding checkpoint does
not complete that contract or waive recovery, movement, wake and release gates.

Register source and target classes with `Registry::raft_cell`. Declare the
ordinary exported classes and named cell bindings in the deployment. Application
code obtains `Context::namespace(binding)` or `Context::exported_namespace`, then
uses `Namespace::get_by_name` and `Stub::rpc`, `Stub::rpc_with_signal` or
`Stub::call`. JSON methods registered with `RaftClass::json_cell_rpc` retain their
existing direct SDK contract and gain an internal binding wire contract when
bindings are declared. `RaftClass::cell_rpc` registers owned JSON or structured
values. Cycles and UTF-16 values remain structured values. Leased capabilities
require their session adapter.

The host validates every declared target before ingress. A binding selects a
class namespace in the same cluster and immutable application configuration.
A cross-class target must have exactly one matching namespace; missing and
ambiguous targets refuse startup. The current class uses its source namespace.
Binding dispatch has no Fleet or weaker-durability fallback.

## Source intent and target proof

Each call records its source intent before proving the source transaction. It
fixes the target name, candidate incarnation, enrollment identity, operation,
input, child identity, source/target durability and original absolute expiry.
Group-local applied metadata and an invocation-local verified history cache may
supply a locator; neither is a target execution authority or durability proof.
The cache retains one charged entry and does not survive the invocation.

An unknown name uses its proven candidate and enrollment identity. A concurrent
creator may win; dispatch to that different history requires a new durable source
selection first. Once business dispatch is possible, the target history, input
and command identity cannot change, including when its outcome becomes unknown.
The target checks its current incarnation and independent minimum durability.
Every live lookup, enrollment, proof, retry and dispatch consumes the original
monotonic request deadline.

A successful call verifies the target receipt's identity, history and durability
before marking its private source intent resolved. The next source effect or final
parent acknowledgement proves that resolution; it needs no separate redundant
checkpoint. Only an explicit overload refusal before target admission retries
automatically, with identical input/history/identity and the original deadline.
Application failures and ambiguous outcomes are not automatically retried.

## Sequences and retained obligations

The source has 64 persistent exclusive outgoing lanes. A global lane serial
allocates enrollment identities and guards lane release. Child command sequences
are contiguous separately for each destination history, so switching cells does
not skip a target's next sequence. Destination counters are indexed, capped at
65,536 entries and subject to the normal SQL image budget. New source incarnations
have a separate lane epoch. Retained restart preserves names and counters.
The binding configuration digest changes with these internal semantics; old
prototype namespaces refuse schema admission rather than interpreting the old
counter layout. Cellar-owned format markers remain at 1.

`cells::CallError` preserves the actual child identity, selected history when
known, original cause and ambiguity. Cancellation after admission may leave owned
work running; cancellation does not prove non-commit. A known target refusal is
reported as known only after the source proves that refusal. Non-admission has
not consumed the target sequence, so its intent and busy lane remain reserved.
Unknown outcomes likewise retain their exact identity and lane. A future recovery
owner must reconcile or seal these sequences before releasing them; it must never
replay known-refused business input.

Completed calls release lanes and retain sequence counters; parent completion
removes only resolved intents. Business-state erasure preserves protocol counters,
names and receipts and refuses unresolved calls. Authenticated manifest summaries
count unresolved calls and prevent hibernation while obligations remain. Node/group
SQL admission charges retained call/reply buffers through response handoff.

## Archive publication scheduling

Managed archive registration uses `CellGroupArchivePublisher::GroupLeader`.
New upload allocations prefer the local Raft leader; follower registrations
acquire and independently verify advancing published prefixes. A role sample
schedules work and grants no serving, quorum or archive authority. Journaled
reservations/publications reconcile after role loss. Explicitly selected
single-replica publishers use `LocalReplica`, including the Docker qualification
service. Every leader-eligible managed voter needs an archive configuration.

Managed registrations use a 100 ms minimum archive retry/coalescing interval,
validated within 10–60,000 ms and driven by the existing controller timer. The
first newly waiting S3 proof initiates one immediate attempt; subsequent waiters
share its work and bounded schedule. Requests keep their original deadlines.
This scheduling interval is not a disaster-recovery RPO or response-time promise.
Quorum acknowledgements remain independent of asynchronous archival.

Followers may acquire an older published prefix while later local entries are
unarchived. Its route, complete physical closure and local term/index match must
verify before it can advance the historical reclamation frontier. It cannot
prove a newer command result. S3 output still checks the exact archived receipt
and current birth; local compaction still requires its own admitted checkpoint.

## Remaining qualification and delivery

The focused public regression exercises ordinary named calls, repeated calls,
multiple destinations, owned structured values, recursive self-calls with live
SQL cursors, transaction refusal, cancellation, exact receipt replay and retained
RF3 restart. It uses both image forms and source Quorum/S3 requests against a target
whose minimum is Quorum. It does not qualify a target namespace with an S3 minimum,
cross-class namespace selection or concurrent creation.

Autonomous reconciliation after process loss and sequence sealing remain required,
as do deadline/refusal exhaustion, capacity, leader-loss and movement/wake/disaster
qualification. SQL/KV bindings, committed due work and the remaining public/provider
primitive adapters are separate unfinished deliveries. Do not claim the full
HTTP/RPC primitive-family case from a focused binding pass.

The current per-configuration report at `9e02f3f6` passes Quorum source requests
in both image forms on native storage and real RustFS. S3 source requests fail
in both forms under the original five-second deadlines. The target namespace
minimum in these cases is Quorum. The combined binding case remains failed;
these results do not establish complete HTTP/RPC-family support.
