<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Ordinary named-cell binding delivery

Purpose: Specify the ordinary binding adapter and its remaining qualification boundary.

This CG-17 implementation is in progress. It does not establish complete primitive
support or waive the recovery, movement, wake, provider and release gates.

Register the source and target classes with `Registry::raft_cell`. Declare the
ordinary exported classes and named cell bindings in the deployment. Application
code obtains `Context::namespace(binding)`, then uses `Namespace::get_by_name`
and `Stub::rpc` or `Stub::call`. JSON methods registered with
`RaftClass::json_cell_rpc` retain their existing direct SDK contract. They also
receive an internal binding wire contract when bindings are declared.
`RaftClass::cell_rpc` registers methods accepting owned JSON or structured values
through this binding wire. Cycles and UTF-16 values remain structured values.
Leased capabilities still require their session adapter.

The host validates every declared target before ingress. A binding selects a
class namespace in the same cluster and immutable application configuration.
A cross-class target must have exactly one matching namespace; missing and
ambiguous targets refuse startup. The current class uses its source namespace.
Binding dispatch has no Fleet or weaker-durability fallback.

Each call records a source intent before proving its source transaction. Fresh
linearizable discovery selects the target history. For a missing name, the source
first proves its candidate incarnation and enrollment identity. A concurrent
creator may win before the source proves its final target selection. Once the
selection is proven, the business command keeps that incarnation, input and child
identity even if its outcome becomes unknown. The target's minimum durability is
independent of the source's proof. Both use the original request deadline.

The source has 64 persistent, exclusive outgoing lanes. Each lane allocates a
monotonic child sequence and remains busy until the source proves the target
outcome. This permits interleaved callers without allowing a later sequence to
invalidate an earlier outstanding call. New source cell incarnations allocate a
separate lane epoch. Completed calls retain lane counters; parent completion
removes only resolved intents. Business-state erasure preserves counters, names
and receipts and refuses unresolved calls. Node/group SQL admission charges
retained call and reply buffers through response handoff.

`cells::CallError` preserves the admitted child command, selected history when
known, original cause and ambiguity. Cancellation after admission can leave work
running; it cannot prove non-commit. A known target refusal is reported as known
only after the source proves its terminal intent. An unknown target outcome keeps
its lane and exact identity. Manifest summaries retain unresolved-call counts and
prevent group hibernation while they remain unresolved.

Remaining delivery work includes autonomous reconciliation after process loss,
concurrent creation and capacity qualification, managed movement/wake/disaster
qualification, and the remaining ordinary SQL/KV and provider adapters. Do not
claim the full HTTP/RPC primitive-family case from the focused binding regression.
