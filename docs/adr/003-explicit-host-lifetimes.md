<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# ADR 003: Keep host policy explicit and return lifecycle evidence

Status: observed baseline. Source: `c24feb2`. Recorded: 2026-10-12.

## Context and decision

Embedded `Node` receives caller-owned executors, listeners, transports, identity
and policies. Library defaults do not read ambient environment. The node closes
and joins owned work, then returns `NodeOutcome`; the process CLI owns signals
and exit mapping. Both stop modes drain complete event drivers before changing
cell lifecycle.

![Stop lifecycle](../diagrams/shutdown-flow.svg)

## Consequences

Applications retain executors and directory ownership through completion.
Stop has an absolute semantic budget and a separate stall window; blocking join
work can outlast that budget. Forced closure and retired-event counts do not
prove durability. A stopped result can contain incomplete phases.

Platform integration must inspect `ShutdownReport`, retain typed fences, keep
untracked host work supervised and test cancellation/startup/rollout races.
Environment adapters remain at explicit process boundaries. See
[embedding](../10-embedding.md), [lifecycle](../../crates/cellar/node/lifecycle.rs),
[SDK](../../crates/cellar/sdk.rs).
