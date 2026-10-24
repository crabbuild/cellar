<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Routing, RPC and command contracts

Call cells through a declared namespace so Cellar can route each request to
its authoritative owner. Use HTTP-shaped fetch calls or structured remote
procedure calls (RPC), with explicit identity and retry contracts for requests
whose results may be lost.

`Context::namespace` resolves the binding. `get_by_name` selects a stable named
cell; `id_from_name`, `id_from_string` and `get` support explicit IDs. Bindings
and identity configuration must match the compiled classes.

![Interactive routing flow](diagrams/routing-flow.svg)

## Route authority

The actor resolves ownership and can dispatch locally or through an
authenticated peer path. Conditional bucket writes establish cell ownership.
Ownership carries an epoch, so stale completions and handles cannot authorize
a new activation. A cached route is a hint with lifecycle controls; it is not a
second source of authority.

`ActorPolicy::max_cached_routes` limits idle, unowned routing history. Zero
disables that cache. Owned cells, alarms, queued calls and unfinished work keep
their records independently. Route-cache size does not bound residents or
whole-process memory. Applications select this policy explicitly; the CLI
defaults to 2,048.

A placement route carries the exact peer load-sample timestamp used to select
its epoch-zero candidate. The dispatcher returns that timestamp with a capacity
refusal even if the idle route has already been evicted. The core then excludes
the rejected sample without retaining per-cell history. A delayed refusal cannot
invalidate a newer sample's route or reservations, or act as an ownership epoch.

## Choose the call shape

| Surface | Appropriate use | Contract to define |
| --- | --- | --- |
| Cell `fetch` | HTTP-shaped entity interface | Method, route, body bounds, status and response schema |
| Cell `rpc` / `call` | Structured internal entity operation | Method, argument/result schema, remote errors |
| Service `fetch` / `rpc` | A declared application/entrypoint boundary | Service version, capabilities, props and authorization |
| Outbound `Context::fetch` | External HTTP service | Deadline, idempotency and ambiguous outcomes |

Use `encode_rpc`, `decode_rpc` and `decode_rpc_result` for the framework's
structured value and remote-error envelopes. The structured decoder has a
64 MiB ceiling; set lower business payload budgets. Service capabilities retain
their originating execution domain, and copied props are subject to its loader
policy. Cancellation-aware variants are available on cell and service calls.

## Design for ambiguous responses

![A client retries the same command ID and digest after a lost reply; the cell reads its retained receipt and returns the stored result.](diagrams/command-retries.svg)

The first mutation may commit even when the reply is lost. Retrying the same
command identity lets the application return its stored result without applying
the mutation again. Cellar still checks output proof before releasing that result.

The following is a proposed application protocol, not a built-in envelope:

```json
{
  "api_version": 1,
  "command_id": "tenant_scoped_command_id",
  "expected_version": 7,
  "operation": "reserve",
  "payload": {"units": 2}
}
```

Authenticate the caller, derive the tenant and cell, validate the payload, then
atomically compare/store the command digest, mutation and result. A duplicate
ID with the same digest returns the receipt; a different digest is a conflict.
An expected version is an application optimistic-concurrency convention.

A timeout, connection loss, fencing or failed durability proof does not imply
the business mutation never happened. Query the command status or retry with
the same ID. Retry only failures permitted by the contract, with bounded
attempts, backoff, jitter and an overall deadline. Avoid retry amplification
across multiple call layers. Validation and authorization failures should not
be retried without a change in input or authority.

## Dependency cycles and rollout

Keep the service call graph shallow. A call chain retains upstream request
slots while downstream calls settle; cycles and mutual waits can exhaust
admission. Exercise nested calls with the actual configured limits. Record
version compatibility, error mapping and retry ownership in the
[service contract](templates/service-contract.md). Deploy additive changes
before consumers require them, and retain old readers during rolling upgrades.

Sources: [cell stubs](../crates/cellar/application/cells.rs),
[services](../crates/cellar/application/service.rs), [actor](../crates/cellar/actor/mod.rs).
