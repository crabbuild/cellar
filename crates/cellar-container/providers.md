<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Implement a container provider

[Container guide](README.md) · [Bridge protocol](protocol.md)

Start with the [status-only provider example](README.md#add-a-provider), then use
this contract when implementing a backend. A provider advertises only the
operations it can execute; guests discover that subset through capabilities.

## Trait methods

| Method | Responsibility |
| --- | --- |
| `Provider::name` | Stable backend name for discovery |
| `Provider::operations` | Advertise only fully implemented operations; subsets are supported |
| `Provider::environment` / `max_resources` | Declare the execution environment and enforced handle cap; defaults mean unknown |
| `Provider::synchronous_running` / `ProviderSession::running_snapshot` | Opt in to an immediate running-state read; the shared session still checks event authority |
| `Provider::queued_lifecycle` / `ProviderSession::preflight` | Opt in to lifecycle admission; validate backend prerequisites without I/O or effects |
| `Provider::open` | Allocate an invocation-local `ProviderSession`; perform no I/O |
| `ProviderSession::execute` | Run the operation using the original native context and the supplied limits |

The shared session enforces event liveness, current turn, activation epoch and
host ownership before and after dispatch. `Native` additionally checks local
class/image/engine requirements; another backend need not declare a Docker image.

## Backend responsibilities

| Concern | Provider must supply |
| --- | --- |
| Ownership / fencing | Stable cell identity, ownership epoch and provider-side fencing; local checks cannot revoke an already-accepted remote mutation |
| Isolation | Explicit image, CPU/memory, runtime, filesystem and network policies; refuse unsupported security settings before allocation |
| Bounded I/O | Private invocation handles, run invalidation, bounded frames/bodies and concurrent stdout/stderr collection |
| Port authority | Forward only to the granted port; return redirects without following them |
| Cleanup | Cancel/join pending work, release invocation streams, and reconcile or destroy instances on ownership loss, shutdown and explicit destroy |
| Crash recovery | Provider-side leases/expiry and orphan reconciliation for node crashes |
| Outcome reporting | Ordinary errors for uncertain mutations; attach `Validation` only when provider effects provably did not occur |

Native bridge fences and resource accounting do not automatically protect a
remote service. A lifecycle/exec-only provider may refuse TCP or WebSockets;
capability discovery must make that subset explicit.

