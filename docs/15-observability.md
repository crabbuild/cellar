# Observability and service objectives

Observe business outcomes and framework proofs separately. An HTTP response,
a retired task and a stopped process convey different information. This guide
combines existing observation surfaces with proposed dashboards and SLOs.

![Drain proof phases](diagrams/shutdown-flow.svg)

## Existing surfaces

The public `/.well-known/cellar/health` endpoint returns success only when the
node is not draining, the fleet-readiness gate has opened, and the actor is
healthy. `Node::is_ready` exposes the corresponding embedded observation.
Readiness includes a startup gate; it is not a complete continuous guarantee
that every cell, store or external service meets its business SLO.

`NodeOutcome` separates requested stop, cancellation, fencing, core failure
and replication failure. Preserve the typed `FenceReason`; a closed reporting
channel is a core/supervision failure, not proof that authority was lost.
`ShutdownReport` exposes phase booleans, pending work and timeout/deadline state.
`HostServices::application_work` exposes pending and retired event counts.
`HostMetricsSample` includes CPU, RSS, in-use bytes, cgroup observations and
file descriptor usage; the embedding host can supply a metrics sampler.

Telemetry is off by default. `CELLAR_OTEL=1` selects the bucket sink;
`CELLAR_OTEL=<collector base URL>` selects OTLP/http-protobuf.
`CELLAR_OTEL_BUCKET`, retention/flush controls and standard sampler/exporter
settings are resolved by the telemetry adapter. Its schema is `v0-unstable` at
this baseline. The bounded exporter sheds telemetry when saturated and counts
drops; absent spans do not prove absent application work. Never use sampled
telemetry alone as the command receipt ledger.

## Proposed dashboard contract

| View | Signals to collect | Operator decision |
| --- | --- | --- |
| Business | Successful/failed/ambiguous commands, command receipt reconciliation | Correctness and customer impact |
| Latency | End-to-end percentiles by operation, admission and proof wait | Saturation versus store/follower delay |
| Authority | Fences with reason, recovery progress, ownership/handoff failures | Stop rollout and preserve evidence |
| Work | Pending event drivers, dispatches, alarm overdue age, queue lag | Drain budget, backlog and missed work |
| Resources | RSS/cgroup, disk/free space, FDs, residents and cached routes | Capacity and leaks |
| Durability | Bucket/follower proof outcomes, upload lag, log/recovery failures | Acknowledgement safety and recovery exposure |
| Export | Dropped telemetry, collector failures | Observation coverage |

These are dashboard requirements, not promised metric names or a bundled
Prometheus endpoint. Map each to an actual log/API/host measurement in your
environment. Keep labels low-cardinality; cell IDs and tenant IDs belong in
controlled drill-down data rather than every time series. Redact secrets and
customer content, and restrict access/retention.

## Define an SLO with evidence

For each operation, choose a service-level indicator, a business-approved
threshold/window and an error-budget policy. Define whether rejected admission
and ambiguous writes count as failures; count them consistently. Choose a
separate overdue-work objective for asynchronous completion. Recovery objectives
must include state reconciliation and readiness, not just process startup.

Page on failed proof/fencing, sustained user-impacting failures, missing recovery
coverage or breached backlog objectives according to the on-call policy. Set
thresholds from measured production-like traffic; the local reference p99 is
not a suitable universal alert threshold. Sources:
[health handler](../crates/cellar/node/mod.rs),
[telemetry](../crates/cellar/observability/telemetry.rs),
[metrics](../crates/cellar/execution/services.rs),
[outcomes](../crates/cellar/node/lifecycle.rs).
