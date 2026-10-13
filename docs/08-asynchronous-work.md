# Alarms, queues, workflows and scheduled work

Choose the mechanism from its durability and coordination needs. The SDK has
alarms, durable wake infrastructure, Queue, Workflow and cron. Application teams
still define business delivery guarantees, deduplication and external effects.

The proposed [transactional outbox and durable event delivery design](../plans/002-outbox-streams-subscriptions.md)
specifies framework-managed emission, streams, and resumable subscriptions.
Those APIs are not part of the implemented mechanisms documented below.

![Interactive asynchronous delivery](diagrams/async-flow.svg)

| Mechanism | Use it for | Correctness boundary |
| --- | --- | --- |
| `Context::wait_until` | Bounded work attached to an event after its response | Participates in drain; no crash-survival guarantee by itself |
| Cell alarm | A persisted cell-local deadline | Handler must tolerate failure/re-execution |
| Durable wake discovery | Discover cells with persisted wake/alarm obligations | Framework recovery and ownership cover wake lifecycle |
| Queue | Decouple producer and consumer throughput | Define ack, retry, deduplication, poison-message handling |
| Workflow | Multi-step durable business process | Stable replay and external-effect idempotency |
| Cron | Periodically create/check work | Invocation is not proof of one completed business job |

## Cell alarms

Use `Storage::set_alarm` with an absolute millisecond timestamp and handle
`Invocation::Alarm { scheduled_ms }`. `get_alarm` and `delete_alarm` inspect or
clear it. Persist work state and schedule changes consistently with your
business mutation. On invocation, load durable state, determine what remains,
and finish, retry with a bounded policy, or record terminal failure.

An alarm is not a hard real-time SLA. Delays can include ownership recovery,
load and dependency outages. Define overdue-work metrics and reconciliation.
Alarm/wake coverage must survive eviction and shutdown; do not discard the
record because a cell is idle.

## Queue settlement

The producer exposes `send`, `send_value`, `send_batch` and `metrics`.
Consumer `queue::Event` provides messages, queue identity and settlement methods.
Messages expose `ack` and `retry`; events expose `ack_all` and `retry_all`.
Read the configured consumer policy and test its retry behavior instead of
assuming a universal delivery count or exactly-once execution.

Give business messages stable IDs. Commit a consumer receipt with its local
state change before acknowledging. External calls require receiver idempotency
or reconciliation. Classify a poison message, retain enough evidence to inspect
it, and define the application's quarantine/manual-replay path. A quarantine
table and replay tool are proposed application features, not a documented
automatic dead-letter service.

## Workflows and cron

Register workflow handlers through `Registry::workflow`. Workflow instances
support creation, status, event delivery, pause/resume, restart, termination and
deletion. Replay uses named recorded steps; keep step names and meaning stable
for running instances. Test old instances after a new executable is deployed.
Capture nondeterministic decisions through the durable step mechanism rather
than recomputing them on replay. An external step may run and lose its response;
use stable receiver-side keys.

Cron reaches `Invocation::Scheduled` with `cron::Scheduled`; `no_retry` can
decline retry. Use a job/time-window ID to deduplicate application jobs and
design a catch-up policy for downtime. Avoid assuming a schedule can replace
an audit of missed business work.

Before releasing async work, test crash before/after state commit, after the
external effect and before receipt/ack; recovery with pending alarms; workflow
version change; duplicate delivery; and a dependency outage beyond the retry
budget. Sources: [Queue](../crates/cellar/application/queue.rs),
[Workflow](../crates/cellar/application/workflow.rs),
[replay](../crates/cellar/application/workflow/replay.rs),
[alarms](../crates/cellar/application/storage.rs), [cron](../crates/cellar/application/cron.rs).
