# Operating a Cellar application

Start nodes, remove them gracefully and triage failures through your supervisor
and authenticated operator interface. Use the checks below to decide when
traffic can resume and when a stop is complete. Confirm command options with
the deployed binary's `--help`; route data changes through the cell owner.

![Interactive stop flow](diagrams/shutdown-flow.svg)

## Start and accept traffic

Confirm executable digest/target, configuration, bucket/prefix, credentials,
logical node identity and unique local directory. Start the compiled application
under its supervisor. Keep internal ingress protected. Wait for startup recovery,
authority and real readiness, then run an authenticated business read/write
probe. Capture the effective limits and source revision with the node record.

For an embedded Python host on Unix, the shipped `cellar supervise` command
provides finite startup/liveness probes, process-group termination and joined,
bounded restarts. Probe through Python itself so an interpreter stalled by
application code cannot remain healthy through a Rust-only endpoint. Follow the
[deployment recipe](../tools/python/README.md#process-supervision)
and inspect both the supervisor lifecycle and the application's combined drain
report. A forced stop uses ordinary ownership recovery; it cannot certify drain.

If startup recovery fails, keep traffic out and preserve predecessor logs and
lease/recovery pointers. Investigate credentials, store/range reads, peer
reachability, available disk and deadlines. Repeatedly deleting local state or
replacing leases can destroy evidence rather than resolve the failure.

## Graceful removal or restart

Choose a stop mode from the intended restart:

| Mode | Use when | Additional requirement |
| --- | --- | --- |
| `Handoff` | The logical node leaves and another owner should take over | Retain evidence needed for successor recovery |
| `Preserve` | The same logical node resumes at the same address with local state and ownership | Complete clean-reload preparation |

Both modes drain ingress, complete event drivers, cells and durability. Event
draining includes background work, nested calls and prior generations.

For `Preserve`, the drained cells publish their final per-cell database positions
before the node log checks whether it can seal. A durable bundle alone does not
prove that the per-cell layout can restore those writes. The clean-reload
certificate is published only after preparation, durability quiescence and the
final authority check succeed; failed proof still requires ordinary recovery.

Require these report conditions before recording a graceful stop:

```text
ingress_drained && application_work_drained && cells_drained
pending_application_events == 0 && pending_dispatches == 0
log_quiesced && local_durability_closed
!drain_timed_out && !deadline_elapsed
Preserve additionally: clean_reload_prepared
```

The node closes and joins owned work before returned completion; retain both
caller executors through that completion. Started blocking work can outlast the
semantic drain budget. A zero CLI exit or `Stopped` variant alone is insufficient
proof. Cancellation, drop or forced termination supplies no graceful proof.
Record incomplete phases and expect lease-expiry recovery to handle unfinished
ownership.

## Failure triage

| Observation | First actions | Resume criterion |
| --- | --- | --- |
| `Fenced` with reason | Remove traffic, retain typed reason and lease/log evidence | Recovery and valid authority on a supervised node |
| `CoreFailed` | Preserve failure/logs, contain traffic, inspect supervision | Tested fix or qualified replacement |
| `ReplicationFailed` / unproven output | Treat failed commands as potentially ambiguous; reconcile IDs | Proof path restored and receipts verified |
| Admission refusal / high latency | Inspect nested work, hot keys, limits, store/disk latency | SLO and backlog stable with measured headroom |
| Disk full / hydration failure | Preserve files, inspect space and recovery requirements | Qualified safe cleanup/expansion and verified reads |
| Incomplete drain | Retain report and recovery pointers; halt rollout advance | Takeover and post-takeover writes reconcile |

Do not call a lost fencing-report channel a genuine authority loss. Keep
`NodeOutcome` semantics in incident evidence. For ambiguous client writes,
query receipts or retry the same command ID; avoid issuing a new ID automatically.

## Routine maintenance

Review capacity/backlog and overdue alarms, exercise credential rotation,
inspect telemetry coverage, run a restore drill, and qualify upgrades of the
store/host/kernel/dependencies. Route SQL/KV/Queue operations through the owning
cell; reuse Blob's shared backend for Blob operations. Back up a recovery set
before approved destructive maintenance. Record incidents using the
[incident template](templates/incident-record.md).

Sources: [shutdown/outcomes](../crates/cellar/node/lifecycle.rs),
[stop modes](../crates/cellar/actor/mod.rs), [embedded supervisor](../crates/cellar/node/embedded.rs).
