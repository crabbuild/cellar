# Durable report workflow

Checkpoint a multi-step job and publish a repeatable external effect.

## How it works

POST starts a named report. The workflow journals prepare-report, captures its
rendered bytes in a durable stream, sleeps durably, then writes a Blob under a
stable instance-derived key. GET observes status and output. The
same external effect can be retried if a crash precedes its step checkpoint: stable keys
make this overwrite repeatable. Step names and semantics are persisted contracts;
version workflow classes when changing them. Registered `RollbackOptions`
provide durable compensation when a workflow fails or termination requests it;
compensation effects also need receiver-side idempotency.

The handler receives a `Step` and uses `step.run("prepare-report", callback)`,
`step.run_stream("render-report", callback)`, `step.sleep("delivery-delay", 100.0)`
and `step.run("publish-report", callback)`. The saved `StreamOutput` becomes a
fresh readable stream on replay and feeds `Blob::put_stream`.
Durations can also be strings such as `"1 minute"`. Configured steps use
`run_with` with `StepConfig`; event waits use `wait_for_event` with a type string
or `WaitOptions::new("approve").timeout("1 hour")`.

The binding supports batch creation, retention defaults, cron schedules and
cursor-based instance event subscriptions. See the [async work guide](../../docs/08-asynchronous-work.md)
for their Rust signatures and lifecycle contracts.
This project's defaults retain successful or terminated reports for seven days
and errored reports for thirty days.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar-main --bin cellar --locked -- \
  dev examples/workflow/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' -d '{"title":"Cellar report"}' http://127.0.0.1:9876/workflows/report1
sleep 1
curl -sS -w '\n' http://127.0.0.1:9876/workflows/report1
curl -sS -w '\n' http://127.0.0.1:9876/blobs/report-report1
```

## Expected output and interpretation

Creation returns HTTP 202 with `{"id":"report1"}`. Poll GET until `state` is `Complete`.
The final response is:

```json
{"state":"Complete","output":{"key":"report-report1","report":{"format":"json","report":{"title":"Cellar report"}}}}
```

The Blob contains `{"format":"json","report":{"title":"Cellar report"}}`. Its key links
workflow status to the actual external effect. Stop and restart without `--clean`, then
read status and Blob again. Use another instance name for another report.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/workflow/.cellar`. Restart with the same command without `--clean`
to retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).

The project declares a `reports-v1` diagram for the dashboard. Open its workflow
workspace to preview the definition, then inspect a retained instance’s execution
path and activity timeline. Definition snapshots follow each execution’s retention
and remain distinct from executable version pinning.
