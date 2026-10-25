<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Queues, retry and dead letters

Deliver jobs with retry, dead letters and idempotent consumer effects.

## How it works

POST enqueues a JSON job. retry_once retries the first delivery; fail retries until the
configured dead-letter queue. The consumer commits a stable job key and counter effect
together before ack. GET reports backlog, GET /queues/dead reports dead letters.
Delivery is at least once. Deduplication records need a retention policy sized to your
replay window.

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
cargo run -p cellar --bin cellar --locked -- \
  dev examples/queues/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"id":"job1","retry_once":true}' http://127.0.0.1:9876/queues
sleep 5
curl -sS -w '\n' http://127.0.0.1:9876/counters/queue-effects
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"id":"job1"}' http://127.0.0.1:9876/queues
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"id":"bad1","fail":true}' http://127.0.0.1:9876/queues
sleep 5
curl -sS -w '\n' http://127.0.0.1:9876/queues
curl -sS -w '\n' http://127.0.0.1:9876/queues/dead
curl -sS -w '\n' http://127.0.0.1:9876/counters/queue-effects
```

## Expected output and interpretation

Each POST returns HTTP 202 with `{"accepted":true}`. The retried job eventually gives
`{"count":1}` at `/counters/queue-effects`. Reusing `job1` does not add another effect.
The poison job contributes no successful effect; it eventually appears in `DEAD` with
`backlog_count:1`, while `JOBS` returns to `backlog_count:0`. Backlog bytes and
timestamps vary.

Repeat reads if they race delivery. A dead letter means the retry budget was exhausted,
not that the job succeeded. This recipe leaves dead letters available for inspection.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/queues/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
