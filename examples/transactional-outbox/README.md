# Atomic order and outbox

This queue-backed recipe manages its own publication loop. The newer
[durable stream example](../durable-streams/README.md) uses framework `tx.emit`,
subscription leases and inbox receipts; that primitive remains under implementation.

Commit business state with pending publication, then deliver with a stable consumer key.

## How it works

POST persists an order and alarm atomically. The alarm re-arms before sending a bounded
batch to JOBS, then marks sent records. An interrupted send may redeliver; the queue
consumer deduplicates a stable cell-and-order key before applying its effect. There is
no cross-cell transaction or exactly-once external delivery. GET lists the first 100
order ids and sent flags. A large application needs pagination, archival and backlog
quotas.

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
  dev examples/transactional-outbox/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"id":"order1","amount":10}' http://127.0.0.1:9876/outbox/shop
sleep 3
curl -sS -w '\n' http://127.0.0.1:9876/outbox/shop
curl -sS -w '\n' http://127.0.0.1:9876/counters/queue-effects
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"id":"order1","amount":10}' http://127.0.0.1:9876/outbox/shop
curl -sS -i -X POST -H 'Content-Type: application/json' \
  -d '{"id":"order1","amount":11}' http://127.0.0.1:9876/outbox/shop
```

## Expected output and interpretation

Creation returns HTTP 202 with `{"id":"order1","accepted":true}`. GET eventually returns
`{"orders":[["order1",1]]}`; the second column is the publication flag. The consumer
eventually reads `{"count":1}`. Repeat GET if delivery is still in progress.

Repeating the same payload is accepted without another row or effect. Changing the
payload under that id returns HTTP 409. `sent=1` means queue publication completed;
inspect the consumer separately to verify its effect. Restart without `--clean` and read
both again. A crash between publication and marking may redeliver, so consumer
deduplication is required.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/transactional-outbox/.cellar`. Restart with the same command
without `--clean` to retain it; add `--clean` on the next start to reset this example.
Stop before running another example on port 9876, or choose another `--port` and change
the request URLs. JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
