# SQL guestbook

Use native SQL with parameterized inserts and persistent shared data.

## How it works

A native SQL binding addresses a framework database cell. Inserts use bound parameters;
reads return the newest 100 rows. The table is initialized idempotently. For evolving
schemas use SQL migrations and the rollout plan.

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
  dev examples/sql/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"message":"hello Rust"}' http://127.0.0.1:9876/sql
curl -sS -w '\n' http://127.0.0.1:9876/sql
curl -sS -i -X POST -d 'not JSON' http://127.0.0.1:9876/sql
```

## Expected output and interpretation

Both successful responses have `success:true` and `results:[{"id":1,"message":"hello
Rust"}]`, plus execution metadata under `meta`. Metadata varies. Malformed JSON returns
HTTP 400. Another valid message gets a new id and appears first. Stop and restart
without `--clean`, then GET `/sql` to verify the table and messages survive. The
local SQL binding uses SQLite and requires no external storage credentials.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/sql/.cellar`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
