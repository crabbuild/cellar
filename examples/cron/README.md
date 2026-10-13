# Scheduled occurrences

Perform periodic work from real scheduler invocations.

## How it works

A real minute cron invokes the compiled application. The handler routes to a counter
cell and deduplicates by expression plus scheduled timestamp. GET /cron observes
completed occurrences. No HTTP request fabricates a cron event.

Read the [Rust source](../native_examples.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[native_examples.rs](../native_examples.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/cron/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' http://127.0.0.1:9876/cron
sleep 65
curl -sS -w '\n' http://127.0.0.1:9876/cron
```

## Expected output and interpretation

The first response is normally `{"count":0}`, but may already be positive if a minute
boundary passed during startup. After 65 seconds the count should increase by at least
one. A retry of the same scheduled occurrence does not double-count it. Reading this
route does not fabricate a cron event. The smoke runner's `--with-cron` check waits for
a real minute boundary.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/cron/.cellar/native-dev`. Restart with the same command without `--clean` to
retain it; add `--clean` on the next start to reset this example. Stop before running
another example on port 9876, or choose another `--port` and change the request URLs.
JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
