<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Team service boundaries

Make service boundaries explicit and keep state with the service that owns it.

## How it works

The API application receives HTTP and forwards through LEDGER to a separately registered
cohosted ledger application. The ledger owns COUNTERS. The project packages both
application manifests in one compiled executable; this demonstrates a service contract,
not independent release of the two services. Keep application versions and cell identity
stable during rollout.

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
  dev examples/team-services/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' http://127.0.0.1:9876/counters/alpha
curl -sS -w '\n' http://127.0.0.1:9876/counters/beta
```

## Expected output and interpretation

The responses are `{"count":1}`, `{"count":1}` and `{"count":0}`. Public ingress is the
API; the state belongs to the ledger application's namespace. Restart without `--clean`
and read alpha to see 1 again. This is a real service binding across two manifests. Both
factories are currently in one executable/package; independently deployed teams need the
rollout work in the roadmap.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/team-services/.cellar`. Restart with the same command without
`--clean` to retain it; add `--clean` on the next start to reset this example. Stop
before running another example on port 9876, or choose another `--port` and change the
request URLs. JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
