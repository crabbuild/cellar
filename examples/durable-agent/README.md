<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable native tool loop

Checkpoint a bounded tool sequence and advance it through durable alarms.

## How it works

A named job checkpoints two deterministic Rust tools using storage plus alarms: count
words, then generate a report. GET reports stage, result and bounded step count. A
second POST with the same cell name returns 409. No model credentials are needed. This
provides the durability pattern for agent builders without an LLM integration.
See the [roadmap](../ROADMAP.md) for provider integration, budgets and approval
requirements.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

For a runnable Python model integration, see the
[PydanticAI durable agent](../python-agent/README.md). It includes an offline test
model, an OpenAI configuration and checkpointed tool execution.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/durable-agent/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST -H 'Content-Type: application/json' \
  -d '{"text":"native durable tools"}' http://127.0.0.1:9876/agents/demo
sleep 1
curl -sS -w '\n' http://127.0.0.1:9876/agents/demo
curl -sS -i -X POST -H 'Content-Type: application/json' \
  -d '{"text":"another job"}' http://127.0.0.1:9876/agents/demo
```

## Expected output and interpretation

POST normally returns a job at `stage:"count"` with `steps:0`. Poll GET until:

```json
{"text":"native durable tools","stage":"complete","steps":2,"words":3,"summary":"Document contains 3 words"}
```

The fields identify the checkpoint, completed tools and result. A second POST to that
name returns HTTP 409; choose a new name for another job. Restart without `--clean` and
GET to verify the completed checkpoint persists. This demonstrates deterministic durable
tools rather than model inference or arbitrary shell execution.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/durable-agent/.cellar`. Restart with the same command without
`--clean` to retain it; add `--clean` on the next start to reset this example. Stop
before running another example on port 9876, or choose another `--port` and change the
request URLs. JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
