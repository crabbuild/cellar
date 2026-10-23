# Durable Python agent with PydanticAI

A runnable [PydanticAI](https://ai.pydantic.dev/) agent uses Cellar cells for its
notebook, model conversation, tool receipts and durable wakeups. Each `/agents/NAME`
is an independent job. Python runs inside the compiled Rust host through PyO3;
no Python worker service, container or separate workflow engine is required.

The example includes a deterministic PydanticAI `FunctionModel` so you can run
and test recovery without credentials. It is a test model, not an LLM. An explicit
configuration switches the same agent to OpenAI. The model has one tool,
`save_note`, which writes into its own cell's notebook. It cannot execute shell
commands, send messages or make arbitrary business mutations.

## Run

Use the repository Rust toolchain and CPython **3.14.5 or 3.14.5t** with shared
libpython, as described in the [Python setup](../python/README.md). From the repo
root, with a target directory unique to this checkout:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-my-checkout"
export PYO3_PYTHON="$(command -v python3.14)"
"$PYO3_PYTHON" -m venv examples/python-agent/.venv
examples/python-agent/.venv/bin/python -m pip install \
  --require-hashes --only-binary :all: -r examples/python-agent/requirements.lock
export CELLAR_PYTHON_SITE_PACKAGES="$(examples/python-agent/.venv/bin/python -c 'import sysconfig; print(sysconfig.get_path("purelib"))')"
export CELLAR_AGENT_MODEL=demo
cargo run -p cellar --bin cellar --locked -- \
  dev examples/python-agent/project.json --no-watch
```

Use `python3.14t` and a fresh venv for free-threaded Python. Source and the SDK
facade are embedded in the executable; changing Python source requires rebuilding
and restarting. Ambient Python import paths are ignored by the embedded runtime.

Using the printed server address:

```sh
curl -X POST http://127.0.0.1:9876/agents/coffee \
  -H 'content-type: application/json' \
  -d '{"prompt":"Remember that our team prefers coffee without sugar"}'
curl http://127.0.0.1:9876/agents/coffee
```

POST returns **202**, acknowledging a durable job intent. Poll GET until `phase`
is `complete`; the demo performs three steps: model tool selection, notebook
commit, and model answer. The result contains `notes` and `answer`. The agent name
is the idempotency key: the same POST replays its acceptance receipt, including
after restart or completion; different input at the same name returns **409**.
There is one task per name; use a new name for a new task.

To try an actual model, set these before starting the host:

```sh
export CELLAR_AGENT_MODEL=gpt-4.1-mini
read -r -s OPENAI_API_KEY
export OPENAI_API_KEY
```

The Rust entry point explicitly passes these two values into Python. Credentials
are not embedded in packages or saved in cell history. Live calls incur provider
charges. The example configures no provider SDK retries, a ten-second model-step
deadline, eight durable attempts per job, at most two model requests per attempt,
16 committed steps, four tool calls per model result, bounded outputs and 128 KiB
of conversation history. These are example limits, not a billing cap. Choose a
model/provider and timeouts appropriate to your application. No live credentialed
provider call is required by the automated tests.

## Recovery and tool semantics

[app.py](app.py) uses PydanticAI's [deferred tools](https://ai.pydantic.dev/deferred-tools/)
and [message serialization](https://ai.pydantic.dev/message-history/):

1. POST atomically commits input, the selected model and the first Cellar alarm.
2. Before model I/O, an alarm charges an attempt and saves a recovery alarm.
   PydanticAI's `save_note` declaration defers execution; it has no side effect.
3. The model's conversation and requested tool calls commit with the next alarm.
4. A later alarm validates every tool call, then commits notebook effects,
   tool results and the next phase in one checked batch.
5. PydanticAI resumes from saved messages and `DeferredToolResults`, eventually
   checkpointing the final answer and removing the alarm.

A process can disappear between these steps. The next owner reconstructs the
Python object from durable state; no coroutine or in-memory agent instance needs
to survive. Alarm delivery timestamps are not operation identities. Repeated or
late deliveries use the stored phase and checked snapshot. An interrupted model
request may repeat and incur another charge; there is no exactly-once LLM-call
claim. The notebook and its tool receipt are atomic within Cellar.

A Cellar storage error with an unknown outcome is allowed to escape. The next
invocation reloads durable state instead of blindly replaying a mutation. A
conflicting update refuses the whole batch. `DELETE /agents/NAME` checkpoints
cancellation and removes the alarm; an already running model may finish its
network request, but its stale result cannot overwrite that cancellation.
Completed and failed tasks remain terminal. Provider errors are stored as fixed
codes, never exception text or provider response bodies.

For payments, email or other external tools, persist intent first and use a stable
idempotency key accepted by the destination, plus receipt reconciliation. Moving
an arbitrary external call into this example's local tool phase does **not** make
it atomic with SQLite. This example is an application-level integration using
PydanticAI's public APIs, not a generic PydanticAI durable-execution backend.

## Verify restart and isolation

Keep the development environment above:

```sh
PYTHONPATH="$CELLAR_PYTHON_SITE_PACKAGES" "$PYO3_PYTHON" -B examples/python-agent/test_app.py
cargo build -p cellar --bin cellar --locked
python3 examples/python-agent/smoke.py --cellar "$CARGO_TARGET_DIR/debug/cellar"
```

For CPython 3.14t add `--expect-free-threaded` to the smoke command. The real HTTP
smoke test uses private state and ephemeral ports. It stops a host with a saved
tool intent and pending alarm, starts a new process, verifies one notebook effect
and the resumed answer, then checks receipt replay, four independent agents and
cancellation. Both processes must produce complete native/Python shutdown reports.
Unit tests inject atomic write failure, duplicate alarms, model cancellation,
concurrent cancellation, invalid tool batches and provider failure/retry exhaustion.
They also inject a lost acknowledgment after creation, model checkpoint and tool
checkpoint commits: the next invocation reloads saved state, replays the creation
receipt, and resumes without repeating the committed model or notebook step.
This process-replacement test does not qualify multi-node failover or production
provider availability.

For a manual restart drill, set `"step_delay_ms":10000` in a new job, stop after
GET reports `"phase":"tools"`, then restart the same project without `--clean`.
Its persisted alarm completes the job. `--clean` intentionally deletes tutorial
state; do not use it for recovery tests.

## Build your own agent

Start from [app.py](app.py) and keep the durable phase transitions around your
PydanticAI model and tools:

| Change | Where | Contract to preserve |
| --- | --- | --- |
| Job input | `Start` | Validate before committing; a name identifies one immutable request. |
| Agent instructions and tool schemas | `make_agent` | Tools raise `CallDeferred` before doing any business work. |
| Provider and model settings | `model_step` | Use saved history/results, bound requests, and close the client within the invocation. |
| Local business tools | The `tools` branch in `DurableAgent.alarm` | Validate the whole batch, then commit effects, receipts and the next alarm together. |
| Status returned to users | `DurableAgent.fetch` | Read durable state; keep provider credentials and internal errors out of responses. |

The execution path is `POST → model → tools → model → complete`; a model may
request additional bounded tool rounds. The saved `phase`, `history`, `calls`
and `results` let a new Python object continue the job. Cellar owns persistence
and wakeups; PydanticAI owns model messages, tool schemas and response parsing.
Do not move durable work into an unawaited `asyncio` task: those tasks disappear
with the process. External tools need the intent/idempotency/reconciliation
protocol described above.

## Package and deploy

[package.py](package.py) uses the shared verified Python package builder. It pins
PydanticAI and transitive dependencies by version and wheel hashes, captures the
runtime/source/dependency inventory, and rebuilds the executable bound to it:

```sh
python3 examples/python-agent/package.py --runtime 3.14.5 \
  --prefix /absolute/new/agent-environment \
  --output /absolute/new/agent-package
```

Install `uv==0.11.19` before running the builder. The prefix parent must exist and
the prefix itself must be new. Use `--runtime 3.14.5t` for the other ABI. Packages
are platform-specific and require the captured OS/native-library cohort. Publish
and restore with the ordinary Cellar commands:

```sh
cellar deploy /absolute/new/agent-package --bucket YOUR_BUCKET
cellar install /absolute/new/agent-package --bucket YOUR_BUCKET \
  --output /absolute/application-v1 --python-root /absolute/new/agent-environment
```

Provision provider credentials at process startup, separately from the archive.
See [environment deployment](../../tools/python/README.md) for offline restoration,
verified install, fleet selection and process supervision. The local dev server
has no authentication: add authentication, authorization and tenant-scoped agent
names at ingress before exposing it. Prompt/history contents are application data;
define retention and access rules for your deployment.
