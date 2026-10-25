<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Durable Pi agents on Cellar

This example runs the real `@earendil-works/pi-agent-core` 1.4.0 agent loop in
Node.js beside Cellar's Rust host. Each `DurableAgent` cell owns one task, its
model-response journal, tool receipts and final transcript. Cellar owns SQLite,
durability, ownership and alarms. No JavaScript engine enters the Rust core.

The example includes a deterministic model fixture and an optional OpenAI
provider through `@earendil-works/pi-ai`. The fixture replaces only the model
service: Pi still validates arguments, executes tools and continues its loop.
It always requests an increment of three, regardless of prompt text.

## Build and run

Use Node 24+ and the repository Rust toolchain on macOS or Linux:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/pi-agent --ignore-scripts
npm run --prefix examples/pi-agent build
npm test --prefix examples/pi-agent
cargo build --locked -p cellar-javascript --no-default-features --features node --bin cellar-pi-agent-example
CELLAR_PI_HOST="$CARGO_TARGET_DIR/debug/cellar-pi-agent-example" npm run --prefix examples/pi-agent test:e2e
```

For local development, choose fresh writable locations for the development
bucket and node files. The bucket must survive replacement of a node:

```sh
export CELLAR_NODE_ENTRY="$PWD/examples/pi-agent/dist/app.js"
export CELLAR_NODE_EXECUTABLE="$(command -v node)"
export CELLAR_PI_PROVIDER=fixture
export CELLAR_NODE=pi-demo
export CELLAR_BUCKET=cellar-dev
export CELLAR_INTERNAL_DEV_STORE="$PWD/.cellar/pi-demo-bucket.sqlite"
export CELLAR_TEST_DATA_DIR="$PWD/.cellar/pi-demo-node"
export CELLAR_DURABILITY=bucket
export CELLAR_HANDLER_BUDGET_S=60
mkdir -p .cellar
"$CARGO_TARGET_DIR/debug/cellar-pi-agent-example" --listen 127.0.0.1:8080 --internal-listen 127.0.0.1:8081
```

Submit a task and poll its status:

```sh
curl -i http://127.0.0.1:8080/agents/task-1 -d '{"prompt":"Add three"}'
curl http://127.0.0.1:8080/agents/task-1
```

POST returns 202 after the task and wake alarm are durable. The alarm runs Pi;
GET returns the status, attempts, model-step count, tool-receipt count, counter
and completed transcript. Repeating a task ID and prompt returns its existing
status. Reusing an ID with a different prompt returns 409. A new task needs a
new ID. Each task has an independent counter. IDs contain 1–64 ASCII letters,
digits, underscores or hyphens; prompts contain 1–8000 characters.

To use a live model, set `CELLAR_PI_PROVIDER=openai`, `CELLAR_PI_MODEL` to a model
ID in the pinned Pi catalog, and `OPENAI_API_KEY` through your secret manager,
then restart the host. The provider is configured at process startup. Credentials
are not written to task state. Live provider calls incur normal provider costs;
the default CI qualification uses no credentials or paid model calls.

An opt-in live qualification runs two real model/tool conversations and kills
the host after the tool commit and after task completion. It checks nonzero
provider token usage, saved decision replay, final answers and duplicate
submission. Use a local environment file containing `OPENAI_API_KEY`:

```sh
CELLAR_PI_HOST="$CARGO_TARGET_DIR/debug/cellar-pi-agent-example" \
CELLAR_PI_PROVIDER=openai CELLAR_PI_MODEL=gpt-4.1-mini CELLAR_PI_TEST_LIVE=1 \
  node --env-file=/path/to/openai.env examples/pi-agent/test/process.mjs
```

This opt-in command makes paid provider calls and is not enabled in CI. Set
`CELLAR_PI_TEST_REPORT` to an output JSON path to retain the test transcripts;
the report contains task messages and usage, never environment variables.

## Live browser demo

The browser demo adds a local Node HTTP/SSE server beside the Rust host. It
shows actual Pi provider text events as they arrive, saved tool receipts and
checkpoint status. There is no simulated typing or timer-based text playback.
It defaults to OpenAI with `gpt-4.1-mini`; these calls incur provider charges.
After the build steps above, start it from the repository root:

```sh
CELLAR_PI_HOST="$CARGO_TARGET_DIR/debug/cellar-pi-agent-example" \
  node --env-file=/path/to/openai.env examples/pi-agent/demo/launch.mjs
```

Open `http://127.0.0.1:4317`. Submit an increment from 1 to 100. Reloading the
page restores the task from its URL; the sidebar remembers the last 20 task
IDs and prompts in this browser. **New task** creates a fresh counter.
**Restart worker** kills only this demo's Rust host with SIGKILL, starts a new
node with empty local storage, and keeps the same development bucket. The
replacement resumes pending tasks through Cellar alarms. Browser text from an
unfinished attempt is cleared; a saved receipt prevents another increment.

`CELLAR_PI_DEMO_PORT` selects a different UI port. `CELLAR_PI_DEMO_DATA` selects
a persistent directory (default `.cellar/pi-streaming-demo`) for the bucket
and each node's storage. Keep that directory to recover tasks after stopping
the entire demo. Use the same provider/model configuration when restarting.
The launcher locks the directory against concurrent use and joins its child
on SIGINT/SIGTERM. After killing the launcher itself, confirm its worker has
stopped before removing a stale `demo.lock`.

The demo binds to loopback and rejects foreign Host/Origin headers. A random,
rotated worker token authenticates its observation channel; the browser never
receives the token or provider credentials. This is a local development demo,
not a tenant-authenticated public service. Set `CELLAR_PI_PROVIDER=fixture`
for a credentials-free run; the page explicitly labels fixture mode, which
has no live provider text deltas.

### Streaming and durability

The Rust/JavaScript fetch bridge still returns buffered response bodies. SSE
is served by the demo's Node web server. The durable runner consumes the real
provider event stream and sends optional, bounded previews through
`CELLAR_PI_EVENTS_URL` and `CELLAR_PI_EVENTS_TOKEN`, configured by the launcher.
It continues to checkpoint each complete decision **before** handing it to Pi
for tool execution. Tool-receipt events are emitted only after storage sync.
The model, checkpoint and receipt journal remains entirely in Cellar.

Previews are best effort and may be dropped; later text events contain a full
current text snapshot. SSE reconnection restores the gateway's latest preview,
and the browser separately reads the authoritative task status/transcript
from Cellar. Slow readers are disconnected and buffers/connections are capped.
Restarting the gateway loses previews, but retains durable task state. Observer
failures do not fail a task. A worker restart rotates the observer token to
reject late events from the old worker. No thinking blocks or raw provider
errors are sent to the browser.

To verify real streaming and interruption automatically against a running demo:

```sh
CELLAR_PI_TEST_LIVE=1 \
CELLAR_PI_TEST_REPORT="$PWD/.cellar/pi-streaming-live-report.json" \
  npm run --prefix examples/pi-agent test:streaming
```

This paid, opt-in test interrupts the host on the first real text delta,
requires a second attempt to stream, verifies saved tool-receipt replay,
checks the counter stays 13 with one effect, and compares duplicate results.
Use `CELLAR_PI_DEMO_URL` if the demo uses a different port. Default unit tests
also exercise early provider deltas, observer failures, SSE reconnect,
origin/token checks and rejection of old-worker events without paid calls.

## Recovery design

```mermaid
sequenceDiagram
    participant Client
    participant Cell as Rust Cellar cell
    participant Pi as Node / Pi Agent
    participant Model
    Client->>Cell: POST task ID + prompt
    Cell->>Cell: Commit task + alarm; sync
    Cell-->>Client: 202
    Cell->>Pi: Durable alarm
    Pi->>Model: Request next decision
    Model-->>Pi: Assistant response / tool call
    Pi->>Cell: Checkpoint decision; sync
    Pi->>Cell: Atomic tool effect + receipt; sync
    Pi->>Model: Request final response
    Pi->>Cell: Checkpoint response and transcript; clear alarm; sync
```

The runner schedules a replacement alarm before doing work. It recreates Pi's
agent from the persisted prompt on each attempt. Completed model responses are
replayed by step, with a hash of the model and semantic input transcript checked
before reuse. Pi-generated message timestamps and execution durations are excluded from this check.
Tool calls use stable IDs and argument fingerprints. `increment` updates the
counter and its receipt in the same checked Cellar transaction. The durability
barrier completes before Pi receives the result. A lost reply is resolved by
reading the journal, including when the write's outcome was unknown.

The journal is capped at 512 KiB, with at most 16 model steps, 16 distinct tool
receipts and five execution attempts. Provider requests carry a 30-second abort
signal. Retries schedule a wake two seconds ahead; Cellar may also retry failed
alarm delivery. Exhaustion records `failed` and removes the alarm. Diagnostics
from provider exceptions are not exposed through the task API. A failed task's
journal remains available in its cell; the example has no admin reset operation.

A configuration fingerprint binds unfinished runs to the pinned Pi version,
model, system prompt and tool contract. A changed configuration refuses to run
the old task. Recover using the original release; there is no transcript
migration protocol in this example.

## What the qualification proves

The unit suite runs Pi against a checked in-memory storage boundary and covers
checkpoint replay, a lost sync acknowledgement, prompt conflicts, retry
exhaustion and replay divergence. The process suite exercises the native Rust
host, actual Node process, HTTP ingress, SQLite and shared development bucket.
It kills the Rust host with SIGKILL at six boundaries:

| Interruption | Expected recovery |
| --- | --- |
| Durable acceptance, before model dispatch | Alarm starts the task |
| Provider dispatch, before response checkpoint | Provider request repeats |
| Saved model response, before tool execution | Saved decision is reused |
| Atomic tool effect and receipt, before result delivery | Receipt is reused; counter stays 3 |
| Saved final model response, before task completion | Both decisions and the tool receipt replay despite changed execution duration |
| Completed task, before callback returns | Completed transcript is recovered |

Each replacement host uses a new node identity and empty local storage. Tests
use a 250 ms orphan-alarm scan interval (the host default is 60 seconds) and
wait for continuation before sending any task HTTP request. They check one counter increment, provider-call counts,
final transcript, task isolation, idempotent submission and graceful shutdown.
The `test/crash-fixture.mjs` entry owns fault controls; the production `app.ts`
entry contains none. CI runs these tests in the JavaScript/Node workflow.

## Scope and extension points

This is a durable, bounded tool agent example, not a coding-agent sandbox or a
general workflow engine. It has one task per cell and one atomic local tool.
Complete model responses are checkpointed before being handed to Pi; the local
demo streams provisional text separately as described above. Long-running
conversations, approvals, cancellation endpoints,
retention, tenant authentication and production fleet fault/load qualification
are further application work. Put authentication in front of ingress before
exposing the example to other users.

An interrupted model call can be repeated if its response was not checkpointed,
potentially incurring another charge. The counter's single-effect guarantee
does not extend to arbitrary HTTP, shell, filesystem or third-party tool effects.
An external tool needs a durable operation ID plus downstream idempotency or
reconciliation. Do not wrap a non-idempotent tool with “execute, then save” and
claim crash-safe execution. Native Node code retains its ordinary OS authority.

Pi's current [agent-core API](https://github.com/earendil-works/pi/tree/main/packages/agent)
exposes the model and tool loop needed for this adapter. The current npm scope is
`@earendil-works`; older Pi examples use `@mariozechner`.
[OpenCode's SDK](https://opencode.ai/docs/sdk/) is a client to an OpenCode server.
A Cellar integration could durably track its session/message IDs and reconcile
requests, but OpenCode would still own its execution and workspace effects.
That is a separate integration and is not implemented or qualified here.
Other in-process TypeScript SDKs can use this pattern if model decisions and
tool effects expose equivalent checkpoint boundaries.

## Delivery

The native host supports the same inventory-verified Node runtime delivery as
the [Node example](../node/README.md). A fleet build is configured here:

```sh
node tools/javascript/node-package.mjs --runtime examples/pi-agent/runtime.json .cellar/pi-agent-runtime
cargo run --locked -p cellar -- build examples/pi-agent/project.json --output .cellar/pi-agent-package
```

Choose a fresh immutable runtime prefix that exists at the same absolute path
on build and serving machines. These commands capture Node, emitted TypeScript
and the locked npm tree, then bind their inventory to the Rust executable.
Provision live provider credentials at runtime. The process recovery suite uses
the development host and bucket; it does not qualify production secrets,
external tools or live model-provider availability.

To exercise the installed package, move the captured runtime aside to force
restoration from its archive (choose a fresh backup name):

```sh
mv .cellar/pi-agent-runtime .cellar/pi-agent-runtime-captured
CELLAR_PI_PACKAGE="$PWD/.cellar/pi-agent-package" \
  cargo test --locked -p cellar-javascript --no-default-features --features node \
    --test fleet_package pi_agent_package -- --ignored --nocapture
```

This test publishes the staged package to a private development bucket, restores
the runtime, installs and validates the executable, then runs the packaged
`app.ts` entry without fault hooks. Eight concurrent HTTP tasks must complete
with a model/tool/final-answer transcript. It checks malformed requests and
conflicting task IDs, kills the host, and recovers every transcript on a new
node with empty local storage. Thirty-two concurrent duplicate submissions must
leave all results unchanged. The model service is still the deterministic
fixture; this test does not make a live LLM call.
