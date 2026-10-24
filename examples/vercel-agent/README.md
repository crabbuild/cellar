# Durable Vercel AI SDK agent on Cellar

This example runs Vercel's **AI SDK 7.0.152 `ToolLoopAgent`** in a supervised
Node.js process through Cellar's JavaScript bridge. Each named cell owns a task,
model decision journal, atomic tool receipt, counter, and final conversation.
TypeScript is checked and emitted to ordinary JavaScript; Rust supplies routing,
SQLite transactions, durability barriers, alarms, and recovery.

The default fixture implements the model-provider interface and always asks to
add three, regardless of the prompt. The real AI SDK validates the tool input,
executes the tool, constructs its result message, and requests the final answer.
An optional OpenAI mode uses `@ai-sdk/openai` 4.0.101. Neither Vercel hosting nor
the Vercel Workflow SDK is required: Cellar provides durable execution here.

## Build and verify end to end

Use Node 24+ on macOS or Linux and the repository Rust toolchain. From the root:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/vercel-agent --ignore-scripts
npm run --prefix examples/vercel-agent build
npm test --prefix examples/vercel-agent
cargo build --locked -p cellar-javascript --no-default-features --features node --bin cellar-vercel-agent-example
CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
  npm run --prefix examples/vercel-agent test:e2e
```

The process test uses the actual Rust executable, supervised Node child, HTTP
ingress, Cellar SDK bridge, SQLite, and a private development bucket. It kills
the host with SIGKILL at six boundaries. Every replacement has a new node
identity and **empty local storage**, recovering from the surviving bucket.
For unfinished tasks it waits for alarm-driven completion before making a task
HTTP request, proving recovery does not depend on client resubmission.

| Interruption | Recovery assertion |
| --- | --- |
| Durable acceptance | Alarm resumes the task |
| Provider dispatch before checkpoint | Only this uncheckpointed request repeats |
| Saved tool-call decision | Decision replays without a provider call |
| Atomic increment and receipt | Counter remains 3; receipt replays |
| Saved final decision | Both decisions replay; conversation completes |
| Completed task | Conversation and receipt survive unchanged |

The same test runs the production `app.ts` entry with **16 concurrent task
cells**, then cold-recovers them and sends **32 concurrent duplicate
submissions**. Every task retains its complete conversation and one increment.
It prints elapsed time for the concurrent scenario; this demonstrates bounded
concurrency, not a maximum-capacity or production-throughput measurement.
Set `CELLAR_AI_TEST_REPORT=/absolute/report.json` to retain JSON results.
The report includes prompts and conversations, never environment variables.

Unit tests additionally exercise unknown write outcomes, unavailable durability
proofs, request replay divergence, retry exhaustion, invalid tool arguments,
multiple tool calls, changed configuration, and journal size limits. The
[JavaScript CI workflow](../../.github/workflows/javascript-sdk.yml) runs both suites.

## Run the HTTP service

Choose fresh writable paths for the node and development bucket. Keep the
bucket when replacing a node:

```sh
mkdir -p .cellar
export CELLAR_NODE_ENTRY="$PWD/examples/vercel-agent/dist/app.js"
export CELLAR_NODE_EXECUTABLE="$(command -v node)"
export CELLAR_AI_PROVIDER=fixture
export CELLAR_NODE=vercel-ai-demo
export CELLAR_BUCKET=cellar-dev
export CELLAR_INTERNAL_DEV_STORE="$PWD/.cellar/vercel-ai-bucket.sqlite"
export CELLAR_TEST_DATA_DIR="$PWD/.cellar/vercel-ai-node"
export CELLAR_DURABILITY=bucket
export CELLAR_HANDLER_BUDGET_S=60
"$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
  --listen 127.0.0.1:8080 --internal-listen 127.0.0.1:8081
```

In another terminal, accept a task and inspect its journal:

```sh
curl -i http://127.0.0.1:8080/agents/task-1 \
  -H 'content-type: application/json' -d '{"prompt":"Add three"}'
curl -s http://127.0.0.1:8080/agents/task-1
```

POST returns `202 {"status":"pending"}` after the task and its alarm are durable.
Poll GET until `status` is `completed`. The result includes `count: 3`,
`modelSteps: 2`, `toolReceipts: 1`, provider usage, and messages with roles
`user`, `assistant`, `tool`, `assistant`. Each task has an independent counter.
Repeating the same ID and prompt returns the existing status; changing the
prompt returns 409. IDs use 1–64 ASCII letters, digits, underscores or hyphens;
prompts contain 1–8000 characters. Invalid JSON or prompts return 400.

A JavaScript caller uses ordinary HTTP:

```js
const url = 'http://127.0.0.1:8080/agents/task-2';
await fetch(url, {method: 'POST', body: JSON.stringify({prompt: 'Add three'})});
console.log(await (await fetch(url)).json()); // Poll while status is pending.
```

To run a live model, set `CELLAR_AI_PROVIDER=openai`, `CELLAR_AI_MODEL` to your
model ID, and `OPENAI_API_KEY` through your secret manager, then restart. The
model must support function tools. Credentials stay out of Cellar task state.
Live calls incur provider charges. An opt-in recovery test exercises the tool
and completed-task boundaries with real model calls:

```sh
CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
CELLAR_AI_PROVIDER=openai CELLAR_AI_MODEL=YOUR_MODEL CELLAR_AI_TEST_LIVE=1 \
  node --env-file=/path/to/provider.env examples/vercel-agent/test/process.mjs
```

The default tests require no credentials and make no paid calls. The live test
also checks nonzero provider usage. Its two-response assertion expects the model
to follow the example's one-tool-then-final-answer instructions.

## How the bridge makes the agent durable

```text
HTTP client -> Rust application -> ctx.cell("AGENTS", taskId).fetch(...)
                                      |
                         DurableAgent TypeScript handler
                         ctx.storage.apply(task + alarm)
                         ctx.storage.sync() -> HTTP 202
                                      |
                              durable alarm
                                      |
                         real AI SDK ToolLoopAgent
                                      |
                     model middleware: generate or replay
                     save decision + wake; sync before release
                                      |
                       SDK validates and executes increment
                       save counter + receipt + wake; sync
                                      |
                     model middleware: final answer checkpoint
                     save conversation + completed; clear alarm; sync
```

[`durable.ts`](durable.ts) wraps the model with the AI SDK's
[`wrapLanguageModel`](https://ai-sdk.dev/docs/ai-sdk-core/middleware) interface.
The `wrapGenerate` boundary saves complete decisions **before** returning them
to `ToolLoopAgent`. On recovery the runner reconstructs the agent from the
original prompt, checks hashes of each semantic model request, and supplies
saved responses. It does not persist HTTP headers or raw provider request/response
metadata. The configuration fingerprint covers SDK versions, model identity,
instructions and tool contract; unfinished tasks refuse a changed release.

The tool hashes its stable call ID and arguments. Its counter effect and receipt
commit together in one checked Cellar transaction. Every mutation is awaited,
and `toolCallConcurrency: 1` respects the bridge's single-mutation rule.
The task permits only one distinct increment. A failed tool durability barrier
aborts the attempt even though the AI SDK normally converts tool exceptions
into model-visible results. The next alarm reconciles the stored receipt.

Each attempt commits a replacement wake before provider work. The journal is
limited to 512 KiB, with 16 model steps, one distinct tool receipt, five attempts,
a 30-second attempt abort signal, and a two-second retry alarm. Exhaustion records
`failed` and removes the alarm. Detailed provider exceptions are not exposed by
the task API. SDK contexts are invocation-scoped and never cached in cell fields.

[`application.ts`](application.ts) validates HTTP requests and routes named cells;
[`provider.ts`](provider.ts) selects the fixture or live model;
[`main.rs`](main.rs) registers fetch/alarm handlers and the `AGENTS` namespace
through `cellar-javascript::NodeRuntime`. The
[bridge guide](../../crates/cellar-javascript/README.md) explains runtime lifetime,
limits, cancellation and typed operation outcomes.

An interrupted provider request can repeat before its response is checkpointed,
possibly incurring another charge. The atomic increment guarantee applies to
this local transactional tool. External HTTP, filesystem or shell tools require
downstream idempotency or reconciliation keyed by a durable operation ID.
The bridge returns buffered HTTP; streaming responses, general workflows,
approvals, cancellation APIs and production fleet/load qualification are outside
this example. Add authentication before exposing the service to other users.

## Package for fleet delivery

The host also supports the existing inventory-verified Node delivery contract:

```sh
node tools/javascript/node-package.mjs --runtime \
  examples/vercel-agent/runtime.json .cellar/vercel-agent-runtime
cargo run --locked -p cellar -- build examples/vercel-agent/project.json \
  --output .cellar/vercel-agent-package
```

Choose a fresh immutable runtime prefix that will exist at the same absolute
path on build and serving machines. These commands capture Node, emitted
JavaScript, and the locked npm tree and bind their inventory to the executable.
See the [Node delivery guide](../node/README.md#native-fleet-packaging-and-installation)
for publishing, installation and `--node-root`. Runtime credentials are provided
separately. The process suite qualifies the development host and bucket, not
production fleet installation or live-provider availability.

Ctrl-C stops the development host. Remove only the tutorial node and bucket
paths above when their tasks are no longer needed. Build artifacts and emitted
JavaScript can be regenerated from the committed sources and npm lockfile.
