# Durable AI chat with Vercel AI SDK and Cellar

A React/shadcn chat workspace runs a real Vercel AI SDK 7.0.152 `ToolLoopAgent`
through Cellar's supervised Node.js bridge. Streamdown 2.7.1 renders live
Markdown. Cellar owns each turn's SQLite state, model decision journal, tool
receipts, recovery alarms and completed conversation.

The default HTTP host uses a deterministic model fixture. The browser launcher
uses **OpenAI `gpt-6-luna`** by default. Live calls use `@ai-sdk/openai` 4.0.101
through the Responses API, with reasoning effort `none` and provider-side
response storage disabled. Vercel hosting and the Workflow SDK are not required.

## Build and open the chat

Use Node 24+ on macOS or Linux and the repository Rust toolchain. From the root:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/vercel-agent --ignore-scripts
npm ci --prefix examples/vercel-agent/ui --ignore-scripts
npm run --prefix examples/vercel-agent build
npm run --prefix examples/vercel-agent/ui build
cargo build --locked -p cellar-javascript --no-default-features --features node --bin cellar-vercel-agent-example

CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
  node --env-file="$HOME/.openai.env" examples/vercel-agent/script/launch.mjs
```

Open **http://127.0.0.1:4318**. The environment file supplies `OPENAI_API_KEY`
and optionally `OPENAI_BASE_URL`. Override `CELLAR_AI_MODEL` to select another
function-calling model. Provider credentials stay in server processes and are
never included in browser assets, status responses, or task state.

Try this request to exercise every tool:

> Calculate 24 times 7, look up durability, add 13 to the counter once, save a
> note titled Recovery with the key points, create a normal-priority ticket
> titled Verify recovery, then read the state. Show a short results table.

The workspace supports follow-up messages, recent chats, shareable turn URLs,
copying, tool result inspection, reconnecting, and reload recovery. A failed
submission retries its **same ID and prompt**. The composer supports Enter to
send, Shift+Enter for a line break, and IME composition. Mobile navigation and
saved-work panels use accessible shadcn Sheets. The conversation and Markdown
tables have keyboard access; motion respects reduced-motion preferences.

The sidebar reads `/api/conversations`, backed by a cell-owned durable
`ConversationDirectory` in the same bucket as the turns. Browser local storage
is only a display cache: an empty browser can discover saved conversations
without a turn URL. Follow-ups replace their parent in the list, while keeping
the conversation's original title. Conversations sort by the timestamp saved
atomically with their latest checkpoint; opening a chat or polling its status
does not change that timestamp. Equal timestamps use task ID order. Turns with
no recorded activity time sort after dated turns, also by task ID; opening an
older saved link does not invent an update time. Activity metadata lives in the
same durable cells and bucket as the journal and directory.
The directory returns 20 recent conversation
leaves and retains up to 128 turn references, with prompts bounded to 8,000
characters each.

Turn acceptance and directory registration are separate cell writes. The
already persisted turn alarm repairs a crash between them before provider work
starts. Opening an existing saved link also registers it in the directory;
links saved before this directory existed become discoverable when opened.
This directory belongs to the loopback workspace, rather than a browser or an
authenticated tenant.

The example configures 128 bridge invocation slots to accommodate concurrent
ingress, turn, alarm and directory calls. The 16-turn recovery test exercises
that bounded call graph; it is not a maximum-capacity qualification.

**Restart worker** sends SIGKILL to this launcher's host and starts a replacement
with a new node identity and empty local storage. It keeps the development
bucket and independent ticket service. Pending work resumes through alarms.
The saved-work panel distinguishes provisional text from durable results.

Set `CELLAR_AI_PROVIDER=fixture` for a credentials-free browser run. Fixture mode
is labeled and always increments by three regardless of prompt; it does not
claim live model intelligence. The broader tool fixture is confined to tests.
`CELLAR_AI_DEMO_PORT` changes the port; `CELLAR_AI_DEMO_DATA` selects a persistent
directory (default `.cellar/vercel-chat`). Keep that directory to retain tasks
and downstream ticket idempotency records. The launcher locks it against
concurrent use and joins its child on SIGINT/SIGTERM.

For frontend iteration, run the gateway on port 4318 and use
`npm run --prefix examples/vercel-agent/ui dev`. Vite proxies `/api` to the local
gateway; the built UI is served by the gateway itself.

## Use RustFS instead of the development store

The default launcher uses Cellar's SQLite-backed development object store at
`<CELLAR_AI_DEMO_DATA>/bucket.sqlite`. To exercise the real S3 client, run a
separate RustFS server and select `CELLAR_AI_STORE=s3`. S3 mode removes any
inherited `CELLAR_INTERNAL_DEV_STORE` override and refuses incomplete
configuration instead of falling back to a local store.

Start RustFS with its own empty object directory and credentials, following the
[RustFS macOS guide](https://docs.rustfs.com/en/installation/macos) or
[Docker guide](https://docs.rustfs.com/en/installation/container/docker).
Keep the S3 endpoint on loopback for this local example. Put the matching client
configuration in a private environment file outside the repository:

```dotenv
CELLAR_AI_STORE=s3
S3_ENDPOINT=http://127.0.0.1:19000
CELLAR_BUCKET=cellar-agent-demo
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-rustfs-access-key
AWS_SECRET_ACCESS_KEY=your-rustfs-secret-key
```

Create the demo bucket with the AWS CLI, then start the chat. These commands
assume the RustFS endpoint above is already running:

```sh
set -a
. /path/to/rustfs-client.env
set +a
aws --endpoint-url="$S3_ENDPOINT" s3api create-bucket --bucket "$CELLAR_BUCKET"

CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
CELLAR_AI_DEMO_DATA=.cellar/vercel-rustfs-chat \
  node --env-file="$HOME/.openai.env" examples/vercel-agent/script/launch.mjs

aws --endpoint-url="$S3_ENDPOINT" s3api list-objects-v2 --bucket "$CELLAR_BUCKET" \
  --query 'Contents[].{Key:Key,Bytes:Size}'
```

The bucket contains cell `ltx/` database segments, `own.json` ownership records,
and fleet/wakeup metadata. Notes, transcripts, model checkpoints and tool
receipts are inside the replicated cell database, rather than separate JSON
objects. The gateway's `tickets.sqlite` remains the independent downstream
service's database; it is not worker state or a Cellar recovery source.

With the same S3 environment exported, run the recovery suites:

```sh
CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
CELLAR_AI_TEST_REPORT=/tmp/vercel-rustfs-recovery.json \
  npm run --prefix examples/vercel-agent test:e2e

CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
CELLAR_AI_PROVIDER=openai CELLAR_AI_MODEL=gpt-6-luna CELLAR_AI_TEST_LIVE=1 \
CELLAR_AI_TEST_REPORT=/tmp/vercel-rustfs-tools.json \
  node --env-file="$HOME/.openai.env" examples/vercel-agent/test/tool-process.mjs
```

These suites require the AWS CLI in S3 mode and create a fresh bucket per
scenario, leaving those buckets for inspection. They SIGKILL the worker,
physically delete its nonempty data directory, assert that the replacement
starts empty, and wait for pending work to complete through alarms before any
task HTTP request. They also download real S3 LTX objects, record SHA-256
digests, verify a new ownership epoch, and assert that no local `bucket.sqlite`
exists. Duplicate submissions must preserve receipts and provider-call counts.
The deterministic suite also verifies 16 concurrent turns and 32 duplicates.
The [RustFS qualification](../../plans/vercel-agent-rustfs-qualification.md)
records the live results and their limits.

## Tool contracts

| Tool | Kind | Durable behavior |
| --- | --- | --- |
| `calculate` | Pure computation | Bounded numeric operations, no `eval`; exact result replay. Division by zero is a model-visible domain error. |
| `lookup` | Reference read | A fixed reference corpus, with a recorded result for deterministic replay. |
| `increment` | Local mutation | Counter and receipt share one checked transaction; one distinct increment per turn. |
| `saveNote` | Local structured write | Note and receipt commit atomically in the cell's SQLite store. |
| `readState` | State read | Records the counter and previous receipts at this point in the conversation. |
| `createTicket` | External HTTP write | Saves an intent before dispatch, uses a stable idempotency key, and reconciles unknown outcomes. |

The downstream ticket service has its **own SQLite database**, separate from
Cellar. Its operation key, request fingerprint and ticket effect commit together.
If a worker dies after the remote effect but before recording a local receipt,
recovery repeats the HTTP request with the same key and receives the original
ticket. A different payload with that key returns 409. The qualification also
reopens this service's database to verify idempotency survives its restart.
This service is an application example, not an integration with a third-party
ticket vendor. Adapting external tools requires the same downstream guarantee.

All tool input uses strict Zod schemas. Calls execute sequentially to respect
the bridge's one-mutation rule. Invalid tool arguments cannot silently complete
a turn. Provider-executed tools, arbitrary shell/code execution, files and
approvals require additional durability contracts and are explicitly refused
by the text/tool stream collector.

## Streaming and recovery boundaries

```text
Browser                   Node gateway              Rust / Cellar + Node agent
   | POST turn ID + prompt     |                            |
   |-------------------------->|---- native ingress ------->|
   |                           |                task + alarm; sync
   |<--------------------------|<-------- durable HTTP 202 -|
   |                           |                            |
   | SSE connection            |      alarm -> ToolLoopAgent|
   |<----- real text deltas ---|<---- provider doStream ----|
   |                           |       save complete decision; sync
   |                           |       SDK validates/executes tool
   |<----- saved tool event ---|<- effect + receipt; sync --|
   |                           |       save final conversation; sync
   | GET status                |                            |
   |-------------------------->|--- authoritative journal ->|
```

The native JavaScript fetch bridge remains buffered. The local Node gateway
serves SSE independently; opening or reconnecting a stream never starts an
agent. `wrapGenerate` consumes the provider's **real** `doStream`, emits optional
text previews, and withholds the decision from the SDK until its Cellar
checkpoint has a durability proof. There is no simulated typing.

The runner reconstructs `ToolLoopAgent` from saved conversation input on each
attempt. Semantic request hashes must match before reusing decisions. Stable
call IDs and argument fingerprints identify tool receipts. A lost acknowledgement
is reconciled from storage rather than blindly replaying a mutation. A
configuration fingerprint binds pending turns to SDK versions, model identity,
instructions and tool schemas.

Observer delivery is best effort and bounded. Text events carry full current
snapshots, so a later event repairs dropped previews. Slow SSE readers are
disconnected. Restarting a worker rotates its observer/tool token and clears
provisional text; old-worker events are fenced, including requests whose bodies
arrive after rotation. Restarting the gateway loses previews but retains durable
task state. Status polling restores the authoritative conversation. Reasoning
blocks, provider metadata and raw diagnostics are not sent to the browser.

Each turn has a 512 KiB journal, at most 16 model steps and 16 tool receipts,
five attempts, a 60-second attempt abort signal, and a two-second recovery alarm.
A new alarm is durable before provider work. Exhaustion records `failed` and
removes the alarm. Follow-ups copy history only from completed durable parents;
history is bounded to 192 KiB. A cell name permanently identifies one turn.
A new request with the same ID but a different prompt or parent returns 409.

## Repeat the verification

Format the handwritten JavaScript, TypeScript and React code with
`npm run --prefix examples/vercel-agent format`. Check formatting without editing
with `npm run --prefix examples/vercel-agent format:check`. Generated shadcn
components and build output are excluded.

```sh
npm test --prefix examples/vercel-agent
npm exec --prefix examples/vercel-agent/ui -- playwright install chromium
npm run --prefix examples/vercel-agent/ui test:ui

CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
  npm run --prefix examples/vercel-agent test:e2e
CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
  npm run --prefix examples/vercel-agent test:tools
```

These default local commands require no credentials. The unit suite covers storage ambiguity,
replay divergence, journal and retry limits, parent validation, reasoning
redaction, real stream consumption, incomplete output refusal, SSE reconnect,
worker-token fencing, and downstream reconciliation. Six Playwright cases run
on desktop and mobile: streaming, completion, reload without resubmission,
follow-up links, retry identity, responsive sheets, keyboard input, and short
viewports. Browser API responses are fixtures in those UI tests.

The native process suite kills the host at six boundaries: durable acceptance,
provider dispatch, saved model decision, local tool commit, saved final decision,
and completion. Every replacement has a new identity and empty local storage.
Unfinished tasks must complete through alarms **before any task HTTP request**.
It also runs 16 concurrent production-entry tasks, cold recovery, and 32 duplicate
submissions. Timings demonstrate bounded integration, not maximum capacity.
The tool process suite interrupts after a downstream ticket commit and verifies
six completed tool receipts with one downstream effect.

To repeat the **paid live GPT-6 Luna** qualification:

```sh
CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
CELLAR_AI_PROVIDER=openai CELLAR_AI_MODEL=gpt-6-luna \
CELLAR_AI_TEST_LIVE=1 CELLAR_AI_TEST_ALL_BOUNDARIES=1 \
  node --env-file="$HOME/.openai.env" examples/vercel-agent/test/process.mjs

CELLAR_AI_HOST="$CARGO_TARGET_DIR/debug/cellar-vercel-agent-example" \
CELLAR_AI_PROVIDER=openai CELLAR_AI_MODEL=gpt-6-luna CELLAR_AI_TEST_LIVE=1 \
  node --env-file="$HOME/.openai.env" examples/vercel-agent/test/tool-process.mjs
```

The second command also interrupts on the first real text delta, requires that
step to have no first-attempt checkpoint, and verifies a second attempt streams,
reuses saved tool receipts, and leaves the counter at 13 and ticket count at one.
Live tests require nonzero provider usage. `CELLAR_AI_TEST_REPORT` optionally
writes a JSON report containing synthetic task conversations and usage, never
credentials. Keep these reports outside source control.
See the [recorded live qualification](../../plans/vercel-agent-live-qualification.md)
for results and scope. [CI](../../.github/workflows/javascript-sdk.yml) runs the
credential-free suites; it never enables paid tests automatically.

## Direct HTTP and delivery

The native host also runs without the UI. Set `CELLAR_NODE_ENTRY` to the absolute
`examples/vercel-agent/dist/app.js`, `CELLAR_NODE_EXECUTABLE` to Node, and
`CELLAR_NODE`, `CELLAR_BUCKET`, `CELLAR_INTERNAL_DEV_STORE`,
`CELLAR_TEST_DATA_DIR`, `CELLAR_DURABILITY=bucket` as in the
[original Node guide](../node/README.md). POST `/agents/<turn-id>` with
`{"prompt":"Add three","previous":null}` accepts a durable turn; GET reads its
status, public messages, receipts and usage. A follow-up supplies the completed
parent turn ID as `previous`. Prompts contain 1–8000 characters; IDs contain
1–64 ASCII letters, digits, underscores or hyphens.

The native runtime package remains independent of the UI gateway:

```sh
node tools/javascript/node-package.mjs --runtime \
  examples/vercel-agent/runtime.json .cellar/vercel-agent-runtime
cargo run --locked -p cellar -- build examples/vercel-agent/project.json \
  --output .cellar/vercel-agent-package
```

Use a fresh immutable runtime prefix at the same absolute path on build and
serving machines. The [Node delivery guide](../node/README.md#native-fleet-packaging-and-installation)
covers inventory verification, publication, installation and `--node-root`.
Deploy the UI gateway separately with its built `ui/dist` and runtime dependencies.

The gateway deliberately binds to loopback and rejects foreign Host/Origin
headers. Its internal observation and ticket endpoints require a rotating
worker token. The browser receives no such token. This provides a polished,
production-built UI for a **local development service**; public multi-user
hosting still requires tenant authentication/authorization, TLS, retention,
rate limits and an authenticated operations plane. There is no cancellation or
approval API. These tests do not qualify production fleet failover or load.
An uncheckpointed provider request may repeat and incur another charge.
General external effects need downstream idempotency or reconciliation.
