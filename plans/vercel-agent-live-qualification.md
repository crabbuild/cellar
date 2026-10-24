# Vercel AI SDK / GPT-6 Luna live qualification

Validated 2026-10-24 with Node 24.13.1, the Node-only `cellar-javascript` host,
AI SDK 7.0.152, `@ai-sdk/openai` 4.0.101 and OpenAI `gpt-6-luna`. Calls used the
Responses API, reasoning effort `none`, and `store: false`. Credentials were
loaded from a local environment file and were not printed, copied into task
state, or committed. Tests used synthetic prompts and private temporary
SQLite development buckets.

The later [RustFS qualification](vercel-agent-rustfs-qualification.md) exercises
the real S3 backend with physical deletion of worker-local state.

## Results

| Scenario | Observed result |
| --- | --- |
| SIGKILL after durable acceptance | Alarm completed work on a new node without a task HTTP request. Two model calls, one increment. |
| SIGKILL during provider dispatch | Only the uncheckpointed request repeated. Three provider dispatches, two saved decisions, one increment. |
| SIGKILL after saved model decision | Reused the decision; two total provider calls and one increment. |
| SIGKILL after local tool commit | Replayed the receipt; counter remained 3. |
| SIGKILL after final decision | Reused both decisions; final conversation completed without another provider call. |
| SIGKILL after completion | Retrieved the identical completed conversation and receipts. |
| Six-tool run; kill after remote ticket commit, before local receipt | Two attempts completed all six tools. Counter 13, calculation 168, one ticket effect from two downstream HTTP requests. A duplicate submission changed neither state nor provider-call count. |
| Six-tool run; kill on first live text delta | No first-attempt checkpoint for that partial model step. Attempt two streamed again and replayed tool receipts. Counter stayed 13; one downstream ticket effect and one downstream HTTP request. Duplicate submission retained the identical result. |
| Live browser conversation | Used all six tools; rendered a Markdown results table. Restarted worker with empty local storage, then correctly recalled calculation 168, note title, ticket title and counter 13 in a follow-up with no new tool calls. |

All saved live model decisions reported nonzero input and output token usage.
Every replacement native host used a new node identity and empty local storage;
the bucket survived. Tool qualification waited for the completion observation
before issuing a task GET, verifying that the persisted alarm resumed work.
The downstream service's own database was also reopened in the unit suite to
verify its idempotency records survive a service restart.

The deterministic native suite independently passed all six interruption
boundaries, 16 concurrent tasks and 32 duplicate submissions. Its concurrent
scenario took 653 ms on this machine; this is a small integration measurement,
not a maximum-capacity benchmark. The 28 unit tests and six desktop/mobile UI tests passed without
paid calls; see the [example guide](../examples/vercel-agent/README.md#repeat-the-verification)
for commands, test counts and the distinction between live/native and mocked UI
boundaries.

## Scope

This verifies a bounded agent, six application function tools, real text
streaming, Cellar's JavaScript bridge and private development-bucket recovery.
The external ticket service is an independent local SQLite/HTTP service with a
transactional idempotency contract. It does not establish guarantees for a
third-party ticket system, arbitrary external effects, provider-executed tools,
production multi-node partitions, provider availability or maximum throughput.
The UI gateway is loopback-only and is not a tenant-authenticated public service.

Raw live reports remain outside the repository. Repeatable tests are in
`examples/vercel-agent/test/process.mjs` and `test/tool-process.mjs`; paid runs
require explicit `CELLAR_AI_TEST_LIVE=1`. Browser tests use mock API replies and
are supplemented by the real browser conversation described above.
