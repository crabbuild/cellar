# @cellar/sdk

Write JavaScript or TypeScript application handlers and durable cells while
Cellar's Rust host owns routing, SQLite state, ownership and recovery.

This package supplies TypeScript declarations and five buffered HTTP helpers:
`request`, `response`, `jsonResponse`, `utf8` and `bytesToBase64`. The host supplies
`Context` when it invokes a handler. Installing the package alone does not start
a Cellar server or create a storage backend.

The same SDK works with both optional execution backends:

| Backend | Application packaging | Available runtime APIs |
| --- | --- | --- |
| Embedded V8 | Typecheck and bundle JS/TS into a Rust executable | ECMAScript builtins and Cellar bindings |
| Node.js | Compile TypeScript; deliver Node, modules and dependencies beside the Rust host | Node builtins, npm ESM/CommonJS and compatible native addons |

Use the [embedded TypeScript example](../../examples/javascript/README.md) or
[Node.js example](../../examples/node/README.md) for complete build and deployment
instructions. The [adapter guide](../../crates/cellar-javascript/README.md)
describes runtime limits and supported events.

## Named, typed business methods

Protobuf service generation adds a typed entity API through `@cellar/sdk/rpc`.
Generate a contract once and import its client in either JS or TS:

Generated packages use pinned `@bufbuild/protobuf` 2.16.0. Include that runtime
in the application dependencies; the core SDK stays codec-neutral.

```ts
import type { Context } from "@cellar/sdk";
import { CounterClient } from "./generated/counter_cellar.js";

export async function increment(ctx: Context) {
  const counter = CounterClient.bind(ctx, "COUNTERS", "alice");
  const reply = await counter.increment({delta:1n, operationId:"order-123"});
  switch (reply.result.case) {
    case "count": return reply.result.value.value;
    case "outOfRange": throw Error(reply.result.value.reason);
    case "operationConflict": throw Error("Operation ID has different arguments");
    default: throw Error("Counter returned no business result");
  }
}
```

The returned count is `bigint`; convert it to a decimal string before placing it
in JSON. Generated message constructors (`create(ResponseSchema, ...)`) supply
the concrete response type expected by registered handlers. Clients belong to
one invocation; do not retain them for a later event. Mutation retries require
application receipts and the same operation ID.

The same runtime validation applies to plain JavaScript and TypeScript. Client
inputs use generated field names and exact Protobuf types; the SDK refuses
numeric `int64` values, misspelled fields and coercions before submission:

```js
await counter.increment({delta: 1n, operationId: "order-123"}); // Valid.
await counter.increment({delta: 1, operationId: "order-123"});  // InvalidArgument.
await counter.increment({delta: 1n, operationID: "order-123"}); // InvalidArgument.
```

The codec checks nesting, wire elements and the complete encoded size before
creating a message or binary buffer. It preserves optional presence and selected
oneof defaults, accepts valid non-finite Protobuf floats, and refuses malformed
Unicode and shared/resizable/detached byte storage. Native code independently
validates request and response wire bytes. Unknown fields from newer schemas
may be discarded; this API does not promise lossless message forwarding.

### Cancel a named call

Pass `signal` with optional `timeoutMs`. Node applications can use the native
`AbortController`, `AbortSignal.any()` and `AbortSignal.timeout()`. A closed V8
bundle can import an AbortController implementation; the
[runnable V8 fixture](../../tools/rpc/v8.ts) bundles `abort-controller` 3.0.0.
`RpcAbortSignal` describes the shared surface without requiring DOM type libraries.

```js
// For embedded V8: import { AbortController } from "abort-controller";
const controller = new AbortController(); // Node's native global works too.
const pending = counter.increment(
  {delta: 1n, operationId: "order-123"},
  {timeoutMs: 1000, signal: controller.signal},
);

controller.abort();
try {
  await pending;
} catch (error) {
  if (error.code !== "Cancelled") throw error;
  // Cancellation is not a rollback verdict. Reconcile the durable receipt
  // for "order-123" before deciding whether to repeat the mutation.
}
```

```text
AbortSignal -> private control for this admitted call
                         |
                 native AbortSignal
                    /         \
                local       signed peer abort
                    \         /
                 revoke pending SDK authority
                         |
              native operation settles
                         |
           release admission -> reject the JS promise
```

An already-aborted signal fails before encoding/submission with
`Cancelled/not_committed`. Submitted calls retain their native outcome; a late
cancellation can return `unknown` after a successful native result. Earlier
synced writes survive. The SDK waits for native cleanup and removes its signal
subscription before settling; it never races a local rejection against detached
native work. Reasons stay in JavaScript and never enter RPC metadata.
A shared controller may fire from another callback, but retained contexts still
cannot borrow that callback's SDK authority.

Register only the service methods the class implements:

```ts
import { Counter } from "./counter.js";
import { CounterRegistration } from "./generated/counter_cellar.js";

export const cells = {Counter};
export const rpc = {Counter:CounterRegistration};
```

The Rust `Source` also declares `cell_rpc(class, service, contract)`; a consumer
source uses `rpc_contract(contract)`. Native registration and actual guest exports
are checked at startup. Only explicit generated methods are callable, and cell
state still lives in Rust-owned SQLite. Startup compares live provider codec
descriptors with the native artifact, including referenced types, maps, presence,
oneofs, packing and enum values. Clients check their own codecs before encoding
a call; stale code or a missing native contract fails as
`ContractMismatch/not_committed`. Verified JS reflection records are frozen.
See the [runnable Counter](../../tools/rpc/README.md)
for the schema, complete receipt-based business implementation, generated JS/TS
interfaces, host registration and real-node tests. Full fleet and failure
qualification is [still in progress](../../plans/named-typed-rpc/implementation-status.md).

## Install and build

Inside this repository, example applications depend on the local package:

```json
{
  "type": "module",
  "dependencies": {
    "@cellar/sdk": "file:../../packages/cellar-sdk"
  }
}
```

The path above is relative to an application's `package.json` in `examples/`.
For another directory, adjust that path or install a packed archive. From the
repository root, create one with:

```sh
npm pack ./packages/cellar-sdk --pack-destination /tmp
# In your application directory:
npm install /tmp/cellar-sdk-0.1.0.tgz
```

No registry publication is required for this workflow. The archive includes the
ESM helpers, declarations, README and license. Node execution requires Node 24+
in the current reference setup; embedded V8 uses Node only during the build.

To build an existing runnable application from the repository root:

```sh
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/javascript --ignore-scripts
npm run --prefix examples/javascript build
cargo build --locked -p cellar-javascript --features embed --bin cellar-javascript-example
```

Use the repository Rust toolchain and a checkout-specific `CARGO_TARGET_DIR`, as
required by the contributor guide. For Node, use the corresponding
[Node build commands](../../examples/node/README.md#build-and-verify).
TypeScript types disappear during compilation; both backends execute JavaScript.

## A first HTTP handler

Export `fetch` from your application module. Cellar passes a buffered request and
an invocation-scoped context. `ctx.variable` reads a variable configured in the
native deployment; `ctx.now` reads the host clock.

```ts
import {jsonResponse, type Context, type Request} from "@cellar/sdk";

export async function fetch(_req: Request, ctx: Context) {
  const greeting = (await ctx.variable("GREETING")) ?? "Hello from Cellar";

  return jsonResponse({
    greeting,
    timestamp: await ctx.now(),
  });
}
```

The Rust `Source` must register `EventKind::Fetch`. A JavaScript export does not
register an event or provision a binding by itself.

## A complete durable counter

The following `app.ts` routes the final URL segment to a named counter cell.
`POST /counters/alice` adds an amount; `GET /counters/alice` reads the count.
`alice` and `bob` own separate state.

This example uses only APIs available in both backends. It can replace the
counter application in either reference example, using its existing `Counter`
class registration and `COUNTERS` namespace binding.

```ts
import {
  jsonResponse,
  request,
  response,
  type CellarError,
  type Context,
  type Json,
  type Request,
} from "@cellar/sdk";

export class Counter {
  async fetch(req: Request, ctx: Context) {
    if (req.method !== "GET" && req.method !== "POST") {
      return jsonResponse(
        {error: "Use GET or POST"},
        {status: 405, headers: [["allow", "GET, POST"]]},
      );
    }

    const snapshot = await ctx.storage.read("count");
    const count = snapshot.present ? snapshot.value : 0;

    if (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0) {
      throw new Error("Stored counter is invalid");
    }

    if (req.method === "GET") {
      return jsonResponse({count});
    }

    let input: Json;

    try {
      input = req.json();
    } catch {
      return jsonResponse({error: "Expected a UTF-8 JSON body"}, {status: 400});
    }

    if (input === null || typeof input !== "object" || Array.isArray(input)) {
      return jsonResponse({error: "Expected an object with amount"}, {status: 400});
    }

    const amount = input.amount;

    if (typeof amount !== "number" || !Number.isInteger(amount) || amount < 1 || amount > 100) {
      return jsonResponse({error: "amount must be an integer from 1 to 100"}, {status: 400});
    }

    const next = count + amount;

    if (!Number.isSafeInteger(next)) {
      return jsonResponse({error: "Counter limit reached"}, {status: 409});
    }

    try {
      // Check and update execute together in one Rust SQLite transaction.
      // Another request may have changed this cell since the read above.
      await ctx.storage.apply({
        checks: {count: snapshot},
        puts: {count: next},
      });
    } catch (error) {
      const failure = error as CellarError | null;

      if (failure?.code === "Conflict" && failure.outcome === "not_committed") {
        return jsonResponse({error: "Counter changed; read and try again"}, {status: 409});
      }

      // Other failures may have an unknown commit outcome. Do not blindly retry.
      throw error;
    }

    // Return success only after the configured durability requirement is met.
    await ctx.storage.sync();

    return jsonResponse({count: next});
  }

  // The embedded reference host also registers an alarm handler. This example
  // schedules no alarms; keeping this method makes that registration valid.
  async alarm(_event: unknown, ctx: Context) {
    await ctx.storage.apply({delete_alarm: true});
    await ctx.storage.sync();
  }
}

export const cells = {Counter};

export async function fetch(req: Request, ctx: Context) {
  const name = req.url.split("?")[0].split("/").pop();

  if (!name) {
    return jsonResponse({error: "Include a counter name in the URL"}, {status: 400});
  }

  const result = await ctx.cell("COUNTERS", name).fetch(
    request("http://counter/", {
      method: req.method,
      headers: req.headers,
      body: req.body,
    }),
  );

  return response(result.body, {
    status: result.status,
    headers: result.headers,
  });
}
```

The native deployment must map `COUNTERS` to class `Counter`, and the Rust
`Source` must register the application fetch and the cell's handlers. The
[embedded host](../../examples/javascript/main.rs) and
[Node host](../../examples/node/main.rs) show these registrations. The embedded
example uses a fleet deployment; follow its packaging/deployment guide before
expecting a listener. The Node example also supports a local deployment graph.

After building and starting your host with this application on port 8787, run:

```sh
curl -sS -X POST http://localhost:8787/counters/alice \
  -H 'content-type: application/json' -d '{"amount":3}'
# {"count":3}

curl -sS http://localhost:8787/counters/alice
# {"count":3}

curl -sS http://localhost:8787/counters/bob
# {"count":0}

curl -i -X POST http://localhost:8787/counters/alice \
  -H 'content-type: application/json' -d '{"amount":0}'
# HTTP 400; alice remains at 3.
```

These outputs assume fresh cells. Restart against the same configured durable
backend, then GET `alice` again to read its persisted count. JavaScript object
fields survive only an activation; store durable state through `ctx.storage`.
This minimal counter has no request-id deduplication: if a client loses a POST
response, read/reconcile before deciding whether to resubmit it.

## HTTP bodies and headers

There are two representations at the bridge boundary:

| Value | Body | How to use it |
| --- | --- | --- |
| Handler `Request` | `Uint8Array` | Read `body`, call synchronous `text()` or `json()` |
| Nested cell `Response` | `Uint8Array` | Inspect status/headers; read or forward its bytes |
| `request(...)` result | Base64 string | Pass to `ctx.cell(...).fetch(...)` |
| `response(...)` / `jsonResponse(...)` result | Base64 string | Return from a fetch handler |

These are buffered SDK values. `text()` and `json()` reject invalid UTF-8, and
`json()` also rejects invalid JSON or unsupported bridge values. The builders
encode strings as UTF-8; unpaired UTF-16 surrogates become replacement characters.
Do not pre-encode bodies before passing them to `request` or `response`, or you
will encode the base64 text a second time.

Headers are ordered `[name, value]` pairs. A binary download handler looks like:

```ts
import {response, type Request} from "@cellar/sdk";

export function fetch(_req: Request) {
  const bytes = new Uint8Array([0, 127, 128, 255]);

  return response(bytes, {
    headers: [
      ["content-type", "application/octet-stream"],
      ["content-disposition", 'attachment; filename="sample.bin"'],
    ],
  });
}
```

`jsonResponse` prepends `content-type: application/json`; avoid adding a second
content-type unless you intentionally want duplicate headers. There is no
browser `Response` constructor to call in the embedded profile. Cellar's bridge
currently has no streaming-body or WebSocket-upgrade API, even when the Node
application itself can use Node streams.

## Storage, transactions and reusable components

`ctx.storage` belongs to the current cell. Use it inside cell fetch/alarm
handlers, not an application-level HTTP handler. A missing key is represented
by `present: false`; stored JSON `null` has `present: true` and `value: null`.

`apply` can check snapshots, put/delete keys and set/delete an alarm in one
atomic batch. It has no asynchronous transaction callback. The transaction's
success and the configured durability proof are separate steps: await `sync`
before acknowledging writes that require that proof.

Ordinary functions make reusable components. This helper must be called from a
cell invocation, with a context from that invocation:

```ts
import type {Context, Json} from "@cellar/sdk";

export async function savePreferences(ctx: Context, preferences: Json) {
  await ctx.storage.apply({puts: {preferences}});
  await ctx.storage.sync();
}
```

An unconditional put is appropriate when overwriting is intended. For concurrent
read/modify/write operations, use `checks` as in the counter example. Fields on
a reusable object must not hold a `Context` for later calls.

For relational data in a cell, use parameterized local SQL:

```ts
import type {Context} from "@cellar/sdk";

export async function saveNote(ctx: Context, id: string, body: string) {
  await ctx.storage.sql(
    "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT NOT NULL)",
  );

  await ctx.storage.sql(
    "INSERT INTO notes (id, body) VALUES (?, ?) " +
      "ON CONFLICT(id) DO UPDATE SET body = excluded.body",
    [id, body],
  );
  await ctx.storage.sync();

  return ctx.storage.sql("SELECT id, body FROM notes WHERE id = ?", [id]);
}
```

`SqlResult` contains `columns`, `rows` and `written`. Each row is an array aligned
with `columns`. The separate SQL calls above do not form one transaction.
`ctx.sql("DATABASE").query(...)` instead addresses a configured shared SQL
binding and returns a generic `Json` result; it is not this cell's SQLite handle.

## Durable alarms

Store state and its alarm time in the same batch. Register the class's `alarm`
event in the native `Source`, include the class in `cells`, and configure its
namespace binding before routing requests to it.

This cell stores a note for roughly one minute. Alarm scheduling and delivery
are host-managed; expiry is not a precise wall-clock response guarantee.

```ts
import {jsonResponse, type Alarm, type Context, type Request} from "@cellar/sdk";

export class ExpiringNote {
  async fetch(req: Request, ctx: Context) {
    if (req.method === "POST") {
      const expiresAt = (await ctx.now()) + 60_000;

      await ctx.storage.apply({
        puts: {note: req.text(), expiresAt},
        alarm_at_ms: expiresAt,
      });
      await ctx.storage.sync();

      return jsonResponse({expiresAt}, {status: 201});
    }

    const note = await ctx.storage.read("note");
    return jsonResponse({note: note.present ? note.value : null});
  }

  async alarm(_event: Alarm, ctx: Context) {
    const expiry = await ctx.storage.read("expiresAt");
    if (!expiry.present) return;
    if (typeof expiry.value !== "number") throw new Error("Invalid expiry");

    const now = await ctx.now();
    if (expiry.value > now) return;

    // A newer POST can race with this handler. Its changed expiry must prevent
    // this old delivery from deleting the replacement note or its new alarm.
    await ctx.storage.apply({
      checks: {expiresAt: expiry},
      deletes: ["note", "expiresAt"],
      delete_alarm: true,
    });
    await ctx.storage.sync();
  }
}

export const cells = {ExpiringNote};
```

There is one alarm time per cell. Do not supply both `alarm_at_ms` and
`delete_alarm` in the same batch. Repeated deletion is harmless, but real alarm
handlers that call external services need an explicit idempotency/recovery design.

## Shared bindings, queues and scheduled events

Bindings come from the native deployment; calling `ctx.kv("CACHE")` or
`ctx.queue("JOBS")` does not provision those resources. The following producer
requires both bindings:

```ts
import {jsonResponse, type Context, type Request} from "@cellar/sdk";

export async function fetch(_req: Request, ctx: Context) {
  await ctx.kv("CACHE").put(
    "report:monthly:status",
    {state: "queued"},
    {ttl_seconds: 300},
  );

  await ctx.queue("JOBS").send(
    {kind: "build-report", reportId: "monthly"},
    {delay_seconds: 5},
  );

  return jsonResponse({accepted: true}, {status: 202});
}
```

The KV write and queue send above are separate effects, not an atomic transaction.
A failed queue send can leave the KV status set. Use application receipts or a
reconciliation process where this distinction matters.

In a registered queue handler, settle one message with
`await ctx.settle({id: message.id, retry: false})`, or request redelivery with
`await ctx.settle({id: message.id, retry: true, delay_seconds: 30})`.
Omitting `id` applies the settlement to the whole batch. Keep consumers
idempotent; a lost acknowledgement can result in redelivery.

A scheduled handler receives `{cron, scheduled_ms}`. Call `await ctx.noRetry()`
only in a scheduled invocation when its retry should be disabled. Queue and
scheduled handlers must also be registered explicitly in the Rust `Source`.

## Failure handling and limits

`CellarError` describes errors from SDK operations; it is a TypeScript interface,
not a JavaScript class to instantiate or use with `instanceof`. Ordinary parsing
or application errors may have neither `code` nor `outcome`.

| Outcome | Meaning | Application action |
| --- | --- | --- |
| `Conflict` + `not_committed` | A checked batch refused before applying its changes | Read fresh state, then decide whether to try a new batch |
| `ValidationError` + `not_committed` | The operation was refused before execution | Correct the input |
| `unknown` | The error does not prove whether the submitted mutation committed | Reconcile durable state or use an application receipt before retrying |

Keep these invocation rules in mind:

- Await every SDK operation before returning. Start mutations sequentially;
  `Promise.all` over mutations can be rejected as `ConcurrentMutation`.
- Pass the current context to helpers. Never save it on a module, timer or cell
  instance for use by a later invocation.
- Send plain JSON values. Numbers must be finite and integers safe; encode large
  IDs as strings. `BigInt`, `undefined`, functions and custom class instances are
  not bridge JSON values. Runtime nesting, byte and operation limits also apply.
- Direct Node filesystem/network effects do not participate in Cellar storage
  transactions or recovery. A Node process alone does not make those effects
  durable or exactly once.

See the [adapter's authority and lifetime contract](../../crates/cellar-javascript/README.md#understand-invocation-authority-and-object-lifetime)
for cancellation, watchdogs, heap limits and unknown outcomes.

## More complete applications

- [Embedded TypeScript counter](../../examples/javascript/README.md): npm Zod,
  bundling, Rust host, checked writes and native packaging.
- [Node application](../../examples/node/README.md): Node builtins, CommonJS,
  dynamic imports, native addons, React server rendering and relocatable delivery.
- [Durable Pi agent](../../examples/pi-agent/README.md): checkpointed model
  decisions, tool receipts, recovery and a live streaming demonstration.

For a reusable npm component, export ordinary functions/classes plus TypeScript
declarations, a license and an explicit supported SDK range. Test the component
with the actual selected Cellar runtime: npm installation alone does not establish
embedded-runtime compatibility.
