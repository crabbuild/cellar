<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Streaming HTTP, SSE and WebSockets

Cellar's native Rust transport owns connections, routing and durability gates.
The optional Python and JavaScript adapters use the same transport through
invocation-scoped capabilities. TypeScript compiles to JavaScript and uses the
same runtime and the `@cellar/sdk` declarations.

Try the [runnable streaming examples](../examples/streaming/README.md) in Rust,
Python, JavaScript or TypeScript. All backends expose the same SSE progress feed,
worker echo socket and hibernating room with durable message counts, with a shared
network client and restart check.

## What differs from celld

[celld](https://github.com/denoland/celld) embeds V8 and runs Workers applications:
application code uses Workers-style web APIs, including hibernating Durable
Object WebSockets. Cellar exposes Rust SDK contracts and optional language
facades. These facades are not implementations of the complete Workers or browser
API: use the Cellar builders below rather than browser `Response` or `WebSocket`
constructors. Node's own network sockets are separate from SDK socket capabilities.

| Concern | Cellar contract |
| --- | --- |
| SSE | An HTTP response with `text/event-stream`, carrying formatted event chunks. |
| WebSocket | An HTTP/1.1 upgrade followed by text/binary frames in both directions. |
| Ownership | Every SDK operation uses the original native event/activation authority. |
| Durability | A response head, stream chunk or socket frame trails the native output gate for writes it can reveal. |
| Backpressure | Producers await each native byte-pipe write before yielding another chunk. |
| Disconnect | Dropping a response body cancels its native driver, releases admission and revokes further SDK operations. |
| Hibernation | Socket metadata can outlive the language cell instance on the same live host; get a fresh context/socket facade in each event. |
| Restart | Network connections are not replicated. Clients reconnect and replay from application-owned durable state. |

SSE IDs, `Last-Event-ID`, history retention and replay are application protocols.
The helpers format IDs; they do not create a durable event log or resume work.
An observed frame/chunk is not a receipt for unrelated external side effects.

## Rust

Use `Context::websocket_pair`, `accept` or `accept_hibernating`,
`Upgrade::into_response`, and the `Invocation::WebSocketOpen`,
`WebSocketMessage`, `WebSocketClose` variants. For outbound connections use
`Context::connect_websocket`, or accept an upgraded SDK fetch response.
See the [standalone example](../examples/native-websocket/README.md).

For streamed HTTP, create an SDK `IdentityTransformStream`, register its producer
with `Context::wait_until_byte_stream`, and publish each chunk with
`Context::write_stream_chunk`. `Response::from_byte_stream` builds the response.
The [Axum adapter](../crates/cellar-axum/README.md#streaming-and-websockets)
performs this conversion for Axum bodies and `Sse`.

## Python

```python
from cellar import StreamingResponse, sse_event

async def fetch(request, ctx):
    async def events():
        yield sse_event("started", event="progress", id="1")
        # Await model output or other application work here.
        yield sse_event("finished", id="2")
    return StreamingResponse.sse(events())
```

`StreamingResponse(async_iterable, status=200, headers=...)` also sends arbitrary
text/byte chunks. Each chunk is bounded by `max_body_bytes`; the complete stream
can exceed that limit. The runner closes async generators during cleanup.
Storage and binding calls made inside the generator retain the opening context.

```python
from cellar import WebSocketResponse

class Room:
    async def fetch(self, request, ctx):
        socket = await ctx.websocket_pair()
        await socket.set_attachment({"room": "lobby"})
        await socket.accept(hibernating=True, tags=["lobby"])
        return WebSocketResponse(socket)

    async def websocket_message(self, event, ctx):
        await ctx.websocket(event["id"]).send(event["data"])

    async def websocket_close(self, event, ctx):
        pass
```

Register the cell's implemented events with `Source::cell_events`, using
`EventKind::Fetch`, `EventKind::WebSocketMessage`, `EventKind::WebSocketClose`
and, if implemented, `EventKind::WebSocketOpen`. Open/close handlers receive
metadata dictionaries; message `data` is `str` or `bytes`. IDs are opaque decimal
strings. Tags and attachments are bounded JSON values.

Worker sockets use `await socket.accept()` and
`WebSocketResponse(socket, run=consumer)`, where `consumer(socket, ctx)` loops on
`await socket.next()` and returns when the event `type` is `close`. Cell sockets
receive declared invocation handlers instead of that worker pull loop.

`await ctx.namespace(binding).get_by_name(name).fetch_stream(request)` returns a
live response. Return it directly to forward its streaming body or upgrade, or
consume `async for chunk in response.chunks()`, or call `await response.cancel()`. `await ctx.fetch_stream(request)`
uses configured SDK outbound HTTP, including host egress policy and deadlines.
An upgraded live response can be consumed with
`await ctx.accept_websocket(response)`. `await ctx.connect_websocket(url, protocols)`
opens an outbound socket directly. Outbound cell sockets deliver cell events;
worker/client endpoints support `next()`.

## JavaScript and TypeScript

```typescript
import { sseEvent, sseResponse, type Request, type Context } from "@cellar/sdk";

export async function fetch(_request: Request, _ctx: Context) {
  async function* events() {
    yield sseEvent("started", { event: "progress", id: "1" });
    yield sseEvent("finished", { id: "2" });
  }
  return sseResponse(events());
}
```

`streamResponse(chunks, options)` accepts an iterable or async iterable of strings
or `Uint8Array`. `webSocketResponse(socket, options)` returns an upgrade.
Use `await ctx.websocketPair()`, `await socket.accept({hibernating: true, tags})`,
`await socket.send(textOrBytes)`, `await socket.close(code, reason)`,
`await socket.setAttachment(value)` and `await socket.getAttachment()`.
`ctx.websocket(id)` obtains a fresh facade in a cell event. Handler names are
`websocket_open`, `websocket_message`, and `websocket_close`, registered explicitly
with their `EventKind` variants. Message `data` is a string or `Uint8Array`.

For a worker socket, pass `{run: async (socket, ctx) => { ... }}` to
`webSocketResponse`; consume `socket.next()` until a close event. Use
`ctx.cell(binding, name).fetchStream(request)` or `ctx.fetchStream(request)` for a
live response: return it directly, consume `response.chunks()`, or call
`response.cancel()`. `ctx.acceptWebSocket(response)` consumes its upgrade.
`ctx.connectWebSocket(url, protocols)` opens an outbound connection.

These APIs work in both embedded V8 and Node. A disconnected native driver
revokes SDK authority; JavaScript cannot forcibly cancel an arbitrary promise or
Node I/O started by application code. Give that work its own cancellation policy.

## Limits and qualification

Existing invocation, operation-count, bridge-byte, body/frame, host resource and
shutdown limits still apply. Long-lived streams reserve an invocation slot until
they finish; configure capacity and total operation budgets for their lifetime. Live response/socket capability
counts also use `max_batch_operations`; retained response prefixes share the
`max_body_bytes` budget. Awaited socket receives can overlap outgoing sends.
A producer error after the head fails the body; it cannot change the HTTP status.
No generic ASGI/WSGI adapter, browser `EventSource`, automatic reconnect, HTTP/2
extended CONNECT or Workers-compatible global API is provided by this facade.
The FastAPI example's separate ASGI transport remains buffered.

The live-language tests exercise a real node and network handshake, early SSE
chunks, writes before/after the head, forwarded cell streams/upgrades, producer
errors, disconnects, text/binary echo and native shutdown. They do not establish
fleet failover or sustained-load qualification for a new deployment.
