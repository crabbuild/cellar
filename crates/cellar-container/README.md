<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# cellar-container

Container and sandbox provider adapters for native Cellar applications and the
Python, JavaScript and TypeScript facades. The default `Native` provider delegates
to `cellar::sdk::containers`, using the node's Docker or compatible Podman Engine.
It preserves native image loading, network isolation, inactivity policies,
ownership cleanup, exec streams and port capabilities.

The adapter is a host extension point. It does not implement Cloudflare Workers,
Cloudflare Containers or the Cloudflare Sandbox SDK. A remote provider can be
implemented without changing the existing guest container facade, but must
satisfy the lifecycle and isolation contract below. No additional cloud provider
ships in this crate.

Cloudflare Workers, Containers and Sandbox are explicit targets alongside Vercel,
E2B, Daytona and Modal in the
[provider and compatibility delivery plan](../../plans/sandbox-provider-compatibility.md).
The primary pattern follows celld: expose compatible application APIs on Cellar
and execute through the local engine or another declared provider. Local
Cloudflare package compatibility comes first; hosted Cloudflare services are an
optional backend. The plan includes versioned Workers/Containers/Sandbox
profiles, Dynamic Worker isolates and four-language qualification. Those
extensions and remote backends remain pending; the existing provider interface
is the starting point, not a complete sandbox or Workers implementation.
Track delivered slices and outstanding qualification in the
[implementation ledger](../../plans/sandbox-provider-progress.md).

## Select a provider

Rust hosts select a trusted `Arc<dyn Provider>`. A provider declares its supported
`Operation`s and opens a fresh `ProviderSession` for each invocation. The shared
`Session` validates input and checks capabilities before dispatch. Binary results
and combined buffered exec output are checked against the same byte limits
after dispatch; a result failure cannot establish that effects were rolled back. Unsupported
operations return a proven pre-effect validation refusal. Failed submitted
mutations retain an unknown outcome; callers must not retry them blindly.

```rust,no_run
use cellar_container::{Adapter, Limits, Native, Operation};
use cellar::sdk::Context;
use serde_json::json;
use std::sync::Arc;

async fn running(context: &Context) -> anyhow::Result<bool> {
    let adapter = Adapter::new(Arc::new(Native));
    let session = adapter.session(); // retain for this invocation's later calls
    let value = session.execute(context, Operation::Running, json!({}),
        &Limits { max_body_bytes: 1024 * 1024 }).await?;
    Ok(value.as_bool().unwrap_or(false))
}
```

Python and JS/TS hosts use `Source::container_provider(provider)` before starting
their runtime. Omission selects `Native`. Selection applies to the application's
container operations; a provider can route by the context's class or cell identity.
The guest's `ctx.container` API and error envelope stay the same. Provider names,
credentials and routing configuration belong to the host, never to guest input.

## Implement a provider

Implement `Provider::{name, operations, open}` and
`ProviderSession::execute`. The future is polled within the original native SDK
invocation, so providers can use SDK networking and the host's execution-domain
services. The adapter binds a session to exactly one root-cell event and checks
current turn, event liveness, activation epoch and host ownership before and after
dispatch. Every mutation awaits the native container durability/output gate.
Native-only class and engine requirements are checked by `Native`; a remote
provider need not declare a local Docker image.

Provider requirements:

- Use stable cell identity plus ownership epoch and a provider-side fencing token
  to prevent an old node from changing an instance after takeover. Local checks
  cannot cancel a remote mutation that has already been accepted.
- Define image, CPU/memory, runtime, filesystem and network isolation policies.
  Reject unsupported security settings before creating resources. Native bridge
  fences and resource accounting do not apply automatically to a remote backend.
- Keep handles private to the provider session and invalidate process handles
  when a container run changes. Bound live resources, stdout/stderr, binary frames
  and HTTP bodies using `Limits::max_body_bytes`; collect both exec streams
  concurrently. Do not follow redirects through a container port capability.
- Drop must cancel pending work and release invocation streams. Keep container
  lifetime separate from stream lifetime. Reconcile or destroy remote instances
  on ownership loss, host shutdown and explicit destroy; provide provider-side
  leases/expiry for node crashes. Opening a provider session must perform no I/O.
- Declare an operation only when its full semantics are implemented. Report
  uncertain mutations as ordinary errors. Attach `Validation` only when no
  provider effect could have occurred. Gate completion does not provide exactly
  once execution, and returning an error does not roll back external effects.

A provider may support a subset, such as lifecycle plus buffered exec, while
refusing raw TCP or WebSockets. Hosts can query `Adapter::capabilities(&limits)`
without allocating resources. Root-cell guests can query
`ctx.container.capabilities()` (Python: `await ctx.container.capabilities()`).
The version-1 descriptor reports `provider`, `environment` (`linux`,
`worker_isolate` or `unspecified`), `operations`, `max_body_bytes` and
`max_resources` (null means undeclared, not unlimited). Querying metadata does
not start a sandbox or call the provider's effect dispatcher. Native reports
Linux and its enforced 64-handle limit. No snapshot/PTY feature is implied by a
provider name. JS and Python return immutable descriptor values.

## Bridge protocol, version 1

`Operation::name()` is the stable JSON bridge name. Inputs and results are owned
JSON values. Binary data uses standard padded base64. Options use snake case,
including from the JavaScript facade; the facade translates its public API.
Successful void operations return JSON null. IDs and run tokens are opaque strings.

| Operations | Input | Result |
| --- | --- | --- |
| `container.capabilities` | `{}` | versioned provider/environment/operation/limit descriptor |
| `container.running`, `container.monitor`, `container.destroy` | `{}` | boolean, integer exit code, null |
| `container.start` | `{options:{entrypoint?,env?,enable_internet?,labels?}}` | run token |
| `container.signal` | `{signal}` | null |
| `container.set_inactivity_timeout` | `{timeout_ms}` | null |
| `container.exec`, `container.exec_output` | `{options:{cmd,env?,cwd?,user?,combined?},max_bytes?}` | process ID; or `{exit_code,stdout,stderr}` |
| `container.exec.read_stdout`, `.read_stderr`, `.wait`, `.close_stdin` | `{id}` | base64 chunk; exit code; null |
| `container.exec.write`, `.kill` | `{id,data}`; `{id,signal}` | null |
| `container.port.wait_ready` | `{port,timeout_ms}` | null |
| `container.port.fetch` | `{port,request:{url,method,headers,body}}` | `{status,headers,body}` |
| `container.port.connect` | `{port}` | socket ID |
| `container.port.connect_websocket` | `{port,url,headers}` | `{upgraded:true,id,protocol}` or `{upgraded:false,response}` |
| `container.socket.read`, `.write`, `.close_write`, `.close` | `{id}`; write adds `data` | base64 chunk or null |
| `container.websocket.receive` | `{id}` | event described below |
| `container.websocket.send_text`, `.send_binary`, `.close` | `{id,text}`; `{id,data}`; `{id,code,reason}` | null |
| `container.release` | `{id}` | null |

Environment variables, labels and headers are arrays of string pairs. `max_bytes`
is required for `exec_output`, is a combined stdout/stderr budget and cannot
exceed the host limit. Buffered exec closes stdin. Empty stdout/stderr or TCP
chunks signal EOF. WebSocket events are `{type:"open",protocol}`,
`{type:"text",text}`, `{type:"binary",data}`, or
`{type:"close",code,reason,clean}`. Close events retain clean/unclean provenance.
`release` drops exec streams without killing the process, and closes sockets.

Ports are integers 1–65535, signals 1–64 and timeout milliseconds 0–86400000.
The native provider allows at most 64 live handles per invocation. All providers
must observe the same input validation and wire contract; providers may impose
stricter resource limits.

See the [integration guide](../../docs/09-integrations.md) for native engine
configuration, language examples and the celld comparison.

## Validation

Run `cargo test --locked -p cellar-container -p cellar-guest --lib` and the
container integration tests in `cellar-javascript` and `cellar-python`. Their
host-selected audit provider delegates to native while recording actual dispatch,
covering provider selection, binary streams, cancellation, timeout cleanup and
ownership rejection through CPython, Node and embedded V8.
