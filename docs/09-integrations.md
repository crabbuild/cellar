<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Integrations and resource ownership

Use SDK bindings for HTTP, Blob storage, streams, sockets and other external
resources. Their handles retain event, activation and execution-domain
authority. Set timeouts and payload limits, handle cancellation and define
consistency with each external system.

![Integration boundaries](diagrams/architecture.svg)

| Surface | Builder guidance | Source / example |
| --- | --- | --- |
| HTTP and bodies | Use owned request/response APIs, explicit deadlines and bounded parsing | [HTTP](../crates/cellar/application/http.rs), [services](../crates/cellar/application/service.rs) |
| Byte streams | Prefer streaming for large payloads; cancel abandoned streams; bound buffered bytes | [streams](../crates/cellar/application/streams.rs) |
| Blob | Use streaming/ranges, conditional options and bounded paginated listings | [Blob](../crates/cellar/application/blob/mod.rs) |
| Assets and cache | Stage assets with the package; define cache freshness and rebuild behavior | [assets/cache](../crates/cellar/application/assets.rs), [counter](../examples/native-counter/project.json) |
| HTML rewriting | Transform streaming markup within the event lifetime | [rewriter](../crates/cellar/application/html_rewriter/mod.rs) |
| WebSockets | Authenticate upgrades; define reconnect/replay and bounded fanout | [WebSocket example](../examples/native-websocket/main.rs) |
| TCP/TLS | Select transport explicitly; own readers/writers, close and handle cancellation | [sockets](../crates/cellar/application/sockets.rs) |
| Containers | A cell owns the lifecycle; wait for port readiness and budget resource use | [container example](../examples/native-container/README.md) |
| Facets | Use cell-owned facet lifecycle and properties; test parent cancellation | [facets](../crates/cellar/application/facets.rs) |
| Compiled loader | Load registered native application objects with host quotas | [loader](../crates/cellar/application/loader.rs) |

See [streaming and WebSockets across languages](27-streaming-and-websockets.md)
for Rust, Python, JavaScript and TypeScript APIs.

## WebSockets and containers

The SDK includes socket pairs, ordinary and hibernating acceptance, text/binary
sends, attachments, tags and cell socket enumeration. Handle open/message/close
events explicitly. Preserve session facts that must survive reactivation in
durable state or the supported attachment mechanism. A socket is not a global
durable message log; reconnecting clients need an application sequence/catch-up
protocol. Test disconnects during writes and recovery during active sessions.

Cell-owned containers expose start, monitor, destroy, signal, inactivity policy,
ports, execution and forwarding. The container adapter needs an explicitly
managed Docker/Podman environment. The host's native executable, container image,
container filesystem and cell SQLite have different durability boundaries.
Do not assume a writable container layer is replicated by Cellar. Define
credentials, image promotion and process failure behavior. The SDK does not
configure durable volumes or replicate a container's writable layer.

### How cell-owned containers work

Declare a `containers` entry alongside the cell class in the native project:

```json
"containers": [{
  "class_name": "Sandbox",
  "image": "Dockerfile",
  "instance_type": "basic"
}]
```

Each named `Sandbox` cell controls at most one container on its current owner
node. Different cell names have different containers. The cell handler can be
Rust, Python, JavaScript or TypeScript; the program inside the image can use any
language the image supplies. The image does not have to contain a Cellar SDK.
For example, a Python cell can control a Rust executable, a Node server or a
Python interpreter in its container.

```text
Python / JS / TS handler ---- shared Rust guest bridge ---+
                                                       |
Rust handler -------------------------------- native SDK
                                                       |
                           current root-cell ownership + durability gate
                                                       |
                                  Docker Engine API over a Unix socket
                                                       |
                                 one container on the cell owner's node
```

Start, exec and other container mutations use the native output gates: an
external effect waits for the required proof of earlier cell writes. This
ordering does not make container commands transactional or exactly once. A lost
reply can follow a completed command. Record durable job IDs and reconcile
external outcomes before retrying.

Packaging builds/pulls the configured images, saves verified archives with the
native package and carries the fence image. Nodes load the archives from the
fleet bucket instead of pulling a registry at activation time. The existing
[native container walkthrough](../examples/native-container/README.md) covers
image builds, a runnable project, port forwarding and fixed-command execution.
The same deployment declarations apply to language-adapter applications.

An idle cell eviction can leave its container running for the configured
inactivity window and reconnect to it on the same node. Ownership transfer,
reset, node restart and shutdown can destroy it. Store durable job progress in
cell SQLite or bindings, and expect a fresh process after takeover.

### APIs in each language

All language adapters use the original Rust invocation and its ownership
checks. JavaScript and TypeScript share one facade on embedded V8 and Node.
Python uses embedded CPython. TypeScript compiles before deployment.

| Capability | Rust | Python | JavaScript / TypeScript |
| --- | --- | --- | --- |
| Obtain control | `context.container()?` | `ctx.container` | `ctx.container` |
| Running state | `running(context)` | `await running()` | `await running()` |
| Lifecycle | `start`, `monitor`, `destroy`, `signal` | Same async names | Same async names |
| Idle policy | `set_inactivity_timeout` | `set_inactivity_timeout` | `setInactivityTimeout` |
| Discover provider operations/limits | `Adapter::capabilities` | `capabilities` | `capabilities` |
| Select a named provider slot | `Bindings::sessions().get(name)` | `ctx.containers.get(name)` | `ctx.containers.get(name)` |
| Reconcile provider recovery/expiry | `Session::execute(Operation::Reconcile, …)` | `reconcile` | `reconcile` |
| Execute with streams | `exec` | `exec` | `exec` |
| Collect bounded output | `exec(...).output(...)` | `exec_output` | `execOutput` |
| Obtain port | `port(context, port)` | `port(port)` | `port(port)` |
| Readiness | `wait_ready` | `wait_ready` | `waitReady` |
| Buffered HTTP | `fetch` | `fetch` | `fetch` |
| Raw TCP | `connect` | `connect` | `connect` |
| Event-owned WebSocket | `connect_websocket` | `connect_websocket` | `connectWebSocket` |

Rust methods take the executing context explicitly. Adapter facades close over
one invocation: reacquire them from each handler's context. Do not retain a
Python or JS context or container/port/process/socket facade for a later event.
Exec and socket handles are held in a Rust table scoped to one invocation, with
at most 64 live resources. Finishing/cancelling the event drops that table and
closes event-owned network transports. An exec handle's release closes streams
without killing its process; use `kill` when that is the intended effect.
WebSocket and TCP `release` close their transport. A native exec process also
checks its container run, so a previous-run handle cannot control a replacement.

Start options support `entrypoint`, `env`, `enable_internet` and `labels`; env and
labels are arrays of name/value pairs. Internet access defaults to false, subject
to the macOS limitation below. Exec options support `cmd`, `env`, `cwd`, `user`
and `combined`. Adapter timeouts use integral milliseconds up to one day; ports
are 1–65535 and signals are 1–64. Run identities are opaque strings in the
language adapters to avoid JavaScript integer rounding.

Python, inside a declared container cell:

```python
async def fetch(self, request, ctx):
    container = ctx.container
    if not await container.running():
        await container.start(env=[("MODE", "worker")])
    await container.port(8080).wait_ready(timeout_ms=10000)
    output = await container.exec_output(["/app/tool"], max_bytes=4096)
    return Response.json({"exit_code": output.exit_code,
                          "stdout": output.stdout.decode(),
                          "stderr": output.stderr.decode()})
```

JavaScript or TypeScript, inside a registered cell class:

```ts
import {jsonResponse, type Context, type Request} from "@cellar/sdk";

async function runTool(_request: Request, ctx: Context) {
  const container = ctx.container;
  if (!await container.running()) {
    await container.start({env: [["MODE", "worker"]]});
  }
  await container.port(8080).waitReady(10000);
  const output = await container.execOutput({cmd: ["/app/tool"]}, 4096);
  return jsonResponse({exit_code: output.exit_code,
                       stdout_bytes: output.stdout.length,
                       stderr_bytes: output.stderr.length});
}
```

When concurrent events can start the same cell's container, serialize the
running-check/start/readiness sequence within the activation, as the native
walkthrough does. These examples show the API calls, not a startup lock.

`exec_output` / `execOutput` closes stdin, drains stdout and stderr concurrently,
and applies one combined byte cap, no larger than the host body limit. Limit
failure may follow process execution and is an unknown outcome. For interactive
commands, use `exec`, write bytes, close stdin when appropriate, and drain both
output streams concurrently while awaiting `wait`. An empty read marks EOF.
Observations such as monitor, process wait and stream reads can overlap a
mutation; issue control operations sequentially.

Native exec signalling requires `/bin/sh` in the image. A private prelude captures
its container namespace PID before replacing the shell with the literal command;
arguments never enter shell source. The prelude is removed from stdout, and the
shell's builtin delivers subsequent signals. In these images, executable lookup
failures report a nonzero process exit status. Commands must name a nonempty
executable without a leading hyphen (use an explicit path for such filenames).
Images without `/bin/sh` retain direct exec, but process signalling refuses an
unverified PID. Docker's daemon PID is never used inside the container.

HTTP forwarding rewrites the destination to the cell-owned port, preserving
path/query, method, headers and buffered body. Redirects are returned to the
caller. Adapter body, encoded-value, operation and concurrency limits still
apply. Container port HTTP is buffered; arbitrary response streaming and
server-side 101 passthrough are outside this port API. General streaming HTTP
and WebSocket upgrades use the separate
[language transport API](27-streaming-and-websockets.md).

For container WebSockets, Python's `connect_websocket` returns either a
`ContainerWebSocket` or the declined `Response`. JS/TS returns a discriminated
`{upgraded: true, socket, protocol}` or `{upgraded: false, response}` result.
Use `send_text`/`sendText`, `send_binary`/`sendBinary`, `receive`, and `close`.
Receive events are `open`, `text`, `binary`, or `close`; binary payloads are bytes.
The socket belongs to the opening event and closes when it ends. It does not
register hibernating cell WebSocket event handlers.

### Portable files

All language adapters share provider-selected binary read/write and metadata
operations. Native Rust also exposes typed `read_file`, `write_file`,
`stat_file` and `list_files` methods on its container control.

| API | Content / metadata |
| --- | --- |
| Python | `bytes`, frozen metadata and tuple listings |
| JavaScript / TypeScript | `Uint8Array`, frozen metadata and arrays |
| Native Rust | `Vec<u8>`, typed `FilePath` and `FileMetadata` |

Use absolute container paths and explicit byte/entry budgets. Native uses the
engine archive API and does not need image-side shell tools. Directory scans
include descendant data; oversized results refuse in full. File writes are
non-transactional and container disk has different durability from cell state.
See the [file guide and examples](../crates/cellar-container/files.md).

### Docker, Podman and hosting requirements

The optional [cellar-container adapter](../crates/cellar-container/README.md)
provides a shared provider boundary. Its default `Native` provider uses the
engine below. Rust hosts can implement `Provider` and `ProviderSession` for other
container or sandbox services, and select them for Python or JS/TS through
`Source::container_provider`. Sessions retain invocation authority and gate
mutations before provider dispatch. Remote providers must enforce their own
remote fencing, isolation, cleanup and accounting. Only the native Docker/Podman
provider ships today. The
[sandbox provider and compatibility plan](../plans/sandbox-provider-compatibility.md)
includes Vercel, E2B, Daytona, Modal and Cloudflare Workers/Containers/Sandbox.
The primary pattern follows celld: compatible application APIs on Cellar, backed
by the local engine or another declared execution provider. Local Cloudflare
SDK compatibility, additional hosted providers and Dynamic Worker execution have
separate acceptance gates; a provider name alone does not enable them.

Native container control requires a running Linux Docker or Docker-compatible Podman
engine with a reachable Unix socket. At the CLI boundary, use `DOCKER_HOST` with
`unix://...` for a non-default socket. Application-owned hosts can pass
`NodeServices::containers` / `EngineOptions` explicitly. Cellar installs its
network fence before starting managed containers. Container declarations can
select an OCI `runtime` such as `runsc`; the daemon must have it configured.

Linux hosts reach container ports through their bridge addresses. On macOS,
Cellar uses published ports; declare them with image `EXPOSE` metadata or the
class's bounded `exposed_ports` deployment setting, and
`enable_internet=false` cannot enforce the same internal-network behavior.
Use Linux when testing that isolation guarantee. The engine/fence remains a
host-level prerequisite regardless of the handler language.

Running the **Cellar host itself inside Docker** is a separate deployment choice.
Build the application executable for the image's Linux architecture and include
its runtime dependencies: embedded CPython plus verified Python packages, or
Node plus emitted modules and npm dependencies for the Node backend. Embedded
V8 code ships in the executable. Mount persistent Cellar node state and configure
fleet networking and the bucket as for a native host.

If that Dockerized host also controls sibling containers, it needs the engine
socket and network access to their ports. Bind-mount source paths are resolved
on the engine machine, so paths used for container DNS/runtime files must also
be available there. These are deployment responsibilities; selecting a Python
or JavaScript handler does not create an engine or bundle its language runtime
into managed workload images.

### How this differs from celld

The inherited engine concepts are similar: a cell controls a container, images
travel with deployment, ports are capabilities, and container disks are
ephemeral. The application contracts differ:

| Area | Cellar | celld |
| --- | --- | --- |
| Host/application API | Native Rust SDK; optional CPython, embedded V8 and Node adapters | Cloudflare-compatible Workers / Durable Object APIs |
| Container entry point | Rust `Context::container`; typed Python and JS/TS facades | Workers `ctx.container`, `@cloudflare/containers` |
| Container sandbox packages | Native lifecycle, exec and port primitives | Published `@cloudflare/sandbox` compatibility |
| Application packaging | Compiled native applications and declared language runtime artifacts | Workers deployment and compatibility configuration |
| Port HTTP in language adapters | Bounded buffered requests/responses; explicit event-owned WebSocket API | Workers HTTP/WebSocket APIs |

This comparison follows [celld's container guide](https://github.com/denoland/celld/blob/main/docs/services/containers.md).
Cellar does not claim drop-in compatibility with `@cloudflare/containers` or
`@cloudflare/sandbox`, filesystem snapshots, persistent Docker volumes, arbitrary
Docker administration, Windows named-pipe engines or remote TCP/TLS Docker APIs.
Those require separate contracts and qualification. Native control of Docker
workloads in all four application languages does not imply those additional
features.

## Blob and database consistency

Blob objects and SQLite mutations do not form one transaction merely because
both APIs are available in one handler. A proposed upload flow writes an object
under a unique immutable key, verifies it, commits the reference in cell state,
then reconciles abandoned objects. Deletes require a similar reference/retention
policy. Bound multipart work, abort incomplete uploads, and handle conditional
read/write outcomes. Large KV values may use Blob-backed storage; validate the
combined failure and retention behavior for your workload.

## Configure outbound HTTP

Embedded hosts configure SDK HTTP with `NodeConfig::http` and
`NodeServices::http_client`. An optional `HttpClientFactory` supplies DNS, TLS,
credentials, proxies and pool settings; library defaults ignore ambient
proxies. The request deadline includes reading the response body. Cellar
preserves redirect modes and worker subrequest budgets across factory clients.
See [HTTP policy](../crates/cellar/transport/http/policy.rs) and the
[two-host regression](../crates/cellar/tests/support/node_http.rs).

Use the framework path for egress tied to a state mutation so it retains gate
and event authority. Raw network operations in application code can bypass
those lifecycle protections. External destinations still need authentication,
authorization and idempotency. Restrict outbound targets for user-supplied URLs
at the host/network boundary and validate redirects and resolved addresses.

Test integration cancellation, slow consumers, partial bodies, malformed input,
deadline expiry, stale handles, limits and dependency failure. SDK coverage is
listed in [API map](22-api-reference.md); production acceptance belongs to
[testing and qualification](19-testing-qualification.md).
