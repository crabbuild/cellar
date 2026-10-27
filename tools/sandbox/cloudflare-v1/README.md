<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Sandbox 1.0.3: native processes, terminals and Files

This separate, partial profile preserves the
[celld/Sandbox 0.12.9 baseline](../compat/README.md).

```mermaid
flowchart LR
    DO[Application Durable Object] --> Exec[Cellar native container exec]
    Files[Published Files 1.0.3] --> Exec
    Exec --> Process[Invocation-owned Linux process]
    Process --> Shim[Matching sandbox-shim 1.0.3]
    Shim --> Disk[Container filesystem]
```

| Input | Pinned identity |
| --- | --- |
| SDK | `@cloudflare/sandbox@1.0.3`; lockfile integrity and imported source hash |
| Helper | `cloudflare/sandbox@sha256:fc1740c3cba26461f0ab1771287c84018ab2cca43a70ebb823087c85d780aaec` |
| Linux base | Node 24.13.1 bookworm-slim, immutable amd64 manifest in [Dockerfile](Dockerfile) |
| Exec signalling | The base provides `/bin/sh`; native exec captures its namespace PID before replacing it with the command and uses the shell's signal builtin |
| Fixture | Cellar application using the unmodified published `Files` implementation |
| Image admission | Native host configuration; application `start({ image })` is unsupported |

## Process API

```ts
const process = await container.exec(["node", "--version"]);
const output = await process.output();
console.log(new TextDecoder().decode(output.stdout), output.exitCode);
await container.setInactivityTimeout(60_000);
```

| Surface | Contract / limits |
| --- | --- |
| Admission | Wait for earlier queued startup; literal argv, nonempty executable without a leading hyphen |
| Environment / user | Explicit cwd/env; user requires numeric UID:GID |
| `pid` / `isPty` | Immutable allocation metadata; the PID is verified inside the container, never taken from Docker's daemon namespace. Shell-less images report unavailable PID |
| `pty` | `true` defaults to 80 columns / 24 rows; explicit dimensions are 1–65,535. stdin defaults to pipe and stderr must be combined; requires Docker API 1.42+ |
| `resize(cols, rows)` | Returns void; reserves event/callback lifetime before queueing an original-handle native control. Invalid sizes refuse synchronously; failed acknowledgements remain reported event work, with no replay |
| stdout / stderr | Node byte streams with backpressure; pipe, ignore, or combined stderr |
| stdin | Omitted sends EOF; `"pipe"` exposes a byte writer; `ReadableStream<Uint8Array>` pumps automatically in native body-sized chunks |
| `output()` | Collect once, with a combined native body-size limit; overflow fails without successful truncation |
| `exitCode` | Nonzero codes resolve normally |
| `kill(signal)` | Returns void and queues SIGTERM (15) by default. Reserves the original event/callback before effects; invalid signals refuse synchronously and failed acknowledgements remain event failures without replay. Observe completion through `exitCode` |
| Signal | Already aborted refuses allocation; live abort requests SIGKILL; detach listener after exit |
| Controls | Serialize per event; ordinary and cleanup queues each admit at most 64 pending controls |
| Cleanup | Completed streams/exit release native handles; cancellation joins native observation cleanup |

The native signal path requires `/bin/sh`, captures its container namespace PID
before `exec`, and uses the shell's builtin `kill`. Application arguments stay
separate from the static shell source. Images without `/bin/sh` retain direct
exec; signalling refuses an unverified PID. The private identity header is
removed before exposing stdout. Admission bounds pre-header stderr and creates
no background reader until identity is known.

Every handle retains its original invocation. Dropping streams or receiving an
unknown result does not prove termination or undo file writes. Kill or destroy
explicitly when the application requires termination.

## Use a terminal

```ts
const process = await container.exec(["sh"], {
  pty: { cols: 80, rows: 24 },
});
console.log(process.pid, process.isPty);
process.resize(132, 40);
const writer = process.stdin!.getWriter();
await writer.write(new TextEncoder().encode("exit\n"));
await writer.close();
writer.releaseLock();
const output = await process.output();
```

PTY output carries the kernel's terminal discipline, including CRLF and combined
stderr. Ordinary pipe mode preserves binary bytes. A process that exits retires
its native handle even if its default terminal input writer was never opened.
The portable SDK exposes the same controls through `exec_pty`, `info()` and an
acknowledged `resize()`; Cloudflare's resize surface remains void.

## Stream a command's input

```ts
const process = await container.exec(["sha256sum"], {
  stdin: new Blob(["hello"]).stream(),
});
const output = await process.output();
console.log(new TextDecoder().decode(output.stdout));
```

| Source boundary | Behavior |
| --- | --- |
| Admission | Reserve the reader and event/callback lifetime before allocation; locked, non-stream and pre-aborted input refuses before effects |
| Allocation failure | Release the source lock without consuming or cancelling it; never replay the command |
| Flow | Read one source chunk, split it to the existing native limit and await each write acknowledgement before pulling again |
| EOF | Acknowledge native stdin closure, join the pump and release its reader |
| Exit, abort or lost event | Cancel a pending read and join source cancellation; queued source writes recheck cancellation before dispatch |
| Source/write failure | Start native retirement before awaiting application source cancellation; request SIGKILL, join cancellation and preserve the initiating failure in `output()`, with no write replay |
| Callback lifetime | Input controls retain their accepted callback until input settles; admitted exec reads/waits and automatic ignored-output drains keep original event authority afterward. New ordinary reads from a closed callback still refuse |
| Concurrent storage | Existing-handle writes, closure, signals and retirement use native control serialization without occupying the SQLite mutation slot; effect classification and output proofs remain unchanged |
| Transactions | Refuse process creation before provider dispatch; release the unconsumed source |

Stream mode returns `stdin: null`. Its pump remains owned by the original event
after a response, under the existing runtime deadlines. An accepted command may
have effects before a source failure; cleanup is not rollback or remote fencing.

The published file API uses the same process surface:

```ts
import { Files } from "@cloudflare/sandbox";

const files = new Files(this.ctx.container);
await files.mkdir("/workspace/data", { recursive: true });
const response = await files.readFile("/workspace/data/example.bin");
// response.body streams helper output; it is not collected into one host body.
return response;
```

The [complete fixture](fixture.mjs) starts the host-configured image and writes,
renames, stats and reads a binary file across separate events.

## Run qualification

Use a private engine/socket and preloaded Cellar fence image:

```sh
npm ci --ignore-scripts --prefix tools/sandbox/cloudflare-v1
npm test --prefix tools/sandbox/cloudflare-v1
npm run build --prefix tools/sandbox/cloudflare-v1
docker --context YOUR_PRIVATE_CONTEXT build --platform linux/amd64 \
  -t cellar-sandbox-v1:1.0.3 tools/sandbox/cloudflare-v1
docker --context YOUR_PRIVATE_CONTEXT image inspect \
  cellar-sandbox-v1:1.0.3 --format '{{.Id}}'
```

Set the immutable local image ID below. Select Node 24+, the locked Rust
toolchain and this checkout's writable `CARGO_TARGET_DIR`.

```sh
CELLAR_SANDBOX_ENGINE_SOCKET=/path/to/private/docker.sock \
CELLAR_SANDBOX_V1_IMAGE=sha256:YOUR_BUILT_IMAGE_ID \
CELLAR_SANDBOX_FENCE_IMAGE=sha256:YOUR_PRELOADED_FENCE_ID \
cargo test --locked -p cellar-javascript --features node \
  --test sandbox_v1 -- --ignored --test-threads=1
```

| Acceptance | Boundary |
| --- | --- |
| Binary stdout/stderr, nonzero exit and missing executable | Native Linux exec and bounded output; shell-backed missing command exits 127 |
| Terminal identity / initial geometry | Compare the facade PID with the actual Node process PID, confirm tty input/output and read 80×24 from kernel `stty` |
| Resize / terminal channels | Resize to 132×40 before ordered stdin, inspect the new kernel geometry, require combined stderr/CRLF, and refuse malformed/foreign controls before provider dispatch |
| Void controls / transactions | Kill returns void and terminates a ready process through default SIGTERM; observe exit separately. Transaction-bound kill/resize refuse synchronously with proven non-commit before effects |
| Default terminal retirement | A fast terminal command with untouched default stdin still explicitly releases its native handle; no event-drop fallback |
| Source stdin, 2 MiB + 17 bytes | Exact length/SHA-256 through native chunked writes; callback returns before process exit |
| 130 sequential source processes | Byte-for-byte echo, unlocked source readers and reusable retention/handle capacity; existing limits stay unchanged |
| Early exit and invalid source chunks | Cancel pending input, release its reader and preserve the source `TypeError` over observation cancellation |
| Closed callback | New ordinary stdout read and a forged observation flag on a storage write refuse before dispatch; automatic ignored stdout/stderr drains still finish |
| Streamed or buffered process creation in a transaction | Typed `ValidationError` / `not_committed`, zero source pulls and zero provider allocation calls |
| 2 MiB + 17 bytes write/read, rename and stat | Published Files, matching helper and SHA-256 round trip across events; 64 KiB native chunks |
| 70 sequential and two parallel stats | Native handle retirement and mutation admission |
| Missing file | Published `SandboxFileError.is()` and `ENOENT` |
| Quota / live abort | No successful truncation; joined native observation cancellation and explicit container retirement |
| Saved process in later event | Original authority refuses before effects |
| Handle retirement and shutdown | Explicit native release of every allocated process after each route, independently of event-drop cleanup; native cell/application drain, zero bridge invocations/bytes after join, no owned containers, only empty labelled test networks removed |

Ordinary CI verifies inputs, builds the SDK fixture and tests the facade. The
opt-in image campaign uses a 120-second native handler budget, a 30-second JS
execution budget and 2,048 guest operations. Existing 250 ms module fixtures and
the 0.12.9 profile retain their original budgets.

## Repeated-host firewall qualification

The native fence stays within 64 MiB. A terminal own-table observation scopes
nft's cache while the isolation rules are replaced atomically. The
[cache probe](../fence-cache-probe.py) compares the production transaction with
its previous global-cache behavior using `nft --check`; it commits no firewall
changes and removes only its labelled probe containers.

```sh
python3 tools/sandbox/fence-cache-probe.py \
  --context YOUR_PRIVATE_CONTEXT \
  --image sha256:YOUR_PRELOADED_FENCE_ID \
  --require-oom-control
```

`--require-oom-control` requires the previous OOM to reproduce. On a clean
engine, omit it to check production admission and report both measurements.

## Native gateway entrypoints

The separately pinned [gateway fixture](gateways.mjs) imports the unmodified
`DirectoryBackupGateway` and `S3Gateway`. It routes their fetch and control RPC
through native Worker entrypoints, copies per-binding props, and verifies deny
grants and control rejection. It requires no Docker engine or cloud account:

```sh
cargo test -p cellar-javascript --features node --locked --test workers_entrypoints
```

The same test covers class/object entrypoints, configured bindings, public
properties, stateless construction, original-event background work, malformed
input refusal and unknown results after execution. These gateway checks do not
qualify R2 multipart storage, outbound interception, directory backup/restore or
S3 mounts. Those remain required delivery steps with the actual helper/image.

## Remaining compatibility

This does not qualify all Sandbox 1.x operations. Image selection, snapshots,
outbound handlers, S3 mounts,
directory backups, full cancellation/fault/persistence semantics and hosted
Cloudflare remain unsupported or unqualified. The web-stream process facade
requires Node; embedded V8 still lacks that web bootstrap. Shared-framework
[native supervision](../../../crates/cellar-container/supervision.md) has selected
protocol/fleet evidence; live remote accounts and provider-enforced fencing
remain unqualified.

Upstream contracts: [1.0 changes](https://developers.cloudflare.com/sandbox/sdk/migrate/changes-in-1-0/),
[Files](https://developers.cloudflare.com/sandbox/reference/files/),
[helper requirements](https://developers.cloudflare.com/sandbox/reference/) and
[container exec](https://developers.cloudflare.com/containers/api/durable-object-container/).
