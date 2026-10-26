<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Container bridge protocol, version 1

[Container guide](README.md) · [Provider contract](providers.md)

Applications normally use the language facade. Provider implementers use
`Operation::name()` and the owned JSON contract below.

| Convention | Encoding / bound |
| --- | --- |
| Binary data | Standard padded base64 |
| Option keys | `snake_case`, including the JS facade's internal wire values |
| Environment, labels, headers | Arrays of string pairs |
| Void result / IDs | JSON `null` / opaque strings |
| Port / signal / timeout | `1–65535` / `1–64` / integer milliseconds `0–86400000` |
| Buffered exec | Required `max_bytes`, bounded by the host limit; one combined stdout/stderr budget |
| End of stream | Empty stdout/stderr or TCP chunk |

## Lifecycle and discovery

| Operations | Input | Result |
| --- | --- | --- |
| `container.capabilities` | `{}` | Versioned capability descriptor |
| `container.reconcile` | `{}` only | `{state,generation}`; lifecycle evidence and nullable decimal generation |
| `container.running` / `.monitor` / `.destroy` | `{}` | Boolean / integer exit code / `null` |
| `container.start` | `{options:{entrypoint?,env?,enable_internet?,labels?}}` | Run token |
| `container.signal` | `{signal}` | `null` |
| `container.set_inactivity_timeout` | `{timeout_ms}` | `null` |

Reconciliation states are `unbound`, `running`, `stopped`, `deleting` and
`deleted`. The native provider reports `running`/`stopped` and a null generation.
Remote providers retain unresolved creates as errors; absence cannot authorize
replaying them. Reconciliation may durably checkpoint recovery or retirement,
so it is serialized as a control with the mutation output gate. It sends no
provider create, resume or delete request. See the
[state and recovery rules](bindings.md#reconcile-from-a-later-invocation).

## Process I/O

| Operations | Input | Result |
| --- | --- | --- |
| `container.exec` | `{options:{cmd,env?,cwd?,user?,combined?}}` | Process ID |
| `container.exec_output` | `{options:{cmd,env?,cwd?,user?,combined?},max_bytes}` | `{exit_code,stdout,stderr}` |
| `container.exec.read_stdout` / `.read_stderr` | `{id}` | Base64 chunk; empty means EOF |
| `container.exec.wait` | `{id}` | Integer exit code |
| `container.exec.close_stdin` | `{id}` | `null` |
| `container.exec.write` / `.kill` | `{id,data}` / `{id,signal}` | `null` |
| `container.release` | `{id}` | `null`; releases the resource according to its kind |

Buffered exec closes stdin and drains both output streams concurrently. For
interactive exec, write input, close stdin as appropriate, and drain both streams
while awaiting `wait`. Issue control operations sequentially; observations and
stream reads can overlap a control operation.

## Files

| Operations | Input | Result |
| --- | --- | --- |
| `container.file.read` | `{path,max_bytes}` | Base64 regular-file bytes |
| `container.file.write` | `{path,data,mode}` | `null` |
| `container.file.stat` | `{path}` | File metadata |
| `container.file.list` | `{path,max_bytes,max_entries}` | Direct-child metadata array |

Metadata is `{name,kind,size,mode,modified_ms,link_target}`. The last two fields
are nullable; kind is `file`, `directory`, `symlink` or `other`. Paths are
canonical absolute UTF-8 container paths. Mode contains permission bits only.
File operations are serialized controls and retain the mutation output gate.

See [file semantics and archive bounds](files.md). Native listings download a
bounded directory archive including descendant contents; result byte and entry
limits refuse the entire listing. File writes are non-transactional.

## Ports and sockets

| Operations | Input | Result |
| --- | --- | --- |
| `container.port.wait_ready` | `{port,timeout_ms}` | `null` |
| `container.port.fetch` | `{port,request:{url,method,headers,body}}` | `{status,headers,body}` |
| `container.port.connect` | `{port}` | Socket ID |
| `container.port.connect_websocket` | `{port,url,headers}` | `{upgraded:true,id,protocol,headers}` or `{upgraded:false,response}` |
| `container.socket.read` / `.write` / `.close_write` / `.close` | `{id}`; write adds `data` | Base64 chunk or `null` |
| `container.websocket.receive` | `{id}` | One event from the table below |
| `container.websocket.send_text` / `.send_binary` / `.close` | `{id,text}` / `{id,data}` / `{id,code,reason}` | `null` |

Container port HTTP is buffered. Redirects are returned without following them;
arbitrary HTTP streaming and server-side 101 passthrough use the separate
[language transport API](../../docs/27-streaming-and-websockets.md).

| WebSocket event | Wire value |
| --- | --- |
| Open | `{type:"open",protocol}` |
| Text | `{type:"text",text}` |
| Binary | `{type:"binary",data}` |
| Close | `{type:"close",code,reason,clean}`; retains clean/unclean provenance |
