<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Provider operation matrix

[Container guide](README.md) · [E2B profile](e2b.md) · [Vercel profile](vercel.md)

This matrix describes implemented portable operations. Discover the actual
host-selected subset with `ctx.container.capabilities()`; a method's presence in
a language facade does not grant provider support.

| Portable operation | Native Docker / compatible Podman | E2B | Vercel |
| --- | --- | --- | --- |
| Capability discovery | Yes | Yes | Yes |
| Running / start / destroy | Yes | Yes | Yes |
| Custom start entrypoint | Yes | No; host template | No; host image |
| Bounded binary command output | Yes | Yes | Yes; image-local helper |
| Interactive process streams | Yes | No | No |
| Container signal / monitor | Yes | No | No |
| Inactivity timeout | Yes | No; fixed lifetime | No; fixed session timeout |
| Binary file read/write | Yes; engine archives | Yes | Yes |
| File stat / direct-child list | Yes; bounded archive scan | Yes | Yes; helper |
| Port readiness / HTTP forwarding | Yes | No | No |
| Incremental HTTP response reads / cancellation | Yes; [stream contract](streams.md) | No | No |
| Raw TCP / WebSocket forwarding | Yes | No | No |
| Synchronous running / queued lifecycle opt-ins | Yes | No | No |
| PTY / resize, snapshots, GPUs and volumes | Not exposed by this operation set | Not exposed | Not exposed |
| Worker module / isolate invocation | Separate planned operation family | Not implemented | Not implemented |

## Configuration and guarantees

| Boundary | Native | E2B | Vercel |
| --- | --- | --- | --- |
| Host chooses | Engine + image | Account + template | Team/project + prepared image |
| Linux prerequisites | Engine-compatible image | Secure compatible envd; coreutils for writes | Python 3 helper executable in image |
| Ownership descriptor | `native_cell` | `local_checks_and_expiry` | `local_checks_and_expiry` |
| Direct strict-fence profile | Native host's cell/run checks | Constructor refuses | Constructor refuses |
| Durable identity | Native cell/run ownership | Protected resource ID + generation | Protected name + exact session ID + generation |
| Auto reconnect/resume | Native lifecycle policy | Inspect the verified resource; no auto-resume | Inspect with `resume=false`; changed sessions refuse |
| Filesystem durability | Separate from cell SQLite durability | Separate from cell SQLite durability | Separate from cell SQLite durability; persistence disabled |
| Unknown mutation reply | Reconcile; no proof of rollback | Retain intent; no blind replay | Retain intent; no blind replay |

Direct cloud profiles require an explicit weaker-ownership choice. Native checks
cannot revoke remote work already accepted by a service. Fixed expiry does not
establish automatic cleanup on owner loss or node shutdown.

## Evidence is profile-specific

| Profile | Evidence | Still required |
| --- | --- | --- |
| Native portable API | Native-host peers, language integrations and separate real Docker image file/command acceptance | Broader Podman/platform and fault qualification |
| Local Cloudflare baseline | Pinned Containers `0.3.7` and Sandbox `0.12.9`; published package constructors, unchanged basic Sandbox/image routes and in-object/Worker/DO HTTP/direct RPC `execStream()` probes | Full hooks, sleep/wake, Request/Response transfers and SDK upload transport, upgrades, previews, lifetimes and failure qualification |
| E2B | Pinned protocol sources; native host/restart peer and CPython/Node/V8 fixtures | Live VM/account, expiry, ownership takeover, orphan cleanup and stronger fencing |
| Vercel | Pinned SDK `3.9.0` protocol sources; native host/restart peer, actual local helper and CPython/Node/V8 fixtures | Live VM/image/account, expiry, takeover, orphan cleanup and stronger fencing |
| Daytona, Modal, Sandbox 1.x, Dynamic Workers, hosted Cloudflare | Planned targets | Dedicated implementations and independent qualification |

Portable E2B/Vercel support does not establish Vercel/E2B SDK compatibility or
Cloudflare Workers/Containers/Sandbox conformance. The direct providers also
refuse the Workers lifecycle profile's required synchronous/queued opt-ins.
See the [complete delivery plan](../../plans/sandbox-provider-compatibility.md)
and [evidence ledger](../../plans/sandbox-provider-progress.md).
