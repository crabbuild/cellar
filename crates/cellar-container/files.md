<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Portable container files

[Container guide](README.md) · [Provider contract](providers.md) · [Wire reference](protocol.md#files)

Read, write, stat and list files through the selected provider. The default
`Native` provider uses Docker/Podman's archive API: the image needs no shell,
`tar` executable or file-copy helper. Archives are never extracted on the host.

## The same bytes in each language

These examples run inside a root-cell event after its container has started.
The `/workspace` parent directory must already exist in the image.

**Rust — native typed API:**

```rust,ignore
use cellar::sdk::containers::FilePath;

let container = ctx.container()?;
let path = FilePath::new("/workspace/input.bin")?;
container.write_file(ctx, path.clone(), vec![0, 255, 42], 0o640).await?;
let bytes = container.read_file(ctx, path, 4096).await?;
assert_eq!(bytes, [0, 255, 42]);
```

For a host-selected Rust provider, dispatch `Operation::WriteFile` /
`Operation::ReadFile` through the event's `Session`; use the same
[wire inputs](protocol.md#files) as the language adapters.

**Python:**

```python
await ctx.container.write_file("/workspace/input.bin", b"\x00\xff*", mode=0o640)
content = await ctx.container.read_file("/workspace/input.bin", max_bytes=4096)
metadata = await ctx.container.stat_file("/workspace/input.bin")
entries = await ctx.container.list_files("/workspace", max_bytes=65536, max_entries=128)
```

**JavaScript / TypeScript:**

```ts
await ctx.container.writeFile("/workspace/input.bin", new Uint8Array([0, 255, 42]), {
  mode: 0o640,
});
const content = await ctx.container.readFile("/workspace/input.bin", 4096);
const metadata = await ctx.container.statFile("/workspace/input.bin");
const entries = await ctx.container.listFiles("/workspace", {
  max_bytes: 65536,
  max_entries: 128,
});
```

## Paths, limits and outcomes

| Contract | Behavior |
| --- | --- |
| Paths | Absolute UTF-8 Linux container paths, at most 4,096 bytes; refuse NUL, relative paths, dot segments, repeated separators and trailing slash except `/` |
| Read | Regular file bytes; require a complete result within `max_bytes` |
| Write | Create or replace a regular file in an existing parent; permissions `0o000`–`0o777`, default `0o644`; no implicit directory creation |
| Metadata | Name, kind (`file`, `directory`, `symlink`, `other`), byte size, permission bits, nullable `modified_ms` and `link_target` |
| List | Direct children, sorted by name for Native; refuse duplicates or an exceeded budget instead of returning a partial listing |
| Defaults | Language facades request 1 MiB and 128 entries; the host byte limit still applies; at most 4,096 entries |
| Lifetime | File content belongs to the container filesystem; it can survive a later cell event while that container exists |
| Durability | Container disk is separate from durable cell SQLite; writes are not atomic or transactional and takeover may start a fresh container |
| Failure | Invalid input refuses before dispatch; a lost write acknowledgement or invalid provider reply can mean an unknown outcome |

Python metadata is frozen and listings are tuples. JS/TS metadata and listing
arrays are frozen; binary content remains `Uint8Array`. Numeric metadata must
fit the JavaScript safe integer range in every language.

## Native archive limits

| Traffic | Bound |
| --- | --- |
| File download | Requested content budget plus 16 KiB archive overhead; decoded content must fit the requested budget |
| File upload | Host-bounded bytes in one generated, uncompressed TAR entry; permissions are explicit, UID/GID fields are zero |
| Directory download | Requested byte budget plus 16 KiB archive overhead; **includes descendant file data**, even though only direct metadata is returned |
| Returned listing | Both serialized metadata bytes and requested entry count must fit |

A directory with a large descendant can therefore fail a small listing budget.
Choose a suitable bounded directory or increase the budget within host limits.
Native rejects sparse archives, truncated entries, multi-entry file reads and
non-UTF-8 filenames. The engine resolves container paths; concurrent processes
can change their filesystem while a request is running.

File I/O retains the original cell owner/epoch checks and mutation output gate.
Even read/stat/list are serialized controls because they dispatch external I/O.
Cancellation of an in-flight native request drops its owned connection; it does
not establish whether an accepted write completed.

The engine protocol follows [Docker's archive API](https://docs.docker.com/reference/api/engine/version/v1.51/)
and [Moby's copy client](https://github.com/moby/moby/blob/master/client/container_copy.go).
