# Raft Engine host integration

Status: implemented for MR-02, with [storage verification evidence](evidence/MR-02.json).
This audit records source inspection of the published Apache-2.0 `raft-engine`
0.4.2 crate and the resulting adaptations. Its storage tests do not qualify
elections, SQLite image application, S3 recovery or production infrastructure.

The upstream `env::FileSystem` injects log-file create/open/read/write/truncate
and sync operations. Three production paths bypass that interface:

| Source | Observation | Required adaptation |
| --- | --- | --- |
| `file_pipe_log/pipe_builder.rs` | Directory creation, enumeration and lock-file ownership use `std::fs` directly | Route them through the owning host filesystem and retain an exclusive lock guard |
| `file_pipe_log/pipe.rs` | Directory sync opens a direct OS file and unwraps failure | Use injected directory sync and return failure to the quarantining adapter |
| `file_pipe_log/log_file.rs` | A failed log sync panics | Preserve fail-closed behavior through a typed error and permanent engine quarantine |

The engine also owns a metrics thread; its Drop sends termination and joins the
thread. Open/recovery, engine operations and final drop must execute through the
host blocking executor, with shutdown awaiting their completion. Recovery workers
must be joined before an engine is published to the node.

MR-02 retains the actual upstream engine and its log format. A narrow,
attributed dependency patch exposes the missing host operations. It must not
replace Raft storage with MemStorage or silently accept partial filesystem
injection. The patch and the host adapter require fault tests for directory sync,
directory loss, lock exclusion and file sync errors before the task can be DONE.
After any write/sync failure, close the affected shared engine's admission rather
than allowing its in-memory state to establish another durability proof.

The host filesystem also needs explicit random write, truncate, read/write open
and lock-guard capabilities. Unsupported caller-supplied implementations must
return an explicit unsupported error; none may fall back to the direct backend.
Existing Fleet filesystem operations retain their behavior.

The upstream protobuf build helper accepts only a system protoc with major
version 3, and has no bundled macOS ARM binary. The installed protoc 34 therefore
fails the baseline build. Cellar vendors `raft-proto` 0.7.0's unchanged definitions
and runtime helpers, replacing its build step with `protobuf-codegen-pure` 2.28.0.
The selected runtime remains rust-protobuf 2.28.0; the upstream strict enum-read
rewrite is preserved. This patch supports the selected protobuf codec, refuses
the unqualified prost codec, and needs no downloaded protoc executable at build
time. MR-02 must verify Entry/HardState/ConfState round trips through the engine.

The storage worker owns the engine on one execution-domain blocking task.
Commands enter a bounded queue and completion replies follow successful sync.
The worker must stop on domain closure even if a caller retains a handle, drop
the engine and join its metrics thread before reporting close completion. Group
Storage views use a bounded cache published only after sync, so RawNode's
synchronous term/index/entry lookups perform no filesystem I/O. Exhausting that
cache refuses admission; snapshots/compaction reclaim it. This is a cache over
the durable engine, not use of raft-rs MemStorage.

The implemented defaults bound the cache at 256 MiB, queued bytes at 64 MiB,
one batch at 32 MiB, groups at 4,096 and retained entries at 16,384 per group.
Directory scans stop at 4,096 entries. Configuration has finite hard caps.
These are storage limits, not qualified fleet capacity figures. The existing
HostServices domain token supplies task ownership; no second task registry is
introduced. Directory creation syncs every newly created ancestor and its parent.
Both initial engine queue headers and their directory entries are synced before
opening succeeds. Preallocation remains a hint so durable files do not acquire
zero-filled tails whose safe recovery depends on a later close truncation.

Initial group state uses an agreed empty-state snapshot at index/term 1, bound
to the immutable birth configuration. A newly elected leader must still commit
and apply an entry from its own term before business admission. Storage restart
must validate the node/disk identity, group-key mapping and every retained entry.
An existing claimed voter cannot use empty/missing engine state as a new store.

Source inspection also found that upstream Engine::write expects success from
the final shared-writer sync, and Engine::sync passes an empty batch that returns
without syncing. The integration must propagate shared-sync failure to every
writer and quarantine the engine. Close/purge fences must write a nonempty durable
metadata batch; they cannot credit the empty-batch helper as a flush proof.

Use direct path dependencies for the adapters. A workspace-only `[patch]` was
verified to disappear when Cellar is consumed by an external Cargo workspace,
selecting the unadapted upstream crates instead. `vendor/raft` retained every
upstream consensus source file byte-for-byte at MR-02 and changed only its
manifest's raft-proto path at that revision. MR-03 additionally introduces an
explicit host-seeded election RNG constructor; `vendor/raft/UPSTREAM.json`
records its exact source scope. Engine and protobuf paths remain effective for SDK
source consumers. Docker contexts include these sources, and benchmark freshness
hashes include their Rust, Cargo and protobuf inputs. Publishing adapted crates
to a package registry is a separate release operation; this milestone verifies
repository and external path builds.

The pure generator supplies `rustproto.proto` internally. The build script watches
only the checked-in protocol file; watching a missing local copy caused needless
regeneration on every Cargo invocation. Generated codec bytes are unchanged after
removing that watch, and a repeated locked check reuses all three Raft adapters.

The consensus worker's private storage view retains mechanical access to the
last durable cache during engine failure or cancellation. Public `GroupLog`
proof reads remain closed. No new persistence or application proof can use that
private view; the distinction prevents raft-rs from turning a normal storage
closure into a fatal lookup panic while the worker is winding down.

Upstream reference: [Raft Engine 0.4.2](https://docs.rs/raft-engine/0.4.2/raft_engine/).
