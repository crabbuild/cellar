# Raft Engine host integration

Status: MR-02 integration requirement, established by source inspection of the
published Apache-2.0 `raft-engine` 0.4.2 crate. This is not persistence qualification.

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

MR-02 will retain the actual upstream engine and its log format. A narrow,
attributed dependency patch must expose the missing host operations. It must not
replace Raft storage with MemStorage or silently accept partial filesystem
injection. The patch and the host adapter require fault tests for directory sync,
directory loss, lock exclusion and file sync errors before the task can be DONE.
After any write/sync failure, close the affected shared engine's admission rather
than allowing its in-memory state to establish another durability proof.

The host filesystem also needs explicit random write, truncate, read/write open
and lock-guard capabilities. Unsupported caller-supplied implementations must
return an explicit unsupported error; none may fall back to the direct backend.
Existing Fleet filesystem operations retain their behavior.

Upstream reference: [Raft Engine 0.4.2](https://docs.rs/raft-engine/0.4.2/raft_engine/).
