<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Python filesystem cancellation, 2026-10-28

Both CPython 3.14.5 ABIs pass the selected cancellation and native-proof matrix
after preserving filesystem progress on `asyncio.CancelledError`. This is SDK
correctness and lifecycle evidence; production deployment qualification remains
open in the [release ledger](../../../plans/per-cell-filesystem-implementation-status.md).

The [observations](2026-10-28-filesystem-python-cancellation.json) record source,
executable and log hashes, per-case receipts and proof/shutdown summaries. Raw
logs, full reports and frozen executables are retained in the checkout-specific
workspace target's `qualification/python-task-cancellation-qualified/` directory.

## Defect and correction

Cancelling `write_all` before its second transfer lost `code`, `outcome`,
`operation` and `completed_bytes`, despite a confirmed four-byte native write.
The duplicate-shared offset advanced from three to seven and native readback
confirmed the write. The SDK caught `Exception`; Python cancellation derives
from `BaseException`.

Filesystem calls and looping helpers now annotate and re-raise the original
cancellation. The exception retains its type/message and the task stays cancelled.
Its fields report `Cancelled`, `unknown`, the public operation and the confirmed
prefix. Cancellation does not establish rollback or cancel native work by itself.
The parent invocation retains its descriptor authority. A task cancelled before
its coroutine starts cannot execute SDK annotation code.

## Captured profile and results

macOS arm64, Rust 1.98.1, pinned CPython 3.14.5 GIL and free-threaded runtimes;
development Cargo profile with debug information disabled. Each ABI has separate
frozen executables and verified libpython linkage. Captured source hashes match
the files used throughout the completed run.

| Boundary | Per ABI | Observed acceptance |
| --- | --- | --- |
| Python library | 31 tests | All pass |
| Filesystem modes | Normal, read-only, small-budget, task-cancellation | All pass |
| Task cancellation | 12 cases plus 12 local-reopen checks | Exact native bytes/version, confirmed progress, offsets, cookies, host admission and recovered receipts |
| Filesystem proof | 16 cases | fsync/fdatasync/sync/output × deadline/fence × Preserve/Handoff |
| Failed transports before fence | 1 case | Both real proof transports fail after local commit, followed by actual lease mismatch and cold recovery |
| Shared-gate regressions | 7 cases | Four storage deadline/fence cases, proof failure/recovery and queue lost acknowledgement |

The coroutine fixture uses a four-byte transfer ceiling and four host descriptor
slots. It holds coroutine delivery at SDK boundaries around real native calls;
it does not simulate SQLite work. Helpers stop before the first/second transfer,
after the second native transfer, or during capability delivery. Direct byte
calls stop after native processing. All cases verify the actual file state and
save a checked, proven reconciliation receipt without repeating the write.

All 34 filesystem proof cases across both ABIs validate six joined native
outcomes apiece. The exact forced fence is the sole permitted incomplete native
outcome. Interpreter/bridge work settles, stale wrappers refuse calls, four host
slots can be reused, and new-directory cold recovery matches warm readback.
Handoff successors are ready before predecessor shutdown. A new same-inode write
then proves usable successor authority.

## Failed observations retained

The first refreshed run exposed an overly narrow proof-fixture assertion that
required cancellation even when a native Storage/EIO/unknown reply could win.
The fixture now distinguishes the two exact allowed settlements and rejects
unexpected errors or success. A focused probe captured Storage/EIO/unknown;
the final transport-failure runs captured CancelledError/Cancelled/unknown.
Requiring the former on every run was itself a race-sensitive expectation.
The final test preserves the original handler/proof/lease budgets and all cold
recovery, stale-authority and cleanup assertions.

The original cancellation reproduction's raw artifact was lost when the ignored
workspace `.cellar/` directory was removed outside this task. Its observed fields
and executable hash remain recorded. Later failing proof attempts and the final
passing run are retained separately outside that directory.

These cases do not establish physical IPC loss, external-store/network/disk
failure behavior, Linux CI, optimized capacity or a latency/memory guarantee.
The task-cancellation restart reopens local storage; empty-local-state recovery
is asserted separately by the native proof cases.
