<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Native filesystem release CI, 2026-10-28

The focused native notebook passes on all three supported release targets in
[SDK run 40686315091](https://github.com/crabbuild/cellar/actions/runs/40686315091).
The PR head is `0e986468da6b3ed3229246a24fc245460cd48bcb`. Actions checked out
merge commit `a11a0dfb058379b6445117724cb85b83bce161f1`, whose parents are that
head and main `5998b82cac1b14dbf05b8308b4161144c48252bf`. Both head and merge
have tree `c77471a114ce1f274f75a77cd6da020643bd4556`; this equality was verified
from the fetched Git objects.

| Target | Completed job | Retained artifact |
| --- | --- | --- |
| Linux x86_64 | [122809906930](https://github.com/crabbuild/cellar/actions/runs/40686315091/job/122809906930) | [12815264654](https://github.com/crabbuild/cellar/actions/runs/40686315091/artifacts/12815264654) |
| Linux arm64 | [122809907388](https://github.com/crabbuild/cellar/actions/runs/40686315091/job/122809907388) | [12817611209](https://github.com/crabbuild/cellar/actions/runs/40686315091/artifacts/12817611209) |
| macOS arm64 | [122809907515](https://github.com/crabbuild/cellar/actions/runs/40686315091/job/122809907515) | [12817070744](https://github.com/crabbuild/cellar/actions/runs/40686315091/artifacts/12817070744) |

All three jobs build the release CLI and application with Rust 1.98.1, run the
focused filesystem smoke with the application explicitly in release mode, and
verify that both executable hashes remain unchanged through restart. Downloaded
archives match the GitHub artifact SHA-256 digests. Their source, target,
executable hashes, toolchain and smoke logs are verified and recorded in the
[structured evidence](2026-10-28-filesystem-release-ci.json). Local copies live
under the checkout-specific target's `qualification/native-release-ci/`.

The assertions cover bounded input, concurrent appends and idempotent replay,
conflicting IDs, isolation, retained file/state/receipts after a graceful
preserving restart, and a subsequent append. Storage remains the local
development profile. These results close the selected native release-example
target matrix; they do not establish optimized guest/ABI packages, external-store
fault behavior, capacity or deployment recovery objectives. The earlier local
Linux compilation OOM in the [native-example report](2026-10-28-filesystem-native-example.md)
remains an independent observation on the 2 GiB development VM.

## Outstanding CI failure

The separate [JavaScript qualification job](https://github.com/crabbuild/cellar/actions/runs/40686315126/job/122809451754)
fails before its filesystem steps. The provider-journal integration test
`provider_journals_survive_recovery_and_business_clear_without_replaying_creates`
returns HTTP 500 on `superseded`: `handler exceeded 250ms budget`. Its shared
native fixture sets that budget; the route creates two journal revisions and
awaits their durability gates before checking supersession.

The unchanged exact test passes twelve fresh serial local macOS runs, each
including all three preserving restarts, in 0.526–0.566 seconds per whole test.
This does not reproduce or resolve the Linux failure. No handler budget or
correctness assertion was changed. The diagnostic capture and local executable
identity are retained in the structured record. Full CI qualification remains
open independently of the three successful native release jobs.
