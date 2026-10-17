# Cellar application building blocks

Reusable contracts, configuration and codecs for Cellar applications. This crate
works without starting a node or entering an execution domain.

| Module | Owns |
| --- | --- |
| `sql` | SQL request contracts, JSON request normalization and result budgets |
| `kv` | Read/write options, revision conditions and structured-value encoding |
| `blob` | Object metadata, conditions, ranges, checksums and write options |
| `queue` | Message codecs, consumer bindings, settlement contracts and budgets |
| `workflow` | Duration, retry and retention rules, bindings, definitions and result types |
| `cron` | Scheduled occurrence controls and execution-history queries |
| `limits` | Application resource budgets and validation |

```rust
use cellar_app::{
    blob::Range,
    limits::CellLimits,
    workflow::{BindingSettings, Duration},
};

let limits = CellLimits::default();
limits.validate()?;
let range = Range::from_header("bytes=0-1023")?;
assert_eq!(range.length, Some(1024));
assert_eq!(Duration::from("1.5 seconds").milliseconds()?, 1500.0);
BindingSettings::default().validate()?;
# Ok::<(), anyhow::Error>(())
```

`cellar` depends on this crate. It retains `Context`, `Cell`, `Registry`, binding
handles and service-cell implementations, including all ownership, transaction,
I/O and durability checks. Existing `cellar::sdk` and `cellar::application` imports
continue to work through re-exports.

Keep code that operates on owned values here. Pass limits and timestamps
explicitly; obtain them from the execution domain in the host adapter.
`cellar-native` supplies value primitives and `cellar-logic` supplies policy.

From the repository root, with the target directory from [AGENTS.md](../../AGENTS.md):

```sh
cargo test -p cellar-app --locked
cargo clippy -p cellar-app --all-targets --locked -- -D warnings
```
