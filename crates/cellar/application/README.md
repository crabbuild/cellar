# Application building blocks

Applications use `cellar::sdk` for cell handlers, bindings and configuration.
The implementation lives here, alongside the runtime adapters that execute it.

| Component | Provides |
| --- | --- |
| `sql/` | Database bindings, prepared statements and request normalization |
| `kv/` | Namespace bindings, options, validation and structured-value encoding |
| `blob/` | Object storage, metadata, conditions, ranges and checksums |
| `queue/` | Producers, delivery settlement, message codecs and broker configuration |
| `storage/` | Cell state, transactions, outbox emission and consumer inbox receipts |
| `event_stream/` | Durable event history, replay and resumable subscriptions |
| `outbox.rs` | Internal dispatch of committed events to their bound streams |
| `streams.rs` | Byte pipes, readers and writers for HTTP and I/O |
| `workflow/` | Durable steps, retries, checkpointed byte outputs and progress subscriptions |
| `cron/` | Scheduled occurrences and execution history |
| `limits.rs` | Resource budgets, validation and execution-domain lookup |

Use the SDK facade for application code:

```rust
use cellar::sdk::{
    blob::Range,
    workflow::{BindingSettings, Duration},
    CellLimits,
};

fn main() -> anyhow::Result<()> {
    CellLimits::default().validate()?;
    let range = Range::from_header("bytes=0-1023")?;
    assert_eq!(range.length, Some(1024));
    assert_eq!(Duration::from("1.5 seconds").milliseconds()?, 1500.0);
    BindingSettings::default().validate()?;
    Ok(())
}
```

For durable event delivery:

1. Commit business state and `tx.emit(&stream, event)` in one cell transaction.
2. The host dispatches the outbox to its bound stream asynchronously.
3. A named subscription leases a batch. Commit inbox receipts with local effects,
   call `Storage::sync`, then acknowledge the batch to advance its checkpoint.

Byte pipes in `streams` carry bytes; durable event streams in `event_stream`
retain records for replay and independent subscriptions. See the
[event delivery guide](../../../docs/26-event-delivery.md) and
[runnable example](../../../examples/durable-streams/README.md) for publication,
consumption, retries and their durability boundaries.

Within each component, private modules separate values, codecs and validation
from runtime operations. Pass clocks and budgets explicitly to value helpers;
resolve execution-domain services in adapters. Binding operations require a
valid `Context` and retain the host's ownership, transaction and durability checks.

See the [API reference](../../../docs/22-api-reference.md) for bindings and the
[examples](../../../examples/README.md) for complete applications.
