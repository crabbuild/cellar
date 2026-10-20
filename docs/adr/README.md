# Architecture decision records

These records document constraints observed in the reviewed framework baseline.
Use them to understand why native execution, cell authority and explicit host
lifetimes constrain implementation changes. Each record preserves its original
source revision and date; deployment approval belongs in the release record.

| Record | Status | Topic |
| --- | --- | --- |
| [001](001-native-applications.md) | Observed baseline | Compiled native application factories |
| [002](002-cell-authority-and-proof.md) | Observed baseline | Cell authority, epochs and output proof |
| [003](003-explicit-host-lifetimes.md) | Observed baseline | Explicit host policy and returned lifecycle outcomes |
| [004](004-embedded-python.md) | Accepted; qualification open | Invocation-scoped CPython above the native SDK |

For new decisions, assign the next number and record context, alternatives,
decision, consequences, owner, date, compatibility and validation evidence.
Changing cell identity, transaction boundaries, durability posture, isolation,
store assumptions or rollback policy deserves a record. Keep superseded
decisions and link their replacement. See [team delivery](../20-team-delivery.md).
