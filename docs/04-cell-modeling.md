# Model cells and application boundaries

Choose cell boundaries around business rules that need atomic updates. Use
stable identities for those cells, plan for hot keys and define a protocol for
work that crosses cell boundaries.

![Cell and transaction boundaries](diagrams/cell-boundaries.svg)

## Start from invariants

Write the business rule before choosing the partition: “available units never
go below zero,” “a command ID produces at most one debit,” or “a room's sequence
numbers increase.” Put the data needed to enforce one rule into the same cell
when practical. A cell is a durable authority boundary, not a process that must
stay resident forever. Reconstruct transient state from SQLite after activation.

| Domain | Candidate cell | Watch for |
| --- | --- | --- |
| Project collaboration | Tenant/project | Large rooms and fanout skew |
| Inventory | Tenant/SKU/location | A single popular item bottleneck |
| Account state | Tenant/account | Transfers crossing account cells |
| Device ingestion | Tenant/device or measured bucket | Global ordering versus throughput |
| Long business process | Workflow instance | External step retries and version compatibility |

Prefer stable business identifiers over addresses, node IDs, process IDs or
release numbers. Use a canonical naming convention, for example a versioned,
unambiguous encoding of tenant and entity IDs; escaping or length-prefixing
prevents separator collisions. Persist and test the convention. Changing a
class, scope or namespace can change which durable state is addressed.

Derive tenant identity from authenticated server-side authority before building
the cell name. A tenant prefix is useful routing structure; it is not an
authorization check. Test identical entity IDs in different tenants and
attempts to reach another tenant by supplying names directly.

## Concurrency and distribution

Cellar admits concurrent cell fetch/RPC calls according to runtime policy and
uses input/transaction gates to protect relevant execution boundaries. Do not
assume an arbitrary handler's entire body is an uninterrupted serial section:
an await can allow other work. Use transactions or an explicit critical section
for a read-modify-write invariant, and keep network waits outside a synchronous
transaction.

Multiple independent cells can be owned by different nodes. One hot cell still
has one authoritative owner. Split independent keys, bucket append workloads,
or move a read projection outward when the business model permits it. Sharding
changes which invariants remain local and which require coordination; record
that tradeoff in an ADR rather than silently changing a key.

## Cross-cell business transactions

Cellar has no transaction spanning arbitrary cells and no global event order.
For an order/reservation/payment flow, use a workflow or saga: persist an intent,
call each participant with a stable command ID, record the result, then complete
or compensate. Every participant must recognize a replay. A timeout means
“query status or retry the same ID,” not “the participant certainly did nothing.”

For search and reporting, build projections from explicitly published events.
Use an application change feed or the [framework outbox](26-event-delivery.md);
the framework does not automatically publish every business mutation. Define
projection lag, rebuild procedure, retention and source-of-truth behavior.

Before implementation, complete the [application brief](templates/application-brief.md):
identity algorithm, invariant, command/result schema, migration approach, hot
key estimate, state growth and tenant boundary. Exit with a test that exercises
the invariant under concurrency and retries. Next:
[storage](06-storage-transactions.md), [routing](07-routing-rpc.md),
[asynchronous work](08-asynchronous-work.md).
