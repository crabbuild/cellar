# Guide 010: Integrate command/query contracts into an application

Status: proposed developer experience, not a currently runnable SDK tutorial.
Baseline: `0efd8f4d871199542f97f9fe0b74086a38606d02`, 2026-10-19.

The command/query builder, contexts, typed handles, versioned replies, read
policies and Axum route adapters shown below must be implemented by
[Plan 008](008-implementation-plan.md). Their names illustrate the intended
interface; do not paste them into today's SDK and expect compilation. RR-12
must turn this guide into a compiled and tested example before release.

The existing native application integration uses `Registry`, cell factories,
namespace stubs and `Host`/`Node`; see `examples/native-counter/main.rs`.
`cellar-axum::ApplicationBuilder` currently supplies `.cell(...)` and owner-routed
`.cell_route(...)`; see `crates/cellar-axum/builder.rs`. Those existing calls
do not classify operations as replica queries.

## 1. What the application integrates

Application builders add contracts and transport bindings to their existing
application, rather than implement node routing themselves.

| Application defines | Cellar provides |
| --- | --- |
| Cell identities and namespace bindings | Current owner and replica discovery |
| Commands, queries and custom request/result types | Operation decoding/dispatch validation |
| Business rules, schemas and transaction scope | Primary authority and storage capabilities |
| Query calculations and allowed freshness/fallback policy | Snapshot eligibility and replica selection |
| HTTP authentication, authorization and response mapping | Authenticated peer forwarding and lifetime handling |
| Workflow/idempotency/compensation rules | Existing durability gate, version receipts and typed failures |

The builder registers many operations across many classes. An ordinary application
handler, event consumer or workflow can call the same typed cell handles as an
HTTP adapter. No business callback receives a primary server address.

## 2. Organize application-owned code

One possible layout, not a required repository structure:

```text
application/
  accounts/
    requests.rs       command/query inputs and business outputs
    domain.rs         shared pure business rules
    commands.rs       primary callbacks and local transactions
    queries.rs        read-only snapshot callbacks
    contract.rs       operation registration and policy
  inventory/          another cell class and operation collection
  http.rs             public adapters and authorization
  workflows.rs        orchestration across cells
  main.rs             existing application registration and node lifecycle
```

Application modules may contribute registrations and share services through
normal Rust functions, closures and traits. Context capabilities still apply:
an injected helper does not grant a query mutable Cellar storage or command calls.
Native Rust remains trusted application code, not a sandbox.

## 3. Define request and response types

For example, declare Deposit, Withdraw, GetBalance and ListTransactions. A
Deposit payload might contain `command_id` and `amount_cents`; its result contains
`balance_cents`. GetBalance can have an empty payload and return `balance_cents`.
ListTransactions accepts an explicitly bounded page/filter and returns a bounded
collection. Each operation has its own associated response and error mapping.

Use owned JSON codecs for the first release. Contract registration binds types
to stable operation IDs and a compatible contract version. A command ID identifies
one business invocation; the registered Deposit operation ID identifies its
handler. Duplicate handler registration is refused, while repeated invocation
of one handler is normal application behavior.

For reused payload types, use explicit typed operation handles, such as
`deposit_operation` and `withdraw_operation`. Payload type alone cannot choose
between two registrations. Public HTTP clients use URLs and schemas; they do
not need Rust types or internal peer-routing headers.

## 4. Implement business callbacks

Command callbacks run only after Cellar has routed to the authoritative primary.
They can use the primary instance, storage and normal transaction facilities.
For example, a proposed callback has this shape:

```rust
fn deposit(
    self: Arc<Self>,
    context: CommandContext,
    request: Deposit,
) -> HandlerFuture<DepositReply> {
    Box::pin(async move {
        validate_deposit(&request)?;
        let storage = context.storage()?;
        storage.transaction_sync(|tx| {
            apply_deposit_with_receipt(tx, &request)
        })
    })
}
```

`validate_deposit` and `apply_deposit_with_receipt` are application functions.
The latter checks an existing command receipt/digest and updates the balance and
receipt in the same local transaction. Use checked arithmetic and reject command
ID reuse with another payload. Schema initialization remains in the authorized
primary lifecycle, not in a replica query. Returning from this callback does
not itself send success to the client: Cellar still applies its output gate.

Query callbacks receive no primary instance. They read one immutable snapshot
and can reuse pure calculation helpers. For example:

```rust
fn account_balance(
    read: ReadContext,
    _request: GetBalance,
) -> HandlerFuture<BalanceReply> {
    Box::pin(async move {
        let value = read.storage().get_json("balance_cents")?;
        decode_balance_reply(value)
    })
}
```

`decode_balance_reply` belongs to the application. Storage access here is the
proposed bounded read-only interface, not today's mutable Storage handle.
Multiple lookups within this callback share a generation. The callback cannot
register alarms, initialize missing tables, publish outbox work or call commands.

## 5. Register contracts and primary factories

Proposed Rust-like builder:

```rust
let accounts = CellContract::new::<Account>("Account")
    .factory(move |context, identity| {
        Account::create(context, identity, account_services.clone())
    })
    .command::<Deposit>("deposit", Account::deposit)
    .command::<Withdraw>("withdraw", Account::withdraw)
    .query::<GetBalance>("balance", account_balance, balance_policy)
    .query::<ListTransactions>("transactions", account_history, history_policy);

let application = Registry::new(ingress_handler)
    .contract(accounts)?
    .contract(inventory_contract)?;
```

The explicit factory preserves the existing application's initialization and
dependency injection. The contract validates that a primary factory is available
for declared commands before serving traffic. It is never called to execute
replica queries; replicas register the same operation definitions without creating
normal cell instances. Applications with existing factory registration may
reference that validated factory instead of supplying a second one.

Contracts attach to the existing native application executable and generation.
All eligible nodes must have the compatible compiled command/query definitions;
which callbacks actually run depends on each cell's ownership and read placement.
An application need not split into a primary binary and a query binary.

## 6. Connect public requests to typed dispatch

Keep existing HTTP authentication and authorization. The ingress adapter resolves
an authorized namespace/cell identity, decodes the operation payload, calls a
typed handle and maps the returned value/errors to HTTP.

Proposed handler body:

```rust
authorize_account(&principal, &account_id)?;
let account = context.namespace("ACCOUNTS")?.get_by_name(&account_id);

let reply = account.command(&context, deposit_request).await?;
return encode_command_http(reply); // includes business value and version
```

Namespace configuration must still match the declared class. Derive the effective
cell identity from the authenticated tenant/principal where appropriate; a client
supplied account name alone is not permission to access it. Use an authorization
source satisfying the endpoint's requirements, including current revocation
checks when required; do not infer current permission from stale replica state.

For a balance request, decode an optional minimum-version token and call:

```rust
let options = read_options_from_request(&request, balance_policy)?;
let reply = account.query(&context, GetBalance {}, options).await?;
return encode_query_http(reply); // value, actual version and source metadata
```

`encode_*`, `authorize_*` and `read_options_from_request` are application adapters,
not hidden framework business logic. They can be provided by shared application
libraries. The runtime validates tokens, operation kinds and policy ceilings.
External headers cannot grant internal query authority or change a command into
a query. A public version token constrains freshness; it is not authentication.

Applications can instead use thin command/query route bindings over the same
dispatcher to avoid repeating decoding boilerplate. A proposed Axum extension
would let the existing `ApplicationBuilder` attach a contract and bindings with
`(method/path, registered operation handle, authorized cell resolver, codec)`.
For example, bind POST `/accounts/{id}/deposits` to Deposit and GET
`/accounts/{id}/balance` to GetBalance. These adapters must preserve the existing
builder's completed-router authentication layers and native/Axum host parity.
Do not implement another owner-selection policy in the Axum crate. Existing
`.cell_route(...)` remains owner-routed until explicitly replaced by a query
binding. No framework-generated public route is enabled by registration alone.

## 7. Use write versions when fresh follow-up reads matter

Within one application operation:

```rust
let written = account.command(&context, deposit).await?;
let balance = account.query(
    &context,
    GetBalance {},
    ReadOptions::at_least(written.version),
).await?;
```

Across separate HTTP requests, return the opaque command version in the command
response. The client carries it in a documented request field/header on a later
read. A shared SDK/client helper can do this propagation, but no automatic
cross-request session state is assumed. An older snapshot cannot satisfy it;
the runtime waits, selects another eligible replica, follows explicit safe
fallback or returns a freshness error within the original deadline.

If the application accepts a historical balance, use the declared stale policy.
If several values must come from the same cell snapshot, compute them inside
one query callback. Across cells, compose individual operations and preserve
per-cell versions; application workflows handle partial success/compensation.

## 8. Start the application and configure serving

Preserve existing `Host`/embedded `Node` bootstrap, registration, listeners,
execution domains and returned shutdown checks. Contract dispatch is an SDK and
runtime extension, not a replacement deployment system.

Operators enable read replicas through policy: covered existing cells, desired
additional read-copy count, eligible node pools, failure-domain rules and resource
budgets. Cellar automatically selects/replaces replica nodes and initiates safe
recovery of failed primaries, even without an incoming application request.
Business handlers contain no D/E follower configuration or B/C addresses.
All eligible serving nodes run a compatible application/query
digest. Existing shared store, peer authentication and recovery prerequisites
still apply. Use feature-off deployment followed by the qualification/canary
procedure; this guide does not authorize deployment.

Illustrative policy intent is "keep two read replicas on distinct eligible nodes;
repair failures automatically," rather than "always put this account on B/C."
The repair loop reports insufficient capacity/history and retries; it cannot
manufacture missing acknowledged data or provision new machines. A primary
takeover still completes the existing fencing, predecessor recovery and restore.

In local single-node development, explicitly configure permitted owner-local
read fallback so the same query callback uses a safe local immutable snapshot.
After a primary command has created/proven the state, it can be queried without
another server. Querying a cell with no proven snapshot returns SnapshotUnavailable;
it does not silently create the cell. Feature-off calls do not gain replica
behavior until the new runtime is implemented and enabled.

## 9. Adopt gradually and test through the application

1. Keep existing fetch/RPC endpoints and current node startup.
2. Introduce one explicit command/query contract and a compiled integration example.
3. Register primary business callbacks and separate read callbacks.
4. Bind selected public endpoints to typed dispatch, retaining authorization.
5. Prove same-node behavior, command receipts, no query mutation and version flow.
6. Enable selected read replicas and verify actual serving nodes, failures and drain.
7. Add further commands/queries/classes through normal application code changes.

Required builder-level checks include distinct operations/types/policies,
unauthorized target refusal, no primary constructor for queries, native/Axum
dispatch parity, version-preserving query routes and explicit no-replica behavior.
RR-05/RR-07/RR-08 own these interfaces; RR-12 must replace the illustrative
snippets with an actual compiling example, run existing feature modes, and
publish the supported builder contract. Runtime routing and durability still
need the full qualification suite in [Qualification 009](009-qualification.md).
