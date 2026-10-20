# MR-08 read-contract audit

Status: IN_PROGRESS. The [local-read evidence](evidence/MR-08-local-progress.json)
qualifies the preceding local implementation. Network member routing, fallback,
independent LTX catch-up and query-code-independent forwarding have passing
component tests; final source-pinned qualification is being recorded. They do
not close MR-08 or the complete delivery goal.

## Read proof and image ownership

Each local query retains one committed immutable SQLite generation through its
callback. The SQL owner verifies the durable engine marker against that exact
pin before capturing its served position. Protocol-only entries may cover the
same image; a marker naming another image cannot relabel it. Minimum-version
and ReadIndex comparisons use the captured cut. The spanning-query test checks
the callback-observed and returned positions while another replica commits a
newer value.

AtLeast accepts an opaque authenticated token. Scope, application digest,
index/term and expiry are verified before comparison. Ordinary elections preserve
the history; incarnation changes reject comparisons. Ed25519 verification keys
come from explicit server configuration. A conditional public registry checks
retention across rotation/restart and cannot supply an otherwise untrusted key.

Linearizable reads require a fresh safe ReadIndex for the invocation. Status,
commit watermarks, source observations and leader hints cannot complete it.
Role/term changes can drop a RawNode read, so resubmission retains the invocation
identity and original deadline. Sequenced status/submission replies cannot
regress a query that has already acquired its barrier or begun execution.

## Query RPC boundary under qualification

Query RPCs have their own listener, peer connection, queues and byte budgets.
A query waiting for LTX/application progress cannot occupy the control/data
connection it needs. Marker-1 query framing, MAC purposes and TLS exporter labels
are distinct from Raft messages. Replies bind the exact request tag, sequence,
reversed route and payload digest. Tokens are independently verified at receipt;
the serving member is derived from the authenticated target identity.

Headers and payload lengths are bounded before allocation. Authorization checks
the full group/history/schema and physical node/member/disk/process routing view
before reading a body and again around execution. Query frames cannot initialize
a group, vote, propose a command or alter membership. The destination forces local
query execution, so a routed request cannot recursively route to another server.

Candidate lists are bounded to eight distinct members with one optional untried
leader hint. Every attempt keeps the same contract, input, token, consistency and
stale-read policy. The remaining deadline is computed after queueing/handshake;
the origin independently retains the invocation deadline. Cancellation closes
the query connection, drops the destination SDK waiter and joins its SQL work.
SQL interruption observes a node-owned atomic flag. Completed responses retain
byte admission until consumed or dropped.

## Remaining audit and delivery work

- Complete the RPC boundary audit beyond the passing cancellation, independent
  LTX progress and same-consistency fallback histories, including saturation,
  queued deadlines and delayed replies against real TCP faults.
- Exercise captured ordinary Context/Storage while the ordinary invocation is
  still active. Context entry checks and Storage activation checks currently
  require the installed ordinary IoContext; SQL workers install only their
  execution domain. Static inspection is not the complete observable test.
- Integrate tokens and consistency/routing options into native/Axum counter HTTP
  and run the external process drill through those paths.
- Audit directory/ingress routing outside a group's voting/read membership. The
  current RPC origin uses a registered local replica; this does not establish
  arbitrary-node routing for the complete placement/directory architecture.
  A general query origin needs its authenticated node/disk/process identity and
  namespace authorization independently of voting identity. Query-only routes
  must not initialize a local voter or relax consensus-message membership
  checks. Destination scope, code, permission and read-proof validation remain
  mandatory; discovery metadata cannot grant mutation authority.
- Record final source-pinned evidence and requirements coverage before closing
  MR-08. Presence of the seven named cases alone does not prove every contract.

Native Rust callbacks remain trusted and cooperative; the framework is not an
OS sandbox. These local/loopback fixtures do not establish physical power-loss,
zone, production capacity or the remaining S3/archive/recovery/scaling contracts.
