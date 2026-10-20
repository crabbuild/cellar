# MR-08 read-contract audit

Status: IN_PROGRESS. The [local-read evidence](evidence/MR-08-local-progress.json)
qualifies the preceding local implementation. Network member routing, fallback,
independent LTX catch-up and query-code-independent forwarding have passing
component tests; [network evidence](evidence/MR-08-network-progress.json) pins
source, executable digests, commands and qualification limits. The
[HTTP/context evidence](evidence/MR-08-http-progress.json) adds live ordinary
capability capture and actual native/Axum consistency/token requests. They do
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

Two additional [peer histories](evidence/MR-08-peer-progress.json) exercise
saturation and delayed network replies.
One holds a real remote callback while 64 further requests compete for bounded
peer admission: both overload refusal and queued expiry must occur, expired
requests never execute after release, and a concurrent command still commits.
The held query retains its older pinned image; a subsequent minimum-version
query covers the new write after admission capacity is reclaimed.

The other warms an existing TLS query connection and holds its returning
ciphertext in a bounded proxy buffer after the destination callback completes.
The original invocation expires while a newer command commits. Releasing the
proxy cannot let that delayed response satisfy a later invocation; a fresh
exchange must return the newer image and exact serving member. These component
histories do not establish arbitrary-node routing or the full release failure
matrix.

## Query origin identity

The [origin evidence](evidence/MR-08-origin-progress.json) pins this boundary's
56 owning cases, 13 Raft unit cases and executable drill. Queries use a dedicated
route representation. The source carries a typed
NodeIdentity (node/disk/process) and label, without a voting or learner member
ID; the target retains its full replica/history/schema identity. A response
reverses the exact outstanding route, and that orientation cannot decode as a
new request. Unit checks reject injected origin members, missing/noncanonical
target members, zero physical identities and replies bound to another origin.
The query route cannot deserialize as the member-bearing consensus route.

This separates identity encoding from voting without enabling unapproved
origins. The current destination maps the full physical identity and label to
a configured group member, then checks that member and the destination against
the current configuration before body admission and around execution.
Consensus-message authorization is unchanged. Shared pools now own a physical
RaftPeerNode endpoint without retaining the first group's member identity.
Independent namespace grants, installation of pools for authorized nonmembers
and routing-only bindings still need implementation; no
local voter may be invented to supply a query identity. Experimental query
peers must upgrade together for this marker-1 hard cutover. There is no old
query reader or translation fallback.

## Live capability and HTTP checks

`ordinary_context::active_ordinary_context_and_storage_are_refused` holds an
ordinary cell invocation open on the same node/domain while a Raft query uses
its captured Context and Storage. Context storage/namespace/background-work
entry points and Storage read/write/SQL/alarm calls are refused. The original
invocation remains active throughout the query and resumes with its state
unchanged; retirement is not the refusal oracle.

The counter's native and Axum adapters normalize explicit consistency, minimum
token, stale-read policy and bounded member attempts through the same path.
Successful responses expose their SDK-issued token and actual serving member.
The HTTP parity case covers all three modes, local/member routing, duplicate or
mismatched options, invalid bounds and signature/contract refusal without
consistency downgrade. The executable drill runs both HTTP paths on all three
nodes before and after joined restart. Minimum-version and linearizable reads
select another replica without fallback, report that member, and observe
the required committed value. Pre-restart tokens remain usable; another cell's
token is refused even with fallback enabled.

## Remaining audit and delivery work

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
