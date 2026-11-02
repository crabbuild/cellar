<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Cloudflare counter lab

A native Rust Cellar host with SQLite counter state, R2 bucket durability and a
browser latency workbench. The Worker serves the frontend and forwards counter
requests through one Durable Object to one Cloudflare Container. SQLite runs on
the container's local disk; Cellar publishes its durable state to R2.

The configured `standard-1` instance has 0.5 vCPU, 4 GiB RAM and 8 GB disk.
`max_instances: 1` and a fixed Durable Object name bound this example to one
container. It sleeps after ten minutes of inactivity.
The inactivity timeout is configured during readiness; warm requests do not
repeat a container control RPC. Cloudflare's ingress requires the Worker and
Durable Object hops. Cellar itself can expose HTTP directly when deployed on a
host with public ingress. This is a bucket-backed single-node example,
not a three-node multi-Raft benchmark or production
qualification. See Cloudflare's [instance types](https://developers.cloudflare.com/containers/platform-details/instance-types/)
and [container lifecycle API](https://developers.cloudflare.com/containers/api/durable-object-container/).

## Code and correctness

`main.rs` registers a native SDK application. Named cells `demo` and `check`
each own SQLite tables for the counter and client receipts. An action updates
the value, version and receipt in one transaction. `storage.sync().await` waits
for the configured bucket durability proof before returning a successful reply.
Read handlers use the normal framework response gate, which still checks
authority and any unproven state they observed. They do not call explicit
`sync()`: that API requests a write barrier even when no new write occurred.
`entrypoint.sh` explicitly selects bucket durability and supplies the R2 endpoint.

The Worker forwards a mutation once. A failed response can follow commitment;
the UI retains its client identity, sequence and action in per-tab session
storage and offers an explicit retry. Repeating the latest identical action
returns its stored receipt. Conflicting or stale sequences return 409. Every
subsequent action must use the next sequence. Reset retains the receipts.

Each cell accepts at most 256 client identities and retains their sequence
floors permanently. A new identity after this limit receives 429; this keeps
the public example bounded without making an old mutation executable again.
Use a separate deployment/bucket for another test campaign. The API is a shared
public demonstration: it does not provide user authentication or tenant isolation.

## Deploy

Prerequisites: Docker with an explicit working context, Python 3, Node.js and a
Cloudflare account enabled for Workers, Durable Objects, Containers and R2.
The Dockerfile cross-compiles the locked Rust workspace for Linux amd64 and
runs the binary as an unprivileged user. No local Rust installation is needed
for the image build.

Create a private credentials file, such as `~/.cf.env`, with these names:

```sh
CF_ACCOUNT_ID=YOUR_ACCOUNT_ID
CF_API_TOKEN=YOUR_CLOUDFLARE_API_TOKEN
CF_ACCESS_KEY_ID=YOUR_R2_ACCESS_KEY_ID
CF_SECRET_ACCESS_KEY=YOUR_R2_SECRET_ACCESS_KEY
CF_S3_API_ENDPOINT=https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
```

The API token needs the permissions required by Wrangler to deploy this Worker,
its Durable Object and Container, plus R2 bucket creation/listing. The S3 keys
need read/write access to the selected bucket. Keep this file out of source.
The deployment helper reads only the five named settings and uploads the R2
connection values through Worker secrets, without printing their values.

Before deploying a separate copy, edit `wrangler.jsonc`: choose a unique Worker
name and bucket name, and use that same bucket in `r2_buckets` and `R2_BUCKET`.
The supplied names are templates for your deployment. From the repository root:

```sh
npm ci --prefix examples/cloudflare-counter
python3 examples/cloudflare-counter/deploy.py \
  --credentials ~/.cf.env --docker-context YOUR_DOCKER_CONTEXT
python3 examples/cloudflare-counter/verify.py https://YOUR_WORKER.YOUR_SUBDOMAIN.workers.dev
# Also enforce the warm native handler and complete dispatch budgets:
python3 examples/cloudflare-counter/verify.py https://YOUR_WORKER.YOUR_SUBDOMAIN.workers.dev \
  --max-read-cell-ms 10 --max-read-dispatch-ms 10
```

`--max-read-dispatch-ms` bounds the full native dispatch, including admission and
response proof. Warm requests reaching a published local activation reuse its
retirement admission without an R2 lookup. Cold requests, activation and restore
still check storage. Erasure permanently closes local business admission before
its intent is proven and the external marker is published; recovery preserves
that closure. See the [erasure admission contract](../../crates/cellar-container/allocations.md).
Neither native budget includes the browser, network or Cloudflare proxy hops.
See the [latency qualification](../../docs/design/cloudflare-counter-latency.md)
for measured gains and the remaining end-to-end limit.

The helper records the current Git HEAD as the source revision, creates the
selected bucket if absent, installs secrets, then deploys the image and assets.
Source revision identifies the base commit; local source edits are included
in the image.
The helper does not fetch or merge Git changes automatically.

`verify.py` uses the separate `check` cell. It verifies a write, four concurrent
identical retries, conflicting and stale sequence refusals, a compensating write,
readback and a durability timing header. On success it restores the original
counter value and prints a JSON pass record. Run one verifier at a time: another
writer to `check` can invalidate its expected version/value comparisons.

## Use the page

Open the printed URL. Add/subtract one, apply a signed custom amount, reset or
read the count. “Measure 10 reads” samples reads sequentially with a 200 ms pause;
it is an interactive latency sampler, not a TPS saturation load generator.
“Probe R2” performs a real PUT, validated GET and DELETE through the Worker's R2
binding. That probe uses a different path from Cellar's S3 client.

The latest trace and request journal expose the following measurements:

| Measurement | Boundary |
| --- | --- |
| Browser fetch / first byte | Fetch start to body completion / response headers |
| Cloudflare edge | Worker API dispatch through its response |
| Container readiness | Durable Object startup/health wait, with a cold-start flag |
| Container request | Port fetch through full native response body |
| Cellar dispatch | Native dispatch through named-cell reply |
| SQLite work | Handler schema, query and transaction work |
| R2 durability gate | Explicit mutation sync: capture, queueing, publication and output-proof wait |
| DNS, TCP, TLS, download | Browser Resource Timing, where available |
| JSON decode / UI frame | Browser parsing / estimated rendering opportunity |
| R2 PUT, GET, DELETE probe | Separate Worker R2 operations, including GET body readback |

These intervals overlap; do not add them. The durability gate is not bare R2
network latency. Zero can indicate a reused connection; a dash means unavailable.
Reads omit the explicit durability-sync measurement. Their normal response
gate occurs after the handler returns and is included in Cellar dispatch time.
Background tabs do not invent a UI-frame observation. Failed requests remain in
the journal and failure count but are excluded from latency percentiles.

Filter reads/writes/probes and cold starts, select a stage for nearest-rank
p50/p95/p99, inspect a request's identity, or export JSON. The page retains the
last 500 requests in memory in the current tab. Small samples are exploratory
observations and cannot establish production tail latency or capacity.

## Validate and clean up

```sh
npm test --prefix examples/cloudflare-counter
node --check examples/cloudflare-counter/worker.js
node --check examples/cloudflare-counter/public/app.mjs
```

For native compilation and Clippy, select a checkout-specific target directory
as required by the repository's `AGENTS.md`:

```sh
cargo check --locked -p cellar --bin cellar-cloudflare-counter
cargo clippy --locked -p cellar --bin cellar-cloudflare-counter -- -D warnings
```

Deleting browser measurements does not reset durable state. Reset changes the
count but retains request receipts. To retire your deployment, delete its Worker
and Container application with Wrangler or the Cloudflare dashboard. Retain the
R2 bucket while any state may be needed; deleting it destroys the durable state.
Deleting a Worker alone is not a promise that all associated resources and
charges have been removed.
