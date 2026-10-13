# Configuration and policy reference

Configuration is captured per node. Embedded hosts use explicit Rust policy
objects; CLI adapters resolve environment values at startup. Do not assume
library defaults and CLI defaults are the same.

![Configured host boundaries](diagrams/architecture.svg)

## Library defaults at this baseline

| Policy / field | Default | Meaning |
| --- | --- | --- |
| `RuntimePolicy.max_stateless_workers` | 4 | Workers per script pool |
| `max_stateless_requests` | `Some(64)` | Admitted events per script pool; background work retains slots |
| `max_cells_per_worker` | 32 | Residents sharing one worker |
| `max_cell_requests` | 64 | Concurrent fetch/RPC admission per cell |
| `admission_wait` | 1 s | Saturated stateless pool wait; zero rejects immediately |
| `handler_budget` | 300 s | Handler response budget; excludes subsequent background/proof settlement |
| `HttpPolicy.request_timeout` | 120 s | SDK HTTP deadline, including response-body consumption |
| `NodeServices.http_client` | `None` | Default HTTP builder ignores ambient proxies; supply a factory for host transport settings |
| `LoaderPolicy.max_workers` | 256 | Loaded-worker limit per host |
| `max_workers_per_principal` | 255 | Shared script/generation principal limit |
| `max_environment_bytes` / `max_props_bytes` | 1 MiB each | Encoded values; environment includes service route metadata |
| `ShutdownPolicy.total_ms` | 40,000 | Absolute semantic stop budget |
| `token_wait_ms` / `no_progress_ms` | 30,000 / 25,000 | Token wait and stall window within the absolute budget |

These are source defaults, not recommended production sizing. The structured
decoder caps encoded values at 64 MiB, and loader budgets must not exceed it.
Choose lower business-specific bounds where appropriate.

## Common CLI environment adapters

| Variable | Adapter / effect |
| --- | --- |
| `CELLAR_MAX_REQUESTS` | Stateless admission limit; **unset is unbounded in the CLI** |
| `CELLAR_MAX_STATELESS_ISOLATES` | Stateless worker count; CLI defaults to available parallelism |
| `CELLAR_MAX_CELL_REQUESTS` | Per-cell fetch/RPC admission |
| `CELLAR_ADMISSION_WAIT_MS` / `CELLAR_HANDLER_BUDGET_S` | Admission wait / response budget |
| `CELLAR_FETCH_TIMEOUT_S` | CLI captures SDK HTTP request timeout at startup |
| `CELLAR_MAX_RESIDENT_CELLS` | Actor resident-cell limit |
| `CELLAR_MAX_CACHED_ROUTES` | Idle unowned route history; CLI default 2,048, zero disables |
| `CELLAR_LOADER_MAX_WORKERS` | Host loaded-worker limit |
| `CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL` | Principal quota; CLI default derived from host limit |
| `CELLAR_LOADER_MAX_ENV_BYTES` / `CELLAR_LOADER_MAX_PROPS_BYTES` | Copied environment / properties budgets |
| `CELLAR_SHUTDOWN_TOTAL_MS` | CLI stop budget; subsidiary drain windows are derived |
| `CELLAR_OTEL` | Unset/0 disabled; 1 bucket; collector base URL selects OTLP |

Reject obsolete settings rather than assuming they are aliases.
`CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES`, old loader aliases and attempts to
disable the output gate are unsupported. See the
[environment parser](../crates/cellar/cli/environment.rs) for the complete
validated registry and removal messages.

## Fleet and infrastructure policy

`NodeConfig` also captures request-body limit, forwarded-header trust,
generation maximum age, startup recovery deadlines and maintenance cadence.
Public and internal listeners must differ. Local nodes require sticky eviction
ownership to retain their sole copy. Startup recovery backoff must cover the
lease TTL. Use the configuration validator rather than guessing combinations.

HTTP deadlines must be positive and at most `u32::MAX` seconds. The optional
`HttpClientFactory` selects DNS, TLS, credentials, proxies and pool settings.
Cellar applies required redirect policies and worker subrequest budgets.
Ordinary redirect modes reuse host pools; a redirect-limited invocation owns
its accounting client. The CLI explicitly selects reqwest's system-proxy
behavior, while library defaults ignore ambient proxy settings.

`ReplicationOptions` controls upload/compaction, restoration/hydration and
durability timeout. `LogOptions` controls pipeline, follower windows, streams,
hedging, eviction and publication timeout. Tune from measurements of the actual
store and disks. Increasing concurrency can increase contention and pending
memory; it does not establish additional authority.

Record a redacted effective configuration, source SHA, executable digest,
target, limits and infrastructure identity with each release and benchmark.
Credentials belong in host credential providers/secret infrastructure, not
application manifests committed to source. Changes to resource limits require
a representative load run and saturation test. Sources:
[runtime](../crates/cellar/runtime/policy.rs),
[loader](../crates/cellar/runtime/workers/loader_policy.rs),
[HTTP](../crates/cellar/transport/http/policy.rs),
[Node config](../crates/cellar/node/embedded.rs),
[shutdown](../crates/cellar/node/lifecycle.rs).
