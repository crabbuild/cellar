# Tenant isolation and idempotent accounts

Authenticate before routing, isolate tenant state and deduplicate account operations.

## How it works

A bearer token resolves to an allowed tenant before business identity routing. The cell
name includes tenant and account. A transaction commits balance and an idempotency
receipt atomically; conflicting reuse and insufficient funds return 409 without changing
balance. local-acme/local-beta are public local teaching tokens: replace TENANT_TOKENS
with a real claims resolver before deployment, and add receipt retention, audit and
quotas.

Read the [Rust source](main.rs) and [project configuration](project.json). The
handler is a module compiled into `cellar-native-examples` by
[main.rs](../main.rs), rather than a standalone `rustc` input.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, the workspace's native C/C++ build tools,
and curl. Run commands from the repository root. Follow [AGENTS.md](../../AGENTS.md)
when selecting a target directory for this checkout. The local walkthrough needs no
external service or cloud credential.

In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/multi-tenant/project.json --no-watch --clean
```

Wait for the `ready` URL before sending requests. The first run compiles the native
executable. `--clean` resets only this project's local tutorial state; omit it when
restarting to test persistence.

In terminal 2:

```sh
curl -sS -w '\n' -X POST -H 'Authorization: Bearer local-acme' \
  -H 'Content-Type: application/json' -d '{"id":"credit1","delta":10}' \
  http://127.0.0.1:9876/tenants/acme/accounts/main
curl -sS -w '\n' -X POST -H 'Authorization: Bearer local-acme' \
  -H 'Content-Type: application/json' -d '{"id":"credit1","delta":10}' \
  http://127.0.0.1:9876/tenants/acme/accounts/main
curl -sS -i -X POST -H 'Authorization: Bearer local-acme' \
  -H 'Content-Type: application/json' -d '{"id":"debit1","delta":-20}' \
  http://127.0.0.1:9876/tenants/acme/accounts/main
curl -sS -w '\n' -H 'Authorization: Bearer local-acme' \
  http://127.0.0.1:9876/tenants/acme/accounts/main
curl -sS -w '\n' -H 'Authorization: Bearer local-beta' \
  http://127.0.0.1:9876/tenants/beta/accounts/main
curl -sS -i -H 'Authorization: Bearer local-beta' \
  http://127.0.0.1:9876/tenants/acme/accounts/main
```

## Expected output and interpretation

The first two requests both return `{"id":"credit1","delta":10,"balance":10}`. The retry
does not credit twice. The debit returns HTTP 409 with `insufficient balance`; acme
still reads `{"balance":10}` because the transaction rolled back. Beta reads
`{"balance":0}` and gets 403 trying to read acme. Missing credentials return 401.
Reusing `credit1` with delta 11 returns 409.

A duplicate receipt records the balance at that operation; GET reads the current
balance. Restart without `--clean` and read acme to verify persistence. These public
tokens are local fixtures, so replace the resolver before deploying.

## Stop, restart and cleanup

Press Ctrl-C in terminal 1 and let the host stop. Local state lives in
`examples/multi-tenant/.cellar/native-dev`. Restart with the same command without
`--clean` to retain it; add `--clean` on the next start to reset this example. Stop
before running another example on port 9876, or choose another `--port` and change the
request URLs. JSON field order is not significant.

The [catalog](../README.md) includes automated verification, packaging and the
[application-builder roadmap](../ROADMAP.md).
