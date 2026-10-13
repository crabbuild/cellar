# Standalone native counter host

Use the original small Rust host when learning packaging, asset routing and
application-version headers independently of the combined cookbook.

## How it works

[native_counter.rs](../native_counter.rs) registers Counter factories for v1
and v2. The [project](../native-counter-project.json) selects v1, declares the
counter namespace and packages the existing counter website. Each named cell
stores its count in SQLite. HTTP responses include `x-native-version` and
`x-native-instance`, identifying the application version and runtime instance.

## Run end to end

Prerequisites: Rust 1.94.1 or newer, Cargo, native C/C++ build tools and curl.
Run from the repository root; follow [AGENTS.md](../../AGENTS.md) for the target
directory. In terminal 1:

```sh
cargo run -p cellar --bin cellar --locked -- \
  dev examples/native-counter-project.json --no-watch --clean
```

Wait for `ready`. The clean start resets this project's tutorial state. In
terminal 2:

```sh
curl -sS -i -X POST http://127.0.0.1:9876/counters/alpha
curl -sS -i http://127.0.0.1:9876/counters/alpha
curl -sS http://127.0.0.1:9876/
curl -sS -i http://127.0.0.1:9876/assets-binding/index.html
```

## Expected output and interpretation

POST and GET both return HTTP 200 with `{"count":1}` and
`x-native-version: v1`. `x-native-instance` is a runtime identifier and can
change; it is not a durable business id. Both asset routes return the packaged
counter HTML; the binding route also includes the application version header.
Open `http://127.0.0.1:9876/` in a browser to use the interactive counter page.

Stop, restart **without `--clean`**, and GET alpha: the count remains 1. The
version header describes code selection while the count describes durable state.
The [cookbook counter](../counter/README.md) adds transactions and native RPC.

## Stop and cleanup

Press Ctrl-C and let the host stop. State is in `examples/.cellar/native-dev`.
Add `--clean` on the next start to reset this project. Stop before another host
uses port 9876, or select another `--port` and adjust the URLs. The
[catalog](../README.md) covers packaging and deployment.
