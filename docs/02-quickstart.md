# Run and package your first application

Use the checked-in counter to learn the actual development path before adapting
it to your domain. Run commands from the repository root. Rust manifests
declare minimum Rust 1.94.1; SDK CI currently selects 1.98.1. Treat the checked-in
manifest and workflow as authoritative for this baseline.

![Request routing](diagrams/routing-flow.svg)

## Prerequisites and state

Install the declared Rust toolchain and its platform build prerequisites. The
counter uses a local store and needs no production bucket. Container examples
add Docker or Podman. On the development machine, use a private build directory
on the workspace volume as required by [repository guidance](../AGENTS.md):

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-docs"
cargo run -p cellar --bin cellar --locked -- \
  dev examples/native-counter-project.json
```

Choose a different target directory if another checkout uses that name. `dev`
builds the selected native binary and watches project sources. The project file
resolves paths relative to its directory. Its state is under
`examples/.cellar/native-dev`; restarting the dev command preserves it.
`--no-watch` runs one build, `--logs` exposes node logs, and `--clean`
intentionally resets this project's local development state.

In another terminal:

```sh
curl -i -X POST http://127.0.0.1:9876/counters/alpha
curl -i http://127.0.0.1:9876/counters/alpha
curl -i http://127.0.0.1:9876/counters/beta
curl -i http://127.0.0.1:9876/
```

On fresh state, the first POST and following GET return `{"count":1}`; beta
returns `{"count":0}`. Repeated POSTs increase alpha. Stop and restart `dev`, then
read alpha to demonstrate persistence. Existing state changes these numbers.
The root serves the example assets. The counter is a teaching example and
does not implement command deduplication; retrying an ambiguous POST can
increment twice.

## Read the implementation

In [native_counter.rs](../examples/native_counter.rs), the root handler parses
`/counters/NAME`, obtains `context.namespace("COUNTERS")`, and calls the named
stub. The `Counter` cell reads and writes `count` through `context.storage()`.
`Registry::cell` registers its factory. The graph declares the class and binding,
and `Host::local(...).register(...).run()` owns process execution.
The [project file](../examples/native-counter-project.json) selects the Cargo
package, binary, profile, application version, binding and static assets.

For a new application, copy this shape into your own Rust package, rename the
script/class/binding consistently, and decide a stable cell identity scheme.
Keep application factories compiled into the selected binary. Start with one
entity and one command, then add authorization and request IDs before exposing
mutations to retrying clients.

## Build a package

```sh
cargo run -p cellar --bin cellar --locked -- \
  build examples/native-counter-project.json --output counter-package
```

The package contains `package.json`, the digest-addressed native executable and
staged application assets. Build rejects JavaScript engine dependencies and
verifies the package. This example selects the dev profile; a release project
should explicitly select the intended production profile and target.
Publishing and activating it are separate steps in
[packaging and deployment](12-packaging-deployment.md).

If startup fails, inspect compiler stderr, check the ports are free, and confirm
the Cargo paths and binary name match. If state appears absent, confirm the
project location and dev-state directory before resetting anything. Completion
means observing independent names, persistence across restart, and a package
with a manifest; these commands are instructions, not a claim they were run
during the documentation change.
