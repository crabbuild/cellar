# Node.js and TypeScript applications

This example runs an actual Node.js process beside the Rust Cellar host. Rust
owns cell activation, routing, SQLite transactions and durability. Application
code uses normal Node ESM/CommonJS resolution and installed npm packages.

The example checks Zod validation, CommonJS Lodash, dynamic imports, filesystem
reads, Buffer/crypto, timers, Node streams, global fetch, React server rendering,
and the prebuilt native Argon2 addon. Its Counter class stores state through
Cellar and recovers it after both the Rust node and Node process restart.

## Build and verify

Use Node 24+, npm, and the repository Rust toolchain. Build on the target OS and
CPU: native addons and the Node executable are platform-specific. The current
process supervisor supports Unix (macOS and Linux); Windows is refused.

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/node --ignore-scripts
npm run --prefix examples/node build
cargo test --locked -p cellar-javascript --no-default-features --features node
cargo build --locked -p cellar-javascript --features node --bin cellar-node-example
```

This uses `tsc` to emit the module tree into `dist`. Dependencies remain in
`node_modules`; neither CommonJS nor dynamic imports are forced through a
bundler. The example's native addon supplies prebuilt platform binaries, so its
installation requires no lifecycle scripts. Other packages may require reviewed
install/build scripts and target system libraries.

## Run and deliver

For development, select the application and Node executable at the CLI boundary:

```sh
export CELLAR_NODE_ENTRY="$PWD/examples/node/dist/app.js"
export CELLAR_NODE_EXECUTABLE="$(command -v node)"
"$CARGO_TARGET_DIR/debug/cellar-node-example" --help
```

An unpackaged executable embeds a local Counter deployment graph. Start it with the same
bucket, node-storage and listener settings as other Cellar native hosts. GET
`/compatibility` runs the package checks. POST an integer from 1 through 100 to
`/alice` increments that cell; GET `/alice` reads its durable counter.

To deliver a directory with no build-time checkout or npm download dependency,
create a packaging configuration with explicit application files:

```sh
mkdir -p .cellar
node --input-type=module -e '
import {writeFileSync} from "node:fs";
writeFileSync(".cellar/node-delivery.json", JSON.stringify({
  host: process.env.CARGO_TARGET_DIR + "/debug/cellar-node-example",
  application: "../examples/node", entry: "dist/app.js",
  files: ["dist", "package.json", "package-lock.json", "node_modules"]
}));'
node tools/javascript/node-package.mjs .cellar/node-delivery.json .cellar/node-delivery
.cellar/node-delivery/launch.sh --help
```

The packager copies the actual Node executable,
Rust host, modules and dependencies, resolving local SDK symlinks. Every file is
covered by a SHA-256 inventory. `launch.sh` verifies file contents, Node version,
OS and CPU before invoking the host. It forwards host arguments and works after
moving the directory. Treat the manifest as part of the trusted release; hashes
are integrity checks, not signatures. The Node binary may still require system
libraries provided by its distribution.

The relocatable directory above is useful for local-graph delivery. For native
fleet publication, prepare the runtime before compiling the host as follows.

## Native fleet packaging and installation

```sh
node tools/javascript/node-package.mjs --runtime examples/node/runtime.json .cellar/node-runtime
cargo run --locked -p cellar -- build examples/node/project.json --output .cellar/node-fleet-package
```

`runtime.json` selects only Node, emitted code and installed dependencies. The
runtime-only packager resolves symlinks and refuses an existing output directory.
`project.json` declares `node_environment` with its root, runtime executable,
entry module and working directory. The example uses `.cellar/node-runtime`;
for a production release, choose a fresh immutable absolute prefix available at
the same path on build and serving machines, and update the project root before
building. Packages must be built on the deployment OS/CPU; this implementation
accepts macOS arm64/x64 and GNU/Linux arm64/x64 runtime identities.

`cellar build` captures a bounded file inventory and content-addressed archive
before Cargo, embeds that exact inventory into the host, and binds it to the
executable digest and target in `package.json`. The native deployment declares
`node-runtime-v1`; older hosts refuse this required feature. Changes to any
runtime, module, dependency, entry path or executable require a new build and
process. The archive format and native host protocol remain at version 1.

Use the normal native CLI to deliver the resulting package:

```sh
cellar deploy .cellar/node-fleet-package --bucket YOUR_BUCKET
cellar install .cellar/node-fleet-package --bucket YOUR_BUCKET \
  --output /path/to/new-cellar-host --node-root /exact/compiled/runtime/prefix
```

The installer verifies the archive, target, inventory and every content blob,
then publishes a new runtime directory atomically without overwriting an existing
prefix. A matching prefix can be reused; a different one is refused. Pass the
exact prefix recorded in `package.json`, with its parent already present.
The new executable is installed only after runtime restoration succeeds. Use
your existing supervisor to activate it after draining the old host.

The process qualification publishes into a private SQLite development bucket,
installs the actual executable, exercises npm and native addons through HTTP,
and recovers acknowledged counter writes in a second process with fresh local
storage:

```sh
CELLAR_NODE_PACKAGE="$PWD/.cellar/node-fleet-package" \
  cargo test --locked -p cellar-javascript --features node --test fleet_package -- --ignored
```

CI moves the captured runtime aside first, so installation must restore the
runtime and every application dependency from the published archive.

The packaged example selects `Host::fleet()` and verifies the compiled inventory
before launching Node. `CELLAR_NODE_ENTRY` and `CELLAR_NODE_EXECUTABLE` cannot
override a packaged inventory. Host admission also rejects any deployment whose
Node inventory differs from the process inventory, before application factories
run. An unpackaged build retains the local graph and environment settings above.

Runtime prefixes are immutable release inputs. Keep caches, uploaded files and
other mutable data outside them. Node and native addons may still depend on OS
shared libraries: build/test on the target distribution and provision those
system prerequisites. System library closure capture, Windows supervision and
production multi-node qualification are separate remaining gates.

## Contracts and limits

Use `NodeRuntime::run`, `NodeOptions::new(absolute_entry)` and
`Source::module().events(...).cell(...)`. Set `NodeOptions::working_directory`
explicitly when packages use relative filesystem paths; otherwise the child
inherits the host directory. The example selects its application root.
ESM named exports, default application
objects and CommonJS `module.exports` are supported. Handler signatures and
`@cellar/sdk` types match the embedded V8 adapter. Node-only Cargo builds do not
link V8. The feature is opt-in; native-only applications stay engine-free.

AsyncLocalStorage scopes private context calls across promises, timers and Node
I/O. Rust independently checks invocation/activation liveness, pending operations,
byte limits, and mutation serialization. Cancellation revokes Cellar authority;
a submitted mutation can still have an unknown outcome. Always await effects.
The channel is authenticated and separate from application stdout/stderr.

Startup has `Options::startup_timeout`. The Node event-loop heartbeat has
`NodeOptions::heartbeat_timeout` (two seconds by default), replacing the embedded
V8 uninterrupted-turn watchdog. `Options::turn_timeout` applies only to V8.
Node's old-space limit is derived from `heap_bytes`; it is not an RSS cap. A
blocked event loop or crashed worker stops the application and triggers native
shutdown. Bootstrap kills the owned Unix process group and waits for the Node
child, without replaying operations. Apps must not detach child processes from
that group; external supervision owns any deliberately detached processes.

Node code has normal OS capabilities. Filesystem writes, network requests and
other direct Node effects are outside Cellar's transaction, replay and cancellation
contracts. Cancelled JavaScript may continue its own work, but cannot use a revoked
Cellar context. Use process/container limits for trusted application deployment.

Node streams and WebSockets can be used by npm code for its own I/O. The Cellar
bridge itself still uses bounded buffered HTTP values; it does not expose Cellar
streaming, WebSocket upgrades, workflows, RPC or JS-authored Raft commands. React
server rendering is supported by the example; browser DOM execution and arbitrary
framework deployment adapters are separate integrations. Compatibility tests
qualify the listed packages and paths, not every npm package or Node platform.
