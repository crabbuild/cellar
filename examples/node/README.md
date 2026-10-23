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

The executable embeds a local Counter deployment graph. Start it with the same
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

This directory is a separate delivery format, not a `cellar deploy`/`cellar install`
native package. Automatic fleet provisioning and fleet deployment inventory
binding for external Node resources remain future work. Deploy the complete
verified directory using a process supervisor and the local graph; replace the
whole directory/process when application code or dependencies change. Do not
use `CELLAR_NATIVE_FLEET` to bypass this boundary.

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
