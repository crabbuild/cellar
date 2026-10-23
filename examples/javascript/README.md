# TypeScript cells in a Rust executable

The application imports Zod from npm and implements a persistent counter with
TypeScript cell classes. The Rust entry point embeds the checked JavaScript
bundle and runs the native Cellar host.

From the repository root, with Node/npm and the repository Rust toolchain:

```sh
export CARGO_TARGET_DIR="$HOME/Workspace/crabbuild-target/cellar-<checkout-name>"
npm ci --prefix tools/javascript --ignore-scripts
npm ci --prefix examples/javascript --ignore-scripts
npm run --prefix examples/javascript build
cargo build --locked -p cellar-javascript --features embed --bin cellar-javascript-example
cargo test --locked -p cellar-javascript --features embed
cargo run --locked -p cellar -- build examples/javascript/project.json --output .cellar/javascript-package
```

The generated `.cellar/javascript-package` uses the existing native package,
executable digest and target validation. Follow the native packaging guide for
`cellar deploy` and `cellar install`, then start the installed executable with
the same fleet configuration as a Rust application. Node and `node_modules`
are build-time inputs only. No Deno installation or runtime npm download is used.

The real-node test starts local listeners and exercises the example without a
fleet account. For a running deployment, POST an integer from 1 to 100 to a
named path, such as `curl -X POST -d '3' http://localhost:8787/alice`; GET that
path to read the counter. Different names own separate durable cells.

`javascript.json` controls strict typechecking and bundling. The build produces
`app.bundle.js` and an inventory with bundle/lockfile digests. Generated inputs
are ignored by Git; rebuild them before Cargo. `project.json` explicitly opts
into the JavaScript adapter. Changing the source, dependencies or SDK requires
rebuilding and replacing the native executable. The build never publishes npm
packages or deploys infrastructure by itself.

The profile supports ECMAScript business modules and compatible npm packages,
not Node.js applications or arbitrary browser packages. See the adapter guide
for the exact supported APIs and remaining qualification gates.

For Node built-ins, native addons, dynamic imports and React server rendering,
use the [Node.js backend](../node/README.md).
