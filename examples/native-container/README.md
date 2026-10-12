# Native container cells

The Rust host routes `/sandboxes/NAME/PATH` to a named `Sandbox` cell. The cell
starts one Python HTTP container, waits for its port and forwards the request.
The response identifies the cell, so different names demonstrate separate
business entities and processes.

Run from the repository root with a local Docker or Podman engine:

```
cargo run -p cellar --bin cellar -- dev examples/native-container-project.json
curl http://127.0.0.1:9876/sandboxes/alice/hello
curl http://127.0.0.1:9876/sandboxes/bob/hello
```

`cellar build` stages the Rust executable, application image and fence image in
one package. `cellar deploy` uploads its verified resources before publishing
the fleet selector. The installed `cellar-native-container` executable registers
the `native-container` application at code version `v1`.

The container disk is ephemeral. Put durable state in `Context::storage` or a
binding. See the [container guide](../../docs/services/containers.md) for
ownership, event authority, image integrity and isolation boundaries.
