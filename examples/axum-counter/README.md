# One counter application, two HTTP hosts

Both binaries register the [same handlers and builder](lib.rs). Named counters
use Cellar SQLite state and its existing ownership and durability machinery.
Run these commands from the repository root. On the development machine, set
`CARGO_TARGET_DIR` to your checkout's mounted-workspace target first.

Native Cellar HTTP and application package workflow:

```sh
cargo run -p cellar --bin cellar --locked -- dev examples/axum-counter/project.json
# In another terminal:
curl -X POST http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/openapi.json
```

The project embeds `cellar-host` and its factories. It explicitly enables
`openapi`, because Cellar package builds use the project's declared features
and disable Cargo default features. `dev` retains project state.
Build a package with `cellar build examples/axum-counter/project.json --output
counter-package`. A generic Cellar executable does not contain these factories.

Axum embedding the same runtime:

```sh
cargo run -p cellar-axum-counter --bin axum-host --locked -- 127.0.0.1:9876 /tmp/axum-counter-state
curl -X POST http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/counters/alpha
curl http://127.0.0.1:9876/outside
curl http://127.0.0.1:9876/openapi.json
```

Stop with Ctrl-C, then restart with the same directory to recover the counter.
`axum-host` supplies bound listeners, executors, explicit policies and peer
services. It closes admission before draining, watches node and server failures,
joins both under a deadline, and checks the returned shutdown report. The fixed
peer secret is only for this local example; fleet hosts must supply their own
shared authentication, storage and authenticated log transport via `NodeFleet`.

Offline contract export and reproducible generated client:

```sh
cargo run -p cellar-axum-counter --bin export-openapi --locked -- /tmp/counter-openapi.json
python3 examples/axum-counter/generate_client.py /tmp/counter-openapi.json /tmp/counter-client.rs
cargo test -p cellar-axum-counter --all-features --locked
```

[openapi.json](openapi.json) and [generated_client.rs](generated_client.rs) are
checked-in integration fixtures. The deliberately small generator rejects
unsupported schemas; production applications may use another OpenAPI 3.1
client generator. Exporting constructs only the declarations. The generated
client tests exercise HTTP and encoded cell names through both hosts.

The optional feature can be disabled for either host:

```sh
cargo check -p cellar-axum-counter --no-default-features --bins --locked
```
