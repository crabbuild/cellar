# Containers

A Rust cell can run one container beside its handler, reach its ports, execute
commands, monitor exits and stop it. The container follows the cell's ownership
on a node. Its disk is ephemeral; durable application state belongs in cell
storage or a binding. The container surface is experimental.

The [native example](../../examples/native_container.rs) runs a
Python HTTP process for each named business cell. Build and develop it with:

```
cellar build examples/native-container-project.json --output package
cellar dev examples/native-container-project.json
```

The project declares its image and cell class:

```json
{
  "cells": ["Sandbox"],
  "containers": [{
    "class_name": "Sandbox",
    "image": "native-container/Dockerfile",
    "instance_type": "basic",
    "runtime": "runsc"
  }]
}
```

`image` names a Dockerfile relative to the native project, or a registry image
reference. Each container class must appear once in `cells`. The optional
`runtime` requires that OCI runtime on every node serving the class.

## Rust SDK

Get the handle with `context.container()?` inside a root cell. Retain it between
events and pass the current context to each operation. A handle cannot operate
from a different cell, a retired event, a different activation or a facet.
Reactivating the same cell at the same epoch does not revive an old handle.

```rust,ignore
use cellar::sdk::containers::{StartOptions, ExecOptions};

let container = context.container()?;
if !container.running(&context)? {
    container.start(&context, StartOptions::default()).await?;
}
let port = container.port(&context, 8080)?;
port.wait_ready(&context, std::time::Duration::from_secs(20)).await?;
let process = container.exec(&context, ExecOptions {
    cmd: vec!["sh".into(), "-c".into(), "printf hello".into()],
    ..Default::default()
}).await?;
process.close_stdin(&context).await?;
let output = process.output(&context, 1024 * 1024).await?;
```

- `start` accepts entrypoint, environment, Internet policy and user labels, and
  awaits startup. It returns the new run number.
- `running` reports cached engine state; `monitor` awaits the current run's exit.
- `destroy`, `signal` and `set_inactivity_timeout` control the root process.
- `port.fetch` streams HTTP requests and responses. It returns redirects to the
  caller. `port.upgrade_websocket` supplies an explicit WebSocket upgrade API;
  `port.connect` opens an event-owned TCP/TLS socket.
- `exec` exposes stdin, separate or combined stdout/stderr, wait and signal.
  `output` drains both streams concurrently with a combined byte limit.
  Exec handles cannot act on a later container run. Dropping a handle releases
  its streams and registry entry; it does not kill its process.

Starts, destruction, signals, inactivity changes, exec and stdin effects wait
for the cell's durability proof before reaching the engine. Port traffic uses
ordinary HTTP, WebSocket and TCP gates. Port capabilities access their own
container independently of global outbound policy.

Containers receive `CELLAR_CELL_ID`, `CELLAR_CELL_CLASS` and `CELLAR_NODE_ID`.
The engine also supplies environment fields used by third-party container
images. Application overrides are passed through `StartOptions::env`.

## Images and delivery

Build uses the Docker CLI on `PATH`, or `CELLAR_DOCKER`. A compatible Podman CLI
works. Production image platforms follow the native executable's architecture;
`CELLAR_CONTAINER_PLATFORM` overrides this. Development uses the local engine's
platform. A container package currently accepts one executable architecture;
build separate packages for different architectures. Mixed fleets remain work.

A native package stages application images and the `cellar-fence` image under
`images/`. Each manifest pins their references, archive SHA-256 and length. The
`container-images-v1` feature gate rejects nodes that cannot load that format.
`cellar deploy` verifies local archives and existing remote copies, streams new
archives into the fleet bucket and publishes deployment selectors afterward.
The publisher does not need a Docker daemon. Nodes load verified archives from
the bucket instead of pulling from registries. Node image loading currently
buffers an archive in memory.

Each container node needs a Docker or Podman engine. `DOCKER_HOST` must name a
Unix socket; otherwise the host discovers common local engine sockets. A node
without an engine can serve other classes but cannot activate a container class.

## Isolation and lifecycle

The engine drops Linux capabilities, sets `no-new-privileges`, uses the engine's
seccomp profile, limits the process count to 1024 and runs an init to reap
orphans. Containers on the same bridge cannot reach each other.

Before starting a container, Cellar runs its fence image to install nftables
rules. Starts fail if the fence cannot be installed. The rules block connections
to the node, private networks and metadata endpoints. `enable_internet: false`
uses an internal bridge on Linux. Development on macOS keeps egress enabled
because the internal bridge does not publish ports there.

`CELLAR_CONTAINER_RUNTIME` chooses a node-wide OCI runtime; a class's `runtime`
overrides it. A named runtime receives a mounted resolver configuration, using
`CELLAR_CONTAINER_DNS` or the engine's default public resolvers. A missing runtime
causes startup to fail.

An idle cell eviction keeps the container for its inactivity window, ten minutes
by default. Reactivation on the same node adopts it when the image and settings
are unchanged. A changed spec replaces the old container before adoption.
Failed removals retain the handle for retry; ownership release returns an error
to the node lifecycle. Idle removal failures retry every thirty seconds.
Ownership transfer, reset, node restart and node stop destroy it.
The node accounts for running containers'
memory limits when reporting capacity. `max_instances` is a sampled fleet limit;
simultaneous starts can exceed it temporarily.

The SDK tests exercise native routing, authority, proof rejection, exec stream
handling and package integrity against deterministic engine peers. They do not
establish real-engine network isolation or complete fleet recovery conformance.
