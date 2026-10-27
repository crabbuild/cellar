#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualify native routing in owned Linux containers on an explicit Docker context."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import uuid


ROOT = Path(__file__).resolve().parents[3]


def run(arguments, *, timeout=120):
    return subprocess.run(
        arguments, check=True, text=True, capture_output=True, timeout=timeout
    ).stdout.strip()


def toml_value(value):
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, list):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(
            f"{key} = {toml_value(item)}" for key, item in value.items()
        ) + " }"
    raise TypeError(f"unsupported workspace dependency field: {type(value).__name__}")


def prepare_context(destination, rust_image):
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]
    source = ROOT / "crates/cellar-egress"
    manifest = tomllib.loads((source / "Cargo.toml").read_text())
    dependencies = set(manifest["dependencies"])
    for target in manifest.get("target", {}).values():
        dependencies.update(target.get("dependencies", {}))
    shutil.copytree(source, destination / "crates/cellar-egress")
    shutil.copy2(ROOT / "Cargo.lock", destination / "Cargo.lock")
    (destination / "Cargo.toml").write_text(
        '[workspace]\nresolver = "2"\nmembers = ["crates/cellar-egress"]\n\n'
        "[workspace.dependencies]\n"
        + "".join(
            f"{name} = {toml_value(workspace['dependencies'][name])}\n"
            for name in sorted(dependencies)
        )
    )
    # Prune unused workspace packages with metadata, then verify that every
    # selected registry version still comes from the repository's locked graph.
    # The actual container build remains --locked.
    run([
        "cargo", "metadata", "--format-version", "1",
        "--manifest-path", str(destination / "Cargo.toml"),
    ])
    original = tomllib.loads((ROOT / "Cargo.lock").read_text())["package"]
    subset = tomllib.loads((destination / "Cargo.lock").read_text())["package"]
    def registry(packages):
        return {
            (item["name"], item["version"], item["source"], item.get("checksum"))
            for item in packages if "source" in item
        }
    if not registry(subset) <= registry(original):
        raise RuntimeError("qualification build changed locked registry inputs")
    cache = hashlib.sha256(str(ROOT).encode()).hexdigest()[:16]
    (destination / "Dockerfile").write_text(
        f"FROM {rust_image}\n"
        "RUN apt-get update && apt-get install -y --no-install-recommends nftables "
        "&& rm -rf /var/lib/apt/lists/*\n"
        "WORKDIR /src\nCOPY . .\n"
        "ENV CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2\n"
        f"RUN --mount=type=cache,id=cellar-egress-{cache}-registry,target=/usr/local/cargo/registry,sharing=locked "
        f"--mount=type=cache,id=cellar-egress-{cache}-target,target=/src/target,sharing=locked "
        "find crates/cellar-egress -name '*.rs' -exec touch {} + "
        "&& cargo build -p cellar-egress --examples --bin cellar-egress "
        "--features helper --locked "
        "&& mkdir -p /opt/cellar-egress/bin/examples "
        "&& cp target/debug/cellar-egress /opt/cellar-egress/bin/ "
        "&& cp target/debug/examples/network-qualification "
        "target/debug/examples/helper-qualification "
        "target/debug/examples/helper-retirement-qualification "
        "/opt/cellar-egress/bin/examples/\n"
        'ENTRYPOINT ["/opt/cellar-egress/bin/examples/network-qualification"]\n'
    )
    return {
        str(path.relative_to(destination)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((destination / "crates/cellar-egress").rglob("*"))
        if path.is_file() and path.suffix in {".rs", ".toml"}
    }


def qualify(options):
    owner = uuid.uuid4().hex[:12]
    prefix = f"cellar-egress-{owner}"
    network = f"{prefix}-network"
    peer = f"{prefix}-peer"
    qualifier = f"{prefix}-qualifier"
    image = f"cellar-egress-qualification:{owner}"
    docker = ["docker", "--context", options.context]
    containers = []
    network_created = False
    image_created = False
    options.output.mkdir(parents=True, exist_ok=True)
    evidence = options.output / owner
    evidence.mkdir()
    source_revision = run(["git", "-C", str(ROOT), "rev-parse", "HEAD"])
    peer_source = evidence / "peer.mjs"
    shutil.copy2(Path(__file__).with_name("peer.mjs"), peer_source)
    try:
        with tempfile.TemporaryDirectory(prefix="cellar-egress-build-") as temporary:
            context = Path(temporary)
            source_files = prepare_context(context, options.rust_image)
            source_files["tools/sandbox/egress/peer.mjs"] = hashlib.sha256(peer_source.read_bytes()).hexdigest()
            print("Building locked native network qualifier", flush=True)
            build = subprocess.run(
                [*docker, "build", "--label", f"cellar.qualifier={owner}",
                 "-t", image, str(context)],
                text=True, capture_output=True, timeout=600,
            )
            (evidence / "build.log").write_text(build.stdout + build.stderr)
            build.check_returncode()
            image_created = True
        run([*docker, "network", "create", "--label", f"cellar.qualifier={owner}", network])
        network_created = True
        peer_id = run([
            *docker, "create", "--name", peer, "--label", f"cellar.qualifier={owner}",
            "--network", network, "--cap-drop", "ALL", "--memory", "64m",
            "--network-alias", "cellar-egress-peer",
            "--pids-limit", "64", "--entrypoint", "node", options.node_image, "/peer.mjs",
        ])
        containers.append(peer_id)
        run([*docker, "cp", str(peer_source), f"{peer_id}:/peer.mjs"])
        run([*docker, "start", peer_id])
        run([
            *docker, "exec", peer_id, "node", "--input-type=module", "-e",
            "import net from 'node:net';\n"
            "const deadline = setTimeout(() => process.exit(1), 5000);\n"
            "function probe() {\n"
            "  const socket = net.connect(9080, '127.0.0.1');\n"
            "  socket.once('connect', () => {\n"
            "    clearTimeout(deadline); socket.destroy();\n"
            "  });\n"
            "  socket.once('error', () => setTimeout(probe, 50));\n"
            "}\nprobe();\n",
        ], timeout=10)
        # Inspect only this owned peer; no daemon inventory or user environments.
        peer_ip = run([*docker, "inspect", "--format",
                       "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", peer_id])
        campaigns = {}
        for name, executable in [
            ("kernel", "network-qualification"), ("helper", "helper-qualification"),
            ("retirement", "helper-retirement-qualification"),
        ]:
            qualifier_id = run([
                *docker, "create", "--name", f"{qualifier}-{name}",
                "--label", f"cellar.qualifier={owner}",
                "--network", network, "--cap-drop", "ALL", "--cap-add", "NET_ADMIN",
                "--sysctl", "net.ipv4.ip_unprivileged_port_start=0",
                "--sysctl", "net.ipv6.conf.all.disable_ipv6=0",
                "--sysctl", "net.ipv6.conf.lo.disable_ipv6=0",
                "--memory", "256m", "--pids-limit", "128",
                "--entrypoint", f"/opt/cellar-egress/bin/examples/{executable}",
                image, f"{peer_ip}:9080",
            ])
            containers.append(qualifier_id)
            print(f"Checking actual {name} network boundary", flush=True)
            observed = subprocess.run(
                [*docker, "start", "-a", qualifier_id],
                text=True, capture_output=True, timeout=45,
            )
            (evidence / f"{name}.log").write_text(observed.stdout + observed.stderr)
            observed.check_returncode()
            exit_code = run([*docker, "inspect", "--format", "{{.State.ExitCode}}", qualifier_id])
            if exit_code != "0":
                raise RuntimeError(f"{name} qualifier exited {exit_code}; see {evidence}")
            campaigns[name] = json.loads(observed.stdout)
        report = {"campaigns": campaigns}
        report["image"] = run([*docker, "image", "inspect", "--format", "{{.Id}}", image])
        report["rust_image"] = run([*docker, "image", "inspect", "--format", "{{.Id}}", options.rust_image])
        report["node_image"] = run([*docker, "image", "inspect", "--format", "{{.Id}}", options.node_image])
        report["kernel"] = run([*docker, "info", "--format", "{{.KernelVersion}}"])
        report["source_revision"] = source_revision
        report["source_files"] = source_files
        (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"Network qualification passed: {evidence / 'result.json'}", flush=True)
    finally:
        cleanup_errors = []
        for container in reversed(containers):
            try:
                run([*docker, "rm", "-f", container])
            except (subprocess.SubprocessError, OSError) as error:
                cleanup_errors.append(f"container {container}: {error}")
        if network_created:
            try:
                run([*docker, "network", "rm", network])
            except (subprocess.SubprocessError, OSError) as error:
                cleanup_errors.append(f"network {network}: {error}")
        if image_created:
            try:
                run([*docker, "image", "rm", image])
            except (subprocess.SubprocessError, OSError) as error:
                cleanup_errors.append(f"image {image}: {error}")
        if cleanup_errors:
            detail = "Owned qualification cleanup failed: " + "; ".join(cleanup_errors)
            if error := sys.exception():
                error.add_note(detail)
            else:
                raise RuntimeError(detail)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rust-image", default="rust:1.98.1-bookworm")
    parser.add_argument("--node-image", default="node:24.13.1-bookworm-slim")
    qualify(parser.parse_args())


if __name__ == "__main__":
    main()
