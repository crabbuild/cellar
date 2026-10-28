#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualify native routing in owned Linux containers on an explicit Docker context."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import uuid


ROOT = Path(__file__).resolve().parents[3]
ENGINE_INPUTS = (
    "Cargo.toml", "Cargo.lock", "crates/cellar/Cargo.toml",
    "crates/cellar/containers/docker.rs",
    "crates/cellar/containers/mod.rs", "crates/cellar/containers/network.rs",
    "crates/cellar/containers/network_options.rs", "crates/cellar/containers/idle.rs",
    "crates/cellar/containers/ports.rs", "crates/cellar/execution/domain.rs",
    "crates/cellar/execution/services.rs",
    "crates/cellar/tests/native_container_egress.rs",
    "crates/cellar/tests/support/container_network.rs",
    "tools/sandbox/egress/service.mjs",
    "tools/sandbox/egress/application.mjs", "tools/sandbox/egress/qualify.py",
)


def engine_inputs():
    return {relative: hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
            for relative in ENGINE_INPUTS}


def run(arguments, *, timeout=120):
    return subprocess.run(
        arguments, check=True, text=True, capture_output=True, timeout=timeout
    ).stdout.strip()


def resolve_rust_image(docker, requested, local_source):
    # BuildKit may cache a base without registering its tag in the daemon's
    # image store. Resolve before building, then pin the captured input instead
    # of trying to inspect a possibly absent/moved tag after successful tests.
    descriptor = (
        '{"id":{{json .Id}},"digests":{{json .RepoDigests}},'
        '"os":{{json .Os}},"arch":{{json .Architecture}}}'
    )
    inspect = [*docker, "image", "inspect", "--format", descriptor, requested]
    try:
        raw = run(inspect)
    except subprocess.CalledProcessError as error:
        if "No such image" not in (error.stderr or ""):
            raise
        run([*docker, "pull", requested], timeout=600)
        raw = run(inspect)
    info = json.loads(raw)
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", info.get("id", "")):
        raise RuntimeError("Rust base image omitted its immutable Engine ID")
    if info.get("os") != "linux" or info.get("arch") not in {"amd64", "arm64"}:
        raise RuntimeError("network qualifier currently requires a Linux amd64/arm64 Rust image")
    digests = info.get("digests") or []
    if not isinstance(digests, list) or any(
        not isinstance(digest, str)
        or not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]*@sha256:[a-f0-9]{64}", digest)
        for digest in digests
    ):
        raise RuntimeError("Rust base image has invalid registry provenance")
    # BuildKit treats a bare Engine ID as a registry name. RepoDigests can
    # also describe locally built containerd images unavailable in a registry.
    # Always build through our own alias of the exact daemon image instead.
    return {
        "requested": requested, "id": info["id"], "source": local_source,
        "platform": f"linux/{info['arch']}", "digests": digests,
    }


def remove_owned_image_tag(docker, name):
    try:
        run([*docker, "image", "rm", name])
    except subprocess.CalledProcessError as error:
        if "No such image" not in (error.stderr or ""):
            raise


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


def build_engine_test(evidence):
    environment = os.environ.copy()
    environment.setdefault("CARGO_TARGET_DIR", str(ROOT / ".cellar/target-egress-native"))
    environment.update({
        "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0",
        "CARGO_INCREMENTAL": "0", "CARGO_BUILD_JOBS": "2",
    })
    print("Building locked native Engine attach qualifier", flush=True)
    built = subprocess.run(
        ["cargo", "test", "--locked", "-p", "cellar", "--test",
         "native_container_egress", "--no-run", "--message-format=json"],
        cwd=ROOT, env=environment, text=True, capture_output=True, timeout=600,
    )
    (evidence / "engine-build.log").write_text(built.stderr)
    (evidence / "engine-build.jsonl").write_text(built.stdout)
    built.check_returncode()
    executables = [
        item["executable"] for line in built.stdout.splitlines()
        if (item := json.loads(line)).get("reason") == "compiler-artifact"
        and item.get("target", {}).get("name") == "native_container_egress"
        and item.get("executable")
    ]
    if len(executables) != 1:
        raise RuntimeError("native Engine build omitted its unique test executable")
    return executables[0]


def qualify_engine(executable, docker, image, network, owner, options, evidence):
    endpoint = run([*docker, "context", "inspect", options.context,
                    "--format", "{{.Endpoints.docker.Host}}"])
    if not endpoint.startswith("unix://"):
        raise RuntimeError("native Engine qualification requires an explicit Unix socket")
    environment = os.environ.copy()
    environment.update({
        "CELLAR_EGRESS_ENGINE_SOCKET": endpoint.removeprefix("unix://"),
        "CELLAR_EGRESS_ENGINE_IMAGE": image,
        "CELLAR_EGRESS_APP_IMAGE": options.node_image,
        "CELLAR_EGRESS_ENGINE_NETWORK": network,
        "CELLAR_EGRESS_ENGINE_OWNER": owner,
        "CELLAR_EGRESS_ENGINE_RUNTIME": "runc",
    })
    profiles = [
        ("engine", "exact_engine_attach_gates_application_start_and_joins_helper", "CELLAR_EGRESS_ENGINE_RESULT="),
        ("production-engine", "production_cell_runs_own_their_network_helpers", "CELLAR_EGRESS_PRODUCTION_RESULT="),
    ]
    campaigns = {}
    for name, test, marker in profiles:
        print(f"Checking actual {name} helper lifetime", flush=True)
        output = ""
        try:
            observed = subprocess.run(
                [executable, "--exact", test, "--ignored", "--nocapture", "--test-threads=1"],
                env=environment, text=True, capture_output=True, timeout=90,
            )
            output = observed.stdout + observed.stderr
            observed.check_returncode()
        except subprocess.TimeoutExpired as error:
            output = (error.stdout or b"").decode() + (error.stderr or b"").decode()
            raise
        finally:
            (evidence / f"{name}.log").write_text(output)
            pending_failure = sys.exception()
            try:
                cleanup_production(docker, image, owner, output)
            except (subprocess.SubprocessError, OSError, ValueError, RuntimeError) as cleanup:
                if pending_failure:
                    pending_failure.add_note(f"Owned production cleanup failed: {cleanup}")
                else:
                    raise
        results = [json.loads(line.removeprefix(marker))
                   for line in observed.stdout.splitlines() if line.startswith(marker)]
        if len(results) != 1:
            raise RuntimeError(f"{name} qualification omitted its complete evidence")
        campaigns[name] = results[0]
    remaining = run([*docker, "ps", "-aq", "--filter",
                     f"label=cellar.egress.qualifier={owner}"])
    if remaining:
        raise RuntimeError("native Engine campaign left owned containers after joined cleanup")
    return {**campaigns["engine"], "production": campaigns["production-engine"]}


def cleanup_production(docker, image, owner, output):
    """Recover only published owner identities after a failed or killed test."""
    prefix = "CELLAR_EGRESS_PRODUCTION_OWNER="
    namespaces = set()
    for line in output.splitlines():
        _, marker, encoded = line.partition(prefix)
        if not marker:
            continue
        identity = json.loads(encoded)
        if (not isinstance(identity, dict)
            or identity.get("node") != f"network-{owner}"
            or not isinstance(identity.get("host"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", identity["host"])):
            raise RuntimeError("production qualifier published invalid cleanup ownership")
        namespaces.add(identity["host"])
    errors = []
    for namespace in sorted(namespaces):
        try:
            cleanup_namespace(docker, image, owner, namespace)
        except (subprocess.SubprocessError, OSError, ValueError, RuntimeError) as error:
            errors.append(f"{namespace}: {error}")
    if errors:
        raise RuntimeError("; ".join(errors))


def cleanup_namespace(docker, image, owner, namespace):
    filters = ["--filter", f"label=cellar.host={namespace}"]
    ids = run([*docker, "ps", "-aq", *filters,
               "--filter", f"label=cellar.node=network-{owner}"]).splitlines()
    resources = []
    for resource in ids:
        info = json.loads(run([*docker, "inspect", "--format",
                              '{"id":{{json .Id}},"labels":{{json .Config.Labels}}}', resource]))
        labels = info.get("labels") if isinstance(info, dict) else None
        if (not isinstance(labels, dict) or not isinstance(info.get("id"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", info["id"])
            or labels.get("cellar.host") != namespace
            or labels.get("cellar.node") != f"network-{owner}"
            or (labels.get("cellar.network.role") not in {"helper", "application"}
                and labels.get("cellar.fence") != "1")):
            raise RuntimeError("production cleanup refused a foreign container")
        resources.append(info)
    for info in sorted(resources, key=lambda info: info["labels"].get("cellar.network.role") == "helper"):
        run([*docker, "rm", "-f", info["id"]])
    for network in run([*docker, "network", "ls", "-q", *filters]).splitlines():
        info = json.loads(run([*docker, "network", "inspect", "--format",
                              '{"id":{{json .Id}},"labels":{{json .Labels}}}', network]))
        labels = info.get("labels") if isinstance(info, dict) else None
        if (not isinstance(labels, dict) or not isinstance(info.get("id"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", info["id"])
            or labels.get("cellar.host") != namespace):
            raise RuntimeError("production cleanup refused a foreign network")
        run([*docker, "network", "rm", info["id"]])
    # One atomic batch removes only this owner's table, whether it already
    # exists or was never installed. No foreign host rules are enumerated.
    table = f"cellar_{namespace[:24]}"
    subprocess.run([
        *docker, "run", "--rm", "-i", "--runtime", "runc",
        "--label", f"cellar.qualifier={owner}", "--network", "host",
        "--cap-drop", "ALL", "--cap-add", "NET_ADMIN", "--memory", "64m",
        "--pids-limit", "64", "--entrypoint", "/usr/sbin/nft", image, "-f", "-",
    ], input=f"table inet {table}\ndelete table inet {table}\n",
        text=True, capture_output=True, check=True, timeout=15)


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
        'ENTRYPOINT []\n'
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
    local_source = f"cellar-egress-rust-input:{owner}"
    docker = ["docker", "--context", options.context]
    containers = []
    network_created = False
    image_created = False
    local_source_attempted = False
    options.output.mkdir(parents=True, exist_ok=True)
    evidence = options.output / owner
    evidence.mkdir()
    source_revision = run(["git", "-C", str(ROOT), "rev-parse", "HEAD"])
    peer_source = evidence / "peer.mjs"
    shutil.copy2(Path(__file__).with_name("peer.mjs"), peer_source)
    try:
        native_sources = engine_inputs()
        rust_input = resolve_rust_image(docker, options.rust_image, local_source)
        # Record cleanup intent before the mutation, including a lost tag
        # response. Removing our unique alias preserves the user's image.
        local_source_attempted = True
        run([*docker, "tag", rust_input["id"], local_source])
        (evidence / "rust-input.json").write_text(json.dumps(rust_input, indent=2) + "\n")
        engine_test = build_engine_test(evidence)
        with tempfile.TemporaryDirectory(prefix="cellar-egress-build-") as temporary:
            context = Path(temporary)
            source_files = prepare_context(context, rust_input["source"])
            source_files["tools/sandbox/egress/peer.mjs"] = hashlib.sha256(peer_source.read_bytes()).hexdigest()
            source_files.update(native_sources)
            print("Building locked native network qualifier", flush=True)
            build = subprocess.run(
                [*docker, "build", "--pull=false", "--platform", rust_input["platform"],
                 "--label", f"cellar.qualifier={owner}",
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
            "--runtime", "runc",
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
                "--runtime", "runc",
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
        campaigns["engine"] = qualify_engine(
            engine_test, docker, image, network, owner, options, evidence,
        )
        if native_sources != engine_inputs():
            raise RuntimeError("native Engine qualification inputs changed during the campaign")
        report = {"campaigns": campaigns}
        report["image"] = run([*docker, "image", "inspect", "--format", "{{.Id}}", image])
        report["rust_image"] = rust_input["id"]
        report["rust_input"] = rust_input
        report["node_image"] = run([*docker, "image", "inspect", "--format", "{{.Id}}", options.node_image])
        report["kernel"] = run([*docker, "info", "--format", "{{.KernelVersion}}"])
        report["runtime"] = "runc"
        report["engine_version"] = run([*docker, "version", "--format", "{{.Server.Version}}"])
        report["source_revision"] = source_revision
        report["source_files"] = source_files
        (evidence / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"Network qualification passed: {evidence / 'result.json'}", flush=True)
    finally:
        cleanup_errors = []
        # Reconcile this campaign's label after a native timeout/lost create
        # response. Never enumerate or remove resources outside this owner.
        try:
            owned = run([*docker, "ps", "-aq", "--filter",
                         f"label=cellar.egress.qualifier={owner}"]).splitlines()
            for container in owned:
                run([*docker, "rm", "-f", container])
        except (subprocess.SubprocessError, OSError) as error:
            cleanup_errors.append(f"native Engine resources: {error}")
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
        if local_source_attempted:
            try:
                remove_owned_image_tag(docker, local_source)
            except (subprocess.SubprocessError, OSError) as error:
                cleanup_errors.append(f"Rust input tag {local_source}: {error}")
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
