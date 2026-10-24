#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Verify the native SDK's resolved build graph excludes JavaScript engines."""

import json
import subprocess
import sys


def main():
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--manifest-path", "crates/cellar/Cargo.toml",
        "--all-features", "--locked", "--format-version", "1",
    ], text=True))
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    # These roots explicitly select optional JavaScript runtimes. The
    # qualification executable deliberately hosts every backend together; it is
    # not a native-only application. Do not filter traversal edges: any native
    # root that depends on either package must still fail this check.
    runtime_roots = {"cellar-javascript", "cellar-rpc-qualification"}
    for package in packages.values():
        if package["name"] != "cellar-rpc-qualification":
            continue
        if package["features"].get("default") != []:
            sys.exit("RPC qualification must not enable runtimes by default")
        adapters = [dependency for dependency in package["dependencies"]
                    if dependency["name"] in {"cellar-javascript", "cellar-python"}]
        if len(adapters) != 2 or any(not dependency["optional"] for dependency in adapters):
            sys.exit("RPC qualification adapters must require an explicit feature")
    roots = [root for root in metadata["workspace_members"]
             if packages[root]["name"] not in runtime_roots]
    reachable = set()
    pending = list(roots)
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        reachable.add(package_id)
        pending.extend(dependency["pkg"] for dependency in nodes[package_id]["deps"])
    engines = {
        "v8", "deno_core", "boa_engine", "quickjs", "quick-js", "libquickjs-sys",
        "rquickjs", "rquickjs-core", "rquickjs-sys", "javascriptcore",
        "javascriptcore-rs", "javascriptcore-rs-sys", "mozjs", "mozjs_sys",
    }
    found = sorted(packages[package_id]["name"] for package_id in reachable
                   if packages[package_id]["name"] in engines)
    if found:
        sys.exit(f"Cellar native-only graph activates a JavaScript engine: {found}")
    print(f"Cellar native-only workspace (all features): {len(reachable)} reachable packages; no JavaScript engine")


if __name__ == "__main__":
    main()
