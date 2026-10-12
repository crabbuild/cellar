#!/usr/bin/env python3
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
    roots = metadata["workspace_members"]
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
        sys.exit(f"Cellar workspace activates a JavaScript engine: {found}")
    print(f"Cellar workspace (all features): {len(reachable)} reachable packages; no JavaScript engine")


if __name__ == "__main__":
    main()
