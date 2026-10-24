#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Prove prerequisite refusal before Python source or its Rust host can run.

Builds a private bootstrap-only fixture from the repository lock. Use a private
output directory and the checkout's dedicated Cargo target directory. Runtime
roots are read only; only an extra private native fixture is faulted.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib

import native_prerequisites


RUST = r'''
use anyhow::{Context as _, Result, ensure};
use cellar_python::{Options, PythonRuntime, Source};
use std::{path::Path, sync::{Arc, atomic::{AtomicBool, Ordering}}};
fn main() -> Result<()> {
    let marker = std::env::args().nth(1).context("fixture marker argument")?;
    let refuse = std::env::args().nth(2).as_deref() == Some("refuse");
    ensure!(!Path::new(&marker).exists(), "fixture marker must start absent");
    let entered = Arc::new(AtomicBool::new(false));
    let observed = entered.clone();
    // SAFETY: this private main owns initialization; no Python or host threads
    // exist. Invalid prerequisites must return before starting either side.
    let result = unsafe { PythonRuntime::run(Options::default(),
        Source::new(include_str!("app.py"), "PRIVATE-NATIVE-PREREQUISITE-SOURCE.py"),
        move |_, _| { observed.store(true, Ordering::Release); Ok(()) }) };
    if refuse {
        let error = match result { Err(error) => error, Ok(_) => anyhow::bail!("unqualified runtime started") };
        ensure!(error.to_string() == "Python runtime inventory verification failed", "wrong prerequisite failure stage");
        ensure!(!format!("{error:?}").contains("PRIVATE"), "private prerequisite diagnostic escaped");
        ensure!(!entered.load(Ordering::Acquire) && !Path::new(&marker).exists(), "unqualified runtime executed source or admitted its host");
    } else {
        ensure!(result?.complete() && entered.load(Ordering::Acquire) && Path::new(&marker).is_file(), "qualified runtime failed bootstrap");
    }
    Ok(())
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--native-host", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    original = json.loads(args.inventory.read_text())
    output = args.output.resolve()
    if any(output.is_relative_to(Path(root)) for root in original["roots"]):
        raise ValueError("Qualification output must be outside runtime roots")
    output.mkdir(parents=True, exist_ok=False)
    repository = Path(__file__).resolve().parents[2]
    source = output / "app.py"
    marker = output / "PRIVATE-source-executed"
    source.write_text("from pathlib import Path\nPath(" + repr(str(marker)) + ").write_text('source')\n")
    (output / "main.rs").write_text(RUST)
    manifest = output / "Cargo.toml"
    manifest.write_text('''[workspace]
[package]
name = "cellar-python-prerequisite-fixture"
version = "0.0.0"
edition = "2021"
[[bin]]
name = "cellar-python-prerequisite-fixture"
path = "main.rs"
[dependencies]
anyhow = "1"
cellar-python = { path = ''' + json.dumps(str(repository / "crates/cellar-python")) + ''', features = ["embed"] }
[profile.dev.package.sha2]
opt-level = 3
[profile.dev.package.rsa]
opt-level = 3
''')
    shutil.copyfile(repository / "Cargo.lock", output / "Cargo.lock")
    subprocess.run(["cargo", "metadata", "--offline", "--manifest-path", str(manifest), "--format-version", "1"],
                   stdout=subprocess.DEVNULL, check=True)
    packages = lambda lock: {(p["name"], p["version"], p.get("source")) for p in tomllib.loads(lock.read_text())["package"]}
    if packages(output / "Cargo.lock") - packages(repository / "Cargo.lock") != {("cellar-python-prerequisite-fixture", "0.0.0", None)}:
        raise ValueError("Prerequisite fixture changed locked dependencies")
    c_source = output / "extra.c"
    c_source.write_text("int cellar_extra_prerequisite(void) { return 7; }\n")
    extra = output / ("extra.dylib" if sys.platform == "darwin" else "extra.so")
    subprocess.run(["cc", "-dynamiclib" if sys.platform == "darwin" else "-shared", "-fPIC", str(c_source), "-o", str(extra)], check=True)
    inventory = copy.deepcopy(original)
    inventory["source"] = dict(sha256=hashlib.sha256(source.read_bytes()).hexdigest(), filename="PRIVATE-NATIVE-PREREQUISITE-SOURCE.py",
                               descriptor=dict(events=[], cells={}), distributions=[])
    inventory["prerequisites"] = native_prerequisites.capture(inventory, args.native_host, [extra])
    environment = dict(os.environ, PYO3_PYTHON=inventory["executable"])
    target_dir = args.target_dir.resolve()
    library_dirs = sorted({str(Path(path).parent) for path, entry in inventory["files"].items()
                           if entry["kind"] == "file" and Path(path).name.startswith("libpython3.14")
                           and (".so" in Path(path).name or path.endswith(".dylib"))})
    if not library_dirs:
        raise ValueError("Prerequisite fixture has no inventoried libpython")
    linker = ["-C", "link-arg=-Xlinker", "-C", "link-arg=--disable-new-dtags"] if sys.platform == "linux" else []
    for directory in library_dirs:
        linker += ["-C", "link-arg=-Xlinker", "-C", "link-arg=-rpath", "-C", "link-arg=-Xlinker", "-C", "link-arg=" + directory]

    def build(name, value):
        file = output / (name + ".json")
        file.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
        with (output / (name + "-build.log")).open("w") as log:
            subprocess.run(["cargo", "rustc", "--locked", "--manifest-path", str(manifest), "--target-dir", str(target_dir),
                            "--bin", "cellar-python-prerequisite-fixture", "--", *linker],
                           env=dict(environment, CELLAR_PYTHON_INVENTORY=str(file)), stdout=log, stderr=subprocess.STDOUT, check=True)
        executable = output / name
        shutil.copy2(target_dir / "debug/cellar-python-prerequisite-fixture", executable)
        return executable

    def run(executable, refuse):
        marker.unlink(missing_ok=True)
        result = subprocess.run([str(executable), str(marker), "refuse" if refuse else "valid"],
                                capture_output=True, timeout=20, env=environment)
        if result.returncode != 0:
            raise RuntimeError((result.stdout + result.stderr).decode(errors="replace"))
        if b"PRIVATE" in result.stdout + result.stderr:
            raise RuntimeError("private prerequisite diagnostic escaped")
        if marker.exists() == refuse:
            raise RuntimeError("source admission disagrees with prerequisite refusal")

    valid = build("valid", inventory)
    run(valid, False)
    content = extra.read_bytes()
    try:
        extra.write_bytes(content[:-1] + bytes([content[-1] ^ 1]))
        run(valid, True)
        extra.unlink()
        run(valid, True)
    finally:
        extra.write_bytes(content)
    run(valid, False)
    wrong_platform = copy.deepcopy(inventory)
    field = "build" if sys.platform == "darwin" else "kernel_release"
    wrong_platform["prerequisites"]["platform"][field] += "-unqualified"
    run(build("wrong-platform", wrong_platform), True)
    if sys.platform == "darwin":
        wrong_cache = copy.deepcopy(inventory)
        uuid = wrong_cache["prerequisites"]["platform"]["shared_cache_uuid"]
        wrong_cache["prerequisites"]["platform"]["shared_cache_uuid"] = ("0" if uuid[0] != "0" else "1") + uuid[1:]
        run(build("wrong-cache", wrong_cache), True)
    missing_graph = copy.deepcopy(inventory)
    missing_graph["prerequisites"]["dependencies"].pop(inventory["executable"])
    run(build("missing-graph", missing_graph), True)
    print("PASS native prerequisites: valid bootstrap; OS/cache mismatch, missing/tampered external library and missing dependency record refuse before Python source or Rust host admission")


if __name__ == "__main__":
    main()
