#!/usr/bin/env python3
"""Refuse an actual opposite-ABI libpython in a disposable unbound example host.

Inventory-bound deployments independently verify library bytes before bootstrap.
This checks the adapter's earlier C-only guard even without a bound inventory.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from baseline import Host, digest, require, save


def interpreter(path, environment):
    query = """import json, pathlib, sys, sysconfig
print(json.dumps(dict(version=list(sys.version_info[:2]), free_threaded='t' in sys.abiflags,
    library=str((pathlib.Path(sysconfig.get_config_var('LIBDIR')) / sysconfig.get_config_var('LDLIBRARY')).resolve()))))
"""
    environment = dict(environment)
    if sys.platform == "linux":
        # setup-python shared builds can need an explicit library directory to
        # run the inspector itself. Select it from this interpreter, not ambient
        # LD_LIBRARY_PATH (which may select the wrong provider).
        directory = path.parent.parent / "lib"
        if directory.is_dir():
            environment["LD_LIBRARY_PATH"] = str(directory)
    result = subprocess.run([str(path), "-I", "-B", "-c", query], env=environment,
                            check=True, capture_output=True, text=True, timeout=10)
    value = json.loads(result.stdout)
    require(value["version"] == [3, 14] and Path(value["library"]).is_file(), "CPython 3.14 shared library required")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", type=Path, required=True, help="unbound python-baseline example executable")
    parser.add_argument("--compiled-python", type=Path, required=True)
    parser.add_argument("--opposite-python", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new private evidence directory")
    args = parser.parse_args()
    require(sys.platform in ("darwin", "linux"), "unsupported loader qualification platform")
    host = args.host.resolve(strict=True)
    # Inspector execution must not itself inherit a loader or sysconfig override.
    clean = {key: value for key, value in os.environ.items() if not key.startswith(
        ("LD_", "DYLD_", "PYTHON", "_PYTHON")) and key != "GLIBC_TUNABLES"}
    compiled = interpreter(args.compiled_python.resolve(strict=True), clean)
    opposite = interpreter(args.opposite_python.resolve(strict=True), clean)
    require(compiled["free_threaded"] != opposite["free_threaded"], "opposite threading ABI required")
    if sys.platform == "linux":
        clean["LD_LIBRARY_PATH"] = str(Path(compiled["library"]).parent)
    args.output.mkdir(parents=True, exist_ok=False)
    document = {"format": 1, "status": "running", "host_sha256": digest(host),
                "compiled": compiled, "opposite": opposite,
                "opposite_library_sha256": digest(Path(opposite["library"]))}
    control = None
    child = None
    try:
        control_root = args.output / "matched"
        control_root.mkdir()
        control = Host(host, "python-defaults", control_root, "primary", environment=clean)
        control.readiness()
        document["matched_runtime"] = control.python_runtime()
        require(document["matched_runtime"]["free_threaded"] == compiled["free_threaded"], "host does not use the declared compiled interpreter ABI")
        document["matched_shutdown"] = control.stop()
        mismatch_root = args.output / "mismatched"
        mismatch_root.mkdir()
        environment = dict(clean)
        if sys.platform == "darwin":
            loader = args.output / "loader"
            loader.mkdir()
            (loader / Path(compiled["library"]).name).symlink_to(opposite["library"])
            environment["DYLD_LIBRARY_PATH"] = str(loader.resolve())
        else:
            # The executable may have an absolute DT_NEEDED/RPATH. Preload the
            # opposite provider so the stable opaque inspection API resolves to
            # it without rewriting or replacing any original runtime artifact.
            environment["LD_PRELOAD"] = opposite["library"]
        started = time.monotonic_ns()
        with (args.output / "mismatched.log").open("wb") as output:
            child = subprocess.Popen([str(host), "python-defaults", str(mismatch_root.resolve()), "primary"],
                                     env=environment, stdout=output, stderr=output)
            status = child.wait(timeout=10)
        log = (args.output / "mismatched.log").read_text(errors="replace")
        document.update(mismatched_status=status, elapsed_ns=time.monotonic_ns() - started,
                        mismatched_host_admitted=(mismatch_root / "ready.json").exists())
        require(status == 1, f"wrong-library startup must fail normally, never signal/timeout: {status}; see {args.output}")
        require("loaded CPython threading ABI differs from the compiled interpreter" in log, f"loaded ABI was not refused by the host guard; see {args.output}")
        require(not any((mismatch_root / name).exists() for name in ("ready.json", "complete.json", "native-complete.json")), "wrong-library host admitted work")
        document["status"] = "complete"
        print("PASS actual opposite-ABI library refused normally; matched host runs and joins", flush=True)
    except BaseException as error:
        document.update(status="failed", failure=f"{type(error).__name__}: {error}")
        raise
    finally:
        if child is not None and child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        if control is not None:
            control.cleanup()
        save(args.output / "loaded-abi.json", document)


if __name__ == "__main__":
    main()
