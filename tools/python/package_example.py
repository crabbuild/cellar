#!/usr/bin/env python3
"""Build a compiled Python example with a verified, offline-restorable environment."""
import argparse
import os
from pathlib import Path
import shlex
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def package(example, binary):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prefix", type=Path, required=True, help="new immutable absolute runtime prefix")
    parser.add_argument("--output", type=Path, required=True, help="new package directory outside the prefix")
    parser.add_argument("--runtime", choices=["3.14.5", "3.14.5t"], default="3.14.5")
    parser.add_argument("--version", default="v1", help="compiled application version for process replacement")
    args = parser.parse_args()
    if not args.prefix.is_absolute() or args.prefix.exists() or args.prefix.is_symlink():
        parser.error("--prefix must be a new absolute path; existing environments are never modified")
    prefix = args.prefix.parent.resolve(strict=True) / args.prefix.name
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(prefix):
        parser.error("--output must be a new path outside --prefix")
    if not os.environ.get("CARGO_TARGET_DIR") or not Path(os.environ["CARGO_TARGET_DIR"]).is_absolute():
        parser.error("set CARGO_TARGET_DIR to an absolute writable directory unique to this checkout")
    target = Path(os.environ["CARGO_TARGET_DIR"]).resolve()
    prefix.mkdir()
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    env.pop("CELLAR_PYTHON_INVENTORY", None)

    def run(*command):
        command = [str(part) for part in command]
        print("+ " + shlex.join(command), flush=True)
        subprocess.run(command, cwd=ROOT, env=env, check=True)

    run("uv", "python", "install", args.runtime, "--install-dir", prefix / "runtime", "--no-bin")
    python = subprocess.check_output(
        ["uv", "python", "find", "--managed-python", args.runtime],
        env=dict(env, UV_PYTHON_INSTALL_DIR=str(prefix / "runtime")), text=True).strip()
    env["PYO3_PYTHON"] = python
    env["CELLAR_PYTHON_SITE_PACKAGES"] = str(prefix / "site")
    run(python, "-I", "-B", "-m", "pip", "download", "--require-hashes", "--only-binary", ":all:",
        "-r", example / "requirements.lock", "--dest", prefix / "wheels")
    run(python, "-I", "-B", "-m", "pip", "install", "--no-index", "--require-hashes", "--only-binary", ":all:",
        "--find-links", prefix / "wheels", "--target", prefix / "site", "-r", example / "requirements.lock")
    run("cargo", "build", "-p", "cellar-python", "--bin", binary, "--features", "embed", "--locked")
    inventory, archive, project = (prefix / name for name in ("inventory.json", "environment.tar.gz", "project.json"))
    triple = next(line.removeprefix("host: ") for line in subprocess.check_output(
        ["rustc", "-vV"], text=True, env=env).splitlines() if line.startswith("host: "))
    tool = ROOT / "tools/python/environment.py"
    run(python, tool, "capture", "--python", python, "--site-packages", prefix / "site",
        "--native-host", target / "debug" / binary, "--source", example / "app.py",
        "--filename", "app.py", "--descriptor", example / "descriptor.json", "--target", triple,
        "--output", inventory)
    run(python, tool, "bundle", inventory, "--prefix", prefix, "--output", archive)
    run(python, tool, "project", example / "project.json", "--inventory", inventory,
        "--archive", archive, "--version", args.version, "--output", project)
    run("cargo", "build", "-p", "cellar", "--bin", "cellar", "--locked")
    run(target / "debug/cellar", "build", project, "--output", output)
    run(python, tool, "verify", inventory)
    print(f"Built {output}; install with --python-root {prefix}", flush=True)

