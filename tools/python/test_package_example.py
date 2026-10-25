# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Packaging must capture and deploy the same Cargo profile and Python ABI."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import package_example


class PackageProfiles(unittest.TestCase):
    def test_project_without_override_preserves_existing_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, inventory, archive = (root / name for name in ("source.json", "inventory.json", "archive"))
            original = dict(cargo_manifest=str(package_example.ROOT / "Cargo.toml"),
                            profile="lab", primary=dict(application_version="v1"))
            source.write_text(json.dumps(original))
            inventory.write_text("{}")
            archive.write_bytes(b"archive")
            subprocess.run([
                sys.executable, "-B", str(package_example.ROOT / "tools/python/environment.py"),
                "project", str(source), "--inventory", str(inventory), "--archive", str(archive),
                "--output", str(root / "output.json"),
            ], check=True, capture_output=True, text=True, timeout=5)
            self.assertEqual(json.loads((root / "output.json").read_text())["profile"], "lab")
            self.assertEqual(json.loads(source.read_text()), original)

    def test_capture_and_final_project_select_the_built_host(self):
        real_run = subprocess.run
        for runtime in ("3.14.5", "3.14.5t"):
            for profile in (None, "dev", "lab", "release"):
                with self.subTest(runtime=runtime, profile=profile), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory).resolve()
                    prefix, target = root / "environment", root / "target"
                    example = package_example.ROOT / "examples/python-fastapi"
                    binary = "cellar-python-fastapi"
                    selected = profile or "dev"
                    artifact = target / ("debug" if selected == "dev" else selected) / binary
                    interpreter = prefix / "runtime" / runtime / "python"
                    captured = []
                    deployed = []

                    def output(command, **kwargs):
                        if command[:3] == ["uv", "python", "find"]:
                            self.assertEqual(command[-1], runtime)
                            self.assertEqual(kwargs["env"]["UV_PYTHON_INSTALL_DIR"], str(prefix / "runtime"))
                            return str(interpreter) + "\n"
                        self.assertEqual(command, ["rustc", "-vV"])
                        return "host: aarch64-apple-darwin\n"

                    def run(command, **kwargs):
                        if command[:2] == ["cargo", "build"] and binary in command:
                            self.assertEqual(command[command.index("--profile") + 1], selected)
                            self.assertEqual(kwargs["env"]["PYO3_PYTHON"], str(interpreter))
                            self.assertNotIn("CELLAR_PYTHON_INVENTORY", kwargs["env"])
                            artifact.parent.mkdir(parents=True)
                            artifact.write_text(runtime)
                        elif len(command) > 2 and command[1].endswith("environment.py"):
                            if command[2] == "project":
                                # Exercise the real generator, including argument parsing.
                                real_run([sys.executable, "-B", *command[1:]], capture_output=True, text=True, **kwargs)
                            elif command[2] == "capture":
                                host = Path(command[command.index("--native-host") + 1])
                                self.assertEqual(host, artifact)
                                self.assertEqual(host.read_text(), runtime)
                                captured.append(host)
                                Path(command[command.index("--output") + 1]).write_text("{}")
                            elif command[2] == "bundle":
                                Path(command[command.index("--output") + 1]).write_bytes(b"archive")
                        elif command[0] == str(target / "debug/cellar"):
                            config = json.loads(Path(command[2]).read_text())
                            self.assertEqual(config["profile"], selected)
                            self.assertEqual(config["primary"]["application_version"], "test-v2")
                            self.assertEqual(config["python_environment"]["inventory"], str(prefix / "inventory.json"))
                            self.assertEqual(config["cargo_manifest"], str(package_example.ROOT / "Cargo.toml"))
                            deployed.append(config)
                        return subprocess.CompletedProcess(command, 0)

                    argv = ["package.py", "--prefix", str(prefix), "--output", str(root / "package"),
                            "--runtime", runtime, "--version", "test-v2"]
                    if profile is not None:
                        argv.extend(["--profile", profile])
                    with patch.dict(os.environ, CARGO_TARGET_DIR=str(target), CELLAR_PYTHON_INVENTORY="stale"), \
                            patch.object(sys, "argv", argv), \
                            patch.object(subprocess, "check_output", side_effect=output), \
                            patch.object(subprocess, "run", side_effect=run), contextlib.redirect_stdout(io.StringIO()):
                        package_example.package(example, binary)
                    self.assertEqual(captured, [artifact])
                    self.assertEqual(len(deployed), 1)

    def test_invalid_profile_refuses_before_provisioning(self):
        with tempfile.TemporaryDirectory() as directory:
            prefix = Path(directory) / "runtime"
            result = subprocess.run([
                sys.executable, "-B", str(package_example.ROOT / "examples/python-fastapi/package.py"),
                "--prefix", str(prefix), "--output", str(Path(directory) / "package"),
                "--profile", "debug",
            ], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("invalid choice", result.stderr)
            self.assertFalse(prefix.exists())


if __name__ == "__main__":
    unittest.main()
