# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Qualification must retain its safety checks and negative oracle under -O."""
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest


class QualificationGuards(unittest.TestCase):
    def run_qualifier(self, host, inventory, scratch):
        return subprocess.run([
            sys.executable, "-O", str(Path(__file__).with_name("qualify_inventory.py")),
            "--host", str(host), "--inventory", str(inventory),
            "--scratch-prefix", str(scratch),
        ], capture_output=True, text=True, timeout=5)

    def test_optimized_client_refuses_filesystem_root_before_host_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            marker, host = root / "host-entered", root / "host"
            host.write_text("#!/bin/sh\nprintf entered > " + shlex.quote(str(marker)) + "\n")
            host.chmod(0o755)
            inventory = root / "inventory.json"
            inventory.write_text(json.dumps(dict(roots=[], module_paths=[])))
            result = self.run_qualifier(host, inventory, root.anchor)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("scratch prefix cannot be a filesystem root", result.stderr)
            self.assertFalse(marker.exists(), "root refusal executed the supplied host")

    def test_optimized_client_rejects_a_host_that_accepts_corrupt_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            home, site = root / "runtime", root / "site"
            source, extension = site / "numpy/__init__.py", site / "pydantic_core/_core.so"
            library = home / "lib/libpython3.14.so"
            for path in (source, extension, library):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"private fixture contents")
            host = root / "always-successful-host"
            host.write_text("#!/bin/sh\nexit 0\n")
            host.chmod(0o755)
            inventory = root / "inventory.json"
            inventory.write_text(json.dumps(dict(
                roots=[str(home), str(site)], module_paths=[str(site)],
                home=str(home), executable=str(host), source=dict(filename="fixture.py"),
                files={str(path): dict(kind="file") for path in (source, extension, library)},
            )))
            result = self.run_qualifier(host, inventory, root)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("corrupted environment admitted host", result.stderr)
            self.assertNotIn("PASS verified inventory", result.stdout)
            self.assertEqual(source.read_bytes(), b"private fixture contents")


if __name__ == "__main__":
    unittest.main()
