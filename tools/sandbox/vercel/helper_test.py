# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Exercise the actual remote helper locally; no Vercel VM/account is used."""
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

HELPER = Path(__file__).resolve().parents[3] / "crates/cellar-container/vercel/helper.py"


def run(request):
    result = subprocess.run([sys.executable, "-I", "-c", HELPER.read_text()],
                            input=json.dumps({"format": 1, **request}).encode(),
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=8)
    assert not result.stderr, result.stderr
    frames = [json.loads(line) for line in result.stdout.splitlines()]
    return result.returncode, frames


@unittest.skipUnless(os.name == "posix", "the Vercel helper targets Linux")
class HelperTests(unittest.TestCase):
    def test_large_both_pipes_do_not_deadlock(self):
        code = "import sys; sys.stderr.buffer.write(b'\\xff' * (512*1024)); sys.stderr.flush(); sys.stdout.buffer.write(b'\\x00' * (512*1024))"
        status, frames = run({"operation": "exec", "max_bytes": 1024 * 1024,
                              "deadline_ms": 4000,
                              "options": {"cmd": [sys.executable, "-I", "-c", code]}})
        self.assertEqual(status, 0)
        channels = {"stdout": b"", "stderr": b""}
        for frame in frames[:-1]:
            channels[frame["stream"]] += base64.b64decode(frame["data"], validate=True)
        self.assertEqual(channels, {"stdout": b"\x00" * (512 * 1024),
                                    "stderr": b"\xff" * (512 * 1024)})
        self.assertEqual(frames[-1], {"end": 0})

    def test_binary_commands_keep_channels_child_exit_cwd_and_env(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = str(Path(directory).resolve())
            code = "import os,sys; assert os.getcwd()==sys.argv[1]; assert os.environ['CELLAR_TEST']=='exact'; sys.stdout.buffer.write(bytes([0,255,42])); sys.stderr.buffer.write(bytes([254,128])); sys.exit(7)"
            status, frames = run({"operation": "exec", "max_bytes": 5, "deadline_ms": 2000,
                                  "options": {"cmd": [sys.executable, "-I", "-c", code, directory],
                                              "env": {"CELLAR_TEST": "exact"}, "cwd": directory}})
            self.assertEqual(status, 0)
            channels = {"stdout": b"", "stderr": b""}
            for frame in frames[:-1]:
                channels[frame["stream"]] += base64.b64decode(frame["data"], validate=True)
            self.assertEqual(channels, {"stdout": b"\x00\xff*", "stderr": b"\xfe\x80"})
            self.assertEqual(frames[-1], {"end": 7})

    def test_files_keep_large_binary_modes_symlinks_and_existing_parents(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "é-?.bin"
            for data in (b"", b"\x00\xff*" * (256 * 1024)):
                status, frames = run({"operation": "write", "path": str(path),
                                      "data": base64.b64encode(data).decode(), "mode": 0o640})
                self.assertEqual((status, frames), (0, [{"result": None}, {"end": 0}]))
                self.assertEqual(path.read_bytes(), data)
                self.assertEqual(path.stat().st_mode & 0o777, 0o640)
            _, frames = run({"operation": "stat", "path": str(path)})
            self.assertEqual(frames[0]["result"]["kind"], "file")
            self.assertEqual(frames[0]["result"]["size"], path.stat().st_size)
            (root / "link").symlink_to(path.name)
            _, frames = run({"operation": "list", "path": directory, "max_entries": 2, "max_bytes": 4096})
            self.assertEqual([entry["name"] for entry in frames[0]["result"]], ["link", path.name])
            self.assertEqual(frames[0]["result"][0]["link_target"], path.name)
            for request in ({"operation": "list", "path": directory, "max_entries": 1, "max_bytes": 4096},
                            {"operation": "list", "path": directory, "max_entries": 2, "max_bytes": 1},
                            {"operation": "write", "path": str(root / "missing/child"), "data": "", "mode": 0o600}):
                status, frames = run(request)
                self.assertEqual(status, 1)
                self.assertIn("error", frames[-1])
            self.assertFalse((root / "missing").exists())

    def test_output_limit_and_deadline_refuse_and_join_the_child(self):
        status, frames = run({"operation": "exec", "max_bytes": 2, "deadline_ms": 2000,
                              "options": {"cmd": [sys.executable, "-I", "-c", "import sys; sys.stdout.buffer.write(b'xxx')"]}})
        self.assertEqual(status, 1)
        self.assertIn("error", frames[-1])
        started = time.monotonic()
        status, frames = run({"operation": "exec", "max_bytes": 64, "deadline_ms": 1000,
                              "options": {"cmd": [sys.executable, "-I", "-c", "import os,time; print(os.getpid(),flush=True); time.sleep(30)"]}})
        self.assertEqual(status, 1)
        self.assertLess(time.monotonic() - started, 5)
        pid = int(base64.b64decode(next(frame["data"] for frame in frames if frame.get("stream") == "stdout")))
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        self.assertIn("error", frames[-1])


if __name__ == "__main__":
    unittest.main()
