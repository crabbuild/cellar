"""Check placement through the CLI/native-child boundary used by benchmarks."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

from perf import start_host
import affinity


class Placement(unittest.TestCase):
    def test_partial_placement_refuses_before_creating_output_or_launching_tools(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "results"
            result = subprocess.run([
                sys.executable, "-B", str(Path(__file__).with_name("offered_perf.py")),
                "--client", "/nonexistent/client", "--client-profile", "lab",
                "--cellar", "/nonexistent/cellar", "--project", "example=/nonexistent/project",
                "--output", str(output), "--host-cpus", "0",
            ], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("provide both --host-cpus and --client-cpus", result.stderr)
            self.assertFalse(output.exists())

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux kernel affinity required")
    def test_cli_native_child_and_threads_inherit_mask_and_escape_is_refused(self):
        allowed = affinity.available()
        for escape in (False, True):
            if escape and len(allowed) < 2:
                continue  # Expansion needs a second permitted logical CPU.
            with self.subTest(escape=escape), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                proof = root / "native.json"
                requested = allowed[:1]
                child_source = f'''
import json, os, threading, time
from pathlib import Path
if {escape!r}:
    os.sched_setaffinity(0, {allowed!r})
result = dict(pid=os.getpid(), main=sorted(os.sched_getaffinity(0)))
def worker():
    result["thread"] = sorted(os.sched_getaffinity(0))
    Path({str(proof)!r}).write_text(json.dumps(result))
    time.sleep(30)
thread = threading.Thread(target=worker)
thread.start()
thread.join()
'''
                cli = root / "cellar-fixture"
                cli.write_text(f'''#!{sys.executable}
import signal, subprocess, sys, time
from pathlib import Path
child = subprocess.Popen([sys.executable, "-I", "-B", "-c", {child_source!r}])
def stop(*args):
    child.terminate()
    child.wait(timeout=5)
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop)
deadline = time.monotonic() + 5
while not Path({str(proof)!r}).exists():
    if time.monotonic() >= deadline:
        stop()
    time.sleep(.01)
print("ready http://127.0.0.1:1", flush=True)
while True:
    time.sleep(1)
''')
                cli.chmod(0o755)
                project = root / "project.json"
                project.write_text(json.dumps(dict(cargo_manifest="Cargo.toml", primary={})))
                args = SimpleNamespace(cellar=cli, project=project, rust_threads=2)
                if escape:
                    with self.assertRaisesRegex(RuntimeError, "measurement thread changed CPU affinity"):
                        start_host(args, root / "case", host_cpus=requested)
                    # Refusal also joins the CLI and its child.
                    with self.assertRaises(ProcessLookupError):
                        os.kill(json.loads(proof.read_text())["pid"], 0)
                else:
                    host, _, pid = start_host(args, root / "case", host_cpus=requested)
                    try:
                        observed = json.loads(proof.read_text())
                        self.assertEqual(observed, dict(pid=pid, main=requested, thread=requested))
                        self.assertEqual(affinity.available(), allowed, "parent affinity changed")
                    finally:
                        host.stop()


if __name__ == "__main__":
    unittest.main()
