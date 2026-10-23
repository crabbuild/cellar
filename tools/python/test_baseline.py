"""Failure evidence survives concurrent clients and disposable host cleanup."""
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

import baseline


class BaselineFailures(unittest.TestCase):
    def test_cleanup_still_reaps_a_nonterminating_host(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "host"
            # exec leaves no grandchild behind when cleanup kills the fixture.
            executable.write_text('#!/bin/sh\nexec sleep 30\n')
            executable.chmod(0o755)
            host = baseline.Host(executable, "native", root, "primary")
            started = time.monotonic()
            host.cleanup()
            self.assertLess(time.monotonic() - started, 5)
            self.assertIsNotNone(host.process.returncode)
            exit_state = json.loads((root / "primary-exit.json").read_text())
            self.assertTrue(exit_state["killed_by_cleanup"])
            self.assertNotEqual(exit_state["returncode"], 0)

    def test_cleanup_retains_late_host_diagnostic_and_exit_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executable = root / "host"
            executable.write_text(
                '#!/bin/sh\nprintf "proof failed: " >&2\n'
                'touch "$2/started"\n'
                'while [ ! -f "$2/release" ]; do sleep 0.01; done\n'
                'printf "deadline elapsed\\n" >&2\nexit 7\n'
            )
            executable.chmod(0o755)
            host = baseline.Host(executable, "native", root, "primary")
            try:
                deadline = time.monotonic() + 5
                while not (root / "started").exists():
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(.005)
                wait = host.process.wait

                def release(**kwargs):
                    (root / "release").touch()
                    return wait(**kwargs)

                with patch.object(host.process, "wait", side_effect=release):
                    host.cleanup()
                self.assertEqual((root / "primary.log").read_text(), "proof failed: deadline elapsed\n")
                self.assertEqual(json.loads((root / "primary-exit.json").read_text()), {
                    "returncode_before_cleanup": None, "killed_by_cleanup": False, "returncode": 7,
                })
            finally:
                if host.process.poll() is None:
                    host.process.kill()
                    host.process.wait(timeout=5)
                host.log.close()

    def test_first_worker_error_is_not_replaced_by_secondary_abort(self):
        attempts = {}
        expected = {}

        class Host:
            origin = "unused"

            def observations(self):
                return {}

        class Connection:
            def __init__(self, origin):
                self.count = 0

            def request(self, method, route, payload):
                self.count += 1
                worker = int(route.rsplit("-", 1)[1])
                if self.count == 2:
                    if worker == 1:
                        raise OSError("original transport failure")
                    # Worker 1's finally publishes attempts only after setting
                    # the shared failure flag. Worker 0 then sees that flag on
                    # its next iteration, producing the secondary abort.
                    deadline = time.monotonic() + 5
                    while 1 not in attempts:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("worker 1 did not fail")
                        time.sleep(.001)
                return json.dumps({"value": self.count}).encode()

            def close(self):
                pass

        with patch.object(baseline, "Connection", Connection):
            with self.assertRaisesRegex(OSError, "original transport failure"):
                baseline.phase(Host(), "counter", 0, 2, 2, 1, expected, attempts)
        self.assertEqual(expected, {}, "failed phase published an acknowledged ledger")
        self.assertEqual(len(attempts[1]), 2, "failed mutation was retried")
        self.assertEqual(attempts[1][-1]["outcome"], "unknown")
        self.assertEqual(attempts[1][-1]["error"], {
            "type": "OSError", "message": "original transport failure",
        })


if __name__ == "__main__":
    unittest.main()
