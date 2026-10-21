#!/usr/bin/env python3
"""Exercise report refusal against a real complete default-workload evidence file."""
import argparse
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class QualificationReport(unittest.TestCase):
    document = None

    def run_report(self, document, succeeds):
        with tempfile.TemporaryDirectory(prefix="cellar-default-report-") as root:
            path = Path(root) / "evidence.json"
            path.write_text(json.dumps(document))
            for flags in ([], ["-O"]):
                result = subprocess.run([sys.executable, *flags, str(Path(__file__).with_name("operating_defaults_report.py")), str(path)],
                                        capture_output=True, timeout=10)
                self.assertEqual(result.returncode == 0, succeeds, result.stderr.decode())
                if not succeeds:
                    self.assertNotIn(b"| Runtime |", result.stdout)

    def test_complete_evidence(self):
        self.run_report(self.document, True)

    def test_wrong_recovered_receipt(self):
        document = deepcopy(self.document)
        document["cases"][0]["recovery"]["reads"][0]["value"] += 1
        self.run_report(document, False)

    def test_fabricated_percentile(self):
        document = deepcopy(self.document)
        document["cases"][0]["phases"][0]["latency_ns"]["p99"] += 1
        self.run_report(document, False)

    def test_unproven_default_saturation(self):
        document = deepcopy(self.document)
        for observation in document["cases"][0]["saturation"]["observations"]:
            snapshot = observation["runtime"]["interpreter"]
            if snapshot is not None:
                snapshot["work"]["active_invocations"] = 0
        self.run_report(document, False)

    def test_incomplete_native_drain(self):
        document = deepcopy(self.document)
        document["cases"][0]["shutdown"]["native"]["local_durability_closed"] = False
        self.run_report(document, False)

    def test_unreleased_operation(self):
        document = deepcopy(self.document)
        document["cases"][0]["shutdown"]["python"]["bridge"]["operations"]["pending"] = 1
        self.run_report(document, False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    args = parser.parse_args()
    QualificationReport.document = json.loads(args.evidence.read_text())
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(QualificationReport))
    sys.exit(not result.wasSuccessful())
