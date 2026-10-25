# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Cleanup evidence must observe native admissions, including in old JSON."""
from copy import deepcopy
import unittest

from operating_defaults_report import bridge_settled


def settled():
    operations = dict.fromkeys(("pending", "queued", "running", "replies_pending",
                               "admitted", "delivered", "abandoned_queued",
                               "abandoned_running", "undelivered", "started",
                               "succeeded", "failed"), 0)
    return dict(native_invocations=0, queued_bytes=0, pending_reservations=0,
                operations=operations)


class BridgeReportTests(unittest.TestCase):
    def test_zero_native_work_is_settled(self):
        bridge_settled(settled())

    def test_missing_native_observation_is_not_zero(self):
        value = settled()
        del value["native_invocations"]
        with self.assertRaises(ValueError):
            bridge_settled(value)

    def test_nonzero_and_malformed_native_observations_are_refused(self):
        for count in (1, -1, False, None, "0"):
            value = deepcopy(settled())
            value["native_invocations"] = count
            with self.subTest(count=count), self.assertRaises(ValueError):
                bridge_settled(value)


if __name__ == "__main__":
    unittest.main()
