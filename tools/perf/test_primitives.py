# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import unittest

from primitives import check_rows


def rows(primitive="sql"):
    result = []
    for concurrency in (1, 4):
        for phase in (["warmup", "enqueue"] if primitive == "queue" else ["warmup", "write", "read"]):
            duration = 2 if phase == "warmup" else 3
            measurement = {"concurrency": concurrency, "requestedSeconds": duration, "elapsedSeconds": duration,
                           "errors": 0, "successfulRequests": 2, "attempts": 2, "latencyMicros": [1000, 4000],
                           "requestsPerSecond": 2 / duration, "p50Ms": 1, "p95Ms": 4, "p99Ms": 4}
            if primitive == "queue":
                measurement.update(batchMessages=8, acknowledgedMessages=16, enqueueAndDrainSeconds=duration + 1,
                                   verifiedMessagesPerSecondIncludingDrain=16 / (duration + 1))
            result.append({"repeat": 0, "primitive": primitive, "imageMode": "materialized", "phase": phase,
                           "durability": "quorum", "voters": 3, "voterProcesses": 1, "verified": True,
                           "measurement": measurement})
    return result


class PrimitiveEvidenceTests(unittest.TestCase):
    def test_recomputes_latency_and_requires_complete_matrix(self):
        check_rows(rows(), "sql", "materialized", 3, 1)
        for mutation in ("percentile", "missing", "duration", "errors", "verification"):
            with self.subTest(mutation=mutation):
                value = rows()
                if mutation == "percentile":
                    value[1]["measurement"]["p99Ms"] = 1
                elif mutation == "missing":
                    value.pop()
                elif mutation == "duration":
                    value[1]["measurement"]["elapsedSeconds"] = 2
                elif mutation == "errors":
                    value[1]["measurement"]["errors"] = 1
                else:
                    value[1]["verified"] = False
                with self.assertRaises(Exception):
                    check_rows(value, "sql", "materialized", 3, 1)

    def test_queue_requires_drained_message_accounting(self):
        value = rows("queue")
        check_rows(value, "queue", "materialized", 3, 1)
        for field, replacement in (("acknowledgedMessages", 15), ("enqueueAndDrainSeconds", 0),
                                   ("verifiedMessagesPerSecondIncludingDrain", 0)):
            changed = copy.deepcopy(value)
            changed[1]["measurement"][field] = replacement
            with self.subTest(field=field), self.assertRaises(Exception):
                check_rows(changed, "queue", "materialized", 3, 1)
