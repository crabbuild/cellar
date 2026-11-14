# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Explicit small-voter profiles retain a bounded VM and object-store reserve."""

import unittest

from local import GIB, required_resources


class LocalResourceTests(unittest.TestCase):
    def test_three_two_gib_voters_keep_the_store_and_observer_reserve(self):
        self.assertEqual(required_resources(3, 2), (5, 8 * GIB + GIB // 2))
        self.assertEqual(required_resources(3), (5, 11 * GIB + GIB // 2))

    def test_refuses_unqualified_node_and_memory_profiles(self):
        for nodes, memory in [(2, 2), (3, 1), (3, 4), (3, True), (True, 2)]:
            with self.assertRaises(ValueError):
                required_resources(nodes, memory)


if __name__ == '__main__':
    unittest.main()
