# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Canonical routing vectors shared with the native runner's public types."""
import json
from pathlib import Path
import unittest

from raft_groups import Layout, group_members
from raft_topology import verify_reply


class SharedPopulation(unittest.TestCase):
    def test_native_and_controller_use_the_same_frozen_vectors(self):
        vectors = json.loads(Path(__file__).with_name('raft_group_vectors.json').read_text())
        self.assertEqual({v['occupancy'] for v in vectors}, {1, 32, 128})
        for case in vectors:
            with self.subTest(case=case['occupancy']):
                layout = Layout(case['total'], case['occupancy'], case['class_name'], case['generation'])
                self.assertEqual(layout.json(case['nodes']), case['layout'])
                self.assertEqual(sum(g['cells'] for g in layout.groups), case['total'])
                self.assertEqual(sum(n['replica_cells'] for n in layout.placement(case['nodes'])), case['total'] * 3)

    def test_shared_history_changes_neither_cell_assignment_nor_placement(self):
        old, new = (Layout(257, 128, 'Workload', generation) for generation in (0, 1))
        self.assertEqual(old.assignment, new.assignment)
        self.assertEqual(old.placement(5), new.placement(5))
        self.assertTrue(all(a['group'] != b['group'] and a['incarnation'] != b['incarnation']
                            for a, b in zip(old.groups, new.groups)))
        self.assertEqual(group_members(29, 30), (30, 1, 2))
        self.assertEqual([g['cells'] for g in old.groups], [128, 128, 1])

    def test_invalid_population_and_numeric_aliases_are_refused(self):
        for total, occupancy, generation in [(0, 32, 0), (1000001, 32, 0), (True, 32, 0),
                                             (1, 0, 0), (1, 129, 0), (1, True, 0), (1, 32, True)]:
            with self.subTest(total=total, occupancy=occupancy), self.assertRaises(ValueError):
                Layout(total, occupancy, 'Workload', generation)
        for nodes in (2, 1001, True):
            with self.assertRaises(ValueError):
                group_members(0, nodes)
        for ordinal in (-1, True):
            with self.assertRaises(ValueError):
                group_members(ordinal, 3)

    def test_logical_history_and_physical_witness_are_checked_separately(self):
        layout = Layout(2, 32, 'Workload')
        group = layout.group(0)
        reply = {'cell': 'Workload:0', 'incarnation': '04' * 16, 'group': group['group'],
                 'group_incarnation': group['incarnation'], 'sequence': 2, 'index': 19,
                 'term': 3, 'version_token': 'opaque'}
        verify_reply(reply, 0, layout)
        for key, value in [('cell', 'Workload:1'), ('incarnation', '05' * 16),
                           ('group', '05' * 16), ('group_incarnation', '05' * 16),
                           ('sequence', True), ('index', 0), ('term', 0), ('version_token', '')]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                verify_reply({**reply, key: value}, 0, layout)


if __name__ == '__main__':
    unittest.main()
