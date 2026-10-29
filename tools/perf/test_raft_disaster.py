# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Destructive controller refuses changed ownership and unconfirmed stops."""
import json
import unittest

from raft_disaster import LABEL, fence_and_delete


class Engine:
    def __init__(self):
        self.nodes = {str(n)*64: {'volume': f'voter-{n}', 'running': True, 'owner': 'run'} for n in range(1, 4)}
        self.volumes = {f'voter-{n}': 'run' for n in range(1, 4)}
        self.old = [(key, value['volume']) for key, value in self.nodes.items()]
        self.mutations = []
        self.ignore_kill = False

    def __call__(self, *args):
        if args[0] == 'inspect':
            node = self.nodes[args[1]]
            return json.dumps([{'Id': args[1], 'Config': {'Labels': {LABEL: node['owner']}},
                'Mounts': [{'Type': 'volume', 'Name': node['volume']}], 'State': {'Running': node['running']}}])
        if args[:2] == ('volume', 'inspect'):
            return json.dumps([{'Labels': {LABEL: self.volumes[args[2]]}}])
        if args[0] == 'ps':
            return '\n'.join(self.nodes)
        if args[:2] == ('volume', 'ls'):
            return '\n'.join(self.volumes)
        self.mutations.append(args)
        if args[0] == 'kill' and not self.ignore_kill:
            self.nodes[args[1]]['running'] = False
        elif args[0] == 'rm':
            del self.nodes[args[1]]
        elif args[:2] == ('volume', 'rm'):
            del self.volumes[args[2]]
        return ''


class DisasterFenceTests(unittest.TestCase):
    def test_deletes_only_exact_owned_voters_and_confirms_absence(self):
        engine = Engine()
        proof = fence_and_delete(engine, 'run', engine.old)
        self.assertEqual(len(proof), 3)
        self.assertFalse(engine.nodes)
        self.assertFalse(engine.volumes)
        self.assertTrue(all(row['container_absent'] and row['disk_absent'] for row in proof))

    def test_foreign_volume_aborts_before_any_destructive_action(self):
        engine = Engine()
        engine.volumes['voter-3'] = 'someone-else'
        with self.assertRaisesRegex(ValueError, 'volume owner'):
            fence_and_delete(engine, 'run', engine.old)
        self.assertEqual(engine.mutations, [])

    def test_foreign_container_aborts_before_any_destructive_action(self):
        engine = Engine()
        engine.nodes['3'*64]['owner'] = 'someone-else'
        with self.assertRaisesRegex(ValueError, 'ownership'):
            fence_and_delete(engine, 'run', engine.old)
        self.assertEqual(engine.mutations, [])

    def test_unconfirmed_stop_never_deletes_disk_or_certifies_fence(self):
        engine = Engine()
        engine.ignore_kill = True
        with self.assertRaisesRegex(ValueError, 'still running'):
            fence_and_delete(engine, 'run', engine.old)
        self.assertEqual(len(engine.volumes), 3)
        self.assertFalse(any(args[0] == 'rm' or args[:2] == ('volume', 'rm') for args in engine.mutations))


if __name__ == '__main__':
    unittest.main()
