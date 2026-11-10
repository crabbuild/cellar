# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""The trusted launcher's complete inventory and durable, irreversible revocation."""
import json
import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import threading
import unittest

from group_disaster import Launcher, fence_and_delete
from raft_disaster import LABEL


class LauncherTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.launcher = Launcher(self.root, 'owned')

    def populate(self):
        return [(self.launcher.launch(0, f'node{n}', f'disk{n}', lambda n=n: str(n)*64), f'disk{n}')
                for n in range(1, 4)]

    def test_reload_refuses_revoked_generation_before_invoking_docker(self):
        old = self.populate()
        self.assertEqual(self.launcher.revoke(0), old)
        reloaded = Launcher(self.root, 'owned')
        with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
            reloaded.launch(0, 'recreated', 'lost', lambda: self.fail('Docker called after revocation'))
        self.assertEqual(reloaded.revoke(0), old)
        self.assertEqual(reloaded.launch(1, 'fresh', 'new', lambda: 'a'*64), 'a'*64)

    def test_incomplete_inventory_cannot_certify_a_fence(self):
        self.launcher.launch(0, 'only-one', 'disk', lambda: '1'*64)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.revoke(0)
        self.assertEqual(json.loads(self.launcher.path.read_text())['revoked'], [0])
        reloaded = Launcher(self.root, 'owned')
        with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
            reloaded.launch(0, 'late', 'late-disk', lambda: self.fail('late launch admitted'))

    def test_unknown_launch_outcome_prevents_fence(self):
        self.populate()
        with self.assertRaisesRegex(RuntimeError, 'unknown outcome'):
            self.launcher.launch(0, 'uncertain', 'disk4', lambda: (_ for _ in ()).throw(RuntimeError('unknown outcome')))
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            Launcher(self.root, 'owned').revoke(0)

    def test_duplicate_container_ids_prevent_fence(self):
        for n in range(3):
            self.launcher.launch(0, f'node{n}', f'disk{n}', lambda: 'a'*64)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.revoke(0)

    def test_foreign_run_cannot_adopt_the_launch_ledger(self):
        with self.assertRaisesRegex(ValueError, 'foreign launcher ledger'):
            Launcher(self.root, 'foreign')

    def test_component_includes_obsolete_launches_and_restarts_stay_revoked(self):
        old = self.populate()
        for n in range(4, 8):
            generation = 0 if n == 4 else 1
            identifier = self.launcher.launch(generation, f'node{n}', f'disk{n}', lambda n=n: str(n)*64)
            old.append((identifier, f'disk{n}'))
        fresh = self.launcher.launch(2, 'fresh', 'fresh-disk', lambda: 'f'*64)
        self.assertEqual(self.launcher.revoke_component({0: 3, 1: 3}), old)
        reloaded = Launcher(self.root, 'owned')
        for identifier, _ in old:
            with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
                reloaded.restart(identifier, lambda: self.fail('revoked restart admitted'))
        self.assertEqual(reloaded.restart(fresh, lambda: 'started'), 'started')
        with self.assertRaisesRegex(ValueError, 'unknown restart identity'):
            reloaded.restart('e'*64, lambda: self.fail('unknown restart admitted'))

    def test_one_incomplete_generation_closes_the_entire_component(self):
        self.populate()
        self.launcher.launch(1, 'partial', 'partial-disk', lambda: 'a'*64)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.revoke_component({0: 3, 1: 3})
        reloaded = Launcher(self.root, 'owned')
        self.assertEqual(reloaded.read()['revoked'], [0, 1])
        for generation in (0, 1):
            with self.assertRaisesRegex(ValueError, 'deployment permanently revoked'):
                reloaded.launch(generation, 'late', 'late-disk', lambda: self.fail('launch admitted'))

    def test_certificate_requires_revocation_and_positive_container_and_disk_absence(self):
        old = self.populate()
        docker = DockerInventory(old)
        digest = 'a'*64
        with self.assertRaisesRegex(ValueError, 'not permanently revoked'):
            self.launcher.certificate_payload(digest, {0: 3}, docker)
        self.launcher.revoke(0)
        with self.assertRaisesRegex(ValueError, 'old writer or disk still exists'):
            self.launcher.certificate_payload(digest, {0: 3}, docker)
        docker.containers.clear()
        with self.assertRaisesRegex(ValueError, 'old writer or disk still exists'):
            self.launcher.certificate_payload(digest, {0: 3}, docker)
        docker.volumes.clear()
        payload = self.launcher.certificate_payload(digest, {0: 3}, docker)
        self.assertEqual(len(payload), 65)
        self.assertEqual(payload[:33], bytes([1]) + bytes.fromhex(digest))
        self.assertNotEqual(payload[33:], bytes(32))
        # A replacement outside the revoked component cannot change its receipt.
        self.launcher.launch(1, 'fresh', 'fresh-disk', lambda: 'f'*64)
        self.assertEqual(Launcher(self.root, 'owned').certificate_payload(digest, {0: 3}, docker), payload)
        inventory = {'format': 1, 'run_id': 'owned', 'revoked': [0],
                     'nodes': sorted(self.launcher.read()['nodes'][:3], key=lambda row: (row['generation'], row['name']))}
        self.assertEqual(payload[33:], hashlib.sha256(json.dumps(
            inventory, sort_keys=True, separators=(',', ':')).encode()).digest())

    def test_unknown_launch_blocks_receipt_even_when_docker_is_empty(self):
        self.populate()
        with self.assertRaisesRegex(RuntimeError, 'lost create reply'):
            self.launcher.launch(0, 'uncertain', 'uncertain-disk',
                                 lambda: (_ for _ in ()).throw(RuntimeError('lost create reply')))
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.revoke(0)
        with self.assertRaisesRegex(ValueError, 'incomplete writer inventory'):
            self.launcher.certificate_payload('a'*64, {0: 3}, DockerInventory([]))

    def test_docker_failure_cannot_be_interpreted_as_absence(self):
        self.populate()
        self.launcher.revoke(0)
        def unavailable(*_):
            raise RuntimeError('Docker unavailable')
        with self.assertRaisesRegex(RuntimeError, 'Docker unavailable'):
            self.launcher.certificate_payload('a'*64, {0: 3}, unavailable)

    def test_partial_deletion_resumes_without_recreating_a_writer(self):
        old = self.populate()
        self.launcher.revoke(0)
        docker = DockerInventory(old)
        docker.lost_delete_reply = old[0][0]
        with self.assertRaisesRegex(RuntimeError, 'lost remove reply'):
            fence_and_delete(docker, 'owned', old)
        with self.assertRaisesRegex(ValueError, 'old writer or disk still exists'):
            self.launcher.certificate_payload('a'*64, {0: 3}, docker)
        self.assertEqual(len(fence_and_delete(docker, 'owned', old)), 3)
        self.assertEqual(docker.containers, {})
        self.assertEqual(docker.volumes, {})
        self.assertEqual(len(self.launcher.certificate_payload('a'*64, {0: 3}, docker)), 65)
        self.assertEqual(len(fence_and_delete(docker, 'owned', old)), 3)

    def test_foreign_volume_blocks_all_deletions(self):
        old = self.populate()
        docker = DockerInventory(old)
        docker.volumes[old[-1][1]] = 'foreign'
        with self.assertRaisesRegex(ValueError, 'volume owner changed'):
            fence_and_delete(docker, 'owned', old)
        self.assertEqual(len(docker.containers), 3)
        self.assertEqual(len(docker.volumes), 3)

    def test_pending_launch_materializes_before_the_revocation_inventory(self):
        old = self.populate()
        reloaded = Launcher(self.root, 'owned')
        admitted, release = threading.Event(), threading.Event()
        def create():
            admitted.set()
            if not release.wait(5):
                raise RuntimeError('test launch was not released')
            return 'a'*64
        with ThreadPoolExecutor(max_workers=2) as executor:
            launch = executor.submit(self.launcher.launch, 0, 'retained', 'retained-disk', create)
            try:
                self.assertTrue(admitted.wait(5))
                revoked = executor.submit(reloaded.revoke, 0)
            finally:
                release.set()
            self.assertEqual(launch.result(timeout=5), 'a'*64)
            self.assertEqual(revoked.result(timeout=5), old + [('a'*64, 'retained-disk')])


class DockerInventory:
    """Authoritative Docker responses with a lost post-removal reply."""
    def __init__(self, nodes):
        self.containers = {identifier: {'Id': identifier, 'Config': {'Labels': {LABEL: 'owned'}},
                                       'Mounts': [{'Type': 'volume', 'Name': volume}],
                                       'State': {'Running': True}}
                           for identifier, volume in nodes}
        self.volumes = {volume: 'owned' for _, volume in nodes}
        self.lost_delete_reply = None

    def __call__(self, *args):
        if args == ('ps', '-aq', '--no-trunc'):
            return '\n'.join(self.containers)
        if args == ('volume', 'ls', '--format', '{{.Name}}'):
            return '\n'.join(self.volumes)
        if args[0] == 'inspect':
            return json.dumps([self.containers[args[1]]])
        if args[:2] == ('volume', 'inspect'):
            return json.dumps([{'Labels': {LABEL: self.volumes[args[2]]}}])
        if args[0] == 'update':
            return ''
        if args[0] == 'kill':
            self.containers[args[1]]['State']['Running'] = False
            return ''
        if args[0] == 'rm':
            del self.containers[args[1]]
            if self.lost_delete_reply == args[1]:
                self.lost_delete_reply = None
                raise RuntimeError('lost remove reply')
            return ''
        if args[:2] == ('volume', 'rm'):
            del self.volumes[args[2]]
            return ''
        raise AssertionError(args)


if __name__ == '__main__':
    unittest.main()
