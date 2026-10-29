# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from raft_fleet import save_new, validate_policy
from raft_fleet_storage import MIB, Storage, loop_binding, validate_storage


def policy():
    return {'owner': 'bounded-test', 'nodes': 5, 'group_capacity': 14,
            'image_bytes': 4 * MIB, 'extent_bytes': 1024 * MIB,
            'replay_extent_bytes': 128 * MIB, 'durable_bytes': 2752 * MIB,
            'storage': {'metadata_bytes': 128 * MIB, 'raft_bytes': 512 * MIB,
                        'images_bytes': 2048 * MIB, 'replay_bytes': 384 * MIB,
                        'object_store_bytes': 4096 * MIB, 'snapshot_scratch_bytes': 128 * MIB,
                        'extent_cache_bytes': 64 * MIB},
            'resources': {'durable_overhead_bytes': 2176 * MIB}}


class BoundedStorageTests(unittest.TestCase):
    def test_shared_host_backing_requires_exact_guest_identity_before_allocation(self):
        for matched in (False, True):
            with self.subTest(matched=matched), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                backing = root / 'shared'
                backing.mkdir()
                storage = object.__new__(Storage)
                storage.host_backing = backing
                storage.backing_disk = root / 'vm'
                storage.prefix = 'cellar-fleet-' + '1' * 24
                storage.root, storage.save = root, save_new
                storage.total = 19 * 1024 ** 3
                storage.check_host_space = Mock(return_value=30 * 1024 ** 3)
                storage.docker = Mock(side_effect=[json.dumps({'ID': 'engine'}), ''])

                def guest(*args):
                    if args[0] == 'sudo':
                        return 'engine'
                    if args[0] == 'cat':
                        return Path(args[1]).read_text().strip() if matched else '{}'
                    return 'virtiofs'

                storage.guest = guest
                if matched:
                    storage.prepare()
                    self.assertEqual(storage.directory, str(backing / storage.prefix))
                    with self.assertRaises(FileExistsError):
                        # Reusing the owner cannot reopen or reformat its files.
                        storage.docker.side_effect = [json.dumps({'ID': 'engine'}), '']
                        storage.prepare()
                else:
                    with self.assertRaisesRegex(ValueError, 'same directory'):
                        storage.prepare()
                    self.assertFalse((root / 'storage-intent.json').exists())
                self.assertTrue((backing / storage.prefix / 'owner.json').exists())
                self.assertTrue(all(call.args[0] in ('info', 'volume')
                                    and 'create' not in call.args for call in storage.docker.call_args_list))

    def test_insufficient_host_backing_refuses_before_any_engine_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            home = Path(folder)
            disk = home / '.colima/_lima/_disks/colima-cellar-raft-perf/datadisk'
            disk.parent.mkdir(parents=True)
            disk.touch()
            docker = Mock()
            with patch('raft_fleet_storage.Path.home', return_value=home), \
                 patch('raft_fleet_storage.os.statvfs', return_value=Mock(f_bavail=1, f_frsize=4096)):
                with self.assertRaisesRegex(ValueError, 'physical VM backing'):
                    Storage(docker, save_new, home, 'cellar-fleet-' + '1' * 24,
                            'io.cellar.fleet.lab=' + '1' * 24, 'image', policy(), disk)
            docker.assert_not_called()

    def test_capacity_accounts_for_all_shared_pools_and_filesystem_overhead(self):
        value = policy()
        validate_policy(value)
        self.assertEqual(validate_storage(value), 19456 * MIB)
        for mutate in (
            lambda p: p['resources'].update(durable_overhead_bytes=2175 * MIB),
            lambda p: p.update(durable_bytes=3072 * MIB),
            lambda p: p.update(extent_bytes=512 * MIB),
            lambda p: p.update(extent_bytes=2048 * MIB),
            lambda p: p.update(replay_extent_bytes=384 * MIB),
            lambda p: p['storage'].update(snapshot_scratch_bytes=512 * MIB),
        ):
            changed = copy.deepcopy(value)
            mutate(changed)
            with self.assertRaises(ValueError):
                validate_policy(changed)

    def test_only_exact_loop_backing_can_be_formatted(self):
        row = {'loopdevices': [{'name': '/dev/loop10', 'back-file': '/owned/node1.img'}]}
        loop_binding(row, '/dev/loop10', '/owned/node1.img')
        for device, backing in [('/dev/vdb', '/owned/node1.img'),
                                ('/dev/loop11', '/owned/node1.img'),
                                ('/dev/loop10', '/another/node1.img')]:
            with self.assertRaises(ValueError):
                loop_binding(row, device, backing)

    def test_lost_format_reply_never_reformats_on_retry(self):
        with tempfile.TemporaryDirectory() as folder:
            storage = object.__new__(Storage)
            storage.root = Path(folder)
            storage.save = save_new
            storage.directory = '/owned'
            storage.prefix = 'cellar-fleet-' + '1' * 24
            storage.volumes = {}
            storage.check_host_space = Mock()
            calls = []

            def guest(*args):
                calls.append(args)
                if 'mkfs.ext4' in args:
                    raise TimeoutError('unknown format outcome')
                if '--find' in args:
                    return '/dev/loop10'
                if '--json' in args:
                    return json.dumps({'loopdevices': [{'name': '/dev/loop10',
                                                       'back-file': '/owned/store.img'}]})
                return ''

            storage.guest = guest
            with self.assertRaises(TimeoutError):
                storage.create('store', 64 * MIB)
            before = list(calls)
            with self.assertRaises(FileExistsError):
                storage.create('store', 64 * MIB)
            self.assertEqual(calls, before)
            self.assertTrue((storage.root / 'storage-store-format-intent.json').exists())

    def test_guest_preserves_argument_boundaries_without_shell(self):
        with patch('raft_fleet_storage.subprocess.run', return_value=Mock(returncode=0, stdout='ok\n')) as run:
            self.assertEqual(Storage.guest(None, 'printf', '%s', 'a b $x'), 'ok')
        self.assertEqual(run.call_args.args[0][-3:], ['printf', '%s', 'a b $x'])


if __name__ == '__main__':
    unittest.main()
