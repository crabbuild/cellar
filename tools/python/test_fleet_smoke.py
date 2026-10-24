"""CPU-controlled fleet runs refuse missing or inconsistent resource evidence."""
import json
import contextlib
import io
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from fleet_smoke import LocalFleet, validate_python_path
from run import Harness

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'crates/cellar-python/python'))
from cellar import Conflict, Request, Snapshot


class CellPathTests(unittest.IsolatedAsyncioTestCase):
    def application(self, mode):
        source = Path(__file__).with_name('fleet_app.py')
        namespace = dict(NODE='node1', LIMITS={}, PATH_MODE=mode)
        # The embedding host owns the real ABI probe. These tests exercise the
        # application with the actual SDK value types on the tooling interpreter.
        with contextlib.redirect_stdout(io.StringIO()), \
                patch.object(sys, '_is_gil_enabled', return_value=True, create=True):
            exec(compile(source.read_text(), str(source), 'exec'), namespace)
        return namespace['Entity']()

    async def test_paths_produce_same_reads_and_checked_writes(self):
        for present in [False, True]:
            for method in ['GET', 'POST']:
                outputs = []
                for mode in ['bridge', 'direct-initial']:
                    with self.subTest(present=present, method=method, mode=mode):
                        state = {'writes': 7, 'payload': 'old'} if present else None
                        snapshot = Snapshot(present, state)
                        storage = SimpleNamespace(read_json=AsyncMock(return_value=snapshot), apply=AsyncMock())
                        context = SimpleNamespace(storage=storage, initial_storage={'state': snapshot})
                        response = await self.application(mode).fetch(
                            Request('http://fixture/cells/a', method, b'new payload'), context)
                        outputs.append((response.status, response.body, response.headers))
                        self.assertEqual(storage.read_json.await_count, int(mode == 'bridge'))
                        if method == 'POST':
                            storage.apply.assert_awaited_once_with(checks={'state': snapshot},
                                puts={'state': {'writes': 8 if present else 1, 'payload': 'new payload'}})
                        else:
                            storage.apply.assert_not_awaited()
                        self.assertEqual(snapshot.value, {'writes': 7, 'payload': 'old'} if present else None)
                self.assertEqual(outputs[0], outputs[1])

    async def test_snapshot_conflict_does_not_retry_or_return_success(self):
        snapshot = Snapshot(True, {'writes': 4, 'payload': 'before'})
        storage = SimpleNamespace(read_json=AsyncMock(), apply=AsyncMock(side_effect=Conflict('changed')))
        context = SimpleNamespace(storage=storage, initial_storage={'state': snapshot})
        with self.assertRaises(Conflict):
            await self.application('direct-initial').fetch(
                Request('http://fixture/cells/a', 'POST', b'after'), context)
        storage.read_json.assert_not_awaited()
        self.assertEqual(storage.apply.await_count, 1)
        self.assertEqual(snapshot.value, {'writes': 4, 'payload': 'before'})

    async def test_missing_declared_snapshot_refuses_without_live_read(self):
        storage = SimpleNamespace(read_json=AsyncMock(), apply=AsyncMock())
        with self.assertRaises(KeyError):
            await self.application('direct-initial').fetch(Request('http://fixture/cells/a'),
                SimpleNamespace(storage=storage, initial_storage={}))
        storage.read_json.assert_not_awaited()
        storage.apply.assert_not_awaited()


class ResourceProofTests(unittest.TestCase):
    def fixture(self, root, placement):
        return LocalFleet(SimpleNamespace(output=Path(root) / 'run', port=12345,
                                          runtime='native', cpu_placement=placement))

    def store(self):
        return dict(Id='private-container-id', State=dict(Pid=123, Running=True),
                    HostConfig=dict(CpusetCpus='3', NanoCpus=1_000_000_000,
                                    Memory=640 * 1024 * 1024, MemorySwap=640 * 1024 * 1024))

    def test_unplaced_fixture_does_not_claim_resource_proof(self):
        with tempfile.TemporaryDirectory() as root:
            harness = self.fixture(root, None)
            with patch('fleet_smoke.command', side_effect=AssertionError('unexpected Docker call')):
                harness.verify_placement('unused')
            self.assertFalse(list(harness.output.glob('*-placement.json')))

    def test_missing_or_changed_store_limits_refuse(self):
        placement = dict(host=[1, 2], client=[0], store=[3], available=[0, 1, 2, 3])
        cases = []
        for field, value in [('CpusetCpus', '2,3'), ('NanoCpus', 500_000_000),
                             ('Memory', 0), ('MemorySwap', -1)]:
            store = self.store()
            store['HostConfig'][field] = value
            cases.append(store)
        stopped = self.store()
        stopped['State']['Running'] = False
        cases.append(stopped)
        for store in cases:
            with self.subTest(store=store), tempfile.TemporaryDirectory() as root:
                harness = self.fixture(root, placement)
                with patch('fleet_smoke.command', return_value=json.dumps([store])):
                    with self.assertRaisesRegex(RuntimeError, 'resource limits'):
                        harness.verify_placement('load-before')
                self.assertFalse(list(harness.output.glob('*-placement.json')))

    def test_container_must_belong_to_local_kernel(self):
        placement = dict(host=[1, 2], client=[0], store=[3], available=[0, 1, 2, 3])
        with tempfile.TemporaryDirectory() as root:
            harness = self.fixture(root, placement)
            with patch('fleet_smoke.command', return_value=json.dumps([self.store()])), \
                    patch.object(Path, 'read_text', return_value='0::/unrelated-container'), \
                    patch('affinity.verify_threads') as verify:
                with self.assertRaisesRegex(RuntimeError, 'local Linux Docker'):
                    harness.verify_placement('load-before')
                verify.assert_not_called()

    def test_affinity_mismatch_cannot_publish_successful_proof(self):
        placement = dict(host=[1, 2], client=[0], store=[3], available=[0, 1, 2, 3])
        with tempfile.TemporaryDirectory() as root:
            harness = self.fixture(root, placement)
            with patch('fleet_smoke.command', return_value=json.dumps([self.store()])), \
                    patch.object(Path, 'read_text', return_value='0::/docker/private-container-id'), \
                    patch('affinity.verify_threads', side_effect=RuntimeError('changed CPU affinity')):
                with self.assertRaisesRegex(RuntimeError, 'changed CPU affinity'):
                    harness.verify_placement('load-before')
            self.assertFalse(list(harness.output.glob('*-placement.json')))


class MeasurementRecoveryTests(unittest.TestCase):
    def test_missing_or_different_python_path_refuses_measurement(self):
        for observed in ({}, {"path_mode": "bridge"}, {"path_mode": None}):
            with self.subTest(observed=observed):
                with self.assertRaisesRegex(RuntimeError, 'path differs'):
                    validate_python_path(observed, 'direct-initial')
        validate_python_path({"path_mode": "direct-initial"}, 'direct-initial')

    def test_reads_preserve_kill_disk_loss_and_acknowledged_recovery_checks(self):
        with tempfile.TemporaryDirectory() as root:
            args = SimpleNamespace(output=Path(root) / 'run', cells=2, warmup_seconds=5,
                                   seconds=3, repeats=2, concurrency=[1, 2], read_measurements=True)
            harness = Harness(args)
            (harness.output / 'one_node_bucket-accounting.json').write_text('{"writes":[1,1]}')
            calls = []

            def load(name, count, operation, concurrency, seconds, label, **kwargs):
                calls.append((operation, concurrency, seconds, label))
                return dict(operation=operation, label=label)

            with patch.object(harness, 'start_nodes'), patch.object(harness, 'start_node'), \
                    patch.object(harness, 'wait_node'), patch.object(harness, 'snapshot'), \
                    patch.object(harness, 'remove_node') as remove, \
                    patch.object(harness, 'clear_disks') as clear, \
                    patch.object(harness, 'load', side_effect=load), patch('run.time.sleep'):
                harness.scenario('one_node_bucket', 1, 'bucket')
            self.assertEqual([item['operation'] for item in harness.results], ['write', 'read'] * 4)
            self.assertEqual([call[1:3] for call in calls if call[0] == 'read'],
                             [(1, 3), (2, 3), (1, 3), (2, 3)])
            self.assertEqual([item['label'] for item in harness.recovery],
                             ['before-recovery', 'bucket-restore', 'preserve-reload'])
            self.assertEqual(remove.call_args_list[0].args, (0, 'one_node_bucket-killed'))
            self.assertEqual(remove.call_count, 3)
            self.assertEqual(clear.call_count, 2)


if __name__ == '__main__':
    unittest.main()
