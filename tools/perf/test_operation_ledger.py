# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Client evidence barriers must not throttle reads or weaken mutation evidence."""
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from dense import Experiment
from raft_workload import Ledger


class ReadWindows(unittest.TestCase):
    def test_32_and_64_readers_share_ten_cells_without_losing_workers(self):
        import collections
        import threading
        state = {'revision': 1, 'left': 7, 'right': 999993, 'payload_sha256': 'a' * 64}
        for readers in (32, 64):
            with self.subTest(readers=readers), tempfile.TemporaryDirectory() as directory:
                joined = threading.Barrier(readers, timeout=5)
                local = threading.local()
                main = threading.current_thread()
                main_times = iter((0.0, 2.0))
                observed, closed = [], []
                lock = threading.Lock()
                def now():
                    return next(main_times) if threading.current_thread() is main else (2.0 if getattr(local, 'done', False) else 0.0)
                class Connection:
                    def __init__(self, host, port, **kwargs):
                        self.member = port - 20000
                    def request(self, method, path, *args):
                        self.cell = int(path.rsplit('/', 1)[1])
                        with lock:
                            observed.append((threading.get_ident(), self.cell))
                    def getresponse(self):
                        joined.wait()
                        return self
                    status = 200
                    def read(self):
                        local.done = True
                        return json.dumps({'state': state, 'served_by': self.member, 'index': 3}).encode()
                    def close(self):
                        with lock:
                            closed.append(1)
                ledger = Ledger(Path(directory) / 'operations.jsonl')
                experiment = Experiment.__new__(Experiment)
                experiment.args = SimpleNamespace(cells=10, port_base=20000)
                experiment.mode, experiment.token = 'quorum', 'test-token'
                experiment.state = {cell: {'state': dict(state), 'sequence': 2, 'version_token': 'token'} for cell in range(10)}
                experiment.ledger, experiment.report = ledger, {'windows': []}
                experiment.save = lambda: None
                try:
                    with patch('dense.http.client.HTTPConnection', Connection), patch('dense.time.monotonic', side_effect=now), patch('os.fsync') as sync:
                        experiment.window('read', readers, 0, 1)
                    self.assertEqual(experiment.report['windows'][0]['successful'], readers)
                    self.assertEqual(experiment.report['windows'][0]['successful_by_worker'], [1] * readers)
                    self.assertEqual(len({worker for worker, cell in observed}), readers)
                    self.assertEqual(collections.Counter(cell for worker, cell in observed),
                                     collections.Counter(worker % 10 for worker in range(readers)))
                    self.assertEqual(len(closed), readers * 3)
                    self.assertEqual(sync.call_count, 1)
                    self.assertEqual(ledger.stats()['durable_records'], readers)
                finally:
                    ledger.close()


    def test_more_writers_than_cells_are_refused_before_admission(self):
        experiment = Experiment.__new__(Experiment)
        experiment.args = SimpleNamespace(cells=10)
        with self.assertRaisesRegex(ValueError, 'concurrency'):
            experiment.window('write', 32, 0, 1)


    def test_one_read_window_uses_one_evidence_sync_after_response_timing(self):
        now = [0.0]
        state = {'revision': 1, 'left': 7, 'right': 999993, 'payload_sha256': 'a' * 64}
        reply = {'state': state, 'served_by': 2, 'version_token': 'token', 'index': 3}
        class Connection:
            def __init__(self, *args, **kwargs):
                pass
            def request(self, *args, **kwargs):
                pass
            def getresponse(self):
                return self
            status = 200
            def read(self):
                now[0] += .002
                return json.dumps(reply).encode()
            def close(self):
                pass
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'operations.jsonl')
            experiment = Experiment.__new__(Experiment)
            experiment.args = SimpleNamespace(cells=1, port_base=20000)
            experiment.mode, experiment.token = 'quorum', 'test-token'
            experiment.state = {0: {'state': state, 'sequence': 2, 'version_token': 'token'}}
            experiment.ledger = ledger
            experiment.report = {'windows': [], 'durability': 'quorum'}
            experiment.save = lambda: None
            with patch('dense.http.client.HTTPConnection', Connection), patch('dense.time.monotonic', side_effect=lambda: now[0]), patch('os.fsync') as sync:
                experiment.window('read', 1, 0, .01)
            ledger.close()
            self.assertEqual(experiment.report['windows'][0]['successful'], 5)
            self.assertEqual(sync.call_count, 1, 'read evidence must share an after-window barrier')
            self.assertEqual(len((Path(directory) / 'operations.jsonl').read_text().splitlines()), 5)


    def test_failed_after_window_sync_does_not_publish_a_result(self):
        now = [0.0]
        closed = []
        class Connection:
            def __init__(self, *args, **kwargs):
                pass
            def request(self, *args, **kwargs):
                pass
            def getresponse(self):
                return self
            status = 200
            def read(self):
                now[0] += .01
                return b'{"state":{"revision":1}}'
            def close(self):
                closed.append(1)
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger')
            experiment = Experiment.__new__(Experiment)
            experiment.args = SimpleNamespace(cells=1, port_base=20000)
            experiment.mode, experiment.token = 'fleet', 'test-token'
            experiment.state = {0: {'state': {'revision': 1}, 'sequence': 2}}
            experiment.ledger = ledger
            experiment.report = {'windows': []}
            experiment.save = lambda: None
            with patch('dense.http.client.HTTPConnection', Connection), patch('dense.time.monotonic', side_effect=lambda: now[0]), patch('os.fsync', side_effect=OSError('evidence disk unavailable')):
                with self.assertRaises(OSError):
                    experiment.window('read', 1, 0, .01)
            self.assertEqual(experiment.report['windows'], [])
            self.assertEqual(len(closed), 3)
            with self.assertRaises(RuntimeError):
                ledger.close()


class DurableEvidence(unittest.TestCase):
    def test_compatible_records_share_a_sync_and_late_records_wait(self):
        import concurrent.futures
        import threading
        import time
        entered, release = threading.Event(), threading.Event()
        syncs = []
        def barrier(_):
            syncs.append(1)
            if len(syncs) == 1:
                entered.set()
                self.assertTrue(release.wait(5))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger'
            ledger = Ledger(path, mode='group')
            with patch('operation_ledger.os.fsync', side_effect=barrier), concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                first = pool.submit(ledger.append, {'event': 'invoke', 'id': 1})
                self.assertTrue(entered.wait(5))
                later = [pool.submit(ledger.append, {'event': 'invoke', 'id': n}) for n in [2, 3]]
                until = time.monotonic() + 5
                while ledger.stats()['records'] != 3 and time.monotonic() < until:
                    time.sleep(.001)
                self.assertEqual(ledger.stats()['records'], 3)
                self.assertFalse(first.done())
                self.assertTrue(all(not x.done() for x in later), 'late appends cannot use the in-flight barrier')
                release.set()
                self.assertEqual(first.result(5), 1)
                self.assertEqual(sorted(x.result(5) for x in later), [2, 3])
            self.assertEqual(len(syncs), 2)
            self.assertEqual(ledger.stats()['durable_records'], 3)
            self.assertEqual(ledger.stats()['largest_sync_records'], 2)
            ledger.close()
            rows = [json.loads(x) for x in path.read_text().splitlines()]
            self.assertEqual([x['order'] for x in rows], [1, 2, 3])
            self.assertEqual({x['id'] for x in rows}, {1, 2, 3})

    def test_sync_failure_wakes_every_waiter_and_poison_survives_close(self):
        import concurrent.futures
        import threading
        import time
        entered, release = threading.Event(), threading.Event()
        def barrier(_):
            entered.set()
            self.assertTrue(release.wait(5))
            raise OSError('injected evidence sync failure')
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger', mode='group')
            with patch('operation_ledger.os.fsync', side_effect=barrier), concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                first = pool.submit(ledger.append, {'id': 1})
                self.assertTrue(entered.wait(5))
                rest = [pool.submit(ledger.append, {'id': n}) for n in [2, 3]]
                end = time.monotonic() + 5
                while ledger.stats()['records'] < 3 and time.monotonic() < end:
                    time.sleep(.001)
                release.set()
                for pending in [first, *rest]:
                    with self.assertRaises((OSError, RuntimeError)):
                        pending.result(5)
            self.assertTrue(ledger.stats()['failed'])
            self.assertEqual(ledger.stats()['durable_records'], 0)
            with self.assertRaises(RuntimeError):
                ledger.append({'id': 4})
            with self.assertRaises(RuntimeError):
                ledger.close()
            self.assertTrue(ledger.stats()['closed'])

    def test_read_window_rejects_mutations_and_retains_a_valid_partial_prefix(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger'
            ledger = Ledger(path)
            with self.assertRaises(RuntimeError):
                with ledger.read_window():
                    ledger.append({'event': 'complete', 'kind': 'read', 'cell': 0})
                    ledger.append({'event': 'invoke', 'kind': 'write', 'cell': 0})
            self.assertTrue(ledger.stats()['failed'])
            self.assertEqual(ledger.stats()['read_batches'], 0)
            with self.assertRaises(RuntimeError):
                ledger.close()
            self.assertEqual(len(path.read_text().splitlines()), 1)

    def test_read_admission_is_bounded_and_failed_windows_cannot_qualify(self):
        for bounds in [{'max_records': 1}, {'max_bytes': 80}]:
            with self.subTest(bounds=bounds), tempfile.TemporaryDirectory() as directory:
                ledger = Ledger(Path(directory) / 'ledger')
                with self.assertRaises(RuntimeError):
                    with ledger.read_window(**bounds):
                        ledger.append({'event': 'complete', 'kind': 'read', 'payload': 'a' * 20})
                        ledger.append({'event': 'complete', 'kind': 'read', 'payload': 'b' * 20})
                self.assertTrue(ledger.stats()['failed'])
                self.assertEqual(ledger.stats()['read_batches'], 0)
                with self.assertRaises(RuntimeError):
                    ledger.close()

    def test_read_flush_failure_is_observable_after_response_completion(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger')
            with patch('operation_ledger.os.fsync', side_effect=OSError('read evidence unavailable')):
                with self.assertRaises(OSError):
                    with ledger.read_window():
                        ledger.append({'event': 'complete', 'kind': 'read'})
                        self.assertEqual(ledger.stats()['durable_records'], 0)
            self.assertTrue(ledger.stats()['failed'])
            with self.assertRaises(RuntimeError):
                ledger.close()

    def test_unbuffered_short_writes_are_completed_before_the_barrier(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger')
            real = ledger.stream
            class Short:
                def write(self, data):
                    return real.write(data[:3])
                def fileno(self):
                    return real.fileno()
                def close(self):
                    real.close()
            ledger.stream = Short()
            ledger.append({'event': 'invoke', 'input': {'a': 7}})
            ledger.close()
            rows = [json.loads(x) for x in (Path(directory) / 'ledger').read_text().splitlines()]
            self.assertEqual(rows, [{'event': 'invoke', 'input': {'a': 7}, 'order': 1}])


class AdmissionAndShutdown(unittest.TestCase):
    def test_pending_capacity_backpressures_and_close_joins_existing_callers(self):
        import concurrent.futures
        import threading
        import time
        entered, release = threading.Event(), threading.Event()
        calls = []
        def sync(_):
            calls.append(1)
            if len(calls) == 1:
                entered.set()
                self.assertTrue(release.wait(5))
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / 'ledger', mode='group', max_pending_records=1)
            with patch('operation_ledger.os.fsync', side_effect=sync), concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
                first = pool.submit(ledger.append, {'id': 1})
                self.assertTrue(entered.wait(5))
                second = pool.submit(ledger.append, {'id': 2})
                time.sleep(.01)
                self.assertEqual(ledger.stats()['records'], 1, 'capacity must precede physical append')
                closed = pool.submit(ledger.close)
                self.assertFalse(closed.done())
                release.set()
                self.assertEqual(first.result(5), 1)
                self.assertEqual(second.result(5), 2)
                closed.result(5)
            self.assertEqual(ledger.stats()['durable_records'], 2)
            self.assertTrue(ledger.stats()['closed'])

    def test_invalid_capacity_fails_before_creating_a_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ledger'
            for values in [{'max_record_bytes': 0}, {'max_pending_records': True}, {'max_pending_bytes': float('inf')}]:
                with self.subTest(values=values), self.assertRaises(ValueError):
                    Ledger(path, **values)
                self.assertFalse(path.exists())


if __name__ == '__main__':
    unittest.main()
