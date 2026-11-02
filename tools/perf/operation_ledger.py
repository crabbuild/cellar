# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bounded benchmark evidence with durable append and read-window batching.

Durable append returns only after its record is covered by a successful fsync.
Read windows may defer safe query evidence; the window qualifies only after its
exit barrier succeeds. No background thread, batching delay or mutation retry.
"""
from contextlib import contextmanager
import json
import os
import threading
import time


class LedgerError(RuntimeError):
    pass


class Ledger:
    def __init__(self, path, *, mode='sync', max_record_bytes=8 * 1024**2,
                 max_pending_bytes=32 * 1024**2, max_pending_records=256):
        if (mode not in ('sync', 'group')
                or any(type(v) is not int or v <= 0 for v in (max_record_bytes, max_pending_bytes, max_pending_records))
                or max_record_bytes > max_pending_bytes):
            raise ValueError('invalid ledger mode or admission limits')
        self.stream = open(path, 'xb', buffering=0)
        self.mode = mode
        self.lock = threading.Condition()
        self.order = self.written = self.durable = 0
        self.max_record_bytes, self.max_pending_bytes = max_record_bytes, max_pending_bytes
        self.max_pending_records = max_pending_records
        self.pending_bytes = self.active = 0
        self.syncing = self.closing = self.closed = False
        self.error = self.batch = None
        self.counters = dict(sync_attempts=0, sync_completed=0, sync_errors=0, sync_ns=0,
                             sync_wait_ns=0, admission_wait_ns=0, bytes_written=0,
                             largest_sync_records=0, read_batches=0, read_records=0,
                             read_peak_buffer_bytes=0, last_read_buffer_bytes=0, read_flush_ns=0)

    def _check(self):
        if self.error is not None:
            raise LedgerError('ledger failed; incomplete evidence cannot qualify') from self.error

    def _fail(self, error):
        with self.lock:
            if self.error is None:
                self.error = error
            self.lock.notify_all()

    def _write(self, data):
        view = memoryview(data)
        while view:
            count = self.stream.write(view)
            if not isinstance(count, int) or count <= 0:
                raise OSError('ledger write made no progress')
            self.counters['bytes_written'] += count
            view = view[count:]

    @staticmethod
    def _read_record(record):
        if record.get('kind') not in (None, 'read') or record.get('operation') not in (None, 'query'):
            return False
        return (record.get('event') == 'complete' and record.get('kind') == 'read'
                or record.get('operation') == 'query' and record.get('event') in ('invoke', 'complete', 'reject')
                or record.get('event') == 'failure' and record.get('kind') == 'read'
                and record.get('mutation_ambiguous') is False)

    def append(self, record):
        with self.lock:
            self._check()
            if self.closing or self.closed or self.batch is not None and self.batch['finishing']:
                raise LedgerError('ledger admission is closed')
            self.active += 1
            batch = self.batch
        try:
            if not isinstance(record, dict) or batch is not None and not self._read_record(record):
                raise ValueError('read windows cannot defer mutation or unclassified evidence')
            # Serialize outside the shared lock. Retain immutable, exactly
            # charged bytes instead of an unbounded graph of response objects.
            payload = json.dumps({k: v for k, v in record.items() if k != 'order'},
                                 sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
            with self.lock:
                self._check()
                order = self.order + 1
                data = payload[:-1] + (b',' if len(payload) > 2 else b'') + b'"order":' + str(order).encode() + b'}\n'
                if len(data) > self.max_record_bytes:
                    raise ValueError('ledger record exceeds admission limit')
                if batch is not None:
                    if len(batch['records']) >= batch['max_records'] or batch['bytes'] + len(data) > batch['max_bytes']:
                        raise ValueError('read evidence buffer exhausted; shorten the window or increase its explicit budget')
                    self.order = order
                    batch['records'].append(data)
                    batch['bytes'] += len(data)
                    self.counters['read_records'] += 1
                    self.counters['read_peak_buffer_bytes'] = max(self.counters['read_peak_buffer_bytes'], batch['bytes'])
                    return order
                wait = time.perf_counter_ns()
                while (self.pending_bytes + len(data) > self.max_pending_bytes
                       or self.written - self.durable >= self.max_pending_records):
                    self.lock.wait()
                    self._check()
                    # Other admitted writers can advance the order while this
                    # caller waits for byte capacity. Rebuild only the suffix.
                    order = self.order + 1
                    data = payload[:-1] + (b',' if len(payload) > 2 else b'') + b'"order":' + str(order).encode() + b'}\n'
                    if len(data) > self.max_record_bytes:
                        raise ValueError('ledger record exceeds admission limit')
                self.counters['admission_wait_ns'] += time.perf_counter_ns() - wait
                self.order = order
                self._write(data)
                self.written = order
                self.pending_bytes += len(data)
                if self.mode == 'sync':
                    self._barrier(order, self.pending_bytes)
                    return order
            self._sync_through(order)
            return order
        except BaseException as error:
            self._fail(error)
            raise
        finally:
            with self.lock:
                self.active -= 1
                self.lock.notify_all()

    def _barrier(self, covered, covered_bytes):
        """Called with the lock for serial mode, outside it for group mode."""
        began = time.perf_counter_ns()
        try:
            os.fsync(self.stream.fileno())
        except BaseException:
            with self.lock:
                self.counters['sync_errors'] += 1
            raise
        finally:
            with self.lock:
                self.counters['sync_attempts'] += 1
                self.counters['sync_ns'] += time.perf_counter_ns() - began
        with self.lock:
            self.counters['sync_completed'] += 1
            self.counters['largest_sync_records'] = max(self.counters['largest_sync_records'], covered - self.durable)
            self.durable = covered
            self.pending_bytes -= covered_bytes
            self.lock.notify_all()

    def _sync_through(self, required, *, forensic=False):
        wait = time.perf_counter_ns()
        with self.lock:
            while True:
                if not forensic:
                    self._check()
                if self.durable >= required:
                    self.counters['sync_wait_ns'] += time.perf_counter_ns() - wait
                    return
                if not self.syncing:
                    break
                self.lock.wait()
            covered, covered_bytes = self.written, self.pending_bytes
            self.syncing = True
            self.counters['sync_wait_ns'] += time.perf_counter_ns() - wait
        try:
            # Coverage is captured after complete unbuffered writes, BEFORE
            # fsync starts. Concurrent later writes require another barrier.
            self._barrier(covered, covered_bytes)
        except BaseException as error:
            self._fail(error)
            raise
        finally:
            with self.lock:
                self.syncing = False
                self.lock.notify_all()
        if not forensic:
            with self.lock:
                self._check()

    @contextmanager
    def read_window(self, *, max_records=200000, max_bytes=128 * 1024**2):
        if any(type(v) is not int or v <= 0 for v in (max_records, max_bytes)):
            raise ValueError('invalid read evidence admission')
        with self.lock:
            self._check()
            if self.closing or self.closed or self.batch is not None or self.active or self.syncing:
                raise LedgerError('read window requires a joined idle ledger')
            batch = dict(records=[], bytes=0, max_records=max_records, max_bytes=max_bytes, finishing=False)
            self.batch = batch
        try:
            yield
        finally:
            self._finish_read_window(batch)

    def _finish_read_window(self, batch):
        began = time.perf_counter_ns()
        try:
            with self.lock:
                if self.batch is not batch or self.closed:
                    raise LedgerError('read window ownership changed')
                batch['finishing'] = True
                while self.active:
                    self.lock.wait()
                # Bounded joined writes avoid one syscall per read evidence
                # record. This work and its sync run after the traffic timer.
                chunks, length = [], 0
                for data in batch['records']:
                    chunks.append(data)
                    length += len(data)
                    if length >= 1024**2:
                        self._write(b''.join(chunks))
                        chunks, length = [], 0
                if chunks:
                    self._write(b''.join(chunks))
                self.written = self.order
                self.pending_bytes = batch['bytes']
            # Preserve a valid partial prefix even after admission/encoding
            # failure, but a successful forensic sync never clears failure.
            self._sync_through(self.order, forensic=True)
            with self.lock:
                self._check()
                self.counters['read_batches'] += 1
        except BaseException as error:
            self._fail(error)
            raise
        finally:
            with self.lock:
                self.counters['last_read_buffer_bytes'] = batch['bytes']
                self.counters['read_flush_ns'] += time.perf_counter_ns() - began
                batch['records'].clear()
                self.batch = None
                self.lock.notify_all()

    def stats(self):
        with self.lock:
            return {**self.counters, 'mode': self.mode, 'records': self.order,
                    'durable_records': self.durable, 'pending_bytes': self.pending_bytes,
                    'failed': self.error is not None, 'closed': self.closed,
                    'max_record_bytes': self.max_record_bytes,
                    'max_pending_bytes': self.max_pending_bytes,
                    'max_pending_records': self.max_pending_records}

    def close(self):
        with self.lock:
            if self.closed:
                return
            self.closing = True
            while self.active or self.syncing:
                self.lock.wait()
            if self.batch is not None and self.error is None:
                self.error = LedgerError('unclosed read window cannot qualify')
            try:
                self.stream.close()
            except BaseException as error:
                self._fail(error)
                raise
            finally:
                self.closed = True
                self.lock.notify_all()
            self._check()
