# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Client phase attribution must preserve HTTP results, failures and ownership."""
from contextlib import contextmanager
import http.client
import io
import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from unittest.mock import patch

from raft_probe_http import ProbeHTTP, ProbeTrace
from raft_run import request


@contextmanager
def server(replies, *, hold=False):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(3)
    release = threading.Event()
    errors = []
    def serve():
        try:
            for reply in replies:
                connection, _ = listener.accept()
                with connection:
                    connection.settimeout(3)
                    headers = b''
                    while b'\r\n\r\n' not in headers:
                        data = connection.recv(4096)
                        if not data or len(headers) > 16384:
                            raise ValueError('invalid fixture request')
                        headers += data
                    if reply:
                        connection.sendall(reply)
                    if hold:
                        release.wait(3)
        except Exception as error:
            errors.append(error)
    worker = threading.Thread(target=serve)
    worker.start()
    try:
        yield f'http://127.0.0.1:{listener.getsockname()[1]}'
    finally:
        release.set()
        worker.join(4)
        listener.close()
        if worker.is_alive() or errors:
            raise AssertionError(f'fixture did not close cleanly: {errors}')


def response(status=200, body=b'{"ok":true}'):
    return f'HTTP/1.1 {status} Fixture\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'.encode() + body


class Phases(unittest.TestCase):
    def test_http_error_body_is_closed_on_success_and_timeout(self):
        class Stalled(io.BytesIO):
            def read(self, *args):
                raise TimeoutError('error body stalled')
        for body in (io.BytesIO(b'{"error":"fixture"}'), Stalled()):
            error = urllib.error.HTTPError('http://127.0.0.1/', 503, 'Fixture', {}, body)
            with self.subTest(stalled=isinstance(body, Stalled)), patch('urllib.request.urlopen', side_effect=error):
                if isinstance(body, Stalled):
                    with self.assertRaises(TimeoutError):
                        request('http://127.0.0.1', '/', 'token')
                else:
                    self.assertEqual(request('http://127.0.0.1', '/', 'token'), (503, {'error': 'fixture'}))
            self.assertTrue(body.closed)

    def test_success_and_http_error_preserve_results_without_credentials(self):
        client = ProbeHTTP()
        for status in (200, 503):
            trace = ProbeTrace()
            with self.subTest(status=status), server([response(status)]) as base:
                result = request(base, '/', 'do-not-record-this-token', timeout=2,
                                 http_trace=trace, http_client=client)
            self.assertEqual(result, (status, {'ok': True}))
            snapshot = trace.snapshot()
            self.assertEqual([r['phase'] for r in snapshot['events']],
                             ['send', 'connect', 'response_headers', 'response_body'])
            self.assertEqual([r['depth'] for r in snapshot['events']], [0, 1, 0, 0])
            self.assertTrue(all(r['outcome'] == 'complete' and r['finished_ns'] >= r['started_ns']
                                for r in snapshot['events']))
            self.assertNotIn('do-not-record', json.dumps(snapshot))
            self.assertNotIn(base, json.dumps(snapshot))
            sent, connected = snapshot['events'][:2]
            self.assertLessEqual(sent['started_ns'], connected['started_ns'])
            self.assertLessEqual(connected['finished_ns'], sent['finished_ns'])

    def test_connect_failure_preserves_urlerror_and_does_not_retry(self):
        trace = ProbeTrace()
        with patch.object(http.client.HTTPConnection, 'connect', side_effect=TimeoutError('connect fixture')) as connect:
            with self.assertRaises(urllib.error.URLError):
                request('http://127.0.0.1:9', '/', 'token', timeout=.1,
                        http_trace=trace, http_client=ProbeHTTP())
        connect.assert_called_once()
        self.assertEqual([(r['phase'], r['error_type']) for r in trace.snapshot()['events']],
                         [('send', 'TimeoutError'), ('connect', 'TimeoutError')])

    def test_header_and_body_timeout_remain_distinct_and_are_not_retried(self):
        cases = [('response_headers', b''),
                 ('response_body', b'HTTP/1.1 200 OK\r\nContent-Length: 99\r\nConnection: close\r\n\r\n{')]
        for phase, reply in cases:
            trace = ProbeTrace()
            with self.subTest(phase=phase), server([reply], hold=True) as base:
                with self.assertRaises(TimeoutError):
                    request(base, '/', 'token', timeout=.05, http_trace=trace, http_client=ProbeHTTP())
            events = trace.snapshot()['events']
            self.assertEqual([r['phase'] for r in events if r['outcome'] == 'failed'], [phase])
            self.assertEqual(events[-1]['error_type'], 'TimeoutError')
            self.assertEqual(sum(r['phase'] == 'connect' for r in events), 1)

    def test_redirect_keeps_context_and_does_not_change_http_behavior(self):
        redirect = b'HTTP/1.1 302 Found\r\nLocation: /next\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'
        trace = ProbeTrace()
        with server([redirect, response()]) as base:
            self.assertEqual(request(base, '/', 'token', timeout=2, http_trace=trace, http_client=ProbeHTTP()),
                             (200, {'ok': True}))
        self.assertEqual(sum(r['phase'] == 'connect' for r in trace.snapshot()['events']), 2)

    def test_trace_is_bounded_and_snapshots_are_independent(self):
        trace = ProbeTrace()
        for _ in range(trace.CAPACITY + 5):
            self.assertEqual(trace.call('send', lambda: 7), 7)
        snapshot = trace.snapshot()
        self.assertEqual((snapshot['started'], snapshot['dropped'], len(snapshot['events'])), (21, 5, 16))
        snapshot['events'][0]['outcome'] = 'tampered'
        self.assertEqual(trace.snapshot()['events'][0]['outcome'], 'complete')

    def test_only_http_reads_enter_the_probe_client(self):
        client = ProbeHTTP()
        for url, method in [('https://127.0.0.1/', 'GET'), ('http://127.0.0.1/', 'POST')]:
            with self.subTest(url=url, method=method), self.assertRaises(ValueError):
                client.open(urllib.request.Request(url, method=method), .1, ProbeTrace())


if __name__ == '__main__':
    unittest.main()
