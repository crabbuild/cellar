"""Local fault control binds ownership before mutation and never retries POST."""
import http.server
import json
from pathlib import Path
import socketserver
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from raft_docker import BoundContainer, discover


NAME, OWNER, ID = 'cellar-mr14-test-node3', 'owned', 'a' * 64


def state(**changes):
    return {'Id': ID, 'Name': '/' + NAME, 'Config': {'Labels': {'io.cellar.mr14.run': OWNER}},
            'State': {'Running': True, 'Paused': False}, **changes}


class Discovery(unittest.TestCase):
    def test_only_selected_local_socket_and_supported_version(self):
        for endpoint, version, valid in [('unix:///tmp/docker.sock', '1.54', True),
                                         ('tcp://localhost:2375', '1.54', False),
                                         ('unix://foreign/tmp/docker.sock', '1.54', False),
                                         ('unix:///tmp/docker.sock?x=1', '1.54', False),
                                         ('unix:///tmp/%00', '1.54', False),
                                         ('unix:///tmp/docker.sock', '1.39', False)]:
            docker = Mock(side_effect=[json.dumps(endpoint), version])
            with self.subTest(endpoint=endpoint, version=version):
                if valid:
                    self.assertEqual(discover(docker, 'selected'), {'socket_path': '/tmp/docker.sock', 'api_version': version})
                else:
                    with self.assertRaises(ValueError):
                        discover(docker, 'selected')
                self.assertEqual(docker.call_args_list[0].args[2], 'selected')


class Ownership(unittest.TestCase):
    def test_unverified_or_changed_identity_never_mutates(self):
        for value in (state(Id='bad'), state(Name='/other'), state(Config={'Labels': {}}), state(Id='b' * 64)):
            with self.subTest(value=value), patch('raft_docker.UnixConnection'):
                target = BoundContainer('/tmp/socket', '1.54', NAME, OWNER)
                with self.assertRaises(ValueError):
                    target('pause', NAME)
                target.request = Mock(side_effect=[state(), value])
                target('inspect', '--format', 'state', NAME)
                with self.assertRaises(ValueError):
                    target('inspect', '--format', 'state', NAME)
                self.assertTrue(all(call.args[0] == 'GET' for call in target.request.call_args_list))
                with self.assertRaises(ValueError):
                    target('unpause', 'cellar-mr14-other-node3')

    def test_ambiguous_post_is_not_retried_and_error_does_not_expose_body(self):
        with patch('raft_docker.UnixConnection') as factory:
            connection = factory.return_value
            target = BoundContainer('/tmp/socket', '1.54', NAME, OWNER)
            target.identity = ID
            connection.getresponse.side_effect = TimeoutError('secret from remote')
            with self.assertRaisesRegex(ValueError, 'outcome requires inspection') as error:
                target('pause', NAME)
            self.assertNotIn('secret', str(error.exception))
            connection.request.assert_called_once_with('POST', '/v1.54/containers/' + ID + '/pause', body=b'')
            connection.close.assert_called_once()

    def test_oversize_or_unexpected_response_refuses(self):
        for status, body in ((500, b'secret'), (200, b'x' * (1024 * 1024 + 1)), (200, b'not json')):
            with self.subTest(status=status, size=len(body)), patch('raft_docker.UnixConnection') as factory:
                response = factory.return_value.getresponse.return_value
                response.status, response.read.return_value = status, body
                target = BoundContainer('/tmp/socket', '1.54', NAME, OWNER)
                with self.assertRaises(ValueError):
                    target('inspect', '--format', 'state', NAME)
                factory.return_value.close.assert_called_once()

    def test_real_unix_http_connection_reuses_socket_and_binds_immutable_id(self):
        requests, connections = [], []
        paused = False
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'
            def setup(self):
                super().setup()
                connections.append(self.connection)
            def do_GET(self):
                requests.append(('GET', self.path))
                body = json.dumps(state(State={'Running': True, 'Paused': paused})).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def do_POST(self):
                nonlocal paused
                requests.append(('POST', self.path))
                paused = self.path.endswith('/pause')
                self.send_response(204)
                self.end_headers()
            def log_message(self, *_):
                pass
        with tempfile.TemporaryDirectory(prefix='mr14-', dir='/tmp') as directory:
            path = str(Path(directory) / 'docker.sock')
            with socketserver.UnixStreamServer(path, Handler) as server:
                worker = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
                worker.start()
                target = BoundContainer(path, '1.54', NAME, OWNER)
                try:
                    self.assertFalse(json.loads(target('inspect', '--format', 'state', NAME))['paused'])
                    target('pause', NAME)
                    self.assertTrue(json.loads(target('inspect', '--format', 'state', NAME))['paused'])
                    target('unpause', NAME)
                    self.assertFalse(json.loads(target('inspect', '--format', 'state', NAME))['paused'])
                finally:
                    target.close()
                    server.shutdown()
                    worker.join()
        self.assertEqual(len(connections), 1)
        self.assertEqual(requests[0], ('GET', '/v1.54/containers/' + NAME + '/json'))
        self.assertTrue(all(path.startswith('/v1.54/containers/' + ID + '/') for _, path in requests[1:]))


if __name__ == '__main__':
    unittest.main()
