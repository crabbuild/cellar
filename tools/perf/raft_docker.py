# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bounded local Engine control for one immutable, owned container identity.

Avoid repeated CLI startup inside measured pause windows. Container lifecycle
creation/removal remains in the runner's CLI adapter. API contract:
https://docs.docker.com/reference/api/engine/version/v1.51/
"""
import http.client
import json
from pathlib import Path
import re
import socket
from urllib.parse import unquote, urlsplit


def require(condition, message):
    if not condition:
        raise ValueError(message)


def discover(docker, context):
    endpoint = json.loads(docker('context', 'inspect', context, '--format', '{{json .Endpoints.docker.Host}}'))
    url = urlsplit(endpoint)
    require(url.scheme == 'unix' and not url.netloc and not url.query and not url.fragment,
            'fault control requires the selected local Unix Docker socket')
    path = unquote(url.path)
    require(Path(path).is_absolute() and '\x00' not in path, 'invalid Docker socket')
    version = docker('version', '--format', '{{.Server.APIVersion}}')
    require(re.fullmatch(r'1\.[0-9]+', version) is not None and int(version.split('.')[1]) >= 40,
            'unsupported Docker Engine API')
    return {'socket_path': path, 'api_version': version}


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=.5)
        self.path = path

    def connect(self):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(self.path)
        except BaseException:
            connection.close()
            raise
        self.sock = connection


class BoundContainer:
    """No mutation is allowed before an exact name/owner/ID binding is checked."""
    def __init__(self, socket_path, api_version, node, owner):
        require(re.fullmatch(r'cellar-mr14-[a-z0-9-]+', node) is not None, 'invalid fault container name')
        require(re.fullmatch(r'1\.[0-9]+', api_version) is not None, 'invalid Engine API version')
        self.name, self.owner, self.identity = node, owner, None
        self.prefix = '/v' + api_version
        self.connection = UnixConnection(socket_path)

    def request(self, method, suffix, expected):
        target = self.identity or self.name
        try:
            self.connection.request(method, self.prefix + '/containers/' + target + suffix,
                                    body=b'' if method == 'POST' else None)
            response = self.connection.getresponse()
            body = response.read(1024 * 1024 + 1)
            require(len(body) <= 1024 * 1024 and response.status == expected, 'Docker fault API response refused')
            return json.loads(body) if body else None
        except Exception:
            # Never include an inspect response or environment in an error.
            # A failed POST is ambiguous; the owner inspects before cleanup.
            self.connection.close()
            raise ValueError('Docker fault API request failed; outcome requires inspection') from None

    def __call__(self, *args):
        require(args and args[-1] == self.name, 'fault API target changed')
        if args[0] == 'inspect':
            require(len(args) == 4 and args[1] == '--format', 'unsupported fault inspection')
            value = self.request('GET', '/json', 200)
            identity = value['Id']
            require(re.fullmatch(r'[0-9a-f]{64}', identity) is not None and value['Name'] == '/' + self.name
                    and value['Config']['Labels'].get('io.cellar.mr14.run') == self.owner
                    and (self.identity is None or self.identity == identity), 'fault container identity or owner changed')
            self.identity = identity
            return json.dumps({'owner': self.owner, 'running': value['State']['Running'], 'paused': value['State']['Paused']})
        require(len(args) == 2 and args[0] in ('pause', 'unpause') and self.identity is not None,
                'fault mutation requires a verified immutable container identity')
        self.request('POST', '/' + args[0], 204)
        return ''

    def close(self):
        self.connection.close()
