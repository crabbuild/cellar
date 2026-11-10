# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Controller-side protocol for real managed Node/RaftCells voter processes.

Physical preparation and attachment run under the sole launcher's assignment
lock. Transport failures remain unknown outcomes; no mutation is auto-retried.
This adapter does not initialize catalogs, invent capacity attestations or sign
fences. The Docker driver must separately qualify those platform operations.
"""
import ipaddress
import json
import urllib.error
import urllib.parse
import urllib.request

from managed_group_fence import identifier, physical_identity
from raft_run import decode_json, require

LIMIT = 64 * 1024
PHYSICAL = frozenset(('start', 'prepare_seed', 'seed', 'attach', 'activate'))


class VoterFailure(RuntimeError):
    def __init__(self, message, *, status=None, reply=None):
        super().__init__(message)
        self.status, self.reply = status, reply
        # A missing/malformed reply cannot prove that a mutation was not entered.
        self.outcome = reply.get('outcome', 'unknown') if isinstance(reply, dict) else 'unknown'


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        return None


class ManagedVoter:
    def __init__(self, launcher, generation, container, endpoint, token, process):
        identifier(process, 16)
        require(isinstance(token, str) and len(token) == 64
                and all(c in '0123456789abcdef' for c in token), 'invalid private voter bearer')
        address = urllib.parse.urlsplit(endpoint)
        host = ipaddress.ip_address(address.hostname)
        require(address.scheme == 'http' and address.port is not None
                and (host.is_loopback or host.is_private) and not host.is_unspecified
                and not host.is_multicast and not address.username and not address.password
                and address.path in ('', '/') and not address.query and not address.fragment,
                'explicit isolated voter endpoint required')
        with launcher.lock():
            value = launcher.read()
            require(type(generation) is int and generation not in value['revoked'],
                    'deployment permanently revoked')
            rows = [row for row in value['nodes']
                    if row['generation'] == generation and row['container_id'] == container]
            require(len(rows) == 1 and container is not None, 'unknown managed voter launch')
            identity = physical_identity(rows[0]['identity'])
        self.launcher, self.generation = launcher, generation
        self.endpoint, self.token, self.process = endpoint.rstrip('/'), token, process
        self.identity = identity
        self.client = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def call(self, action, *, timeout=5):
        require(isinstance(action, dict) and action.get('action') not in PHYSICAL,
                'physical voter operations require a durable assignment')
        return self._request(action, timeout=timeout)

    def _request(self, action, *, timeout=5):
        require(isinstance(action, dict) and 0 < timeout <= 20, 'bounded voter request required')
        body = json.dumps(action, separators=(',', ':'), allow_nan=False).encode()
        require(len(body) <= LIMIT, 'voter request exceeds bound')
        request = urllib.request.Request(self.endpoint + '/control', body, method='POST', headers={
            'content-type': 'application/json', 'authorization': 'Bearer ' + self.token,
            'x-cellar-node': self.identity['node'], 'x-cellar-disk': self.identity['disk'],
            'x-cellar-process': self.process,
        })
        try:
            with self.client.open(request, timeout=timeout) as response:
                payload = response.read(LIMIT + 1)
                require(response.status == 200 and len(payload) <= LIMIT, 'invalid voter reply')
                value = decode_json(payload)
                require(isinstance(value, dict), 'invalid voter reply')
                return value
        except urllib.error.HTTPError as error:
            try:
                payload = error.read(LIMIT + 1)
                value = decode_json(payload) if len(payload) <= LIMIT else None
            except (OSError, ValueError, TypeError):
                value = None
            raise VoterFailure('voter refused; reconcile the original operation',
                               status=error.code, reply=value) from None
        except (OSError, ValueError) as error:
            raise VoterFailure('unknown voter outcome; reconcile the original operation') from error

    def _assigned(self, catalog, namespace, group, incarnation, action):
        history = {'group': identifier(group, 16), 'incarnation': identifier(incarnation, 16)}
        return self.launcher.assign_histories(self.generation, catalog, namespace, [history],
                                              lambda: self._request(action))

    def start(self, catalog, namespace, group, incarnation):
        return self._assigned(catalog, namespace, group, incarnation, {
            'action': 'start', 'group': list(bytes.fromhex(group)),
            'incarnation': list(bytes.fromhex(incarnation)),
        })

    def prepare_seed(self, catalog, namespace, group, incarnation):
        return self._assigned(catalog, namespace, group, incarnation, {
            'action': 'prepare_seed', 'group': list(bytes.fromhex(group)),
            'incarnation': list(bytes.fromhex(incarnation)),
        })

    def seed(self, catalog, namespace, group, incarnation):
        return self._assigned(catalog, namespace, group, incarnation, {
            'action': 'seed', 'group': list(bytes.fromhex(group)),
            'incarnation': list(bytes.fromhex(incarnation)),
        })

    def attach(self, catalog, namespace, group, incarnation, operation, plan, seeds):
        return self._assigned(catalog, namespace, group, incarnation, {
            'action': 'attach', 'group': list(bytes.fromhex(group)),
            'incarnation': list(bytes.fromhex(incarnation)),
            'operation': list(bytes.fromhex(identifier(operation, 16))),
            'plan_digest': list(bytes.fromhex(identifier(plan, 32))),
            'seed_digest': list(bytes.fromhex(identifier(seeds, 32))),
        })

    def activate(self, catalog, namespace, group, incarnation, operation, plan, seeds):
        return self._assigned(catalog, namespace, group, incarnation, {
            'action': 'activate', 'group': list(bytes.fromhex(group)),
            'incarnation': list(bytes.fromhex(incarnation)),
            'operation': list(bytes.fromhex(identifier(operation, 16))),
            'plan_digest': list(bytes.fromhex(identifier(plan, 32))),
            'seed_digest': list(bytes.fromhex(identifier(seeds, 32))),
        })
