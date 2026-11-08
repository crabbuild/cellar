# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Explicit Engine binding and retained ownership journal for CG-14 cohorts.

No delete, prune, context switching or automatic disk erasure is implemented.
Stopping verifies every immutable container identity before signalling any owner.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import time

from raft_run import LABEL, read_json, require

IDENTITY = ('{"id":{{json .Id}},"name":{{json .Name}},'
            '"owner":{{json (index .Config.Labels "' + LABEL + '")}},'
            '"running":{{json .State.Running}},"paused":{{json .State.Paused}},'
            '"exit_code":{{json .State.ExitCode}},"finished":{{json .State.FinishedAt}}}')


def save(path, value):
    """Sync a replacement and its directory; credentials never enter this record."""
    temporary = path.with_suffix(path.suffix + '.tmp')
    with open(temporary, 'w', opener=lambda p, flags: os.open(p, flags | os.O_NOFOLLOW, 0o600)) as stream:
        json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class Engine:
    def __init__(self, context, identity, invoke=None):
        require(isinstance(context, str) and re.fullmatch(r'colima-cellar-multi-raft(?:-[a-z0-9-]+)?', context),
                'an explicit dedicated Docker context is required')
        require(isinstance(identity, str) and re.fullmatch(r'[A-Za-z0-9:-]{8,128}', identity),
                'an explicit Engine identity is required')
        self.context, self.identity = context, identity
        self.invoke = invoke or self.command

    def command(self, *args):
        # Never render argv, environment, unrestricted inspect or stderr: run
        # commands contain generated credentials. Exact logs stay in private roots.
        completed = subprocess.run(['docker', '--context', self.context, *args],
                                   capture_output=True, text=True, timeout=300)
        require(completed.returncode == 0, f'Docker {args[0]} failed; arguments omitted')
        return (completed.stdout + (completed.stderr if args[0] == 'logs' else '')).strip()

    def bind(self):
        require(self.invoke('info', '--format', '{{.ID}}') == self.identity,
                'Docker context now resolves to a different Engine')

    def inspect(self, name, owner, expected=None):
        value = json.loads(self.invoke('inspect', '--format', IDENTITY, expected or name))
        require(value['name'] == '/' + name and value['owner'] == owner
                and re.fullmatch(r'[0-9a-f]{64}', value['id'])
                and (expected is None or value['id'] == expected),
                'container name, label or immutable identity differs')
        require(type(value['running']) is bool and type(value['paused']) is bool,
                'missing container lifecycle observation')
        return value


class OwnedRun:
    def __init__(self, engine, directory, owner, campaign):
        require(re.fullmatch(r'[0-9a-f]{24}', owner) is not None, 'invalid cohort owner')
        self.engine, self.root, self.owner = engine, Path(directory), owner
        self.path = self.root / 'resources.json'
        self.state = {'format': 1, 'context': engine.context, 'engine_id': engine.identity,
                      'directory': str(self.root.resolve()), 'campaign': campaign,
                      'owner': owner, 'containers': {}, 'volumes': [], 'networks': [], 'intents': [], 'stops': []}
        require(not self.path.exists(), 'cohort ownership journal already exists')
        save(self.path, self.state)

    def __call__(self, *args):
        mutation = args[0] in ('run', 'create') or args[:2] in (('volume', 'create'), ('network', 'create'))
        if not mutation:
            if args[0] == 'stats':
                require(args[1:4] == ('--no-stream', '--format', '{{json .}}')
                        and 1 <= len(args[4:]) <= 6 and len(set(args[4:])) == len(args[4:]),
                        'stats requires bounded nonstreaming owned targets')
                require(all(name in self.state['containers'] and self.state['containers'][name]['kind'] == 'process'
                            for name in args[4:]), 'stats cannot observe an unowned process')
                self.engine.bind()
                identities = []
                for name in args[4:]:
                    row = self.state['containers'][name]
                    self.engine.inspect(name, self.owner, row['id'])
                    identities.append(row['id'])
                return self.engine.invoke(*args[:4], *identities)
            if args[0] == 'cp':
                require(len(args) == 3, 'unsupported config copy')
                source, destination = Path(args[1]).resolve(), args[2]
                require(source == (self.root / 'workload/config').resolve()
                        and any(destination == row['id'] + ':/config'
                                for row in self.state['containers'].values() if row['kind'] == 'config'),
                        'config copy must use the exact owned source and helper')
                self.engine.bind()
                return self.engine.invoke(*args)
            require(args[0] in ('info', 'image', 'ps', 'inspect', 'exec', 'logs', 'network', 'volume'),
                    'unsupported cohort Engine operation')
            require(args[0] not in ('network', 'volume') or args[1] == 'inspect',
                    'unsupported resource mutation')
            require(args[0] != 'image' or args[1] == 'inspect', 'unsupported image mutation')
            if args[0] in ('logs', 'exec'):
                name = args[-1] if args[0] == 'logs' else args[1]
                require(name in self.state['containers'], 'unowned container observation')
                row = self.state['containers'][name]
                self.engine.inspect(name, self.owner, row['id'])
            return self.engine.invoke(*args)
        self.engine.bind()
        labels = [args[i + 1] for i, arg in enumerate(args[:-1]) if arg == '--label']
        require(f'{LABEL}={self.owner}' in labels, 'resource creation lacks the exact cohort label')
        if args[0] in ('run', 'create'):
            for index, argument in enumerate(args[:-1]):
                if argument == '-v':
                    require(args[index + 1].split(':', 1)[0] in self.state['volumes'],
                            'container cannot mount an unowned volume or host directory')
                elif argument == '--mount':
                    raise ValueError('unsupported mount syntax; use recorded owned volumes')
                elif argument == '--network':
                    require(args[index + 1] == 'none' or args[index + 1] in self.state['networks'],
                            'container cannot join an unowned network')
        else:
            require(args[-1].startswith('cellar-mr14-' + self.owner + '-')
                    or (args[0] == 'network' and args[-1] == 'cellar-mr14-' + self.owner),
                    'resource name does not belong to the cohort')
        name = args[args.index('--name') + 1] if '--name' in args else None
        if args[0] == 'create' or '-d' in args or '--detach' in args:
            require(name is not None and name.startswith('cellar-mr14-' + self.owner + '-'),
                    'persistent container requires the exact cohort name')
        # A lost Docker reply keeps the original intent. It is never retried with
        # another identity. Stop can bind the exact retained name and label later.
        self.state['intents'].append({'operation': list(args[:2]) if args[0] != 'run' else ['run'],
                                      'name': name, 'at_ns': time.monotonic_ns()})
        save(self.path, self.state)
        result = self.engine.invoke(*args)
        if args[:2] in (('volume', 'create'), ('network', 'create')):
            self.state['volumes' if args[0] == 'volume' else 'networks'].append(args[-1])
            save(self.path, self.state)
        if name is not None:
            identity = self.engine.inspect(name, self.owner)
            require(result == identity['id'], 'creation reply differs from immutable container identity')
            self.state['containers'][name] = {'id': identity['id'], 'kind': 'config' if args[0] == 'create' else 'process'}
            save(self.path, self.state)
        return result


def stop(directory, campaign, engine_factory=Engine):
    """Join only this cohort; retain every container, volume, network and log."""
    root = Path(directory)
    path = root / 'resources.json'
    state = read_json(path)
    require(state['format'] == 1 and state['directory'] == str(root.resolve())
            and state['campaign'] == campaign, 'ownership directory or campaign differs')
    require(re.fullmatch(r'[0-9a-f]{24}', state['owner']) is not None, 'invalid retained owner')
    engine = engine_factory(state['context'], state['engine_id'])
    engine.bind()
    names = set(state['containers']) | {item['name'] for item in state['intents'] if item['name']}
    observed = {}
    # Validate the entire roster before the first signal. A removed or relabelled
    # resource refuses; it cannot be adopted or silently omitted from joined work.
    for name in sorted(names):
        require(name.startswith('cellar-mr14-' + state['owner'] + '-'), 'retained name is not owned')
        expected = state['containers'].get(name, {}).get('id')
        observed[name] = engine.inspect(name, state['owner'], expected)
    for name, before in observed.items():
        engine.bind()
        engine.inspect(name, state['owner'], before['id'])
        if before['paused']:
            engine.invoke('unpause', before['id'])
        if before['running']:
            engine.invoke('stop', '--time', '10', before['id'])
        after = engine.inspect(name, state['owner'], before['id'])
        require(not after['running'] and not after['paused'], 'container failed to join stop')
        state['containers'][name] = {'id': before['id'], 'kind': 'config' if name.endswith('-config-copy') else 'process'}
        state['stops'].append({'before': before, 'after': after, 'at_ns': time.monotonic_ns()})
        save(path, state)
    return {'containers': len(observed),
            'joined_processes': sum(row.get('kind', 'process') == 'process' for row in state['containers'].values()),
            'data_retained': True}
