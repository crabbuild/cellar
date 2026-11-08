# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Per-cohort ext4 disk bounds on an explicitly bound, dedicated Colima Engine.

Backing files are exclusively created and prewritten before formatting. Lost
mutation replies are never retried. Stop retains devices, files and volumes.
"""
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess

from cell_group_resources import save
from raft_fleet_storage import loop_binding
from raft_run import LABEL, require

MIB = 1024**2
GIB = 1024**3
RESERVE = 4 * GIB
RUNTIME = ['--read-only', '--tmpfs', '/tmp:rw,noexec,nosuid,size=67108864',
           '--log-driver', 'json-file', '--log-opt', 'max-size=5m', '--log-opt', 'max-file=2']


def validate(policy, context):
    require(isinstance(policy, dict) and set(policy) == {'colima_home', 'profile', 'backing_disk', 'node_bytes', 'object_store_bytes'},
            'explicit owned disk policy required')
    require(context == 'colima-' + policy['profile'] and re.fullmatch(r'cellar-multi-raft(?:-[a-z0-9-]+)?', policy['profile']),
            'disk profile differs from dedicated Engine context')
    home = Path(policy['colima_home'])
    require(home.is_absolute() and home.is_dir(), 'Colima home must exist')
    expected = home / '_lima/_disks' / context / 'datadisk'
    disk = Path(policy['backing_disk'])
    require(disk.is_absolute() and disk.is_file() and disk.resolve() == expected.resolve(),
            'disk is not the dedicated Colima backing file')
    for key in ('node_bytes', 'object_store_bytes'):
        n = policy[key]
        require(type(n) is int and 128 * MIB <= n <= 64 * GIB and n % MIB == 0, 'invalid explicit disk bound')
    return disk.resolve()


class Disks:
    def __init__(self, engine, policy, directory=None, owner=None, image=None, docker=None):
        self.engine, self.policy = engine, policy
        self.backing_disk = validate(policy, engine.context)
        self.root, self.owner, self.image, self.docker = directory, owner, image, docker
        self.volumes = {}
        self.backing = None

    def guest(self, *args):
        env = dict(os.environ, COLIMA_HOME=self.policy['colima_home'])
        result = subprocess.run(['colima', 'ssh', '--profile', self.policy['profile'], '--', *map(str, args)],
                                capture_output=True, text=True, timeout=300, env=env)
        require(result.returncode == 0 and len(result.stdout) <= 65536, 'owned disk command failed; no automatic retry')
        return result.stdout.strip()

    def preflight(self, total):
        self.engine.bind()
        require(self.guest('sudo', 'docker', 'info', '--format', '{{.ID}}') == self.engine.identity,
                'Colima SSH and context identify different Engines')
        fs = os.statvfs(self.backing_disk)
        host = fs.f_frsize * fs.f_bavail
        guest = json.loads(self.guest('df', '-B1', '--output=avail', '/var/lib/docker')
                           .splitlines()[-1].strip())
        require(type(guest) is int and min(host, guest) >= total + RESERVE,
                'insufficient backing for all retained cohorts and disk reserve')
        return {'host_available_bytes': host, 'guest_available_bytes': guest,
                'planned_bytes': total, 'reserve_bytes': RESERVE, 'backing_disk': str(self.backing_disk)}

    def prepare(self):
        require(self.root is not None and re.fullmatch(r'[0-9a-f]{24}', self.owner), 'invalid disk owner')
        prefix = 'cellar-mr14-' + self.owner
        backing_volume = prefix + '-backing'
        self.docker('volume', 'create', '--label', LABEL + '=' + self.owner, backing_volume)
        raw = json.loads(self.docker('volume', 'inspect', backing_volume))[0]
        docker_root = self.engine.invoke('info', '--format', '{{.DockerRootDir}}')
        expected = str(PurePosixPath(docker_root) / 'volumes' / backing_volume / '_data')
        require(raw['Driver'] == 'local' and not raw.get('Options') and raw['Mountpoint'] == expected
                and raw.get('Labels', {}).get(LABEL) == self.owner, 'backing volume is not owned')
        self.backing = expected
        save(self.root / 'disk-intent.json', {'owner': self.owner, 'engine_id': self.engine.identity,
              'backing_volume': backing_volume, 'backing_directory': expected, 'policy': self.policy})

    def create(self, name):
        require(self.backing is not None and re.fullmatch(r'cellar-mr14-' + self.owner + r'-(store|node[1-5])', name),
                'disk name does not belong to cohort')
        require(name not in self.volumes, 'disk allocation cannot be repeated')
        size = self.policy['object_store_bytes' if name.endswith('-store') else 'node_bytes']
        backing = self.backing + '/' + name + '.img'
        save(self.root / (name + '-disk-intent.json'), {'name': name, 'backing': backing, 'bytes': size})
        self.guest('sudo', 'dd', 'if=/dev/urandom', 'of=' + backing, 'bs=1048576',
                   'count=' + str(size // MIB), 'oflag=direct', 'conv=excl,fsync', 'status=none')
        device = self.guest('sudo', 'losetup', '--find', '--show', '--nooverlap', backing)
        loop_binding(json.loads(self.guest('sudo', 'losetup', '--list', '--json', '--output', 'NAME,BACK-FILE', device)), device, backing)
        save(self.root / (name + '-format-intent.json'), {'device': device, 'backing': backing, 'bytes': size})
        self.guest('sudo', 'mkfs.ext4', '-q', '-F', '-m', '0', '-i', '16384', '-J', 'size=' + str(max(4, size // MIB // 64)),
                   '-E', 'nodiscard,lazy_itable_init=0,lazy_journal_init=0', device)
        identity = self.guest('sudo', 'blkid', '-s', 'UUID', '-o', 'value', device)
        require(re.fullmatch(r'[0-9a-f-]{36}', identity), 'missing bounded filesystem identity')
        self.docker('volume', 'create', '--label', LABEL + '=' + self.owner, '--driver', 'local',
                    '--opt', 'type=ext4', '--opt', 'device=' + device, '--opt', 'o=rw,nodiscard', name)
        self.docker('run', '--rm', '--network', 'none', '--label', LABEL + '=' + self.owner,
                    '-v', name + ':/disk', '--entrypoint', 'rmdir', self.image, '/disk/lost+found')
        measured = json.loads(self.docker('run', '--rm', '--network', 'none', '--label', LABEL + '=' + self.owner,
                    '-v', name + ':/disk:ro', '--entrypoint', 'stat', self.image,
                    '-f', '-c', '{"block_size":%S,"blocks":%b,"available":%a}', '/disk'))
        available = measured['block_size'] * measured['available']
        require(size * 9 // 10 <= available <= size, 'filesystem usable capacity differs from declared disk bound')
        self.volumes[name] = {'volume': name, 'backing': backing, 'device': device, 'uuid': identity,
                              'bytes': size, 'available_bytes': available, 'filesystem': measured}
        save(self.root / 'disks.json', {'owner': self.owner, 'engine_id': self.engine.identity, 'volumes': self.volumes})
        return name

    def verify(self):
        result = []
        for name, row in self.volumes.items():
            binding = json.loads(self.guest('sudo', 'losetup', '--list', '--json', '--output', 'NAME,BACK-FILE', row['device']))
            loop_binding(binding, row['device'], row['backing'])
            identity = self.guest('sudo', 'blkid', '-s', 'UUID', '-o', 'value', row['device'])
            size = int(self.guest('sudo', 'blockdev', '--getsize64', row['device']))
            require(identity == row['uuid'] and size == row['bytes'], 'bounded disk identity or size changed')
            volume = json.loads(self.docker('volume', 'inspect', name))[0]
            require(volume['Name'] == name and volume.get('Labels', {}).get(LABEL) == self.owner
                    and volume['Driver'] == 'local' and volume['Options'] == {'type': 'ext4', 'device': row['device'], 'o': 'rw,nodiscard'},
                    'bounded volume options changed')
            result.append({'volume': volume, 'loop_binding': binding, 'uuid': identity, 'device_bytes': size})
        save(self.root / 'disk-verification.json', result)


def verify_evidence(root, ownership, policy, nodes):
    from raft_run import read_json
    record = read_json(root / 'disks.json')
    require(record['owner'] == ownership['owner'] and record['engine_id'] == ownership['engine_id'], 'bounded disk owner differs')
    expected = {f"cellar-mr14-{ownership['owner']}-node{n}" for n in range(1, nodes + 1)} | {f"cellar-mr14-{ownership['owner']}-store"}
    require(set(record['volumes']) == expected, 'incomplete bounded disk inventory')
    observations = read_json(root / 'disk-verification.json')
    require(len(observations) == len(expected) and {row['volume']['Name'] for row in observations} == expected, 'disk verification inventory differs')
    for observed in observations:
        raw = observed['volume']
        row = record['volumes'][raw['Name']]
        loop_binding(observed['loop_binding'], row['device'], row['backing'])
        require(observed['uuid'] == row['uuid'] and type(observed['device_bytes']) is int
                and observed['device_bytes'] == row['bytes'], 'raw bounded device size or identity differs')
        size = policy['object_store_bytes' if raw['Name'].endswith('-store') else 'node_bytes']
        require(row['bytes'] == size and raw['Labels'][LABEL] == ownership['owner']
                and raw['Options'] == {'type': 'ext4', 'device': row['device'], 'o': 'rw,nodiscard'}
                and size * 9 // 10 <= row['available_bytes'] <= size, 'disk bound differs from raw evidence')
    return {'bounded_volumes': len(expected), 'bytes': sum(row['bytes'] for row in record['volumes'].values())}
