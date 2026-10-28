#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Retained, bounded ext4 volumes for the exclusively owned Colima lab.

Every filesystem has its own prewritten backing file and loop device. Nothing
is reformatted, detached or deleted on retry. These are single-boot lab volumes;
after a VM restart, re-establish the recorded backing/UUID mapping before any
retained-volume maintenance. Never restart the single-use native containers.
"""
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import time

from raft_run import require

MIB = 1024 ** 2
GIB = 1024 ** 3
LOG_BYTES = 32 * MIB
TMPFS_BYTES = 128 * MIB
HOST_RESERVE = 4 * GIB
MOUNTS = {'metadata': '/data', 'raft': '/data/node/raft',
          'images': '/data/node/raft-cells', 'replay': '/data/fleet-replay'}


def validate_storage(policy):
    limits = policy['storage']
    require(set(limits) == {'metadata_bytes', 'raft_bytes', 'images_bytes',
                           'replay_bytes', 'object_store_bytes', 'snapshot_scratch_bytes',
                           'extent_cache_bytes'},
            'explicit storage bounds required')
    require(all(type(n) is int and 64 * MIB <= n <= 64 * GIB and n % MIB == 0
                for n in limits.values()), 'invalid storage bound')
    require(limits['snapshot_scratch_bytes'] * 2 <= limits['raft_bytes'],
            'snapshot scratch leaves no shared-log headroom')
    charge = policy['image_bytes'] * 8
    # Conservative lab preflight for current native workspace policy: 512 MiB
    # WAL, up to 64 MiB per capture (three), 16 MiB housekeeping and five images.
    # Native Config also derives and validates the actual execution reservation.
    require(policy['extent_bytes'] >= 720 * MIB + 5 * policy['image_bytes'],
            'extent inventory cannot cover native WAL and capture ownership')
    require(limits['snapshot_scratch_bytes'] >= charge, 'snapshot scratch below image work')
    images = policy['group_capacity'] * charge + policy['extent_bytes'] + charge + limits['extent_cache_bytes']
    require(images <= limits['images_bytes'] * 9 // 10,
            'image filesystem cannot cover private images, extents and archive replay')
    require(policy['replay_extent_bytes'] + charge <= limits['replay_bytes'] * 9 // 10,
            'replay filesystem cannot cover its inventories')
    overhead = (limits['metadata_bytes'] + limits['raft_bytes'] + limits['replay_bytes']
                + policy['extent_bytes'] + charge + limits['extent_cache_bytes'] + LOG_BYTES)
    require(policy['resources']['durable_overhead_bytes'] >= overhead,
            'durable overhead omits bounded storage pools')
    capacity = sum(limits[role + '_bytes'] for role in MOUNTS)
    require(policy['durable_bytes'] <= capacity * 9 // 10,
            'usable disk budget omits filesystem overhead')
    return policy['nodes'] * capacity + limits['object_store_bytes']


def loop_binding(value, device, backing):
    require(re.fullmatch(r'/dev/loop[0-9]+', device) is not None, 'unexpected loop device')
    rows = value.get('loopdevices')
    require(isinstance(rows, list) and len(rows) == 1
            and rows[0].get('name') == device and rows[0].get('back-file') == backing,
            'loop device does not own the retained backing file')


class Storage:
    def __init__(self, docker, save, root, prefix, label, image, policy, backing_disk):
        self.docker, self.save, self.root = docker, save, root
        self.prefix, self.label, self.image, self.policy = prefix, label, image, policy
        require(re.fullmatch(r'cellar-fleet-[0-9a-f]{24}', prefix) is not None, 'invalid storage owner')
        self.total = validate_storage(policy)
        # The explicit active VM data disk pins which host filesystem supplies
        # thin backing capacity. The caller retains the VM/disk identity evidence.
        expected_disk = Path.home() / '.colima/_lima/_disks/colima-cellar-raft-perf/datadisk'
        require(backing_disk.resolve(strict=True) == expected_disk.resolve(strict=True),
                'storage must use the dedicated Colima data disk')
        self.backing_disk = backing_disk.resolve(strict=True)
        require(self.backing_disk.is_file(), 'missing Colima data disk')
        self.check_host_space(self.total)
        self.volumes = {}

    def check_host_space(self, remaining=0):
        fs = os.statvfs(self.backing_disk)
        available = fs.f_bavail * fs.f_frsize
        require(available >= remaining + HOST_RESERVE, 'insufficient physical VM backing space')
        return available

    def guest(self, *args):
        # Colima preserves argument boundaries. Do not add another shell layer.
        result = subprocess.run(['colima', 'ssh', '--profile', 'cellar-raft-perf', '--',
                                 *[str(arg) for arg in args]],
                                capture_output=True, text=True, timeout=300)
        if result.returncode != 0 and self is not None:
            self.save(self.root / f'storage-failure-{time.time_ns()}.json', {
                'exit': result.returncode, 'stderr': result.stderr[:65536]})
        require(result.returncode == 0, 'owned Colima storage command failed')
        require(len(result.stdout) <= 65536, 'oversized storage response')
        return result.stdout.strip()

    def prepare(self):
        engine = json.loads(self.docker('info', '--format', '{{json .}}'))
        require(self.guest('sudo', 'docker', 'info', '--format', '{{.ID}}') == engine['ID'],
                'Colima SSH and Docker context identify different Engines')
        require(not self.docker('volume', 'ls', '-q', '--filter', 'name=' + self.prefix),
                'retained storage owner already exists')
        self.docker('volume', 'create', '--label', self.label, self.prefix + '-backing')
        volume = json.loads(self.docker('volume', 'inspect', self.prefix + '-backing'))[0]
        expected = PurePosixPath(engine['DockerRootDir']) / 'volumes' / (self.prefix + '-backing') / '_data'
        require(volume['Mountpoint'] == str(expected)
                and volume.get('Labels', {}).get('io.cellar.fleet.lab') == self.prefix[13:]
                and volume['Driver'] == 'local' and not volume.get('Options'),
                'unexpected backing volume identity')
        self.directory = str(expected)
        self.save(self.root / 'storage-intent.json', {
            'engine': engine['ID'], 'backing_disk': str(self.backing_disk),
            'backing_directory': self.directory, 'planned_bytes': self.total,
            'host_available_before': self.check_host_space(self.total),
            'host_reserve_bytes': HOST_RESERVE,
        })

    def create(self, name, size):
        require(re.fullmatch(r'(node[1-5]-(metadata|raft|images|replay)|store)', name) is not None,
                'invalid owned disk name')
        require(name not in self.volumes and 64 * MIB <= size <= 64 * GIB and size % MIB == 0,
                'invalid or repeated disk allocation')
        backing = f'{self.directory}/{name}.img'
        volume = f'{self.prefix}-{name}'
        self.save(self.root / f'storage-{name}-intent.json', {
            'volume': volume, 'backing': backing, 'bytes': size})
        # Random bytes materialize thin backing; fallocate or a sparse truncate
        # would reserve guest addresses without reserving host disk blocks.
        self.guest('sudo', 'dd', 'if=/dev/urandom', 'of=' + backing, 'bs=1048576',
                   'count=' + str(size // MIB), 'conv=excl,fsync', 'status=none')
        self.check_host_space()
        device = self.guest('sudo', 'losetup', '--find', '--show', '--nooverlap', backing)
        binding = json.loads(self.guest('sudo', 'losetup', '--list', '--json',
                                       '--output', 'NAME,BACK-FILE', device))
        loop_binding(binding, device, backing)
        self.save(self.root / f'storage-{name}-format-intent.json', {
            'device': device, 'backing': backing, 'bytes': size})
        # Only this invocation that exclusively created the backing may format
        # it. A lost response is retained for inspection, never retried here.
        self.guest('sudo', 'mkfs.ext4', '-q', '-F', '-m', '0', '-i', '16384', '-E',
                   'nodiscard,lazy_itable_init=0,lazy_journal_init=0', device)
        uuid = self.guest('sudo', 'blkid', '-s', 'UUID', '-o', 'value', device)
        require(re.fullmatch(r'[0-9a-f-]{36}', uuid) is not None, 'missing filesystem UUID')
        self.docker('volume', 'create', '--label', self.label, '--driver', 'local',
                    '--opt', 'type=ext4', '--opt', 'device=' + device,
                    '--opt', 'o=rw,nodiscard', volume)
        # An empty Raft filesystem must contain no pre-existing history. Removing
        # mkfs's empty recovery directory does not touch any native state.
        self.docker('run', '--rm', '--network', 'none', '--label', self.label,
                    '-v', volume + ':/disk', '--entrypoint', 'rmdir', self.image, '/disk/lost+found')
        measured = json.loads(self.docker('run', '--rm', '--network', 'none', '--label', self.label,
                    '-v', volume + ':/disk:ro', '--entrypoint', 'stat', self.image,
                    '-f', '-c', '{"block_size":%S,"blocks":%b,"available":%a}', '/disk'))
        available = measured['block_size'] * measured['available']
        self.save(self.root / f'storage-{name}-measurement.json', measured)
        require(0 < available <= size and available >= size * 9 // 10,
                'filesystem usable capacity is below its planned reserve')
        record = {'volume': volume, 'backing': backing, 'device': device, 'uuid': uuid,
                  'bytes': size, 'available_bytes': available}
        self.save(self.root / f'storage-{name}.json', record)
        self.volumes[name] = record
        print(f'Bounded persistent volume prepared: {name} ({size // MIB} MiB)', flush=True)
        return volume

    def node_mounts(self, node):
        args = []
        for role, destination in MOUNTS.items():
            name = f'node{node}-{role}'
            volume = self.create(name, self.policy['storage'][role + '_bytes'])
            args.extend(['-v', volume + ':' + destination])
        usable = sum(self.volumes[f'node{node}-{role}']['available_bytes'] for role in MOUNTS)
        require(usable >= self.policy['durable_bytes'], 'node usable storage cannot back its profile')
        return args

    def verify(self):
        for record in self.volumes.values():
            volume = json.loads(self.docker('volume', 'inspect', record['volume']))[0]
            require(volume['Driver'] == 'local' and volume.get('Options') == {
                'type': 'ext4', 'device': record['device'], 'o': 'rw,nodiscard'},
                'bounded volume mount changed')
            binding = json.loads(self.guest('sudo', 'losetup', '--list', '--json',
                                           '--output', 'NAME,BACK-FILE', record['device']))
            loop_binding(binding, record['device'], record['backing'])
            require(self.guest('sudo', 'blkid', '-s', 'UUID', '-o', 'value', record['device'])
                    == record['uuid'], 'filesystem identity changed')
        self.save(self.root / 'storage-verification.json', {
            'volumes': self.volumes, 'host_available_after': self.check_host_space(),
            'host_reserve_bytes': HOST_RESERVE,
            'limits': 'Distinct ext4 block devices, prewritten backing, readonly root, bounded logs/tmpfs. '
                      'One shared VM; no independent-host failure qualification.',
        })
