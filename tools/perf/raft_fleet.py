#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Provision fresh managed Colima nodes; journal explicit operator requests.

Provisioning stops at Registered. Platform qualification must supply retained
capacity evidence before Ready -> Docker binding -> Active. No performance or
failure-recovery qualification is inferred from successful startup.
"""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import time

from raft_run import ROOT, RUSTFS, read_json, request, require, verify_image
from run import create_bucket
from raft_topology import members
from raft_fleet_storage import Storage, TMPFS_BYTES, validate_storage

LABEL = 'io.cellar.fleet.lab'


def save_new(path, value):
    """Persist the exact intent before dispatch; never overwrite a prior request."""
    with path.open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    sync_existing(path)


def sync_existing(path):
    # Also called on retries: a preceding process may have written a complete
    # record but failed before syncing either the file or its directory.
    with path.open('rb') as stream:
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def engine(context, failure_directory=None):
    require(context == 'colima-cellar-raft-perf', 'use the dedicated cellar-raft-perf profile')

    def docker(*args):
        result = subprocess.run(['docker', '--context', context, *args],
                                capture_output=True, text=True)
        # Environment arguments contain credentials; do not echo the command.
        if result.returncode != 0 and failure_directory is not None:
            save_new(failure_directory / f'docker-failure-{time.time_ns()}.json',
                     {'operation': args[0], 'exit_code': result.returncode, 'stderr': result.stderr[:65536]})
        require(result.returncode == 0, f'Docker {args[0]} failed (exit {result.returncode})')
        return result.stdout.strip()
    return docker


def validate_policy(policy):
    require(type(policy['nodes']) is int and 3 <= policy['nodes'] <= 5, 'three to five nodes required')
    require(1 <= policy['group_capacity'] <= 4096, 'invalid group capacity')
    require(1024**2 <= policy['image_bytes'] <= 64 * 1024**2 and policy['image_bytes'] % 4096 == 0,
            'invalid image quota')
    charge = policy['image_bytes'] * 8
    require(policy['extent_bytes'] >= charge and policy['replay_extent_bytes'] >= charge,
            'extent inventories cannot cover image work')
    validate_storage(policy)
    require(policy['durable_bytes'] >= policy['group_capacity'] * policy['image_bytes'] * 8
            + policy['resources']['durable_overhead_bytes'], 'unbacked image reservations')
    owner = policy['owner']
    require(isinstance(owner, str) and 1 <= len(owner) <= 64
            and all(c.isascii() and (c.isalnum() or c == '-') for c in owner), 'invalid owner')


def provision(args):
    policy = read_json(args.policy)
    validate_policy(policy)
    count = policy['nodes']
    require(1024 <= args.port_base <= 65535 - count, 'invalid port range')
    require(1 <= args.cells <= 10000, 'invalid cell count')
    require(policy['group_capacity'] >= max(
        sum(node in members(cell, count) for cell in range(args.cells))
        for node in range(1, count + 1)),
            'group capacity is below the planned replicas per node')
    docker = engine(args.context)
    # An existing owned deployment is never replaced by an implicit cleanup.
    require(not docker('ps', '-aq', '--filter', f'label={LABEL}'), 'retained managed deployment exists')
    info = json.loads(docker('info', '--format', '{{json .}}'))
    require(info['OSType'] == 'linux' and info['NCPU'] >= count + 2
            and info['MemTotal'] >= (count * 3 + 4) * 1024**3, 'insufficient VM CPU/memory')
    build = verify_image(args.image, docker)
    root = args.output.resolve()
    require('.cellar' not in root.parts and 'evidence' not in root.parts, 'use an external qualification directory')
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    config = root / 'config'
    config.mkdir(mode=0o700)
    token, access, secret = (secrets.token_hex(32) for _ in range(3))
    (config / 'token').write_text(token)
    save_new(config / 'store.json', {'access_key': access, 'secret_key': secret})
    for index in range(1, count + 1):
        (config / f'process{index}').write_bytes(secrets.token_bytes(16))
    for name in ('ca.der', 'server.der', 'key.der'):
        shutil.copyfile(ROOT / 'crates/cellar/transport/tcp/fixtures' / name, config / name)
    save_new(config / 'fleet.json', policy)
    for path in config.iterdir():
        path.chmod(0o600)
    run_id = secrets.token_hex(12)
    prefix = f'cellar-fleet-{run_id}'
    label = f'{LABEL}={run_id}'
    report = {'format': 1, 'run_id': run_id, 'context': args.context, 'prefix': prefix,
              'build': build, 'policy': policy, 'port_base': args.port_base,
              'cells': args.cells, 'containers': [], 'registered': False,
              'capacity_qualified': False, 'deployed_recovery_qualified': False,
              'limits': {'cpu_millis': 1000, 'memory_bytes': 3 * 1024**3},
              'limitations': ['One shared VM; node separation only.',
                              'Provisioning alone does not attest disk/memory occupancy or activate capacity.',
                              'No throughput, p99, density, recovery, restart or soak qualification.']}
    save_new(root / 'intent.json', report)
    docker = engine(args.context, root)
    try:
        storage = Storage(docker, save_new, root, prefix, label, args.image, policy,
                          args.backing_disk, args.host_backing)
        storage.prepare()
        # Docker copies bytes from the client host. A host bind mount would
        # instead resolve inside Colima, where external workspace volumes may
        # be absent. This helper is never started and owns only this config.
        config_volume = prefix + '-config'
        docker('volume', 'create', '--label', label, config_volume)
        config_helper = docker('create', '--name', prefix + '-config-copy', '--network', 'none',
                               '--label', label, '-v', config_volume + ':/config',
                               '--entrypoint', '/bin/true', args.image)
        report['config_helper'] = config_helper
        report['config_volume'] = config_volume
        docker('cp', str(config) + '/.', config_helper + ':/config')
        network_id = docker('network', 'create', '--label', label, prefix)
        network = json.loads(docker('network', 'inspect', network_id))[0]
        subnet = ipaddress.ip_network(network['IPAM']['Config'][0]['Subnet'])
        require(subnet.version == 4 and subnet.prefixlen <= 24, 'unsupported subnet')
        network_prefix = '.'.join(str(subnet.network_address).split('.')[:3])
        store = prefix + '-store'
        storage.create('store', policy['storage']['object_store_bytes'])
        docker('run', '--rm', '--network', 'none', '--label', label, '-v', store + ':/data',
               '--entrypoint', 'chown', args.image, '10001:10001', '/data')
        store_id = docker('run', '-d', '--name', store, '--label', label, '--restart=no',
                         '--read-only', '--tmpfs', '/tmp:rw,nosuid,nodev,size=134217728',
                         '--tmpfs', '/logs:rw,nosuid,nodev,size=33554432,uid=10001,gid=10001',
                         '--network', prefix, '--network-alias', 'rustfs', '--cpus', '2',
                         '--log-driver', 'local', '--log-opt', 'max-size=8m', '--log-opt', 'max-file=2',
                         '--memory', '4g', '--memory-swap', '4g',
                         '-p', f'127.0.0.1:{args.port_base}:9000', '-v', store + ':/data',
                         '-e', 'RUSTFS_ACCESS_KEY=' + access, '-e', 'RUSTFS_SECRET_KEY=' + secret,
                         '-e', 'RUSTFS_CONSOLE_ENABLE=false', RUSTFS, '/data')
        report['store'] = store_id
        deadline = time.monotonic() + 120
        while True:
            try:
                create_bucket(f'http://127.0.0.1:{args.port_base}', 'raft-perf', access, secret)
                break
            except Exception:
                require(time.monotonic() < deadline, 'RustFS startup deadline')
                time.sleep(1)
        common = ['--network', prefix, '--cpus', '1', '--memory', '3g', '--memory-swap', '3g',
                  '--read-only', '--tmpfs', f'/tmp:rw,nosuid,nodev,size={TMPFS_BYTES}',
                  '--log-driver', 'local', '--log-opt', 'max-size=8m', '--log-opt', 'max-file=2',
                  '-v', config_volume + ':/config:ro', '-e', f'RAFT_NODES={count}',
                  '-e', f'RAFT_CELLS={args.cells}', '-e', 'RAFT_FLEET_CONFIG=/config/fleet.json',
                  '-e', f'RAFT_NETWORK_PREFIX={network_prefix}',
                  '-e', 'AWS_ACCESS_KEY_ID=' + access, '-e', 'AWS_SECRET_ACCESS_KEY=' + secret]
        boot = json.loads(docker('run', '--rm', '--label', label, *common,
                                '-e', 'RAFT_NODE=1', '-e', 'RAFT_FLEET_INIT=1', args.image))
        report['bootstrap'] = boot
        for index in range(1, count + 1):
            name = f'{prefix}-node{index}'
            mounts = storage.node_mounts(index)
            node_id = bytes([index] * 16).hex()
            container = docker('create', '--name', name, '--label', label,
                               '--label', f"io.cellar.fleet.owner={policy['owner']}",
                               '--label', f"io.cellar.fleet.catalog={boot['catalog']}",
                               '--label', f'io.cellar.fleet.node={node_id}',
                               '--label', 'io.cellar.fleet.boot-mode=single-use',
                               '--label', 'io.cellar.fleet.workers=contained',
                               '--restart=no', '--ip', f'{network_prefix}.{index+10}', *common,
                               '--ulimit', 'nofile=65536:65536',
                               *mounts, '-v', '/var/run/docker.sock:/var/run/docker.sock',
                               '-p', f'127.0.0.1:{args.port_base+index}:8082',
                               '-e', f'RAFT_NODE={index}', args.image)
            (config / f'container{index}').write_text(container + '\n')
            (config / f'container{index}').chmod(0o600)
            docker('cp', str(config / f'container{index}'), config_helper + f':/config/container{index}')
            report['containers'].append(container)
            save_new(root / f'container-{index}.json', {'container': container, 'mounts': mounts})
            docker('start', container)
            deadline = time.monotonic() + 120
            while True:
                try:
                    code, status = request(f'http://127.0.0.1:{args.port_base+index}', '/fleet', token)
                    if code == 200:
                        require(status['state'] == 'Registered' and status['boot'] is not None
                                and status['registered_histories'] == 0, 'unexpected managed startup state')
                        save_new(root / f'node-{index}.json', status)
                        break
                except (OSError, ValueError):
                    pass
                require(time.monotonic() < deadline, f'node {index} startup deadline')
                time.sleep(1)
        storage.verify()
        report['registered'] = True
        report['bounded_storage'] = True
    finally:
        save_new(root / 'report.json', report)
    print(f'Managed nodes Registered; resources retained in {args.context}: {prefix}')


def apply(args):
    root = args.run.resolve()
    report = read_json(root / 'report.json')
    require(report['registered'], 'deployment startup was incomplete')
    require(1 <= args.node <= len(report['containers']), 'invalid node')
    body = read_json(args.command)
    require(len(json.dumps(body).encode()) <= 16 * 1024, 'command exceeds limit')
    intent = {'node': args.node, 'command': body}
    digest = hashlib.sha256(json.dumps(intent, sort_keys=True).encode()).hexdigest()
    path = root / f'operator-{digest}.json'
    if path.exists():
        require(read_json(path) == intent, 'retained intent conflict')
    else:
        save_new(path, intent)
    sync_existing(path)
    # Re-running the same command sends the same identity, revision and payload.
    # A conflict never causes this tool to create a different request implicitly.
    code, response = request(f"http://127.0.0.1:{report['port_base']+args.node}", '/fleet',
                             (root / 'config/token').read_text().strip(), method='POST',
                             body=body, timeout=35)
    save_new(root / f'operator-{digest}-response-{time.time_ns()}.json', {'status': code, 'response': response})
    print(json.dumps({'status': code, 'response': response}))
    require(code == 200, 'operator outcome unconfirmed; reconcile the retained exact request')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='mode', required=True)
    setup = commands.add_parser('provision')
    setup.add_argument('--context', default='colima-cellar-raft-perf')
    setup.add_argument('--image', required=True)
    setup.add_argument('--policy', type=Path, required=True)
    setup.add_argument('--output', type=Path, required=True)
    setup.add_argument('--backing-disk', type=Path, default=Path.home()
                       / '.colima/_lima/_disks/colima-cellar-raft-perf/datadisk')
    setup.add_argument('--host-backing', type=Path,
                       help='Existing directory mounted at the same guest path through virtiofs')
    setup.add_argument('--cells', type=int, default=15)
    setup.add_argument('--port-base', type=int, default=19800)
    operator = commands.add_parser('apply')
    operator.add_argument('--run', type=Path, required=True)
    operator.add_argument('--node', type=int, required=True)
    operator.add_argument('--command', type=Path, required=True)
    args = parser.parse_args()
    provision(args) if args.mode == 'provision' else apply(args)


if __name__ == '__main__':
    main()
