# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Exact-container restart after proven native lease fencing.

This is a separate recovery drill. Missing old-process counters remain unknown;
new-process counters must never be subtracted from the old process's samples.
"""
import json
from datetime import datetime
from pathlib import Path
import re
import time

from raft_workload import Ledger
from workload_recovery import number, read_events, recovery_window, require

SCENARIO = 's3-fenced-recovery'
INSPECT = '{"id":{{json .Id}},"name":{{json .Name}},"owner":{{json (index .Config.Labels "io.cellar.mr14.run")}},"image":{{json .Image}},"state":{{json .State}},"restart_policy":{{json .HostConfig.RestartPolicy.Name}},"mounts":{{json .Mounts}}}'


def observe(docker, node):
    value = json.loads(docker('inspect', '--format', INSPECT, node))
    value['state'] = {key: value['state'][key] for key in ('Running', 'Paused', 'OOMKilled', 'ExitCode', 'StartedAt', 'FinishedAt')}
    raw = docker('logs', value['id'])
    require(len(raw) <= 4 * 1024 * 1024, 'restart log exceeds bound')
    events = []
    for line in raw.splitlines():
        if line.startswith('{'):
            event = json.loads(line)
            if event.get('event') in ('embedded_node_started', 'embedded_node_stopped'):
                events.append(event)
    return {**value, 'events': events, 'monotonic_seconds': time.monotonic()}


def identity(row, node, report):
    require(re.fullmatch('[0-9a-f]{64}', row['id']) is not None
            and row['name'] == '/' + node and row['owner'] == report['run_id']
            and row['restart_policy'] in ('no', ''), 'restart target identity or policy differs')
    require(row['state']['Paused'] is False and row['state']['OOMKilled'] is False, 'restart target paused or OOM killed')
    mounts = row['mounts']
    require(len(mounts) == 1 and mounts[0]['Type'] == 'volume' and mounts[0]['Name'] == node
            and mounts[0]['Destination'] == '/data' and mounts[0]['RW'] is True, 'restart data volume differs')
    require(isinstance(row['image'], str) and re.fullmatch('sha256:[0-9a-f]{64}', row['image']), 'restart image identity missing')
    require(number(row['monotonic_seconds']) and isinstance(row['state']['StartedAt'], str)
            and row['state']['StartedAt'] and not row['state']['StartedAt'].startswith('0001'), 'restart process clock missing')


def start_event(event):
    require(event.get('event') == 'embedded_node_started' and type(event.get('lease_ttl_ms')) is int and event['lease_ttl_ms'] == 10000
            and event.get('executors') == 'caller_owned'
            and re.fullmatch('[0-9a-f]{64}', event.get('lease_generation', '')), 'native lease generation or policy missing')
    return event['lease_generation']


def initial_nodes(docker, nodes, report):
    rows = [observe(docker, node) for node in nodes]
    validate_initial(rows, report)
    return rows


def validate_initial(rows, report):
    require(len(rows) == 3, 'missing initial restart owners')
    generations, identities = set(), set()
    for n, row in enumerate(rows, 1):
        identity(row, f"{report['prefix']}-node{n}", report)
        require(row['state']['Running'] is True and len(row['events']) == 1, 'old process is not its initial live generation')
        generations.add(start_event(row['events'][0]))
        identities.add(row['id'])
    require(len(generations) == len(identities) == 3, 'duplicate initial generation or container')


def same_container(row, old, report):
    identity(row, old['name'][1:], report)
    require(all(row[k] == old[k] for k in ('id', 'name', 'owner', 'image', 'mounts', 'restart_policy')), 'restart replaced the bound container, image or volume')


def fenced(row, old, report):
    same_container(row, old, report)
    require(row['state']['Running'] is False and type(row['state']['ExitCode']) is int and row['state']['ExitCode'] == 1
            and row['state']['StartedAt'] == old['state']['StartedAt'], 'old process did not stop before restart')
    require(len(row['events']) == 2 and row['events'][0] == old['events'][0], 'native old-generation evidence changed')
    proof = row['events'][1]
    require(proof.get('event') == 'embedded_node_stopped' and proof.get('outcome') == 'fenced'
            and type(proof.get('fence_code')) is int and proof['fence_code'] == 3 and proof.get('reason') == 'node_lease_expired'
            and proof.get('caller_executors_alive') is True, 'old owner lacks an exact joined expiry fence')


def restarted(row, old, report):
    same_container(row, old, report)
    require(row['state']['Running'] is True and row['state']['StartedAt'] != old['state']['StartedAt']
            and len(row['events']) == 3 and row['events'][:2] == old['events'], 'new process or predecessor proof missing')
    require(datetime.fromisoformat(row['state']['StartedAt']) > datetime.fromisoformat(old['state']['StartedAt']), 'new process start did not advance')
    require(start_event(row['events'][2]) != start_event(row['events'][0]), 'restart reused its old lease generation')
    require(row['health_status'] == 200 and row['binary_sha256'] == report['build']['binary_sha256'], 'restart readiness or executable differs')


def verify_restart(root, report):
    if report['scenario'] != SCENARIO:
        require(not (Path(root) / 'restart.jsonl').exists(), 'undeclared process restart')
        return None
    initial = report['processes_before']
    validate_initial(initial, report)
    start, end = recovery_window(root, report)
    require(all(r['monotonic_seconds'] < report['measurement']['started_monotonic_seconds'] for r in initial), 'initial process evidence is late')
    rows = read_events(Path(root) / 'restart.jsonl')
    require(len(rows) == 8, 'incomplete restart ledger')
    previous = start
    for order, row in enumerate(rows, 1):
        stamp = row['monotonic_seconds']
        require(type(row['order']) is int and row['order'] == order and number(stamp)
                and previous <= stamp <= end, 'restart order or recovery bound differs')
        previous = stamp
    stopped = []
    for n, (row, old) in enumerate(zip(rows[:3], initial), 1):
        require(row['event'] == 'fenced' and type(row['node']) is int and row['node'] == n, 'missing stopped owner')
        fenced(row['observation'], old, report)
        require(start <= row['observation']['monotonic_seconds'] <= row['monotonic_seconds'], 'fence observation precedes measurement end')
        stopped.append(row['observation'])
    ids = [r['id'] for r in stopped]
    require(rows[3]['event'] == 'start_invoked' and rows[3]['container_ids'] == ids
            and rows[4]['event'] == 'start_returned' and type(rows[4]['response_lost']) is bool, 'start mutation differs')
    generations = []
    for n, (row, old) in enumerate(zip(rows[5:], stopped), 1):
        require(row['event'] == 'ready' and type(row['node']) is int and row['node'] == n, 'missing restarted owner')
        restarted(row['observation'], old, report)
        require(rows[4]['monotonic_seconds'] <= row['observation']['monotonic_seconds'] <= row['monotonic_seconds'], 'restart observation clock differs')
        generations.append(row['observation']['events'][-1]['lease_generation'])
    require(len(set(generations)) == 3, 'restarted owners share a generation')
    return {'joined_expiry_fences': 3, 'restarted_containers': 3, 'container_ids': ids,
            'new_lease_generations': generations, 'ready_monotonic_seconds': previous,
            'scope': 'Same persistent volumes and exact containers, after all native owners joined an expired-lease fence. No empty-disk or disaster restore qualification.'}


def restart_nodes(root, report, docker, health, *, clock=time.monotonic, sleep=time.sleep):
    """Observe all three fences before issuing one start operation, without retries."""
    start, end = recovery_window(root, report)
    ledger = Ledger(Path(root) / 'restart.jsonl')
    stopped = []
    try:
        for n, old in enumerate(report['processes_before'], 1):
            while True:
                require(start <= clock() < end, 'old owner did not stop within recovery bound')
                row = observe(docker, old['id'])
                same_container(row, old, report)
                if row['state']['Running'] is False:
                    break
                sleep(min(.1, max(0, end - clock())))
            fenced(row, old, report)
            stopped.append(row)
            ledger.append({'event': 'fenced', 'node': n, 'observation': row, 'monotonic_seconds': clock()})
        ids = [r['id'] for r in stopped]
        require(clock() + 15 <= end, 'insufficient bounded start budget')
        ledger.append({'event': 'start_invoked', 'container_ids': ids, 'monotonic_seconds': clock()})
        require(clock() + 15 <= end, 'journal exhausted bounded start budget')
        response_lost = False
        try:
            docker('start', *ids)
        except ValueError:
            response_lost = True  # Inspect exact identities; never replay start.
        ledger.append({'event': 'start_returned', 'response_lost': response_lost, 'monotonic_seconds': clock()})
        for n, old in enumerate(stopped, 1):
            while True:
                require(clock() + 1 < end, 'restarted owner missed recovery bound')
                row = observe(docker, old['id'])
                same_container(row, old, report)
                code = None
                if row['state']['Running'] is True and len(row['events']) == 3:
                    try:
                        code = health(n, 1)
                    except (OSError, ValueError):
                        pass
                if code == 200:
                    row['health_status'] = code
                    row['binary_sha256'] = docker('exec', old['id'], 'sha256sum', '/proc/1/exe').split()[0]
                    restarted(row, old, report)
                    break
                sleep(min(.1, max(0, end - clock())))
            ledger.append({'event': 'ready', 'node': n, 'observation': row, 'monotonic_seconds': clock()})
    finally:
        ledger.close()
    return verify_restart(root, report)
