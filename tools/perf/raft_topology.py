# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bounded local placement and evidence checks for three-voter groups."""
from raft_groups import cached_layout, profile_layout, group_members


def require(condition, message):
    if not condition:
        raise ValueError(message)


def members(cell, nodes, layout):
    return layout.members(cell, nodes)


def placement(total, nodes, occupancy, class_name='Workload'):
    return cached_layout(total, occupancy, class_name).placement(nodes)


def local_placement(profile):
    topology = profile['topology']
    require(type(topology['nodes']) is int and 3 <= topology['nodes'] <= 5,
            'local topology requires three to five nodes')
    require(topology['voters'] == 3 and topology['failure_domains'] == 1,
            'local topology requires three voters in one Colima VM; Q1 remains a separate gate')
    result = profile_layout(profile).placement(topology['nodes'])
    require(all(0 <= node['replica_groups'] <= 4096 for node in result),
            'placement requires over-capacity local hosts')
    return result


def vm_requirements(nodes):
    require(type(nodes) is int and 3 <= nodes <= 5, 'local topology requires three to five nodes')
    # Distinct CPU sets for every node and RustFS, plus one spare CPU and
    # one GiB beyond the sum of all container memory limits.
    return {'cpus': nodes + 3, 'memory_bytes': (nodes * 3 + 5) * 1024**3}


def verify_topology(rows, total, nodes, occupancy):
    layout = cached_layout(total, occupancy)
    require(isinstance(rows, list) and len(rows) == total * nodes, 'incomplete topology scan')
    seen, physical = set(), set()
    observed = [{'node': n, 'leader_groups': 0, 'replica_groups': 0, 'leader_cells': 0, 'replica_cells': 0}
                for n in range(1, nodes+1)]
    for row in rows:
        cell, node = row['cell'], row['node']
        require(type(cell) is int and 0 <= cell < total and type(node) is int and 1 <= node <= nodes,
                'invalid topology identity')
        require((cell, node) not in seen and row['status'] == 200, 'duplicate or unavailable topology observation')
        seen.add((cell, node))
        view, group = row['response'], layout.group(cell)
        require(type(view['cell']) is int and type(view['node']) is int and view['cell'] == cell and view['node'] == node,
                'topology response identity mismatch')
        require(view['group_ordinal'] == group['ordinal'] and view['group'] == group['group']
                and view['group_incarnation'] == group['incarnation'] and view['group_cells'] == group['cells'],
                'physical history/range population differs from layout')
        assigned = layout.members(cell, nodes)
        require(view['registered'] is (node in assigned), 'actual registration differs from planned voters')
        if node not in assigned:
            continue
        require(view['observed_cells'] == group['cells'], 'applied physical group occupancy differs')
        observed[node-1]['replica_cells'] += 1
        leader = assigned[0]
        require(view['leader'] == leader and view['role'] == ('Leader' if node == leader else 'Voter'),
                'actual role or leader differs from placement')
        fresh = (group['ordinal'], node) not in physical
        physical.add((group['ordinal'], node))
        observed[node-1]['replica_groups'] += int(fresh)
        if node == leader:
            observed[node-1]['leader_cells'] += 1
            observed[node-1]['leader_groups'] += int(fresh)
            require(isinstance(view['voters'], list) and all(type(voter) is int for voter in view['voters'])
                    and view['voters'] == sorted(assigned), 'runtime voter set differs from placement')
        else:
            require(view['voters'] is None, 'follower cannot supply leader-local replication evidence')
    require(observed == layout.placement(nodes), 'observed group/cell placement counts differ')
    return observed


def verify_resources(report, nodes):
    minimum = vm_requirements(nodes)
    require(report['vm']['cpus'] >= minimum['cpus'] and report['vm']['memory_bytes'] >= minimum['memory_bytes'],
            'insufficient recorded VM resources')
    require(len(report['limits']) == nodes, 'missing container resource limits')
    for index, limits in enumerate(report['limits']):
        require(limits == {'NanoCpus': 10**9, 'CpusetCpus': str(index), 'Memory': 3 * 1024**3,
                           'MemorySwap': 3 * 1024**3, 'Tmpfs': {}}, 'node resource limits differ')
    require(report['store_limits'] == {'NanoCpus': 2 * 10**9, 'CpusetCpus': f'{nodes},{nodes+1}',
                                      'Memory': 4 * 1024**3, 'MemorySwap': 4 * 1024**3, 'Tmpfs': {}},
            'object-store resource limits differ')


def verify_populations(rows, planned):
    require(len(rows) == len(planned), 'missing physical group population')
    expected = {row['node']: row['replica_groups'] for row in planned}
    seen = set()
    for row in rows:
        node = row['node']
        require(type(node) is int and node in expected and node not in seen, 'invalid population node')
        seen.add(node)
        groups = row['sample']['resources']['physical_counts']
        for key, value in [('durable_groups', expected[node]), ('attached_groups', expected[node]), ('unattached_groups', 0), ('dormant_groups', 0), ('pending_groups', 0)]:
            require(type(groups.get(key)) is int and groups[key] == value, 'physical group population differs from placement')


def verify_traffic(events, profile):
    """Bind the advertised durability/read path to raw responses and requests."""
    for event in events:
        if profile.get('payload_pattern') == 'random_ascii' and event['event'] == 'invoke' and event['operation'] == 'command':
            from raft_workload import payload_for
            phase, ordinal = event['id'].rsplit(':', 1)
            payload = event['input']['payload']
            require(len(payload) in profile['write_bytes'] and payload == payload_for(profile, phase, int(ordinal), len(payload)),
                    'advertised payload pattern differs from ledger')
        if event['event'] == 'invoke' and event['operation'] == 'query':
            require(event['input']['consistency'] == profile['read_consistency'], 'query contract differs from profile')
        if event['event'] not in ('seed_complete', 'complete') or event['status'] != 200:
            continue
        verify_reply(event['response'], event['cell'], profile_layout(profile))
        if event['event'] == 'seed_complete' or event['operation'] == 'command':
            require(event['response']['durability'] == profile['durability'], 'acknowledged durability differs from profile')
        else:
            require(event['response']['served_by'] == members(event['cell'], profile['topology']['nodes'], profile_layout(profile))[1],
                    'measured query did not execute on the assigned follower')


def verify_reply(reply, cell, layout):
    """Logical history is independent of the shared group's commit witness."""
    group = layout.group(cell)
    require(reply.get('cell') == f'{layout.class_name}:{cell}'
            and reply.get('incarnation') == bytes([4+layout.generation] * 16).hex(),
            'response belongs to another logical cell history')
    require(reply.get('group') == group['group']
            and reply.get('group_incarnation') == group['incarnation'],
            'response commit witness belongs to another physical history')
    for name in ('sequence', 'index', 'term'):
        require(type(reply.get(name)) is int and 0 < reply[name] < 2**64,
                'invalid response version or commit witness')
    require(isinstance(reply.get('version_token'), str) and reply['version_token'],
            'missing logical version token')
