# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Bounded local placement and evidence checks for three-voter groups."""


def require(condition, message):
    if not condition:
        raise ValueError(message)


def members(cell, nodes):
    require(type(nodes) is int and 3 <= nodes <= 5, 'local topology requires three to five nodes')
    require(type(cell) is int and cell >= 0, 'invalid cell identity')
    return tuple((cell + offset) % nodes + 1 for offset in range(3))


def placement(total, nodes):
    members(0, nodes)
    require(type(total) is int and 1 <= total <= 1000000, 'invalid cell population')
    leaders = [total // nodes + int(node < total % nodes) for node in range(nodes)]
    return [{'node': node + 1, 'leader_cells': leaders[node],
             'replica_cells': sum(leaders[(node - offset) % nodes] for offset in range(3))}
            for node in range(nodes)]


def local_placement(profile):
    topology = profile['topology']
    require(topology['voters'] == 3 and topology['failure_domains'] == 1,
            'local topology requires three voters in one Colima VM; Q1 remains a separate gate')
    result = placement(profile['cells']['total'], topology['nodes'])
    require(all(1 <= node['replica_cells'] <= 4096 for node in result),
            'placement requires empty or over-capacity local hosts')
    return result


def vm_requirements(nodes):
    members(0, nodes)
    # Distinct CPU sets for every node and RustFS, plus one spare CPU and
    # one GiB beyond the sum of all container memory limits.
    return {'cpus': nodes + 3, 'memory_bytes': (nodes * 3 + 5) * 1024**3}


def verify_topology(rows, total, nodes):
    require(isinstance(rows, list) and len(rows) == total * nodes, 'incomplete topology scan')
    seen, leaders, replicas = set(), [0] * nodes, [0] * nodes
    for row in rows:
        cell, node = row['cell'], row['node']
        require(type(cell) is int and 0 <= cell < total and type(node) is int and 1 <= node <= nodes,
                'invalid topology identity')
        require((cell, node) not in seen and row['status'] == 200, 'duplicate or unavailable topology observation')
        seen.add((cell, node))
        view = row['response']
        require(type(view['cell']) is int and type(view['node']) is int and view['cell'] == cell and view['node'] == node, 'topology response identity mismatch')
        assigned = members(cell, nodes)
        require(view['registered'] is (node in assigned), 'actual registration differs from planned voters')
        if node not in assigned:
            continue
        replicas[node - 1] += 1
        leader = assigned[0]
        require(view['leader'] == leader and view['role'] == ('Leader' if node == leader else 'Voter'),
                'actual role or leader differs from placement')
        if node == leader:
            leaders[node - 1] += 1
            require(isinstance(view['voters'], list) and all(type(voter) is int for voter in view['voters']) and view['voters'] == sorted(assigned), 'runtime voter set differs from placement')
        else:
            require(view['voters'] is None, 'follower cannot supply leader-local replication evidence')
    observed = [{'node': n + 1, 'leader_cells': leaders[n], 'replica_cells': replicas[n]} for n in range(nodes)]
    require(observed == placement(total, nodes), 'observed placement counts differ')
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
    expected = {row['node']: row['replica_cells'] for row in planned}
    seen = set()
    for row in rows:
        node = row['node']
        require(type(node) is int and node in expected and node not in seen, 'invalid population node')
        seen.add(node)
        groups = row['sample']['resources']['groups']
        for key, value in [('durable_groups', expected[node]), ('attached_groups', expected[node]), ('parked_groups', 0)]:
            require(type(groups.get(key)) is int and groups[key] == value, 'physical group population differs from placement')


def verify_traffic(events, profile):
    """Bind the advertised durability/read path to raw responses and requests."""
    for event in events:
        if event['event'] == 'invoke' and event['operation'] == 'query':
            require(event['input']['consistency'] == profile['read_consistency'], 'query contract differs from profile')
        if event['event'] not in ('seed_complete', 'complete') or event['status'] != 200:
            continue
        if event['event'] == 'seed_complete' or event['operation'] == 'command':
            require(event['response']['durability'] == profile['durability'], 'acknowledged durability differs from profile')
        else:
            require(event['response']['served_by'] == members(event['cell'], profile['topology']['nodes'])[1],
                    'measured query did not execute on the assigned follower')
