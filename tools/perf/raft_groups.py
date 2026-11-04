# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Independent controller model of the native shared-range population."""
import hashlib
from functools import lru_cache

DEFAULT_OCCUPANCY = 32
MAX_OCCUPANCY = 128
NAMESPACE = bytes([2] * 16)
DOMAIN = b'cellar-cell-route-1\0'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def group_members(group, nodes):
    require(type(nodes) is int and 3 <= nodes <= 1000, 'RF3 placement requires three to 1000 nodes')
    require(type(group) is int and group >= 0, 'invalid physical group ordinal')
    return tuple((group + offset) % nodes + 1 for offset in range(3))


class Layout:
    def __init__(self, total, occupancy, class_name, generation=0):
        require(type(total) is int and 1 <= total <= 1000000, 'invalid cell population')
        require(type(occupancy) is int and 1 <= occupancy <= MAX_OCCUPANCY, 'invalid group occupancy')
        require(class_name in ('Counter', 'Workload') and type(generation) is int and generation in (0, 1),
                'invalid population history')
        self.total, self.occupancy = total, occupancy
        self.class_name, self.generation = class_name, generation
        keys = []
        for cell in range(total):
            name = f'{class_name}:{cell}'.encode()
            keys.append((hashlib.sha256(DOMAIN + NAMESPACE + len(name).to_bytes(4, 'big') + name).digest(), name, cell))
        keys.sort()
        self.assignment = [None] * total
        self.groups = []
        for start in range(0, total, occupancy):
            ordinal = start // occupancy
            cells = keys[start:start+occupancy]
            for _, _, cell in cells:
                self.assignment[cell] = ordinal
            self.groups.append({
                'ordinal': ordinal,
                'group': ((ordinal+1).to_bytes(8, 'big') + bytes([5+generation] * 8)).hex(),
                'incarnation': bytes([4+generation] * 16).hex(),
                'cells': len(cells),
            })

    def group(self, cell):
        require(type(cell) is int and 0 <= cell < self.total, 'cell outside configured population')
        return self.groups[self.assignment[cell]]

    def members(self, cell, nodes):
        return group_members(self.group(cell)['ordinal'], nodes)

    def json(self, nodes):
        return {'cells': self.total, 'cells_per_group': self.occupancy,
                'groups': [{**group, 'voters': list(group_members(group['ordinal'], nodes))} for group in self.groups],
                'cell_groups': self.assignment}

    def placement(self, nodes):
        group_members(0, nodes)
        result = [{'node': n, 'leader_groups': 0, 'replica_groups': 0, 'leader_cells': 0, 'replica_cells': 0}
                  for n in range(1, nodes+1)]
        for group in self.groups:
            voters = group_members(group['ordinal'], nodes)
            result[voters[0]-1]['leader_groups'] += 1
            result[voters[0]-1]['leader_cells'] += group['cells']
            for member in voters:
                result[member-1]['replica_groups'] += 1
                result[member-1]['replica_cells'] += group['cells']
        return result


@lru_cache(maxsize=4)
def cached_layout(total, occupancy, class_name='Workload', generation=0):
    return Layout(total, occupancy, class_name, generation)


def profile_layout(profile):
    return cached_layout(profile['cells']['total'], profile['cells']['per_group'])
