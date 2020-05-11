import collections
from .util import (
    group_by,
    xprint
)

def get_anti_affinity_score(nodes):
    score = 0
    # List of offending slaves to return to the caller
    offending = []
    # First, split nodes by host
    host_to_node = group_by(nodes, key=lambda n: n.host)

    # Then, for each set of nodes in the same host, split by
    # related nodes (masters and slaves which are involved in
    # replication of each other)
    for host, nodes in host_to_node.items():
        related = collections.defaultdict(list)
        for n in nodes:
            if n.replicate:
                related[n.replicate].append('s')
            else:
                related[n.node_id].append('m')

        # Now it's trivial to check, for each related group having the
        # same host, what is their local score.
        for node_id, types in related.items():
            if len(types) < 2:
                continue
            # Make sure :m if the first if any
            sorted_types = sorted(types)
            if sorted_types[0] == 'm':
                score += 10000 * (len(types) -1)
            else:
                score += 1 * len(types)

            # Populate the list of offending node
            for n in nodes:
                if n.replicate == node_id and n.host == host:
                    offending.append(n)
    
    return score, offending


def evaluate_anti_affinity(nodes):
    score, *_ = get_anti_affinity_score(nodes)
    if score == 0:
        xprint("[OK] Perfect anti-affinity obtained!")
    elif score >= 10000:
        xprint("[WARNING] Some slaves are in the same host as their master")
    else:
        xprint("[WARNING] Some slaves of the same master are in the same host")


