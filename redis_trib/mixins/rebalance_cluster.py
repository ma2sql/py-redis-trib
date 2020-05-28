from math import ceil, floor
from ..util import query_yes_no, xprint
from ..const import CLUSTER_HASH_SLOTS

class IsNotMasterNode(Exception): pass
class NotExistNode(Exception): pass

class RebalanceCluster:

    __slots__ = ()
 
    def rebalance_cluster(self, custom_weights, use_empty_masters, pipeline, timeout, threshold, simulate):
        weights = self._create_custom_weights(custom_weights) 

        # Assign a weight to each node, and compute the total cluster weight.
        total_weight, nodes_involved = self._assign_weight_to_nodes(weights, use_empty_masters) 

        # TODO: Check cluster, only proceed if it looks sane.
        #check_cluster(:quiet => true)
        #if @errors.length != 0
        #    puts "*** Please fix your cluster problems before rebalancing"
        #            exit 1

        self._calculate_slots_balance(total_weight, threshold)

        # Only consider nodes we want to change
        nodes_to_change = self._get_nodes_to_change()

        self._round_balance(nodes_to_change)

        # Sort nodes by their slots balance.
        sorted_nodes = sorted(nodes_to_change, key=lambda n: n.balance)

        xprint(f">>> Rebalancing across {nodes_involved} nodes. "
               f"Total weight = {total_weight}")

        for n in sorted_nodes:
            print(f"{n} balance is {n.balance} slots")

        self._rebalance(sorted_nodes, pipeline, False) 

 
    def _rebalance(self, nodes_to_change, pipeline, simulate=True):
        '''
        Now we have at the start of the 'sn' array nodes that should get
        slots, at the end nodes that must give slots.
        We take two indexes, one at the start, and one at the end,
        incrementing or decrementing the indexes accordingly til we
        find nodes that need to get/provide slots.
        '''
        dst_idx = 0
        src_idx = len(nodes_to_change) - 1

        while dst_idx < src_idx:
            dst = nodes_to_change[dst_idx]
            src = nodes_to_change[src_idx]
            num_slots = min(map(abs, [dst.balance, src.balance]))
            print([dst.balance, src.balance])

            if num_slots > 0:
                xprint(f"Moving {num_slots} slots from {src} to {dst}")

                # Actaully move the slots.
                reshard_table = self._compute_reshard_table([src], num_slots)
                if len(reshard_table) != num_slots:
                    xprint("*** Assertion failed: Reshard table != number of slots")

                if simulate:
                    print("#" * len(reshard_table))
                else:
                    for _, slot in reshard_table:
                        self._move_slot(src, dst, slot,
                            quiet=False,
                            dots=False,
                            update=True,
                            pipeline=pipeline)
                        print("#", end="")
                print()

            # Update nodes balance.
            dst.balance += num_slots
            src.balance -= num_slots
            if dst.balance == 0:
                dst_idx += 1
            if src.balance == 0:
                src_idx -= 1

    def _create_custom_weights(self, custom_weights):
        weights = {}

        if not custom_weights:
            return weights

        for w in custom_weights.split():
            node_id, weight = w.split('=')
            node = self._get_node_by_abbreviated_id(node_id)
            if not node.is_master():
                raise RedisTribError(f"{node} is not a master")
            weights[node.node_id] = float(weight)

        return weights

    def _assign_weight_to_nodes(self, weights, use_empty_masters):
        total_weight = 0
        nodes_involved = 0

        for n in self._nodes:
            if n.is_master():
                if not use_empty_masters and len(n.slots) == 0:
                    continue
                n.weight = _w if (_w := weights.get(n.node_id)) is not None else 1 
                total_weight += n.weight
                nodes_involved += 1

        return total_weight, nodes_involved

    def _calculate_slots_balance(self, total_weight, threshold):
        '''
        Calculate the slots balance for each node. It's the number of
        slots the node should lose (if positive) or gain (if negative)
        in order to be balanced.
        '''
        threshold_reached = False
        for n in self._nodes:
            if n.is_master():
                if n.weight is None:
                    continue

                expected = int((float(CLUSTER_HASH_SLOTS) / total_weight) * n.weight)
                n.balance = len(n.slots) - expected

                # Compute the percentage of difference between the
                # expected number of slots and the real one, to see
                # if it's over the threshold specified by the user.
                over_threshold = False
                if threshold > 0:
                    if len(n.slots) > 0:
                        err_perc = abs(100 - (100.0 * expected / len(n.slots)))
                        if err_perc > threshold:
                            over_threshold = True 
                    elif expected > 0:
                        over_threshold = True

                if over_threshold:
                    threshold_reached = True

        if not threshold_reached:
            xprint(f"*** No rebalancing needed! "
                   f"All nodes are within the {threshold}% threshold.")

    def _get_nodes_to_change(self):
        return [n for n in self._get_masters() if n.weight is not None]

    def _round_balance(self, nodes_to_change):
        # Because of rounding, it is possible that the balance of all nodes
        # summed does not give 0. Make sure that nodes that have to provide
        # slots are always matched by nodes receiving slots.
        total_balance = sum(n.balance for n in nodes_to_change)
        while total_balance > 0:
            for n in nodes_to_change:
                # https://github.com/antirez/redis/issues/4941
                if n.balance <= 0 and total_balance > 0:
                    n.balance -= 1
                    total_balance -= 1

