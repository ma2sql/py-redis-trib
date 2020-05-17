from ..util import xprint

class ShowCluster:
    def __init__(self, nodes):
        self._nodes = nodes

    def show(self):
        masters = 0
        keys = 0
        for n in self._nodes:
            if n.is_master():
                print(f"{n} ({n.node_id[:8]}...) -> {n.dbsize} keys "\
                      f"| {len(n.slots)} slots | {len(n.replicas)} slaves.")
                masters += 1
                keys += n.dbsize
        xprint(f"[OK] {keys} keys in {masters} masters.")
        print(f"{keys/16384.0:.2f} keys per slot on average.")

