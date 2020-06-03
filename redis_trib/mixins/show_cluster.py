from ..xprint import xprint


class ShowCluster:

    __slots__ = ()

    def show(self):
        masters = 0
        keys = 0
        for n in self._nodes:
            if n.is_master():
                xprint(f"{n} ({n.node_id[:8]}...) -> {n.dbsize} keys "\
                      f"| {len(n.slots)} slots | {len(n.replicas)} slaves.")
                masters += 1
                keys += n.dbsize
        xprint.ok(f"{keys} keys in {masters} masters.")
        xprint(f"{keys/16384.0:.2f} keys per slot on average.")

