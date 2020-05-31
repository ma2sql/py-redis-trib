

class ClusterTimeout:
    
    __slots__ = ()

    def set_timeout(self, timeout):
        # Send CLUSTER FORGET to all the nodes but the node to remove
        xprint(f">>> Reconfiguring node timeout in every cluster node...")
        for n in self._nodes:
            try:
                n.r.config("set","cluster-node-timeout",timeout)
                n.r.config("rewrite")
                ok_count += 1
                xputs "*** New timeout set for #{n}"
            except BaseException as e:
                xprint(f"ERR setting node-timeot for {n}: {e}")
                err_count += 1
 
        xprint(f">>> New node timeout set. {ok_count} OK, {err_count} ERR.")
