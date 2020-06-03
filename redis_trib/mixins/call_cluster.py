from ..util import xprint
import json
import redis


class CallCluster:

    __slots__ = ()

    def call(self, *command):
        results = {}
        for n in self._nodes:
            result = None
            try:
                result = n.r.execute_command(*command)
            except redis.exceptions.ResponseError as e:
                result = str(e)
            results[str(n)] = result

        print(json.dumps(results, indent=4))
