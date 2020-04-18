import unittest
from redis_trib import ClusterNode, group_by


class TestUtil(unittest.TestCase):
    def setUp(self):
        pass

    def test_group_by_host(self):
        addrs = [
            '192.168.56.101:7001',
            '192.168.56.101:7002',
            '192.168.56.102:7001',
            '192.168.56.102:7002',
            '192.168.56.102:7003',
            '192.168.56.103:7001',
            '192.168.56.103:7002',
        ]
        print(group_by(addrs, key=lambda addr: addr.split(':')[0]))

    def tearDown(self):
        pass
