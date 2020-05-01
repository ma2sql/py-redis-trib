import unittest
from redis_trib import CreateClusterStrategy, ClusterNode



def fake_redis(**attrs):
    mock = Mock()
    mock.info.return_value = {'cluster_enabled': 1}
    mock.cluster.return_value = {'cluster_known_nodes': 1}
    mock.configure_mock(**attrs)
    
    def side_effect(*args, **kwargs):
        return_values = {
            'INFO': {'cluster_known_nodes': 1}
            'NODES': attrs.get('nodes'),
        }
        return return_values.get(args[0].upper()) or {}
    mock.side_effect = side_effect 
    return mock


class TestCreateClusterStrategy(unittest.TestCase):
    def setUp(self):
        pass

    def test_load_nodes(self):
        pass
    
    def tearDown(self):
        pass
