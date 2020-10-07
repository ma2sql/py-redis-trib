import unittest
from redis_trib import ClusterNode, group_by

## TODOs
# - open slot에 대한 판단 검증

class TestUtil(unittest.TestCase):
    def setUp(self):
        pass

    # - open slot에 대한 주인 찾기
    def testGetSlotOnwers(self):
        # 동일 슬롯을 소유하는 오너를 2개 이상 만들기
        assertEqual(expected, get_slot_owners())


    def tearDown(self):
        pass

