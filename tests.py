import unittest
from base_db import BaseDB


TEST_DB = 'ab_test.sqlite3'

class TestBaseDBClass(unittest.TestCase):

    def setUp(self):
        self.db = BaseDB(TEST_DB)

    def test1(self):
        pass