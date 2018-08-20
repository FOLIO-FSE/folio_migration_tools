import unittest
from marc_to_folio import Mapper


class TestStuff(unittest.TestCase):
    def test_mapper_create(self):
        self.assertRaises(Exception, Mapper.Mapper())


if __name__ == '__main__':
        unittest.main()
