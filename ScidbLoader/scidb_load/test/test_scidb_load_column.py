import scidb_load_column
import unittest


class TestScidbLoadColumn(unittest.TestCase):
    def test__repr__(self):
        col = scidb_load_column.ScidbLoadColumn("a", "b", True)
        col.dimension_params = [2,3,5,7]

        self.assertEqual("a, b, True, 2, 3, 5, 7", col.__repr__())



if __name__ == '__main__':
    unittest.main()

