import unittest

from icat_data_generate.msp import compute_msp

import pytest


class MinimumSumProductTests(unittest.TestCase):
    def test_msp(self):
        self.assertEqual(compute_msp(20, 20, 100), (10, 10))
        self.assertEqual(compute_msp(5, 50, 100), (5, 20))
        self.assertEqual(compute_msp(50, 5, 100), (20, 5))

        with pytest.raises(ValueError):
            compute_msp(5, 5, 100)


if __name__ == '__main__':
    unittest.main()
