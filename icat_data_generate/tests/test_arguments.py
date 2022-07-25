import unittest
from argparse import ArgumentTypeError

from icat_data_generate.command import arg_convenientnumber_type

import pytest


class ArgumentTests(unittest.TestCase):
    def test_arg_convenientnumber_type(self):
        self.assertEqual(arg_convenientnumber_type("123"), 123)
        self.assertEqual(arg_convenientnumber_type("456k"), 456000)
        self.assertEqual(arg_convenientnumber_type("789m"), 789000000)

        with pytest.raises(ArgumentTypeError):
            arg_convenientnumber_type("blah")

        with pytest.raises(ArgumentTypeError):
            arg_convenientnumber_type("")

        with pytest.raises(ArgumentTypeError):
            arg_convenientnumber_type("m123")

        with pytest.raises(ArgumentTypeError):
            arg_convenientnumber_type("123km")


if __name__ == '__main__':
    unittest.main()
