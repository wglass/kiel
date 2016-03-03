import unittest

from kiel.protocol import primitives


class PrimitivesTests(unittest.TestCase):

    def test_string_repr(self):
        s = primitives.String(u"foobar")

        self.assertEqual(repr(s), repr(u"foobar"))

    def test_array_repr(self):
        a = primitives.Array.of(primitives.Int32)([1, 3, 6, 9])

        self.assertEqual(repr(a), "[1, 3, 6, 9]")
