import struct
import unittest
import zlib

import six

from kiel.protocol import primitives


class PrimitivesTests(unittest.TestCase):

    def test_string_repr(self):
        s = primitives.String(u"foobar")

        self.assertEqual(repr(s), repr(u"foobar"))

    def test_array_repr(self):
        a = primitives.Array.of(primitives.Int32)([1, 3, 6, 9])

        self.assertEqual(repr(a), "[1, 3, 6, 9]")

    def test_string_render_parse_is_stable(self):
        s = primitives.String(u"foobar")

        fmt, values = s.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.String.parse(raw, 0)

        self.assertEqual(value, u"foobar")

    def test_string_render_parse_handles_nonstrings(self):
        s = primitives.String(123)

        fmt, values = s.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.String.parse(raw, 0)

        self.assertEqual(value, u"123")

    def test_bytes_render_parse_is_stable(self):
        b = primitives.Bytes(u"foobar")

        fmt, values = b.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.Bytes.parse(raw, 0)

        self.assertEqual(value, u"foobar")

    def test_bytes_render_parse_handles_nonstrings(self):
        s = primitives.Bytes(123)

        fmt, values = s.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.Bytes.parse(raw, 0)

        self.assertEqual(value, u"123")

    def test_bytes_render_parse_handles_compressed_data(self):
        data = zlib.compress(six.b("Best of times, blurst of times."))

        s = primitives.Bytes(data)

        fmt, values = s.render()

        raw = struct.pack("!" + fmt, *values)

        value, _ = primitives.Bytes.parse(raw, 0)

        self.assertEqual(value, data)
