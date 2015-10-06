import json
import struct
import unittest

from mock import patch

from kiel.compression import snappy


@unittest.skipUnless(snappy.snappy_available, "requires python-snappy")
class SnappyCompressionTests(unittest.TestCase):

    @patch.object(snappy, "snappy_available", False)
    def test_compress_runtime_error_if_snappy_unavailable(self):
        self.assertRaises(
            RuntimeError,
            snappy.compress, "foo"
        )

    @patch.object(snappy, "snappy_available", False)
    def test_decompress_runtime_error_if_snappy_unavailable(self):
        self.assertRaises(
            RuntimeError,
            snappy.decompress, "foo"
        )

    def test_compression_is_stable(self):
        data = json.dumps({"foo": "bar", "blee": "bloo", "dog": "cat"})

        data = snappy.compress(data)
        data = snappy.decompress(data)
        data = snappy.compress(data)
        data = snappy.decompress(data)

        self.assertEqual(
            json.loads(data),
            {"foo": "bar", "blee": "bloo", "dog": "cat"}
        )

    def test_compression_includes_magic_header(self):
        data = json.dumps({"foo": "bar", "blee": "bloo", "dog": "cat"})
        data = snappy.compress(data)

        header = struct.unpack_from("!bccccccbii", data)

        self.assertEqual(header, (-126, 'S', 'N', 'A', 'P', 'P', 'Y', 0, 1, 1))
