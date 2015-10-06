import json
import unittest

from kiel.compression import gzip


class GZipCompressionTests(unittest.TestCase):

    def test_compression_is_stable(self):
        data = json.dumps({"foo": "bar", "blee": "bloo", "dog": "cat"})
        data = data.encode("utf-8")

        data = gzip.compress(data)
        data = gzip.decompress(data)
        data = gzip.compress(data)
        data = gzip.decompress(data)

        self.assertEqual(
            json.loads(data.decode("utf-8")),
            {"foo": "bar", "blee": "bloo", "dog": "cat"}
        )
