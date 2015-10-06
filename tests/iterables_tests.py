import collections
import unittest

from kiel import iterables


class IterablesTests(unittest.TestCase):

    def test_drain_on_list(self):
        data = ["foo", 1, "bar", 9]

        result = list(iterables.drain(data))

        self.assertEqual(len(data), 0)
        self.assertEqual(result, [9, "bar", 1, "foo"])

    def test_drain_on_deque(self):
        data = collections.deque(["foo", 1, "bar", 9])

        result = list(iterables.drain(data))

        self.assertEqual(len(data), 0)
        self.assertEqual(result, ["foo", 1, "bar", 9])

    def test_drain_on_set(self):
        data = set(["foo", 1, "bar", 9])

        result = list(iterables.drain(data))

        self.assertEqual(len(data), 0)
        self.assertEqual(set(result), set(["foo", 1, "bar", 9]))

    def test_drain_on_dict(self):
        data = {"foo": 1, "bar": 9}

        result = {key: value for key, value in iterables.drain(data)}

        self.assertEqual(len(data), 0)
        self.assertEqual(result, {"foo": 1, "bar": 9})
