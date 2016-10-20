import json
import unittest

from kiel.protocol import messages


class MessagesTests(unittest.TestCase):

    def test_message_repr(self):
        msg = messages.Message(
            crc=0,
            magic=0,
            attributes=0,
            key="foo",
            value=json.dumps({"bar": "bazz"})
        )

        self.assertEqual(repr(msg), 'foo => {"bar": "bazz"}')

    def test_messageset_repr(self):
        msg1 = messages.Message(
            crc=0,
            magic=0,
            attributes=0,
            key="foo",
            value=json.dumps({"bar": "bazz"})
        )
        msg2 = messages.Message(
            crc=0,
            magic=0,
            attributes=0,
            key="bar",
            value=json.dumps({"bwee": "bwoo"})
        )
        msg_set = messages.MessageSet([(10, msg1), (11, msg2)])

        self.assertEqual(
            repr(msg_set),
            '[foo => {"bar": "bazz"}, bar => {"bwee": "bwoo"}]'
        )
