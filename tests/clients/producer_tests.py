from tests import cases

from mock import patch
from tornado import testing

from kiel import constants
from kiel.protocol import produce, messages, errors
from kiel.clients import producer


def attribute_key(msg):
    return msg["key"]


def key_partitioner(key, partitions):
    return partitions[key]


class ProducerTests(cases.ClientTestCase):

    def setUp(self):
        super(ProducerTests, self).setUp()

        self.add_broker("kafka01", 9002, broker_id=1)
        self.add_broker("kafka02", 9002, broker_id=3)
        self.add_broker("kafka03", 9002, broker_id=8)

    def test_defaults(self):
        p = producer.Producer(["kafka01", "kafka02"])

        self.assertEqual(p.batch_size, 1)
        self.assertEqual(p.required_acks, -1)
        self.assertEqual(p.ack_timeout, 500)
        self.assertEqual(p.compression, None)

    def test_unknown_compression(self):
        self.assertRaises(
            ValueError,
            producer.Producer, ["kafka01"], compression=5
        )

    @testing.gen_test
    def test_flush_with_no_pending_sends(self):
        self.add_topic("test.topic", leaders=(1,))

        p = producer.Producer(
            ["kafka01"],
            key_maker=attribute_key, partitioner=key_partitioner
        )

        yield p.flush()

        self.assertEqual(self.requests_by_broker, {})

    @testing.gen_test
    def test_routing_to_partitions(self):
        self.add_topic("test.topic", leaders=(1, 1, 8, 3))
        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8000,
                                ),
                            ]
                        ),
                    ]
                ),
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=1,
                                    error_code=errors.no_error,
                                    offset=8000,
                                ),
                            ]
                        ),
                    ]
                ),
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8001,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=3, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=3,
                                    error_code=errors.no_error,
                                    offset=8000,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = [
            {"key": 0, "msg": "foo"},
            {"key": 1, "msg": "bar"},
            {"key": 3, "msg": "bwee"},
            {"key": 0, "msg": "bwoo"},
        ]

        p = producer.Producer(
            ["kafka01"],
            key_maker=attribute_key, partitioner=key_partitioner
        )

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0,
                                            key=0,
                                            value=p.serializer(
                                                {"msg": "foo", "key": 0}
                                            )
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=1,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0,
                                            key=1,
                                            value=p.serializer(
                                                {"msg": "bar", "key": 1}
                                            )
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0,
                                            key=0,
                                            value=p.serializer(
                                                {"msg": "bwoo", "key": 0}
                                            )
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=3,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=3,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0,
                                            key=3,
                                            value=p.serializer(
                                                {"msg": "bwee", "key": 3}
                                            )
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @patch.object(producer, "random")
    @testing.gen_test
    def test_default_routing(self, mock_random):
        self.add_topic("test.topic", leaders=(1, 8))

        choices_to_make = [0, 1]

        def get_next_choice(*args):
            return choices_to_make.pop(0)

        mock_random.choice.side_effect = get_next_choice

        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8000,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=8, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=1,
                                    error_code=errors.no_error,
                                    offset=8000,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = ["foo", "bar"]

        p = producer.Producer(["kafka01"])

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("foo")
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=8,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=1,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bar")
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_batching_messages(self):
        self.add_topic("test.topic", leaders=(1,))

        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8003,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = ["foo", "bar", "bazz"]

        p = producer.Producer(["kafka01"], batch_size=3)

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("foo")
                                        ),
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bar")
                                        ),
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bazz")
                                        ),
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_gzip_compression(self):
        self.add_topic("test.topic", leaders=(1,))

        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8003,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = ["foo", "bar"]

        p = producer.Producer(
            ["kafka01"], batch_size=2, compression=constants.GZIP
        )

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=constants.GZIP,
                                    msgs=[
                                        messages.Message(
                                            magic=0,
                                            attributes=0,
                                            key=None,
                                            value=p.serializer("foo"),
                                        ),
                                        messages.Message(
                                            magic=0,
                                            attributes=0,
                                            key=None,
                                            value=p.serializer("bar"),
                                        )
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_close_flushes_unsent_stuff(self):
        self.add_topic("test.topic", leaders=(1,))

        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8003,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = ["foo", "bar"]

        p = producer.Producer(["kafka01"], batch_size=3)

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assertEqual(self.requests_by_broker[1], [])

        yield p.close()

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("foo")
                                        ),
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bar")
                                        ),
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_retriable_error_code(self):
        self.add_topic("test.topic", leaders=(1,))

        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.request_timed_out,
                                    offset=8001,
                                ),
                            ]
                        ),
                    ]
                ),
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8001,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = ["foo", "bar"]

        p = producer.Producer(["kafka01"], batch_size=1)

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assertEqual(len(self.requests_by_broker[1]), 2)

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("foo")
                                        ),
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bar")
                                        ),
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_leader_not_present_at_first(self):
        self.add_topic("test.topic", leaders=(7,))

        p = producer.Producer(["kafka01"], batch_size=1)

        yield p.produce("test.topic", "foo")

        self.add_topic("test.topic", leaders=(1,))
        yield p.cluster.heal()

        yield p.produce("test.topic", "bar")

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("foo")
                                        ),
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bar")
                                        ),
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_producing_when_closed_never_sends(self):
        self.add_topic("test.topic", leaders=(1,))

        p = producer.Producer(["kafka01"], batch_size=1)

        yield p.close()

        yield p.produce("test.topic", "foo")

        self.assertEqual(self.requests_by_broker[1], [])

    @testing.gen_test
    def test_producing_to_unknown_topic_never_sends(self):
        self.add_topic("test.topic", leaders=(1,))

        p = producer.Producer(["kafka01"], batch_size=1)

        yield p.produce("other.topic", "foo")

        self.assertEqual(self.requests_by_broker[1], [])

    @testing.gen_test
    def test_unretriable_error(self):
        self.add_topic("test.topic", leaders=(1,))

        self.set_responses(
            broker_id=1, api="produce",
            responses=[
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.unknown,
                                    offset=8001,
                                ),
                            ]
                        ),
                    ]
                ),
                produce.ProduceResponse(
                    topics=[
                        produce.TopicResponse(
                            name="test.topic",
                            partitions=[
                                produce.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offset=8001,
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        msgs = ["foo", "bar"]

        p = producer.Producer(["kafka01"], batch_size=1)

        for msg in msgs:
            yield p.produce("test.topic", msg)

        self.assertEqual(len(self.requests_by_broker[1]), 2)

        self.assert_sent(
            broker_id=1,
            request=produce.ProduceRequest(
                required_acks=-1,
                timeout=500,
                topics=[
                    produce.TopicRequest(
                        name="test.topic",
                        partitions=[
                            produce.PartitionRequest(
                                partition_id=0,
                                message_set=messages.MessageSet.compressed(
                                    compression=None,
                                    msgs=[
                                        messages.Message(
                                            magic=0, attributes=0, key=None,
                                            value=p.serializer("bar")
                                        ),
                                    ]
                                )
                            )
                        ]
                    )
                ]
            )
        )
