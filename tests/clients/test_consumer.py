from tests import cases

from tornado import testing, gen

from kiel.protocol import fetch, messages, errors
from kiel.clients import consumer


class FakeConsumer(consumer.BaseConsumer):

    @property
    def allocation(self):
        return self.cluster.topics

    @gen.coroutine
    def determine_offsets(self, topic, start):
        for partition in self.cluster.topics[topic]:
            self.offsets[topic][partition] = 0

    @gen.coroutine
    def wind_down(self):
        pass


class ConsumerTests(cases.ClientTestCase):

    def setUp(self):
        super(ConsumerTests, self).setUp()

        self.add_broker("kafka01", 9002, broker_id=1)
        self.add_broker("kafka02", 9002, broker_id=3)
        self.add_broker("kafka03", 9002, broker_id=8)

    def test_defaults(self):
        c = consumer.BaseConsumer(["kafka01", "kafka02"])

        self.assertEqual(c.max_wait_time, 1000)
        self.assertEqual(c.min_bytes, 1)
        self.assertEqual(c.max_bytes, (1024 * 1024))

    def test_allocation_must_be_defined(self):
        c = consumer.BaseConsumer(["kafka01", "kafka02"])

        with self.assertRaises(NotImplementedError):
            c.allocation

    @testing.gen_test
    def test_determine_offsets_must_be_defined(self):
        c = consumer.BaseConsumer(["kafka01", "kafka02"])

        with self.assertRaises(NotImplementedError):
            yield c.determine_offsets("test.topic")

    @testing.gen_test
    def test_consumer_tracks_offsets(self):
        self.add_topic("test.topic", leaders=(3, 8))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"foo": "bar"}',
                                                )
                                            ),
                                            (
                                                1,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"bwee": "bwoo"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet([]),
                                ),
                            ]
                        )
                    ]
                )
            ]
        )
        self.set_responses(
            broker_id=8, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=1,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"meow": "bark"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=1,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet([])
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = FakeConsumer(["kafka01", "kafka02"])

        yield c.connect()

        msgs = yield c.consume("test.topic")

        possible_orders = [
            [{"meow": "bark"}, {"foo": "bar"}, {"bwee": "bwoo"}],
            [{"foo": "bar"}, {"bwee": "bwoo"}, {"meow": "bark"}],
        ]

        self.assertTrue(
            any([msgs == possibility for possibility in possible_orders])
        )

        self.assert_sent(
            broker_id=3,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=0,
                                offset=0,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=8,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=1,
                                offset=0,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [])

        self.assert_sent(
            broker_id=3,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=0,
                                offset=2,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=8,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=1,
                                offset=1,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_custom_deserializer_and_options(self):
        self.add_topic("test.topic", leaders=(3,))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='cat',
                                                )
                                            ),
                                            (
                                                1,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='dog',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        results = [Exception(), "bark"]

        def deserializer(val):
            result = results.pop(0)
            if isinstance(result, Exception):
                raise result

            return "%s: %s" % (val, result)

        c = FakeConsumer(
            ["kafka01", "kafka02"],
            deserializer=deserializer,
            max_wait_time=500,
            min_bytes=1024, max_bytes=1024
        )

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, ["dog: bark"])

        self.assert_sent(
            broker_id=3,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=500,
                min_bytes=1024,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=0,
                                offset=0,
                                max_bytes=1024,
                            ),
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_consuming_when_closed_is_noop(self):
        self.add_topic("test.topic", leaders=(3,))

        c = FakeConsumer(["kafka01"])

        yield c.connect()

        yield c.close()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, None)

        self.assertEqual(self.requests_by_broker[3], [])

    @testing.gen_test
    def test_consuming_nonexistent_topic(self):
        self.add_topic("test.topic", leaders=(3,))

        c = FakeConsumer(["kafka01"])

        yield c.connect()

        msgs = yield c.consume("other.topic")

        self.assertEqual(c.cluster.heal.call_count, 1)

        self.assertEqual(msgs, [])

        self.assertEqual(self.requests_by_broker[3], [])

    @testing.gen_test
    def test_consuming_unknown_topic_reloads_metadata(self):
        self.add_topic("test.topic", leaders=(3,))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"cat": "dog"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = FakeConsumer(["kafka01"])

        # at this point the cluster hasn't synced, so "test.topic" is unknown
        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"cat": "dog"}])

        self.assertEqual(len(self.requests_by_broker[3]), 1)

    @testing.gen_test
    def test_retriable_code_when_consuming(self):
        self.add_topic("test.topic", leaders=(3,))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.leader_not_available,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet([])
                                ),
                            ]
                        ),
                    ]
                ),
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"cat": "dog"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = FakeConsumer(["kafka01"])

        yield c.connect()

        msgs = yield c.consume("test.topic")

        c.cluster.heal.assert_called_once_with()

        self.assertEqual(msgs, [])

        self.assertEqual(len(self.requests_by_broker[3]), 1)

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"cat": "dog"}])

        self.assertEqual(len(self.requests_by_broker[3]), 2)

    @testing.gen_test
    def test_fatal_code_when_consuming(self):
        self.add_topic("test.topic", leaders=(3,))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.message_size_too_large,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet([])
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = FakeConsumer(["kafka01"])

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [])

        self.assertEqual(len(self.requests_by_broker[3]), 1)

    @testing.gen_test
    def test_offset_out_of_range_error(self):
        self.add_topic("test.topic", leaders=(3,))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.offset_out_of_range,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet([])
                                ),
                            ]
                        ),
                    ]
                ),
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"cat": "dog"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = FakeConsumer(["kafka01"])

        yield c.connect()

        c.offsets["test.topic"][0] = 80
        c.synced_offsets.add("test.topic")

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [])

        self.assert_sent(
            broker_id=3,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=0,
                                offset=80,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"cat": "dog"}])

        self.assert_sent(
            broker_id=3,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=0,
                                offset=0,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_max_bytes_at_partition_level(self):
        self.add_topic("test.topic", leaders=(3, 3))
        self.set_responses(
            broker_id=3, api="fetch",
            responses=[
                fetch.FetchResponse(
                    topics=[
                        fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                fetch.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"foo": "bar"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                                fetch.PartitionResponse(
                                    partition_id=1,
                                    error_code=errors.no_error,
                                    highwater_mark_offset=2,
                                    message_set=messages.MessageSet(
                                        messages=[
                                            (
                                                0,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"bwee": "bwoo"}',
                                                )
                                            ),
                                        ]
                                    )
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = FakeConsumer(["kafka01", "kafka02"], max_bytes=(1024 * 1024))

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"foo": "bar"}, {"bwee": "bwoo"}])

        self.assert_sent(
            broker_id=3,
            request=fetch.FetchRequest(
                replica_id=-1,
                max_wait_time=1000,
                min_bytes=1,
                topics=[
                    fetch.TopicRequest(
                        name="test.topic",
                        partitions=[
                            fetch.PartitionRequest(
                                partition_id=0,
                                offset=0,
                                max_bytes=(512 * 1024),
                            ),
                            fetch.PartitionRequest(
                                partition_id=1,
                                offset=0,
                                max_bytes=(512 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )
