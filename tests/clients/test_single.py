import datetime

from tests import cases

from mock import patch
from tornado import testing

from kiel.protocol import offset, fetch, messages, errors
from kiel.clients import single


class SingleConsumerTests(cases.ClientTestCase):

    def setUp(self):
        super(SingleConsumerTests, self).setUp()

        self.add_broker("kafka01", 9002, broker_id=1)
        self.add_broker("kafka02", 9002, broker_id=3)
        self.add_broker("kafka03", 9002, broker_id=8)

    @testing.gen_test
    def test_allocation_is_all_topics(self):
        self.add_topic("test.topic.1", leaders=(1, 3))
        self.add_topic("test.topic.2", leaders=(8,))

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        self.assertEqual(
            c.allocation,
            {"test.topic.1": [0, 1], "test.topic.2": [0]}
        )

    @testing.gen_test
    def test_default_consumes_from_end_offset(self):
        self.add_topic("test.topic", leaders=(1,))
        self.set_responses(
            broker_id=1, api="offset",
            responses=[
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offsets=[99],
                                )
                            ]
                        )
                    ]
                )
            ]
        )
        self.set_responses(
            broker_id=1, api="fetch",
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
                                                    value='{"cat": "meow"}',
                                                )
                                            ),
                                            (
                                                1,
                                                messages.Message(
                                                    magic=0, attributes=0,
                                                    key=None,
                                                    value='{"dog": "bark"}',
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

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        msgs = yield c.consume("test.topic")

        yield c.close()

        self.assertEqual(msgs, [{"cat": "meow"}, {"dog": "bark"}])

        self.assert_sent(
            broker_id=1,
            request=offset.OffsetRequest(
                replica_id=-1,
                topics=[
                    offset.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset.PartitionRequest(
                                partition_id=0,
                                time=-1,  # alias for 'end of topic'
                                max_offsets=1,
                            )
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=1,
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
                                offset=99,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_beginning_offset(self):
        self.add_topic("test.topic", leaders=(1,))
        self.set_responses(
            broker_id=1, api="offset",
            responses=[
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offsets=[99],
                                )
                            ]
                        )
                    ]
                )
            ]
        )
        self.set_responses(
            broker_id=1, api="fetch",
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
                                    message_set=messages.MessageSet([]),
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        yield c.consume("test.topic", start=single.SingleConsumer.BEGINNING)

        self.assert_sent(
            broker_id=1,
            request=offset.OffsetRequest(
                replica_id=-1,
                topics=[
                    offset.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset.PartitionRequest(
                                partition_id=0,
                                time=-2,  # alias for 'beginning of topic'
                                max_offsets=1,
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_datetime_offset(self):
        self.add_topic("test.topic", leaders=(1,))
        self.set_responses(
            broker_id=1, api="offset",
            responses=[
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offsets=[99],
                                )
                            ]
                        )
                    ]
                )
            ]
        )
        self.set_responses(
            broker_id=1, api="fetch",
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
                                    message_set=messages.MessageSet([]),
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        start = datetime.datetime(2016, 2, 18, 0, 0)
        secs_since_epoch = (
            start - datetime.datetime(1970, 1, 1, 0, 0)
        ).total_seconds()

        yield c.consume("test.topic", start=start)

        self.assert_sent(
            broker_id=1,
            request=offset.OffsetRequest(
                replica_id=-1,
                topics=[
                    offset.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset.PartitionRequest(
                                partition_id=0,
                                time=secs_since_epoch,
                                max_offsets=1,
                            )
                        ]
                    )
                ]
            )
        )

    @patch.object(single, "calendar")
    @testing.gen_test
    def test_timedelta_offset(self, mock_calendar):
        # captured the epoch seconds when first writing this
        written_epoch = 1455849320
        mock_calendar.timegm.return_value = written_epoch

        self.add_topic("test.topic", leaders=(1,))
        self.set_responses(
            broker_id=1, api="offset",
            responses=[
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offsets=[99],
                                )
                            ]
                        )
                    ]
                )
            ]
        )
        self.set_responses(
            broker_id=1, api="fetch",
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
                                    message_set=messages.MessageSet([]),
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        two_days = datetime.timedelta(days=2)

        yield c.consume("test.topic", start=two_days)

        self.assert_sent(
            broker_id=1,
            request=offset.OffsetRequest(
                replica_id=-1,
                topics=[
                    offset.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset.PartitionRequest(
                                partition_id=0,
                                # two days *in the past*
                                time=written_epoch - two_days.total_seconds(),
                                max_offsets=1,
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_retriable_error_for_offset(self):
        self.add_topic("test.topic", leaders=(1,))
        self.set_responses(
            broker_id=1, api="offset",
            responses=[
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.request_timed_out,
                                    offsets=[],
                                )
                            ]
                        )
                    ]
                ),
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.no_error,
                                    offsets=[99],
                                )
                            ]
                        )
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=1, api="fetch",
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
                                    message_set=messages.MessageSet([
                                        (
                                            0,
                                            messages.Message(
                                                magic=0, attributes=0,
                                                key=None,
                                                value='{"cat": "meow"}',
                                            )
                                        ),
                                    ]),
                                ),
                            ]
                        ),
                    ]
                ),
            ]
        )

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"cat": "meow"}])

    @testing.gen_test
    def test_fatal_error_for_offsets(self):
        self.add_topic("test.topic", leaders=(1,))
        self.set_responses(
            broker_id=1, api="offset",
            responses=[
                offset.OffsetResponse(
                    topics=[
                        offset.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset.PartitionResponse(
                                    partition_id=0,
                                    error_code=errors.unknown,
                                    offsets=[],
                                )
                            ]
                        )
                    ]
                ),
            ]
        )

        c = single.SingleConsumer(["kafka01"])

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [])
