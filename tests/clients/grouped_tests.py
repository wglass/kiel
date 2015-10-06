from tests import cases

from mock import patch
from tornado import testing

from kiel import exc
from kiel.protocol import (
    consumer_metadata, offset_fetch, offset_commit, fetch, messages, errors
)
from kiel.clients import grouped


class GroupedClientTests(cases.ClientTestCase):

    def setUp(self):
        super(GroupedClientTests, self).setUp()

        self.add_broker("kafka01", 9002, broker_id=1)
        self.add_broker("kafka02", 9002, broker_id=3)
        self.add_broker("kafka03", 9002, broker_id=8)

        for broker_id in (1, 3, 8):
            self.set_responses(
                broker_id=broker_id, api="consumer_metadata",
                responses=[
                    consumer_metadata.ConsumerMetadataResponse(
                        error_code=errors.no_error,
                        coordinator_id=3,
                        coordinator_host="kafka02",
                        coordinator_port=9002,
                    )
                ]
            )

        allocator_patcher = patch.object(grouped, "PartitionAllocator")
        self.PartitionAllocator = allocator_patcher.start()
        self.addCleanup(allocator_patcher.stop)

        self.allocator = self.PartitionAllocator.return_value
        self.allocator.start.return_value = self.future_value(None)
        self.allocator.stop.return_value = self.future_value(None)

    def test_defaults(self):
        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "my-work-group",
            zk_hosts=["zookeeper01", "zookeeper02", "zookeeper03"]
        )

        self.assertEqual(c.group_name, "my-work-group")
        self.assertEqual(c.allocator, self.allocator)
        self.assertEqual(c.autocommit, True)

        self.PartitionAllocator.called_once_with(
            ["zookeeper01", "zookeeper02", "zookeeper03"],
            "my-work-group", c.name,
            allocator_fn=grouped.naive_allocator,
            on_rebalance=c.synced_offsets.clear
        )

    def test_default_allocator_fn(self):
        mapping = grouped.naive_allocator(
            ["client1:1233", "client1:4444", "client2:5555"],
            [
                "test.topic.1:0",
                "test.topic.1:1",
                "test.topic.2:0",
                "test.topic.2:3",
                "test.topic.2:6",
                "test.topic.2:7",
                "test.topic.4:8",
            ]
        )

        self.assertEqual(
            mapping,
            {
                "client1:1233": {
                    "test.topic.1": [0],
                    "test.topic.2": [3],
                    "test.topic.4": [8],
                },
                "client1:4444": {
                    "test.topic.1": [1],
                    "test.topic.2": [6],
                },
                "client2:5555": {
                    "test.topic.2": [0, 7],
                }
            }
        )

    @testing.gen_test
    def test_connect_determines_coordinator(self):
        self.add_topic("test.topic", leaders=(1, 3))

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "my-work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        self.assertEqual(c.coordinator_id, 3)
        self.allocator.start.assert_called_once_with(
            {"test.topic": [0, 1]}
        )

    @testing.gen_test
    def test_wind_down_stops_allocator(self):
        self.add_topic("test.topic", leaders=(1, 3))

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "my-work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.wind_down()

        self.allocator.stop.assert_called_once_with()

    @testing.gen_test
    def test_retriable_error_getting_coordinator(self):
        self.add_topic("test.topic", leaders=(1, 3))

        for broker_id in (1, 3, 8):
            self.set_responses(
                broker_id=broker_id, api="consumer_metadata",
                responses=[
                    consumer_metadata.ConsumerMetadataResponse(
                        error_code=errors.request_timed_out,
                    ),
                    consumer_metadata.ConsumerMetadataResponse(
                        error_code=errors.no_error,
                        coordinator_id=8,
                        coordinator_host="kafka02",
                        coordinator_port=9002,
                    ),
                ]
            )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "my-work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        self.assertEqual(c.coordinator_id, 8)

    @testing.gen_test
    def test_fatal_error_getting_coordinator(self):
        self.add_topic("test.topic", leaders=(1, 3))

        for broker_id in (1, 3, 8):
            self.set_responses(
                broker_id=broker_id, api="consumer_metadata",
                responses=[
                    consumer_metadata.ConsumerMetadataResponse(
                        error_code=errors.unknown
                    ),
                    consumer_metadata.ConsumerMetadataResponse(
                        error_code=errors.no_error,
                        coordinator_id=8,
                        coordinator_host="kafka02",
                        coordinator_port=9002,
                    ),
                ]
            )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "my-work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        self.assertEqual(c.coordinator_id, None)

    @testing.gen_test
    def test_no_brokers_when_getting_coordinator(self):
        self.add_topic("test.topic", leaders=(1, 3))

        self.mock_brokers = {}

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "my-work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        with self.assertRaises(exc.NoBrokersError):
            yield c.connect()

    @testing.gen_test
    def test_consume_with_autocommit(self):
        self.add_topic("test.topic", leaders=(1, 8))
        self.allocator.allocation = {"test.topic": [1]}

        self.set_responses(
            broker_id=3, api="offset_fetch",
            responses=[
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=0,
                                    offset=80,
                                    metadata="committed, ok!"
                                )
                            ]
                        )
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=3, api="offset_commit",
            responses=[
                offset_commit.OffsetCommitResponse(
                    topics=[
                        offset_commit.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_commit.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=1,
                                )
                            ]
                        ),
                    ]
                ),
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
                                    message_set=messages.MessageSet([
                                        (
                                            80,
                                            messages.Message(
                                                magic=0, attributes=0,
                                                key=None,
                                                value='{"cat": "meow"}',
                                            )
                                        ),
                                    ])
                                ),
                            ]
                        ),
                    ]
                )
            ]
        )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"cat": "meow"}])

        self.assert_sent(
            broker_id=3,
            request=offset_fetch.OffsetFetchRequest(
                group_name="work-group",
                topics=[
                    offset_fetch.TopicRequest(
                        name="test.topic",
                        partitions=[1],
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=3,
            request=offset_commit.OffsetCommitV0Request(
                group="work-group",
                topics=[
                    offset_commit.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset_commit.PartitionRequest(
                                partition_id=1,
                                offset=81,
                                metadata="committed by %s" % c.name
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_consume_without_autocommit(self):
        self.add_topic("test.topic", leaders=(1, 8))
        self.allocator.allocation = {"test.topic": [0, 1]}

        self.set_responses(
            broker_id=3, api="offset_fetch",
            responses=[
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=0,
                                    offset=80,
                                    metadata="committed, ok!"
                                ),
                                offset_fetch.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=1,
                                    offset=110,
                                    metadata="committed, ok!"
                                ),
                            ]
                        )
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=3, api="offset_commit",
            responses=[
                offset_commit.OffsetCommitResponse(
                    topics=[
                        offset_commit.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_commit.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=1,
                                )
                            ]
                        ),
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
                                            80,
                                            messages.Message(
                                                magic=0, attributes=0,
                                                key=None,
                                                value='{"cat": "meow"}',
                                            )
                                        ),
                                    ])
                                ),
                            ]
                        ),
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
                                    message_set=messages.MessageSet([
                                        (
                                            110,
                                            messages.Message(
                                                magic=0, attributes=0,
                                                key=None,
                                                value='{"cat": "meow"}',
                                            )
                                        ),
                                    ])
                                ),
                            ]
                        ),
                    ]
                )
            ]
        )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "work-group",
            zk_hosts=["zk01", "zk02", "zk03"],
            autocommit=False
        )

        yield c.connect()

        yield c.consume("test.topic")

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
                                offset=80,
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
                                offset=110,
                                max_bytes=(1024 * 1024),
                            ),
                        ]
                    )
                ]
            )
        )

        yield c.commit_offsets()

        self.assert_sent(
            broker_id=3,
            request=offset_commit.OffsetCommitV0Request(
                group="work-group",
                topics=[
                    offset_commit.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset_commit.PartitionRequest(
                                partition_id=0,
                                offset=81,
                                metadata="committed by %s" % c.name
                            ),
                            offset_commit.PartitionRequest(
                                partition_id=1,
                                offset=111,
                                metadata="committed by %s" % c.name
                            ),
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_commit_offset_with_large_metadata(self):
        self.add_topic("test.topic", leaders=(1, 8))
        self.allocator.allocation = {"test.topic": [1]}

        self.set_responses(
            broker_id=3, api="offset_fetch",
            responses=[
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=0,
                                    offset=80,
                                    metadata="committed, ok!"
                                )
                            ]
                        )
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=3, api="offset_commit",
            responses=[
                offset_commit.OffsetCommitResponse(
                    topics=[
                        offset_commit.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_commit.PartitionResponse(
                                    error_code=(
                                        errors.offset_metadata_too_large
                                    ),
                                    partition_id=1,
                                )
                            ]
                        ),
                    ]
                ),
                offset_commit.OffsetCommitResponse(
                    topics=[
                        offset_commit.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_commit.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=1,
                                )
                            ]
                        ),
                    ]
                ),
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
                                    message_set=messages.MessageSet([
                                        (
                                            80,
                                            messages.Message(
                                                magic=0, attributes=0,
                                                key=None,
                                                value='{"cat": "meow"}',
                                            )
                                        ),
                                    ])
                                ),
                            ]
                        ),
                    ]
                )
            ]
        )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [{"cat": "meow"}])

        self.assert_sent(
            broker_id=3,
            request=offset_commit.OffsetCommitV0Request(
                group="work-group",
                topics=[
                    offset_commit.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset_commit.PartitionRequest(
                                partition_id=1,
                                offset=81,
                                metadata="committed by %s" % c.name
                            )
                        ]
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=3,
            request=offset_commit.OffsetCommitV0Request(
                group="work-group",
                topics=[
                    offset_commit.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset_commit.PartitionRequest(
                                partition_id=1,
                                offset=81,
                                metadata=""
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_consume_with_offset_fetch_and_commit_errors(self):
        self.add_topic("test.topic", leaders=(1, 8))
        self.allocator.allocation = {"test.topic": [1]}

        self.set_responses(
            broker_id=3, api="offset_fetch",
            responses=[
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.offsets_load_in_progress,
                                    partition_id=1,
                                )
                            ]
                        )
                    ]
                ),
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.request_timed_out,
                                    partition_id=1,
                                )
                            ]
                        )
                    ]
                ),
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.no_error,
                                    partition_id=1,
                                    offset=80,
                                    metadata="committed, ok!"
                                ),
                            ]
                        )
                    ]
                ),
            ]
        )
        self.set_responses(
            broker_id=3, api="offset_commit",
            responses=[
                offset_commit.OffsetCommitResponse(
                    topics=[
                        offset_commit.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_commit.PartitionResponse(
                                    error_code=errors.request_timed_out,
                                    partition_id=1,
                                )
                            ]
                        ),
                    ]
                ),
                offset_commit.OffsetCommitResponse(
                    topics=[
                        offset_commit.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_commit.PartitionResponse(
                                    error_code=errors.unknown,
                                    partition_id=1,
                                )
                            ]
                        ),
                    ]
                ),
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
                                    message_set=messages.MessageSet([])
                                ),
                            ]
                        ),
                    ]
                )
            ]
        )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        yield c.consume("test.topic")

        self.assert_sent(
            broker_id=3,
            request=offset_fetch.OffsetFetchRequest(
                group_name="work-group",
                topics=[
                    offset_fetch.TopicRequest(
                        name="test.topic",
                        partitions=[1],
                    )
                ]
            )
        )
        self.assert_sent(
            broker_id=3,
            request=offset_commit.OffsetCommitV0Request(
                group="work-group",
                topics=[
                    offset_commit.TopicRequest(
                        name="test.topic",
                        partitions=[
                            offset_commit.PartitionRequest(
                                partition_id=1,
                                offset=80,
                                metadata="committed by %s" % c.name
                            )
                        ]
                    )
                ]
            )
        )

    @testing.gen_test
    def test_fatal_error_fetch_offsets(self):
        self.add_topic("test.topic", leaders=(1, 8))
        self.allocator.allocation = {"test.topic": [1]}

        self.set_responses(
            broker_id=3, api="offset_fetch",
            responses=[
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.unknown,
                                    partition_id=1,
                                )
                            ]
                        )
                    ]
                ),
                offset_fetch.OffsetFetchResponse(
                    topics=[
                        offset_fetch.TopicResponse(
                            name="test.topic",
                            partitions=[
                                offset_fetch.PartitionResponse(
                                    error_code=errors.unknown,
                                    partition_id=1,
                                )
                            ]
                        )
                    ]
                ),
            ]
        )

        c = grouped.GroupedConsumer(
            ["kafka01", "kafka02"], "work-group",
            zk_hosts=["zk01", "zk02", "zk03"]
        )

        yield c.connect()

        msgs = yield c.consume("test.topic")

        self.assertEqual(msgs, [])
