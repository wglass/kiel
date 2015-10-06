import collections
import logging

from tests import cases

from mock import patch, Mock
from tornado import testing, gen, iostream

from kiel.protocol import metadata, errors
from kiel import cluster, exc


log = logging.getLogger(__name__)


class ClusterTests(cases.AsyncTestCase):

    def setUp(self):
        super(ClusterTests, self).setUp()

        # (host, port) => mock object
        self.broker_hosts = {}
        # (host, port) => list of requests
        self.sent = collections.defaultdict(list)

        connection_patcher = patch.object(cluster, "Connection")
        Connection = connection_patcher.start()
        self.addCleanup(connection_patcher.stop)

        def get_conn(host, port):
            if (host, port) not in self.broker_hosts:
                raise Exception("no such host!")

            return self.broker_hosts[(host, port)]

        Connection.side_effect = get_conn

    def add_broker(self, host, port, responses=[], connect_error=None):
        conn = Mock()
        conn.host = host
        conn.port = port
        conn.closing = False

        if connect_error:
            conn.connect.return_value = self.future_error(connect_error)
        else:
            conn.connect.return_value = self.future_value(None)

        @gen.coroutine
        def get_next_response(req):
            log.debug("sent to %s:%s: %s", host, port, req)
            self.sent[(host, port)].append(req)
            raise gen.Return(responses.pop(0))

        conn.send.side_effect = get_next_response

        self.broker_hosts[(host, port)] = conn

    def assert_sent(self, host, port, request):
        for sent in self.sent[(host, port)]:
            if request == sent:
                return True

        raise AssertionError(
            "Request not sent to %s:%s: %s" % (host, port, request)
        )

    def test_defaults(self):
        c = cluster.Cluster(["kafka01", "kafka02"])

        self.assertEqual(c.bootstrap_hosts, ["kafka01", "kafka02"])
        self.assertEqual(c.conns, {})
        self.assertEqual(c.topics, {})
        self.assertEqual(c.leaders, {})

    def test_getitem(self):
        c = cluster.Cluster(["kafka01", "kafka02"])

        conn1 = Mock()
        conn2 = Mock()

        c.conns = {1: conn1, 2: conn2}

        self.assertEqual(c[1], conn1)
        self.assertEqual(c[2], conn2)
        with self.assertRaises(KeyError):
            c[3]

    def test_contains(self):
        c = cluster.Cluster(["kafka01", "kafka02"])

        conn1 = Mock()
        conn2 = Mock()

        c.conns = {1: conn1, 2: conn2}

        self.assertEqual(1 in c, True)
        self.assertEqual(2 in c, True)
        self.assertEqual(3 in c, False)

    def test_iteration(self):
        c = cluster.Cluster(["kafka01", "kafka02"])

        conn1 = Mock()
        conn2 = Mock()
        conn3 = Mock()

        c.conns = {1: conn1, 3: conn2, 8: conn3}

        self.assertEqual(set(list(c)), set([1, 3, 8]))

    def test_get_leader(self):
        c = cluster.Cluster(["kafka01", "kafka02"])

        conn1 = Mock()
        conn2 = Mock()

        c.conns = {3: conn1, 8: conn2}
        c.leaders = {
            "test.topic": [8, 3],
            "other.topic": [3],
        }

        self.assertEqual(c.get_leader("test.topic", 0), 8)
        self.assertEqual(c.get_leader("test.topic", 1), 3)
        self.assertEqual(c.get_leader("other.topic", 0), 3)
        with self.assertRaises(KeyError):
            c.get_leader("no.such.topic", 3)

    @testing.gen_test
    def test_start_uses_metadata_api(self):
        self.add_broker(
            "kafka01", 9092,
            responses=[
                metadata.MetadataResponse(brokers=[], topics=[])
            ]
        )

        c = cluster.Cluster(["kafka01", "kafka02:9000"])

        yield c.start()

        self.assert_sent("kafka01", 9092, metadata.MetadataRequest(topics=[]))

    @testing.gen_test
    def test_start_exceptions_in_all_connections(self):
        self.add_broker(
            "kafka01", 9092,
            connect_error=Exception("oh no!")
        )
        self.add_broker(
            "kafka02", 9000,
            connect_error=iostream.StreamClosedError(),
        )
        self.add_broker(
            "kafka03", 9092,
            connect_error=exc.ConnectionError("kafka03", 9092),
        )

        c = cluster.Cluster(["kafka01", "kafka02:9000", "kafka03"])

        with self.assertRaises(exc.NoBrokersError):
            yield c.start()

    def test_stop_closes_all_connections(self):
        c = cluster.Cluster(["kafka01", "kafka02"])

        conn1 = Mock()
        conn2 = Mock()

        c.conns = {3: conn1, 8: conn2}

        c.stop()

        conn1.close.assert_called_once_with()
        conn2.close.assert_called_once_with()

    @testing.gen_test
    def test_heal(self):
        response = metadata.MetadataResponse(
            brokers=[
                metadata.Broker(broker_id=2, host="kafka01", port=9092),
                metadata.Broker(broker_id=8, host="kafka02", port=9000),
                metadata.Broker(broker_id=7, host="kafka03", port=9092),
            ],
            topics=[
                metadata.TopicMetadata(
                    error_code=errors.no_error,
                    name="test.topic",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=0,
                            leader=8,
                            replicas=[7, 2],
                            isrs=[7, 2],
                        ),
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=1,
                            leader=7,
                            replicas=[2, 8],
                            isrs=[2, 8],
                        ),
                    ]
                ),
                metadata.TopicMetadata(
                    error_code=errors.no_error,
                    name="other.topic",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=3,
                            leader=2,
                            replicas=[8, 7],
                            isrs=[7],
                        ),
                    ]
                ),
            ],
        )

        self.add_broker("kafka01", 9092, responses=[response])
        self.add_broker("kafka02", 9000, responses=[response])
        self.add_broker("kafka04", 9092, responses=[response])
        self.add_broker("kafka03", 9092, responses=[])

        c = cluster.Cluster(["kafka01", "kafka02:900"])

        # in the response, stays where it is
        conn1 = cluster.Connection("kafka01", 9092)
        # closing but in response, gets replaced
        conn2 = cluster.Connection("kafka02", 9000)
        conn2.closing = True
        # not in response, gets aborted and dropped
        conn3 = cluster.Connection("kafka04", 9092)

        c.conns = {2: conn1, 8: conn2, 1: conn3}

        def remove_connection():
            del c.conns[1]

        c.conns[1].abort.side_effect = remove_connection

        yield c.heal()

        self.assertEqual(c.topics, {"test.topic": [0, 1], "other.topic": [3]})
        self.assertEqual(
            c.leaders,
            {"test.topic": {0: 8, 1: 7}, "other.topic": {3: 2}}
        )

        self.assertEqual(set(list(c)), set([2, 7, 8]))

        self.assertEqual(c[7], self.broker_hosts[("kafka03", 9092)])

        conn3.abort.assert_called_once_with()

    @testing.gen_test
    def test_heal_with_no_working_connections_raises_no_brokers(self):
        c = cluster.Cluster(["kafka01", "kafka02:900"])

        c.conns[1] = Mock()
        c.conns[1].send.side_effect = iostream.StreamClosedError

        with self.assertRaises(exc.NoBrokersError):
            yield c.heal()

    @testing.gen_test
    def test_heal_with_intial_topic_errors(self):
        initial = metadata.MetadataResponse(
            brokers=[
                metadata.Broker(broker_id=2, host="kafka01", port=9092),
            ],
            topics=[
                metadata.TopicMetadata(
                    error_code=errors.no_error,
                    name="test.topic",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=errors.leader_not_available,
                            partition_id=0,
                            leader=8,
                            replicas=[7, 2],
                            isrs=[7, 2],
                        ),
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=1,
                            leader=7,
                            replicas=[2, 8],
                            isrs=[2, 8],
                        ),
                    ]
                ),
                metadata.TopicMetadata(
                    error_code=errors.replica_not_available,
                    name="other.topic",
                    partitions=[],
                ),
                metadata.TopicMetadata(
                    error_code=errors.unknown_topic_or_partition,
                    name="fake.topic",
                    partitions=[],
                ),
            ],
        )
        fixed = metadata.MetadataResponse(
            brokers=[
                metadata.Broker(broker_id=2, host="kafka01", port=9092),
            ],
            topics=[
                metadata.TopicMetadata(
                    error_code=errors.no_error,
                    name="test.topic",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=0,
                            leader=2,
                            replicas=[],
                            isrs=[],
                        ),
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=1,
                            leader=2,
                            replicas=[],
                            isrs=[],
                        ),
                    ]
                ),
            ],
        )
        self.add_broker("kafka01", 9092, responses=[initial, fixed])

        conn1 = cluster.Connection("kafka01", 9092)

        c = cluster.Cluster(["kafka01", "kafka02:900"])
        c.conns = {2: conn1}

        yield c.heal()

        self.assertEqual(c.topics, {"test.topic": [0, 1]})
        self.assertEqual(c.leaders, {"test.topic": {0: 2, 1: 2}})

    @testing.gen_test
    def test_heal_with_initial_broker_errors(self):
        initial = metadata.MetadataResponse(
            brokers=[
                metadata.Broker(broker_id=2, host="kafka01", port=9092),
                metadata.Broker(broker_id=7, host="kafka02", port=9000),
                metadata.Broker(broker_id=8, host="kafka03", port=9092),
            ],
            topics=[
                metadata.TopicMetadata(
                    error_code=errors.no_error,
                    name="test.topic",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=0,
                            leader=2,
                            replicas=[],
                            isrs=[],
                        ),
                    ]
                ),
            ],
        )
        fixed = metadata.MetadataResponse(
            brokers=[
                metadata.Broker(broker_id=2, host="kafka01", port=9092),
            ],
            topics=[
                metadata.TopicMetadata(
                    error_code=errors.no_error,
                    name="test.topic",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=errors.no_error,
                            partition_id=0,
                            leader=2,
                            replicas=[],
                            isrs=[],
                        ),
                    ]
                ),
            ],
        )
        self.add_broker("kafka01", 9092, responses=[initial, fixed])
        self.add_broker(
            "kafka02", 9000,
            connect_error=iostream.StreamClosedError(),
        )
        self.add_broker(
            "kafka03", 9092,
            connect_error=exc.ConnectionError("kafka03", 9092),
        )

        conn1 = cluster.Connection("kafka01", 9092)

        c = cluster.Cluster(["kafka01", "kafka02:900"])
        c.conns = {2: conn1}

        yield c.heal()

        self.assertEqual([2], list(c.conns.keys()))
