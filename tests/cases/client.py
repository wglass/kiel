import collections

import six
from tornado import gen
from mock import patch, Mock

from .async import AsyncTestCase


class ClientTestCase(AsyncTestCase):

    def setUp(self):
        super(ClientTestCase, self).setUp()

        self.topic_leaders = {}
        self.mock_brokers = {}

        self.responses = collections.defaultdict(dict)
        self.requests_by_broker = collections.defaultdict(list)

        cluster_patcher = patch("kiel.clients.client.Cluster")
        MockCluster = cluster_patcher.start()
        self.addCleanup(cluster_patcher.stop)

        cluster = MockCluster.return_value
        cluster.topics = collections.defaultdict(list)
        cluster.leaders = collections.defaultdict(dict)

        def check_known_broker(broker_id):
            return broker_id in self.mock_brokers

        cluster.__contains__.side_effect = check_known_broker

        def get_mock_broker(broker_id):
            return self.mock_brokers[broker_id]

        cluster.__getitem__.side_effect = get_mock_broker

        def iterate_broker_ids():
            return iter(self.mock_brokers)

        cluster.__iter__.side_effect = iterate_broker_ids

        def get_leader(topic, partition):
            return cluster.leaders[topic][partition]

        cluster.get_leader.side_effect = get_leader

        @gen.coroutine
        def refresh_metadata():
            cluster.topics.clear()
            cluster.leaders.clear()
            for topic, leaders in six.iteritems(self.topic_leaders):
                for partition in list(range(len(leaders))):
                    cluster.topics[topic].append(partition)
                    cluster.leaders[topic][partition] = leaders[partition]

        cluster.start.side_effect = refresh_metadata
        cluster.heal.side_effect = refresh_metadata
        cluster.stop.return_value = self.future_value(None)

    def add_broker(self, host, port, broker_id):
        broker = Mock()

        @gen.coroutine
        def mock_send(request):
            self.requests_by_broker[broker_id].append(request)
            response = self.responses[broker_id][request.api].pop(0)
            if isinstance(response, Exception):
                raise response
            response.correlation_id = request.correlation_id
            raise gen.Return(response)

        broker.send.side_effect = mock_send

        self.mock_brokers[broker_id] = broker

    def add_topic(self, topic_name, leaders):
        self.topic_leaders[topic_name] = leaders

    def set_responses(self, broker_id, api, responses):
        self.responses[broker_id][api] = responses

    def assert_sent(self, broker_id, request):
        for sent in self.requests_by_broker[broker_id]:
            if request == sent:
                return

        raise AssertionError(
            "Request not sent to broker %s: %s" % (broker_id, request)
        )
