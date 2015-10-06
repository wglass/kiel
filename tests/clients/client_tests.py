from tests import cases

from mock import Mock
from tornado import testing, iostream

from kiel import exc
from kiel.clients import client


class ClientTests(cases.ClientTestCase):

    def setUp(self):
        super(ClientTests, self).setUp()

        self.add_broker("kafka01", 9202, broker_id=1)
        self.add_broker("kafka02", 9202, broker_id=8)

    def test_instantiation(self):
        c = client.Client(["kafka01", "kafka02"])

        self.assertEqual(c.closing, False)
        self.assertEqual(c.heal_cluster, False)

    @testing.gen_test
    def test_wind_down_must_be_implemented(self):
        c = client.Client([])

        error = None
        try:
            yield c.wind_down()
        except Exception as e:
            error = e

        self.assertIsInstance(error, NotImplementedError)

    @testing.gen_test
    def test_connect_starts_cluster(self):
        c = client.Client([])

        yield c.connect()

        c.cluster.start.assert_called_once_with()

    @testing.gen_test
    def test_close(self):
        c = client.Client([])
        c.wind_down = Mock()
        c.wind_down.return_value = self.future_value(None)

        yield c.close()

        self.assertEqual(c.closing, True)
        c.wind_down.assert_called_once_with()
        c.cluster.stop.assert_called_once_with()

    @testing.gen_test
    def test_send_connection_error(self):
        self.set_responses(
            broker_id=1, api="metadata",
            responses=[
                exc.ConnectionError("kafka01", 1234)
            ]
        )

        c = client.Client(["kafka01", "kafka02"])

        request = Mock(api="metadata")

        results = yield c.send({1: request})

        self.assert_sent(1, request)

        c.cluster.heal.assert_called_once_with()

        self.assertEqual(results, {})

    @testing.gen_test
    def test_send_stream_closed(self):
        self.set_responses(
            broker_id=1, api="metadata",
            responses=[
                iostream.StreamClosedError(),
            ]
        )

        c = client.Client(["kafka01", "kafka02"])

        request = Mock(api="metadata")

        results = yield c.send({1: request})

        self.assert_sent(1, request)

        self.assertEqual(c.cluster.heal.called, False)

        self.assertEqual(results, {})

    @testing.gen_test
    def test_send_error_on_one_broker(self):
        metadata_response = Mock(api="consumer_metadata")
        self.set_responses(
            broker_id=1, api="metadata",
            responses=[Exception()],
        )
        self.set_responses(
            broker_id=8, api="consumer_metadata",
            responses=[metadata_response]
        )

        c = client.Client(["kafka01", "kafka02"])
        c.handle_consumer_metadata_response = Mock()

        request1 = Mock(api="metadata")
        request2 = Mock(api="consumer_metadata")

        results = yield c.send({1: request1, 8: request2})

        self.assertEqual(
            results,
            {8: c.handle_consumer_metadata_response.return_value}
        )

        self.assert_sent(1, request1)
        self.assert_sent(8, request2)

        c.cluster.heal.assert_called_once_with()

    @testing.gen_test
    def test_send_no_handler(self):
        self.set_responses(
            broker_id=8, api="produce",
            responses=[Mock(api="produce")],
        )

        c = client.Client(["kafka01", "kafka02"])

        error = None
        try:
            yield c.send({8: Mock(api="produce")})
        except Exception as e:
            error = e

        self.assertIsInstance(error, exc.UnhandledResponseError)
        self.assertEqual(error.api, "produce")

    @testing.gen_test
    def test_send_with_handlers(self):
        metadata_response = Mock(api="consumer_metadata")
        fetch_response = Mock(api="fetch")
        self.set_responses(
            broker_id=1, api="fetch",
            responses=[fetch_response],
        )
        self.set_responses(
            broker_id=8, api="consumer_metadata",
            responses=[metadata_response]
        )

        c = client.Client(["kafka01", "kafka02"])
        c.handle_consumer_metadata_response = Mock()
        c.handle_fetch_response = Mock()

        request1 = Mock(api="fetch")
        request2 = Mock(api="consumer_metadata")

        results = yield c.send({1: request1, 8: request2})

        self.assertEqual(
            results,
            {
                1: c.handle_fetch_response.return_value,
                8: c.handle_consumer_metadata_response.return_value
            }
        )

        self.assert_sent(1, request1)
        self.assert_sent(8, request2)

        c.handle_consumer_metadata_response.assert_called_once_with(
            metadata_response
        )
        c.handle_fetch_response.assert_called_once_with(fetch_response)

        self.assertFalse(c.cluster.heal.called)

    @testing.gen_test
    def test_send_with_async_handlers(self):
        self.set_responses(
            broker_id=1, api="consumer_metadata",
            responses=[Mock(api="consumer_metadata")],
        )
        self.set_responses(
            broker_id=8, api="fetch",
            responses=[Mock(api="fetch")],
        )

        c = client.Client(["kafka01", "kafka02"])
        c.handle_consumer_metadata_response = Mock()
        c.handle_consumer_metadata_response.return_value = self.future_value(
            "metadata handled!"
        )
        c.handle_fetch_response = Mock()
        c.handle_fetch_response.return_value = self.future_value(
            "fetch handled!"
        )

        results = yield c.send(
            {1: Mock(api="consumer_metadata"), 8: Mock(api="fetch")}
        )

        self.assertEqual(
            results,
            {
                1: "metadata handled!",
                8: "fetch handled!",
            }
        )

        self.assertFalse(c.cluster.heal.called)

    @testing.gen_test
    def test_send_handler_sets_heal_flag(self):
        self.set_responses(
            broker_id=1, api="fetch",
            responses=[Mock(api="fetch")],
        )
        self.set_responses(
            broker_id=8, api="offset",
            responses=[Mock(api="offset")],
        )

        c = client.Client(["kafka01", "kafka02"])
        c.handle_fetch_response = Mock()
        c.handle_offset_response = Mock()

        def handle_response(response):
            c.heal_cluster = True
            return "%s handled!" % response.api

        c.handle_fetch_response.side_effect = handle_response
        c.handle_offset_response.side_effect = handle_response

        results = yield c.send({1: Mock(api="fetch"), 8: Mock(api="offset")})

        self.assertEqual(
            results,
            {
                1: "fetch handled!",
                8: "offset handled!",
            }
        )

        c.cluster.heal.assert_called_once_with()
