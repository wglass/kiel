import unittest

from kiel import exc


class ExceptionTests(unittest.TestCase):

    def test_broker_connection_error(self):
        e = exc.BrokerConnectionError("kafka01", 4455)

        self.assertEqual(e.host, "kafka01")
        self.assertEqual(e.port, 4455)

        self.assertEqual(str(e), "Error connecting to kafka01:4455")

    def test_broker_connection_error_with_broker_id(self):
        e = exc.BrokerConnectionError("kafka01", 4455, broker_id=8)

        self.assertEqual(e.host, "kafka01")
        self.assertEqual(e.port, 4455)
        self.assertEqual(e.broker_id, 8)

        self.assertEqual(str(e), "Error connecting to kafka01:4455")

    def test_unhanded_response_error(self):
        e = exc.UnhandledResponseError("offset")

        self.assertEqual(e.api, "offset")

        self.assertEqual(str(e), "No handler method for 'offset' api")
