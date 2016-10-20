import struct

from tests import cases

from tornado import testing
from mock import patch, Mock

from kiel import exc
from kiel.protocol import metadata
from kiel.connection import Connection


class ConnectionTests(cases.AsyncTestCase):

    @patch("tornado.iostream.IOStream")
    @testing.gen_test
    def test_connect_sets_stream(self, IOStream):
        IOStream.return_value.connect.return_value = self.future_value(None)

        conn = Connection("localhost", 1234)

        self.assertEqual(conn.stream, None)

        yield conn.connect()

        self.assertEqual(conn.stream, IOStream.return_value)
        IOStream.return_value.connect.assert_called_once_with(
            ("localhost", 1234)
        )

    def test_close(self):
        conn = Connection("localhost", 1234)
        conn.stream = Mock()

        self.assertEqual(conn.closing, False)

        conn.close()

        self.assertEqual(conn.closing, True)
        conn.stream.close.assert_called_once_with()

    @testing.gen_test
    def test_send_when_closing_causes_error(self):
        error = None

        conn = Connection("localhost", 1234)
        conn.closing = True

        try:
            yield conn.send(Mock())
        except exc.BrokerConnectionError as e:
            error = e

        self.assertEqual(error.host, "localhost")
        self.assertEqual(error.port, 1234)

    @testing.gen_test
    def test_future_error_writing_to_stream_aborts(self):

        class FakeException(Exception):
            pass

        conn = Connection("localhost", 1234)
        conn.stream = Mock()
        conn.stream.write.return_value = self.future_error(
            FakeException("oh no!")
        )

        error = None

        try:
            yield conn.send(metadata.MetadataRequest())
        except FakeException as e:
            error = e

        self.assertEqual(str(error), "oh no!")
        self.assertEqual(conn.closing, True)
        conn.stream.close.assert_called_once_with()

    @testing.gen_test
    def test_immediate_error_writing_to_stream_aborts(self):

        class FakeException(Exception):
            pass

        conn = Connection("localhost", 1234)
        conn.stream = Mock()
        conn.stream.write.side_effect = FakeException("oh no!")

        error = None

        try:
            yield conn.send(metadata.MetadataRequest())
        except FakeException as e:
            error = e

        self.assertEqual(str(error), "oh no!")
        self.assertEqual(conn.closing, True)
        conn.stream.close.assert_called_once_with()

    @patch.object(Connection, "read_message")
    @testing.gen_test
    def test_correlates_responses(self, read_message):
        request1 = metadata.MetadataRequest()
        request2 = metadata.MetadataRequest(topics=["example.foo"])

        response1 = metadata.MetadataResponse(
            brokers=[metadata.Broker(broker_id=1, host="broker01", port=333)],
            topics=[
                metadata.TopicMetadata(error_code=0, name="example.foo"),
                metadata.TopicMetadata(error_code=0, name="example.bar"),
            ]
        )
        response1.correlation_id = request1.correlation_id
        response2 = metadata.MetadataResponse(
            brokers=[metadata.Broker(broker_id=1, host="broker01", port=333)],
            topics=[
                metadata.TopicMetadata(error_code=0, name="example.foo"),
            ]
        )
        response2.correlation_id = request2.correlation_id

        # response2 comes over the wire before response1
        responses = [response2, response1]

        def get_next_response(*args):
            return self.future_value(responses.pop(0))

        read_message.side_effect = get_next_response

        conn = Connection("localhost", 1234)
        conn.stream = Mock()
        conn.stream.write.return_value = self.future_value(None)

        actual_responses = [conn.send(request1), conn.send(request2)]

        yield conn.read_loop()

        # first response is the one with two topics
        self.assertEqual(len(actual_responses[0].result().topics), 2)
        self.assertEqual(len(actual_responses[1].result().topics), 1)

    @patch.object(Connection, "read_message")
    @testing.gen_test
    def test_abort_fails_all_pending_requests(self, read_message):
        request1 = metadata.MetadataRequest()
        request2 = metadata.MetadataRequest(topics=["example.foo"])

        mock_responses = [
            Mock(correlation_id=request1.correlation_id),
            Mock(correlation_id=request2.correlation_id),
        ]

        def get_next_response(*args):
            return self.future_value(mock_responses.pop(0))

        read_message.side_effect = get_next_response

        conn = Connection("localhost", 1234)
        conn.stream = Mock()
        conn.stream.write.return_value = self.future_value(None)

        responses = [conn.send(request1), conn.send(request2)]

        conn.abort()
        conn.abort()  # second abort is a no-op

        for response in responses:
            error = response.exception()
            self.assertEqual(error.host, "localhost")
            self.assertEqual(error.port, 1234)

    @testing.gen_test
    def test_read_message(self):
        response_format = "".join([
            "!", "i", "i", "h%dsi" % len("broker01"),  # array of brokers
            "i", "hh%ds" % len("example.foo"),  # array of topics
            "i",  # subarray of partitions
            "hii", "i", "ii", "i", "i",  # partition 1 details
            "hii", "i", "ii", "i", "ii",  # partition 2 details
        ])
        raw_response = struct.pack(
            response_format,
            1,  # there is 1 broker
            8, len("broker01"), b"broker01", 1234,  # broker id,host,port
            1,  # there is 1 topic
            0, len("example.foo"), b"example.foo",  # topic name, no error
            2,  # there are 2 topics
            0, 1, 1,  # partition ID 1, leader is broker 1
            2, 2, 3, 1, 2,  # two replicas: on 2 & 3, one ISR: broker 2
            0, 2, 3,  # partition ID 2, leader is broker 3
            2, 1, 2, 2, 2, 1,  # two replicas: on 1 & 2, both are in ISR set
        )

        raw_data = [
            # size of full response (incl. correlation)
            struct.pack("!i", struct.calcsize(response_format) + 4),
            struct.pack("!i", 555),  # correlation id
            raw_response
        ]

        def get_raw_data(*args):
            return self.future_value(raw_data.pop(0))

        conn = Connection("localhost", 1234)
        conn.api_correlation = {555: "metadata"}

        conn.stream = Mock()
        conn.stream.read_bytes.side_effect = get_raw_data

        message = yield conn.read_message()

        expected = metadata.MetadataResponse(
            brokers=[
                metadata.Broker(broker_id=8, host="broker01", port=1234)
            ],
            topics=[
                metadata.TopicMetadata(
                    error_code=0, name="example.foo",
                    partitions=[
                        metadata.PartitionMetadata(
                            error_code=0,
                            partition_id=1,
                            leader=1,
                            replicas=[2, 3],
                            isrs=[2]
                        ),
                        metadata.PartitionMetadata(
                            error_code=0,
                            partition_id=2,
                            leader=3,
                            replicas=[1, 2],
                            isrs=[2, 1]
                        ),
                    ]
                ),
            ]
        )

        self.assertEqual(message, expected)
