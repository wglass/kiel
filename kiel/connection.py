import contextlib
import logging
import socket
import struct
import sys

from tornado import ioloop, iostream, gen, concurrent

from kiel.exc import ConnectionError
from kiel.protocol import (
    metadata, coordinator,
    produce, fetch,
    offset, offset_commit, offset_fetch
)


log = logging.getLogger(__name__)

# all messages start with a 4-byte signed integer representing raw payload size
size_struct = struct.Struct("!i")
# all responses start with a 4-byte correlation ID to match with the request
correlation_struct = struct.Struct("!i")

response_classes = {
    "metadata": metadata.MetadataResponse,
    "produce": produce.ProduceResponse,
    "fetch": fetch.FetchResponse,
    "offset": offset.OffsetResponse,
    "offset_commit": offset_commit.OffsetCommitResponse,
    "offset_fetch": offset_fetch.OffsetFetchResponse,
    "group_coordinator": coordinator.GroupCoordinatorResponse
}


class Connection(object):
    """
    This class represents a single connection to a single broker host.

    Does not protect against any exceptions when connecting, those are expected
    to be handled by the cluster object.

    The main use of this class is the `send()` method, used to send protocol
    request classes over the wire.

    .. note::
      This is the only class where the ``correlation_id`` should be used.
      These IDs are used to correlate requests and responses over a single
      connection and are meaningless outside said connection.
    """
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)

        self.stream = None
        self.closing = False

        self.api_correlation = {}
        self.pending = {}

    @gen.coroutine
    def connect(self):
        """
        Connects to the broker host and fires the ``read_loop`` callback.

        The socket is wrapped in a tornado ``iostream.IOStream`` to take
        advantage of its handy async methods.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.stream = iostream.IOStream(sock)

        log.info("Connecting to broker %s:%d", self.host, self.port)
        yield self.stream.connect((self.host, self.port))

        ioloop.IOLoop.current().add_callback(self.read_loop)

    def close(self):
        """
        Sets the ``closing`` attribute to ``True`` and calls ``close()`` on the
        underlying stream.
        """
        self.closing = True
        self.stream.close()

    @contextlib.contextmanager
    def socket_error_handling(self, failure_message):
        """
        helper contextmanager for handling errors during IOStream operations.

        Handles the StreamClosedError case by setting the ``closing`` flag,
        logs any unexpected exceptions with a failure message.
        """
        try:
            yield
        except iostream.StreamClosedError:
            self.closing = True
        except Exception:
            if not self.closing:
                log.exception(failure_message)
            self.abort()

    def send(self, message):
        """
        Sends a serialized request to the broker and returns a pending future.

        If any error occurs when writing immediately or asynchronously, the
        `abort()` method is called.

        The retured ``Future`` is stored in the ``self.pending`` dictionary
        keyed on correlation id, so that clients can say

          ``response = yield conn.send(message)``

        and expect the correctly correlated response (or a raised exception)
        regardless of when the broker responds.
        """
        f = concurrent.Future()

        if self.closing:
            f.set_exception(ConnectionError(self.host, self.port))
            return f

        payload = message.serialize()
        payload = size_struct.pack(len(payload)) + payload

        self.api_correlation[message.correlation_id] = message.api
        self.pending[message.correlation_id] = f

        def handle_write(write_future):
            with self.socket_error_handling("Error writing to socket."):
                write_future.result()

        with self.socket_error_handling("Error writing to socket."):
            self.stream.write(payload).add_done_callback(handle_write)

        return f

    @gen.coroutine
    def read_loop(self):
        """
        Infinite loop that reads messages off of the socket while not closed.

        When a message is received its corresponding pending Future is set
        to have the message as its result.

        This is never used directly and is fired as a separate callback on the
        I/O loop via the `connect()` method.
        """
        while not self.closing:
            with self.socket_error_handling("Error reading from socket."):
                message = yield self.read_message()
                self.pending.pop(message.correlation_id).set_result(message)

    def abort(self):
        """
        Aborts a connection and puts all pending futures into an error state.

        If ``sys.exc_info()`` is set (i.e. this is being called in an exception
        handler) then pending futures will have that exc info set.  Otherwise
        a ``ConnectionError`` is used.
        """
        if self.closing:
            return

        log.warn("Aborting connection to %s:%s", self.host, self.port)

        self.close()
        self.api_correlation.clear()
        while self.pending:
            cid, pending = self.pending.popitem()
            exc_info = sys.exc_info()
            if any(exc_info):
                pending.set_exc_info(sys.exc_info())
            else:
                pending.set_exception(ConnectionError(self.host, self.port))

    @gen.coroutine
    def read_message(self):
        """
        Constructs a response class instance from bytes on the stream.

        Works by leveraging the ``IOStream.read_bytes()`` method.  Steps:

        1) first the size of the entire payload is pulled
        2) then the correlation id so that we can match this response to the
           corresponding pending Future
        3) the api of the resonse is looked up via the correlation id
        4) the corresponding response class's deserialize() method is used to
           decipher the raw payload
        """
        raw_size = yield self.stream.read_bytes(size_struct.size)
        size = size_struct.unpack(raw_size)[0]

        raw_correlation = yield self.stream.read_bytes(correlation_struct.size)
        correlation_id = correlation_struct.unpack_from(raw_correlation)[0]

        size -= correlation_struct.size

        raw_payload = yield self.stream.read_bytes(size)
        api = self.api_correlation.pop(correlation_id)

        response = response_classes[api].deserialize(raw_payload)
        response.correlation_id = correlation_id

        raise gen.Return(response)
