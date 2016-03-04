import binascii
import hashlib
import os
import socket
import struct

from six import BytesIO

from kiel.constants import CLIENT_ID, API_VERSION, API_KEYS

from .part import Part
from .primitives import Int16, Int32, String


machine_hash = hashlib.md5()
machine_hash.update(socket.gethostname().encode("UTF-8"))
machine_bytes = machine_hash.digest()[0:4]

#: Seed value for correlation IDs, based on the machine name and PID
last_id = (int(binascii.hexlify(machine_bytes), 16) + os.getpid() & 0xffffff)


def generate_correlation_id():
    """
    Creates a new ``correlation_id`` for requests.

    Increments the ``last_id`` value so each generated ID is unique for
    this machine and process.
    """
    global last_id

    last_id += 1

    return last_id


class Request(Part):
    """
    Base class for all requests sent to brokers.

    A specialized subclass of ``Part`` with attributes for correlating
    responses and prefacing payloads with client/api metadata.
    """
    api = None

    def __init__(self, **kwargs):
        super(Request, self).__init__(**kwargs)

        self.client_id = CLIENT_ID
        self.api_key = API_KEYS[self.api]
        self.api_version = API_VERSION
        self.correlation_id = generate_correlation_id()

    def serialize(self):
        """
        Returns a bytesring representation of the request instance.

        Prefaces the output with certain information::

          api_key => Int16
          api_version => Int16
          correlation_id => Int32
          client_id => String

        Since this is a ``Part`` subclass the rest is a matter of
        appending the result of a ``render()`` call.
        """
        buff = BytesIO()

        preamble_parts = (
            ("api_key", Int16),
            ("api_version", Int16),
            ("correlation_id", Int32),
            ("client_id", String),
        )

        preamble_format, data = self.render(preamble_parts)

        payload_format, payload_data = self.render()

        fmt = "".join(["!", preamble_format, payload_format])
        data.extend(payload_data)

        buff.write(struct.pack(fmt, *data))

        return buff.getvalue()
