import logging
import struct
import zlib

from kiel.constants import GZIP, SNAPPY
from kiel.compression import gzip, snappy

from .part import Part
from .primitives import Int8, Int32, Int64, Bytes


log = logging.getLogger(__name__)


class MessageSet(object):
    """
    Class representing a set of `Message` instances.

    Kafka's compression scheme works by taking a set of messages, compressing
    them with the chosen compression scheme, and then wrapping the result as
    the value of an envelope message, called the "message set".
    """
    def __init__(self, messages):
        self.messages = messages

    @classmethod
    def compressed(cls, compression, msgs):
        """
        Returns a `MessageSet` with the given messages optionally compressed.

        If compression is used, the message set is rendered, compressed, and
        then a *new* message set is created with the raw output as a value
        in a single message.

        If no compression is set, the returned instance will have a
        (<offset>, <message>) tuple for each given message, where the offset
        is -1.
        """
        if not compression:
            return cls([(-1, msg) for msg in msgs])

        set_format, set_data = cls([(-1, msg) for msg in msgs]).render()

        # compressed message sets are nested and don't include the size
        set_format = set_format[1:]
        set_data.pop(0)

        raw_set = struct.pack("!" + set_format, *set_data)

        if compression == GZIP:
            compressed_set = gzip.compress(raw_set)
        elif compression == SNAPPY:
            compressed_set = snappy.compress(raw_set)

        container_msg = Message(
            magic=0,
            attributes=compression,
            key=None,
            value=compressed_set
        )

        return cls([(-1, container_msg)])

    def render(self):
        """
        Returns a tuple of format and data suitable for ``struct.pack``.

        Each (<offset>, <message>) tuple in ``self.messages`` is `render()`-ed
        and the output collected int a single format and data list, prefaced
        with a single integer denoting the size of the message set.
        """
        format = ["i"]
        data = []
        total_size = 0

        for offset, message in self.messages:
            offset_format, offset_data = Int64(offset).render()
            message_format, message_data = message.render()

            message_size = struct.calcsize(message_format)
            size_format, size_data = Int32(message_size).render()

            message_format = "".join([
                offset_format, size_format, message_format
            ])
            total_size += struct.calcsize("!" + message_format)

            format.append(message_format)
            data.extend(offset_data)
            data.extend(size_data)
            data.extend(message_data)

        data.insert(0, total_size)

        return "".join(format), data

    def __eq__(self, other):
        """
        Tests equivalence of message sets.

        Merely checks the equivalence of the ``messages`` attributes.
        Compression is handled implicitly since messages containing the
        compressed value of the same sub-messages should still be equivalent.
        """
        return self.messages == other.messages

    def __repr__(self):
        return "[%s]" % ", ".join([str(m) for _, m in self.messages])

    @classmethod
    def parse(cls, buff, offset, size=None):
        """
        Given a buffer and offset, returns the parsed `MessageSet` and offset.

        Starts by determining the size of the raw payload to parse, and
        continuously parses the ``Int64`` offset and ``Int32`` size of a
        message then the `Message` itself.

        If a parsed message's flags denote compression, `parse()` is called
        recursively on the message's value in order to unpack the compressed
        nested messages.
        """
        if size is None:
            size, offset = Int32.parse(buff, offset)

        end = offset + size

        messages = []
        while not offset == end:
            try:
                message_offset, offset = Int64.parse(buff, offset)
                _, offset = Int32.parse(buff, offset)  # message size
                message, offset = Message.parse(buff, offset)
            except struct.error:
                # ending messages can sometimes be cut off
                break

            if message.attributes:  # compression involved, set is nested
                set_size = len(message.value)
                nested_set, _ = cls.parse(message.value, 0, size=set_size)
                messages.extend(nested_set.messages)
            else:
                messages.append((message_offset, message))

        return cls(messages), offset


class Message(Part):
    """
    Basic ``Part`` subclass representing a single Kafka message.
    ::

      Message =>
        crc => Int32
        magic => Int8
        attributes => Int8
        key => Bytes
        value => Bytes
    """
    parts = (
        ("crc", Int32),
        ("magic", Int8),
        ("attributes", Int8),
        ("key", Bytes),
        ("value", Bytes),
    )

    def render(self):
        """
        Renders just like the base ``Part`` class, but with CRC32 verification.
        """
        format, data = super(Message, self).render(self.parts[1:])

        payload = struct.pack("!" + format, *data)

        crc = zlib.crc32(payload)
        if crc > (2**31):
            crc -= 2**32

        format = "i%ds" % len(payload)

        return format, [crc, payload]

    @classmethod
    def parse(cls, buff, offset):
        """
        Given a buffer and offset, returns the parsed `Message` and new offset.

        Parses via the basic ``Part`` parse method, but with added
        decompression support.

        If a parsed message's attributes denote that compression has been used,
        the value is run through the corresponding ``decompress()`` method.
        """
        message, offset = super(Message, cls).parse(buff, offset)

        # the compression scheme is stored in the lowest 2 bits of 'attributes'
        compression = message.attributes & 0b00000011

        if not compression:
            return message, offset

        if compression == GZIP:
            message.value = gzip.decompress(message.value)
        elif compression == SNAPPY:
            message.value = snappy.decompress(message.value)

        return message, offset

    def __eq__(self, other):
        """
        Tests equivalency of two messages by comparing the ``key`` and
        ``value``.
        """
        return self.key == other.key and self.value == other.value

    def __repr__(self):
        return "%s => %s" % (self.key, self.value)
