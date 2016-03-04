import struct

import six


class Primitive(object):
    """
The most basic structure of the protocol.  Subclassed, never used directly.

Used as a building block for the various actually-used primitives outlined
in the Kafka wire protcol docs:

https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
    """
    fmt = None

    def __init__(self, value):
        self.value = value

    def render(self):
        """
        Returns a two-element tuple with the ``struct`` format and list value.

        The value is wrapped in a list, as there are some primitives that deal
        with multiple values.  Any caller of `render()` should expect a list.
        """
        return self.fmt, [self.value]

    @classmethod
    def parse(cls, buff, offset):
        """
        Given a buffer and offset, returns the parsed value and new offset.

        Uses the ``fmt`` class attribute to unpack the data from the buffer
        and determine the used up number of bytes.
        """
        primitive_struct = struct.Struct("!" + cls.fmt)

        value = primitive_struct.unpack_from(buff, offset)[0]
        offset += primitive_struct.size

        return value, offset

    def __eq__(self, other):
        """
        Basic equality method that tests equality of the ``value`` attributes.
        """
        return self.value == other.value

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.value)


class VariablePrimitive(Primitive):
    """
    Base primitive for variable-length scalar primitives (strings and bytes).
    """
    size_primitive = None

    def render(self):
        """
        Returns the ``struct`` format and list of the size and value.

        The format is derived from the size primitive and the length of the
        resulting encoded value (e.g. the format for a string of 'foo' ends
        up as 'h3s'.

        .. note ::
          The value is expected to be string-able (wrapped in ``str()``) and is
          then encoded as UTF-8.
        """
        size_format = self.size_primitive.fmt

        if self.value is None:
            return size_format, [-1]

        value = self.value

        if not isinstance(value, six.binary_type):
            if not isinstance(value, six.string_types):
                value = str(value)

            value = value.encode("utf-8")

        size = len(value)

        fmt = "%s%ds" % (size_format, size)

        return fmt, [size, value]

    @classmethod
    def parse(cls, buff, offset):
        """
        Given a buffer and offset, returns the parsed value and new offset.

        Parses the ``size_primitive`` first to determine how many more bytes to
        consume to extract the value.
        """
        size, offset = cls.size_primitive.parse(buff, offset)
        if size == -1:
            return None, offset

        var_struct = struct.Struct("!%ds" % size)

        value = var_struct.unpack_from(buff, offset)[0]

        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            pass

        offset += var_struct.size

        return value, offset


class Int8(Primitive):
    """
    Represents an 8-bit signed integer.
    """
    fmt = "b"


class Int16(Primitive):
    """
    Represents an 16-bit signed integer.
    """
    fmt = "h"


class Int32(Primitive):
    """
    Represents an 32-bit signed integer.
    """
    fmt = "i"


class Int64(Primitive):
    """
    Represents an 64-bit signed integer.
    """
    fmt = "q"


class String(VariablePrimitive):
    """
    Represents a string value, length denoted by a 16-bit signed integer.
    """
    size_primitive = Int16

    def __repr__(self):
        return repr(self.value)


class Bytes(VariablePrimitive):
    """
    Represents a bytestring value, length denoted by a 32-bit signed integer.
    """
    size_primitive = Int32


class Array(Primitive):
    """
    Represents an array of any arbitrary `Primitive` or ``Part``.

    Not used directly but rather by its ``of()`` classmethod to denote an
    ``Array.of(<something>)``.
    """
    item_class = None

    @classmethod
    def of(cls, part_class):
        """
        Creates a new class with the ``item_class`` attribute properly set.
        """
        copy = type(
            "ArrayOf%s" % part_class.__name__,
            cls.__bases__, dict(cls.__dict__)
        )
        copy.item_class = part_class

        return copy

    def render(self):
        """
        Creates a composite ``struct`` format and the data to render with it.

        The format and data are prefixed with a 32-bit integer denoting the
        number of elements, after which each of the items in the array value
        are ``render()``-ed and added to the format and data as well.
        """
        value = self.value
        if value is None:
            value = []

        fmt = [Int32.fmt]
        data = [len(value)]

        for item_value in value:
            if issubclass(self.item_class, Primitive):
                item = self.item_class(item_value)
            else:
                item = item_value

            item_format, item_data = item.render()
            fmt.extend(item_format)
            data.extend(item_data)

        return "".join(fmt), data

    @classmethod
    def parse(cls, buff, offset):
        """
        Parses a raw buffer at offset and returns the resulting array value.

        Starts off by `parse()`-ing the 32-bit element count, followed by
        parsing items out of the buffer "count" times.
        """
        count, offset = Int32.parse(buff, offset)

        values = []
        for _ in range(count):
            value, new_offset = cls.item_class.parse(buff, offset)

            values.append(value)
            offset = new_offset

        return values, offset

    def __repr__(self):
        return "[%s]" % ", ".join(map(repr, self.value))
