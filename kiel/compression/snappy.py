from __future__ import absolute_import

import struct

try:
    import snappy as snappy
    snappy_available = True
except ImportError:  # pragma: no cover
    snappy_available = False

from six import BytesIO


DEFAULT_VERSION = 1
MIN_COMPAT_VERSION = 1

MAGIC_HEADER = (
    -126, b'S', b'N', b'A', b'P', b'P', b'Y', 0,
    DEFAULT_VERSION, MIN_COMPAT_VERSION
)

BLOCK_SIZE = 32 * 1024  # 32kb, in bytes

raw_header = struct.pack("!bccccccbii", *MAGIC_HEADER)


def compress(data):
    """
    Compresses given data via the snappy algorithm.

    The result is preceded with a header containing the string 'SNAPPY' and the
    default and min-compat versions (both ``1``).

    The block size for the compression is hard-coded at 32kb.

    If ``python-snappy`` is not installed a ``RuntimeError`` is raised.
    """
    if not snappy_available:
        raise RuntimeError("Snappy compression unavailable.")

    buff = BytesIO()
    buff.write(raw_header)

    for block_num in range(0, len(data), BLOCK_SIZE):
        block = data[block_num:block_num + BLOCK_SIZE]
        compressed = snappy.compress(block)

        buff.write(struct.pack("!i", len(compressed)))
        buff.write(compressed)

    result = buff.getvalue()

    buff.close()

    return result


def decompress(data):
    """
    Decompresses the given data via the snappy algorithm.

    If ``python-snappy`` is not installed a ``RuntimeError`` is raised.
    """
    if not snappy_available:
        raise RuntimeError("Snappy compression unavailable.")

    buff_offset = len(raw_header)  # skip the header
    length = len(data) - len(raw_header)

    output = BytesIO()

    while buff_offset <= length:
        block_size = struct.unpack_from("!i", data, buff_offset)[0]
        buff_offset += struct.calcsize("!i")

        block = struct.unpack_from("!%ds" % block_size, data, buff_offset)[0]
        buff_offset += block_size

        output.write(snappy.uncompress(block))

    result = output.getvalue()

    output.close()

    return result
