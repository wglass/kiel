from __future__ import absolute_import

import gzip

from six import BytesIO


def compress(data):
    """
    Compresses a given bit of data via the ``gzip`` stdlib module.

    .. note::

      This assumes the given data is a byte string, already decoded.
    """
    buff = BytesIO()

    with gzip.GzipFile(fileobj=buff, mode='w') as fd:
        fd.write(data)

    buff.seek(0)
    result = buff.read()

    buff.close()

    return result


def decompress(data):
    """
    Decompresses given data via the ``gzip`` module.

    Decoding is left as an exercise for the client code.
    """
    buff = BytesIO(data)

    with gzip.GzipFile(fileobj=buff, mode='r') as fd:
        result = fd.read()

    buff.close()

    return result
