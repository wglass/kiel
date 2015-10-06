from .part import Part


class Response(Part):
    """
    Base class for all api response classes.

    A simple class, has only an ``api`` attribute expected to be defined by
    subclasses, and a `deserialize()` classmethod.
    """
    api = None

    @classmethod
    def deserialize(cls, raw_bytes):
        """
        Deserializes the given raw bytes into an instance.

        Since this is a subclass of ``Part`` but a top-level one (i.e. no other
        subclass of ``Part`` would have a ``Response`` as a part) this merely
        has to parse the raw bytes and discard the resulting offset.
        """
        instance, _ = cls.parse(raw_bytes, offset=0)

        return instance
