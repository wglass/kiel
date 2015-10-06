from .part import Part
from .request import Request
from .response import Response
from .messages import MessageSet
from .primitives import Array, String, Int16, Int32, Int64


api_name = "fetch"


__all__ = [
    "FetchRequest",
    "TopicRequest",
    "PartitionRequest",
    "FetchResponse",
    "TopicResponse",
    "PartitionResponse",
]


class PartitionRequest(Part):
    """
    ::

      PartitionRequest =>
        partition_id => Int32
        offset => Int64
        max_bytes => Int32
    """
    parts = (
        ("partition_id", Int32),
        ("offset", Int64),
        ("max_bytes", Int32),
    )


class TopicRequest(Part):
    """
    ::

      TopicRequest =>
        name => String
        partitions => [PartitionRequest]
    """
    parts = (
        ("name", String),
        ("partitions", Array.of(PartitionRequest)),
    )


class FetchRequest(Request):
    """
    ::

      FetchRequest =>
        replica_id => Int32
        max_wait_time => Int32
        min_bytes => Int32
        topics => [TopicRequest]
    """
    api = "fetch"

    parts = (
        ("replica_id", Int32),
        ("max_wait_time", Int32),
        ("min_bytes", Int32),
        ("topics", Array.of(TopicRequest)),
    )


class PartitionResponse(Part):
    """
    ::

      PartitionResponse =>
        partition_id => Int32
        error_code => Int16
        highwater_mark_offset => Int64
        message_set => MessageSet
    """
    parts = (
        ("partition_id", Int32),
        ("error_code", Int16),
        ("highwater_mark_offset", Int64),
        ("message_set", MessageSet),
    )


class TopicResponse(Part):
    """
    ::

      TopicResponse =>
        name => String
        partitions => [PartitionResponse]
    """
    parts = (
        ("name", String),
        ("partitions", Array.of(PartitionResponse)),
    )


class FetchResponse(Response):
    """
    ::

      FetchResponse =>
        topics => [TopicResponse]
    """
    api = "fetch"

    parts = (
        ("topics", Array.of(TopicResponse)),
    )
