from .part import Part
from .request import Request
from .response import Response
from .primitives import Array, String, Int16, Int32, Int64


api_name = "offset_fetch"

__all__ = [
    "OffsetFetchRequest",
    "TopicRequest",
    "OffsetFetchResponse",
    "TopicResponse",
    "PartitionResponse",
]


class TopicRequest(Part):
    """
    ::

      TopicRequest =>
        name => String
        partitions => [In32]
    """
    parts = (
        ("name", String),
        ("partitions", Array.of(Int32)),
    )


class OffsetFetchRequest(Request):
    """
    ::

      OffsetFetchRequest =>
        group_name => String
        topics => [TopicRequest]
    """
    api = "offset_fetch"

    parts = (
        ("group_name", String),
        ("topics", Array.of(TopicRequest)),
    )


class PartitionResponse(Part):
    """
    ::

      PartitionResponse =>
        partition_id => Int32
        offset => Int64
        metadata => String
        error_code => Int16
    """
    parts = (
        ("partition_id", Int32),
        ("offset", Int64),
        ("metadata", String),
        ("error_code", Int16),
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


class OffsetFetchResponse(Response):
    """
    ::

      OffsetFetchResponse =>
        topics => [TopicResponse]
    """
    api = "offset_fetch"

    parts = (
        ("topics", Array.of(TopicResponse)),
    )
