from .part import Part
from .request import Request
from .response import Response
from .primitives import Int16, Int32, Int64, String, Array


api_name = "offset"

__all__ = [
    "OffsetRequest",
    "TopicRequest",
    "PartitionRequest",
    "OffsetResponse",
    "TopicResponse",
    "PartitionResponse",
]


class PartitionRequest(Part):
    """
    ::

      PartitionRequeset =>
        partition_id => Int32
        time => Int64
        max_offsets => Int32
    """
    parts = (
        ("partition_id", Int32),
        ("time", Int64),
        ("max_offsets", Int32),
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


class OffsetRequest(Request):
    """
    ::

      OffsetRequest =>
        replica_id => Int32
        topics => [TopicRequest]
    """
    api = "offset"

    parts = (
        ("replica_id", Int32),
        ("topics", Array.of(TopicRequest)),
    )


class PartitionResponse(Part):
    """
    ::

      PartitionResponse =>
        partition_id => Int32
        error_code => Int16
        offsets => [Int64]
    """
    parts = (
        ("partition_id", Int32),
        ("error_code", Int16),
        ("offsets", Array.of(Int64)),
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


class OffsetResponse(Response):
    """
    ::

      OffsetResponse =>
        topics => [TopicResponse]
    """
    api = "offset"

    parts = (
        ("topics", Array.of(TopicResponse)),
    )
