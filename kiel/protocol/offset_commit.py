from .part import Part
from .request import Request
from .response import Response
from .primitives import Array, String, Int16, Int32, Int64


api_name = "offset_commit"

__all__ = [
    "OffsetCommitV2Request",
    "OffsetCommitV1Request",
    "OffsetCommitV0Request",
    "TopicV1Request",
    "TopicRequest",
    "PartitionV1Request",
    "PartitionRequest",
    "OffsetCommitResponse",
    "TopicResponse",
    "PartitionResponse",
]


class PartitionRequest(Part):
    """
    ::

      PartitionV1Request =>
        partition_id => Int32
        offset => Int64
        metadata => String
    """
    parts = (
        ("partition_id", Int32),
        ("offset", Int64),
        ("metadata", String),
    )


class PartitionV1Request(Part):
    """
    ::

      PartitionV1Request =>
        partition_id => Int32
        offset => Int64
        timestamp => Int64
        metadata => String
    """
    parts = (
        ("partition_id", Int32),
        ("offset", Int64),
        ("timestamp", Int64),
        ("metadata", String),
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


class TopicV1Request(Part):
    """
    ::

      TopicRequest =>
        name => String
        partitions => [PartitionV1Request]
    """
    parts = (
        ("name", String),
        ("partitions", PartitionV1Request),
    )


class OffsetCommitV0Request(Request):
    """
    ::

      OffsetCommitV0Request =>
        group => String
        topics => [TopicRequest]
    """
    api = "offset_commit"

    parts = (
        ("group", String),
        ("topics", Array.of(TopicRequest)),
    )


class OffsetCommitV1Request(Request):
    """
    ::

      OffsetCommitV1Request =>
        group => String
        generation => Int32
        consumer_id => Int32
        topics => [TopicV1Request]
    """
    api = "offset_commit"

    parts = (
        ("group", String),
        ("generation", Int32),
        ("consumer_id", Int32),
        ("topics", Array.of(TopicV1Request)),
    )


class OffsetCommitV2Request(Request):
    """
    ::

      OffsetCommitV2Request =>
        group => String
        generation => Int32
        consumer_id => Int32
        retention_time => Int64
        topics => [TopicRequest]
    """
    api = "offset_commit"

    parts = (
        ("group", String),
        ("generation", Int32),
        ("consumer_id", Int32),
        ("retention_time", Int64),
        ("topics", Array.of(TopicRequest)),
    )


class PartitionResponse(Part):
    """
    ::

      PartitionResponse =>
        partition_id => Int32
        error_code => Int16
    """
    api = "offset_commit"

    parts = (
        ("partition_id", Int32),
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


class OffsetCommitResponse(Response):
    """
    ::

      OffsetCommitResponse =>
        topics => [TopicResponse]
    """
    api = "offset_commit"

    parts = (
        ("topics", Array.of(TopicResponse)),
    )
