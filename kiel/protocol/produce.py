from .part import Part
from .request import Request
from .response import Response
from .messages import MessageSet
from .primitives import Int16, Int32, Int64, Array, String


api_name = "produce"

__all__ = [
    "ProduceRequest",
    "TopicRequest",
    "PartitionRequest",
    "ProduceResponse",
    "TopicResponse",
    "PartitionResponse",
]


class PartitionRequest(Part):
    """
    ::

      PartitionRequest =>
        partition_id => Int32
        message_set => MessageSet
    """
    parts = (
        ("partition_id", Int32),
        ("message_set", MessageSet),
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


class ProduceRequest(Request):
    """
    ::

      ProduceRequest =>
        required_acs => Int16
        timeout => Int32
        topics => [TopicRequest]
    """
    api = "produce"

    parts = (
        ("required_acks", Int16),
        ("timeout", Int32),
        ("topics", Array.of(TopicRequest)),
    )


class PartitionResponse(Part):
    """
    ::

      PartitionResponse =>
        partition_id => Int32
        error_code => Int16
        offset => Int64
    """
    parts = (
        ("partition_id", Int32),
        ("error_code", Int16),
        ("offset", Int64),
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


class ProduceResponse(Response):
    """
    ::

      ProduceResponse =>
        topics => [TopicResponse]
    """
    api = "produce"

    parts = (
        ("topics", Array.of(TopicResponse)),
    )
