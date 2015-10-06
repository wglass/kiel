from .part import Part
from .request import Request
from .response import Response
from .primitives import Int16, Int32, Array, String


api_name = "metadata"

__all__ = [
    "MetadataRequest",
    "MetadataResponse",
    "TopicMetadata",
    "PartitionMetadata",
    "Broker",
]


class MetadataRequest(Request):
    """
    ::

      MetadataRequest =>
        topics => [String]
    """
    api = "metadata"

    parts = (
        ("topics", Array.of(String)),
    )


class Broker(Part):
    """
    ::

      Broker =>
        broker_id => Int32
        host => String
        port => Int32
    """
    parts = (
        ("broker_id", Int32),
        ("host", String),
        ("port", Int32),
    )


class PartitionMetadata(Part):
    """
    ::

      PartitionMetadata =>
        error_code => Int16
        partition_id => Int32
        leader => Int32
        replicas => [Int32]
        isrs => [Int32]
    """
    parts = (
        ("error_code", Int16),
        ("partition_id", Int32),
        ("leader", Int32),
        ("replicas", Array.of(Int32)),
        ("isrs", Array.of(Int32)),
    )


class TopicMetadata(Part):
    """
    ::

      TopicMetadata =>
        error_code => Int16
        name => String
        partitions => [PartitionMetadata]
    """
    parts = (
        ("error_code", Int16),
        ("name", String),
        ("partitions", Array.of(PartitionMetadata)),
    )


class MetadataResponse(Response):
    """
    ::

      MetadataResponse =>
        brokers => [Broker]
        topics => [TopicMetadata]
    """
    api = "metadata"

    parts = (
        ("brokers", Array.of(Broker)),
        ("topics", Array.of(TopicMetadata)),
    )
