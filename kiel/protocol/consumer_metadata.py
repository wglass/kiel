from .request import Request
from .response import Response
from .primitives import String, Int16, Int32


api_name = "consumer_metadata"

__all__ = [
    "ConsumerMetadataRequest",
    "ConsumerMetadataResponse",
]


class ConsumerMetadataRequest(Request):
    """
    ::

      ConsumerMetadataRequest =>
        group_id => String
    """
    api = "consumer_metadata"

    parts = (
        ("group", String),
    )


class ConsumerMetadataResponse(Response):
    """
    ::

      ConsumerMetadataResponse =>
        error_code => Int16
        coordinator_id => Int32
        coordinator_host => String
        coordinator_port => Int32
    """
    api = "consumer_metadata"

    parts = (
        ("error_code", Int16),
        ("coordinator_id", Int32),
        ("coordinator_host", String),
        ("coordinator_port", Int32),
    )
