from .request import Request
from .response import Response
from .primitives import String, Int16, Int32


api_name = "group_coordinator"

__all__ = [
    "GroupCoordinatorRequest",
    "GroupCoordinatorResponse",
]


class GroupCoordinatorRequest(Request):
    """
    ::

      GroupCoordinatorRequest =>
        group_id => String
    """
    api = "group_coordinator"

    parts = (
        ("group", String),
    )


class GroupCoordinatorResponse(Response):
    """
    ::

      GroupCoordinatorResponse =>
        error_code => Int16
        coordinator_id => Int32
        coordinator_host => String
        coordinator_port => Int32
    """
    api = "group_coordinator"

    parts = (
        ("error_code", Int16),
        ("coordinator_id", Int32),
        ("coordinator_host", String),
        ("coordinator_port", Int32),
    )
