from .request import Request
from .response import Response
from .primitives import String, Int16, Int32


api_name = "heartbeat"


__all__ = [
    "HeartbeatRequest",
    "HeartbeatResponse"
]


class HeartbeatRequest(Request):
    """
    ::

      HeartbeatRequest =>
        group_id => String
        generation_id = Int32
        member_id => String
    """
    api = "heartbeat"

    parts = (
        "group_id", String,
        "generation_id", Int32,
        "member_id", String,
    )


class HeartbeatResponse(Response):
    """
    ::

      HeartbeatResponse =>
        error_code => Int16
    """
    api = "heartbeat"

    parts = (
        "error_code", Int16
    )
