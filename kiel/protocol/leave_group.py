from .request import Request
from .response import Response
from .primitives import String, Int16


api_name = "leave_group"


__all__ = [
    "LeaveGroupRequest",
    "LeaveGroupResponse",
]


class LeaveGroupRequest(Request):
    """
    ::

      LeaveGroupRequest =>
        group_id => String
        member_id => String
    """
    api = "leave_group"

    parts = (
        ("group_id", String),
        ("member_id", String),
    )


class LeaveGroupResponse(Response):
    """
    ::

      LeaveGroupResponse =>
        error_code => Int16
    """
    api = "leave_group"

    parts = (
        ("error_code", Int16),
    )
