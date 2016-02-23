from .part import Part
from .request import Request
from .response import Response
from .primitives import Array, Int16, String


api_name = "list_groups"


__all__ = [
    "ListGroupsRequest",
    "ListGroupsResponse",
    "Group",
]


class ListGroupsRequest(Request):
    """
    ::

      ListGroupsRequest =>
    """
    api = "list_groups"

    parts = ()


class Group(Part):
    """
    ::

      Group =>
        group_id => String
        protocol_type = > String
    """
    parts = (
        ("group_id", String),
        ("protocol_type", String),
    )


class ListGroupsResponse(Response):
    """
    ::

      ListGroupsResponse =>
        error_code => Int16
        groups => [Group]
    """
    api = "list_groups"

    parts = (
        ("error_code", Int16),
        ("groups", Array.of(Group)),
    )
