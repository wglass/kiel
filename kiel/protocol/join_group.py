from .part import Part
from .request import Request
from .response import Response
from .primitives import Array, String, Bytes, Int16, Int32


api_name = "join_group"


__all__ = [
    "JoinGroupRequest",
    "JoinGroupResponse",
    "GroupProtocol",
    "Member",
]


class GroupProtocol(Part):
    """
    ::

      GroupProtocol =>
        name => String
        version => Int16
        subscription => Array.of(String)
        user_data => Bytes
    """
    parts = (
        ("name", String),
        ("version", Int16),
        ("subscription", Array.of(String)),
        ("user_data", Bytes),
    )


class JoinGroupRequest(Request):
    """
    ::

      JoinGroupRequest =>
        group_id => String
        session_timeout => Int32
        member_id => String
        protocol_type => String
        group_protocols => [GroupProtocol]
    """
    api = "join_group"

    parts = (
        ("group_id", String),
        ("session_timeout", Int32),
        ("member_id", String),
        ("protocol_type", String),
        ("group_protocols", Array.of(GroupProtocol)),
    )


class Member(Part):
    """
    ::

      Member =>
        member_id => String
        metadata => Bytes
    """
    parts = (
        ("member_id", String),
        ("metadata", Bytes),
    )


class JoinGroupResponse(Response):
    """
    ::

      JoinGroupResponse =>
        error_code => Int16
        generation_id => Int32
        protocol => String
        leader_id => String
        member_id => String
        members => [Member]
    """
    api = "join_group"

    parts = (
        ("error_code", Int16),
        ("generation_id", Int32),
        ("protocol", String),
        ("leader_id", String),
        ("member_id", String),
        ("members", Array.of(Member)),
    )
