from .part import Part
from .request import Request
from .response import Response
from .primitives import Array, String, Bytes, Int16, Int32


api_name = "sync_group"


__all__ = [
    "SyncGroupRequest",
    "SyncGroupResponse",
    "MemberAssignment",
    "TopicAssignment",
]


class TopicAssignment(Part):
    """
    ::

      TopicAssignment =>
        name => String
        partitions => [Int32]
    """
    parts = (
        ("name", String),
        ("partitions", Array.of(Int32)),
    )


class Assignment(Part):
    """
    ::

      Assignment =>
        version => Int16
        topics => [TopicAssignment]
        user_data => Bytes
    """
    parts = (
        ("version", Int16),
        ("topics", Array.of(TopicAssignment)),
        ("user_data", Bytes),
    )


class MemberAssignment(Part):
    """
    ::

      MemberAssignment =>
        member_id => String
        assignment => Assignment
    """
    parts = (
        ("member_id", String),
        ("assignment", Assignment),
    )


class SyncGroupRequest(Request):
    """
    ::

      SyncGroupRequest =>
        group_id => String
        generation_id => Int32
        member_id => String
        assignments => [MemberAssignment]
    """
    api = "sync_group"

    parts = (
        ("group_id", String),
        ("generation_id", Int32),
        ("member_id", String),
        ("assignments", Array.of(MemberAssignment)),
    )


class SyncGroupResponse(Response):
    """
    ::

      SyncGroupResponse =>
        error_code => Int16
        assignments => [MemberAssignment]
    """
    api = "sync_group"

    parts = (
        ("error_code", Int16),
        ("assignments", Array.of(MemberAssignment)),
    )
