from .part import Part
from .request import Request
from .response import Response
from .primitives import Array, String, Int16, Int32, Bytes


api_name = "describe_groups"


__all__ = [
    "DescribeGroupsRequest",
    "DescribeGroupsResponse",
    "GroupDescription",
    "MemberDescription",
    "Assignment",
    "TopicAssignment",
]


class DescribeGroupsRequest(Request):
    """
    ::

      DescribeGroupRequest =>
        groups => [String]
    """
    api = "describe_groups"

    parts = (
        ("groups", Array.of(String)),
    )


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


class MemberDescription(Part):
    """
    ::

      MemberDescription =>
        member_id => String
        client_id => String
        client_host => String
        metadata => Bytes
        assignment => Assignment
    """
    parts = (
        ("member_id", String),
        ("client_id", String),
        ("client_host", String),
        ("metadata", Bytes),
        ("assignment", Assignment),
    )


class GroupDescription(Part):
    """
    ::

      GroupDescription =>
        error_code => Int16
        group_id => String
        state => String
        protocol_type => String
        protocol => String
        members => [MemberDescription]
    """
    parts = (
        ("error_code", Int16),
        ("group_id", String),
        ("state", String),
        ("protocol_type", String),
        ("protocol", String),
        ("members", Array.of(MemberDescription)),
    )


class DescribeGroupsResponse(Response):
    """
    ::

      DescribeGroupResponse =>
        groups => [GroupDescription]
    """
    api = "describe_groups"

    parts = (
        ("groups", Array.of(GroupDescription)),
    )
