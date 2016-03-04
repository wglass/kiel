import collections
import itertools
import logging

import six
from tornado import gen

from kiel import constants, exc
from kiel.protocol import coordinator, offset_fetch, offset_commit, errors
from kiel.zookeeper.allocator import PartitionAllocator

from .consumer import BaseConsumer


log = logging.getLogger(__name__)


class GroupedConsumer(BaseConsumer):
    """
    Consumer class with coordinated resource allocation among like members.

    Uses an instance of a ``PartitionAllocator`` to determine which topics and
    partitions to consume.  Whenever the allocation is rebalanced, each
    consumed topic will have its partition offsets re-determined.

    Constructed similarly to the ``SingleConsumer`` class except for extra
    paramters ``group``, ``zk_hosts``, ``partition_allocator`` and
    ``autocommit``.
    """
    def __init__(
            self,
            brokers,
            group,
            zk_hosts,
            deserializer=None,
            partition_allocator=None,
            autocommit=True,
            max_wait_time=1000,  # in milliseconds
            min_bytes=1,
            max_bytes=(1024 * 1024),
    ):
        super(GroupedConsumer, self).__init__(
            brokers, deserializer, max_wait_time, min_bytes, max_bytes
        )

        self.group_name = group

        self.coordinator_id = None

        self.allocator = PartitionAllocator(
            zk_hosts, self.group_name, self.name,
            allocator_fn=partition_allocator or naive_allocator,
            on_rebalance=self.synced_offsets.clear
        )

        self.topics_to_commit = set()
        self.autocommit = autocommit

    @property
    def allocation(self):
        """
        Proxy property for the topics/partitions determined by the allocator.
        """
        return self.allocator.allocation

    @gen.coroutine
    def connect(self):
        """
        Overriding ``connect()`` that handles the allocator and coordinator.

        Simple augmentation of the base class method that starts the allocator
        and calls `determine_coordinator()`.
        """
        yield super(GroupedConsumer, self).connect()
        yield self.allocator.start(self.cluster.topics)
        yield self.determine_coordinator()

    @gen.coroutine
    def consume(self, topic, start=None):
        """
        Overriding ``consume()`` that handles committing offsets.

        This is where the ``autocommit`` flag comes into play.  If the flag
        is set we call `commit_offsets()` here right off the bat.
        """
        result = yield super(GroupedConsumer, self).consume(topic)

        if topic not in self.synced_offsets:
            raise gen.Return([])

        self.topics_to_commit.add(topic)

        if self.autocommit:
            yield self.commit_offsets()

        raise gen.Return(result)

    @gen.coroutine
    def determine_coordinator(self):
        """
        Determines the ID of the broker that coordinates the group.

        Uses the "consumer metadata" api to do its thing.  All brokers
        contain coordinator metadata so each broker in the cluster is tried
        until one works.
        """
        request = coordinator.GroupCoordinatorRequest(group=self.group_name)
        determined = False
        while not determined:
            broker_ids = list(self.cluster)
            if not broker_ids:
                raise exc.NoBrokersError
            for broker_id in broker_ids:
                results = yield self.send({broker_id: request})
                determined = results[broker_id]
                if determined:
                    break

    def handle_group_coordinator_response(self, response):
        """
        Handler for consumer metadata api responses.

        These responses are relatively simple and successful ones merely list
        the ID, host and port of the coordinator.

        Returns ``True`` if the coordinator was deterimend, ``False`` if not.
        """
        determined = False
        if response.error_code == errors.no_error:
            log.info("Found coordinator: broker %s", response.coordinator_id)
            self.coordinator_id = response.coordinator_id
            determined = True
        elif response.error_code in errors.retriable:
            self.heal_cluster = True
            determined = False
        else:
            log.error("Got error %s when determining coordinator")
            determined = True

        return determined

    @gen.coroutine
    def determine_offsets(self, topic, start=None):
        """
        Fetches offsets for a given topic via the "offset fetch" api.

        Simple matter of sending an OffsetFetchRequest to the coordinator
        broker.

        .. note::

          The ``start`` argument is actually ignored, it exists so that the
          signature remains consistent with the other consumer classes.
        """
        log.info("Fetching offsets for consumer group '%s'", self.group_name)
        request = offset_fetch.OffsetFetchRequest(
            group_name=self.group_name,
            topics=[
                offset_fetch.TopicRequest(
                    name=topic, partitions=list(self.allocation[topic])
                )
            ]
        )

        retry = True
        while retry:
            result = yield self.send({self.coordinator_id: request})
            retry = result[self.coordinator_id]

    def handle_offset_fetch_response(self, response):
        """
        Handler for offset fetch api responses.

        Sets the corresponding entry in the ``self.offsets`` structure for
        successful partition responses.

        Raises a ``NoOffsetsError`` exception if a fatal, non-retriable error
        is encountered.

        Returns ``True`` if the operation should be retried, ``False`` if not.
        """
        retry = False

        topic = response.topics[0].name
        for partition in response.topics[0].partitions:
            code = partition.error_code
            if code == errors.no_error:
                log.debug(
                    "Got offset %d for group %s topic %s partition %d",
                    partition.offset, self.group_name, topic,
                    partition.partition_id
                )
                self.offsets[topic][partition.partition_id] = partition.offset
            elif code == errors.offsets_load_in_progress:
                log.info(
                    "Offsets load in progress for topic %s partition %s" +
                    " retrying offset fetch.", topic, partition.partition_id
                )
                retry = True
            elif code in errors.retriable:
                self.heal_cluster = True
                retry = True
            else:
                log.error(
                    "Got error %s for topic %s partition %s",
                    constants.ERROR_CODES[code], topic, partition.partition_id
                )
                raise exc.NoOffsetsError

        return retry

    @gen.coroutine
    def commit_offsets(self, metadata=None):
        """
        Notifies Kafka that the consumer's messages have been processed.

        Uses the "v0" version of the offset commit request to maintain
        compatability with clusters running 0.8.1.
        """
        if metadata is None:
            metadata = "committed by %s" % self.name

        log.debug("Committing offsets for consumer group %s", self.group_name)
        request = offset_commit.OffsetCommitV0Request(
            group=self.group_name,
            topics=[
                offset_commit.TopicRequest(
                    name=topic,
                    partitions=[
                        offset_commit.PartitionRequest(
                            partition_id=partition_id,
                            offset=self.offsets[topic][partition_id],
                            metadata=metadata
                        )
                        for partition_id in partition_ids
                    ]
                )
                for topic, partition_ids in six.iteritems(self.allocation)
                if topic in self.topics_to_commit
            ]
        )

        results = yield self.send({self.coordinator_id: request})
        retry, adjust_metadata = results[self.coordinator_id]

        if adjust_metadata:
            log.warn("Offset commit metadata '%s' was too long.", metadata)
            metadata = ""
        if retry:
            yield self.commit_offsets(metadata=metadata)

    def handle_offset_commit_response(self, response):
        """
        Handles responses from the "offset commit" api.

        For successful responses the affected topics are dropped from the set
        of topics that need commits.

        In the special case of an ``offset_metadata_too_large`` error code
        the commit is retried with a blank metadata string.
        """
        retry = False
        adjust_metadata = False

        for topic in response.topics:
            for partition in topic.partitions:
                code = partition.error_code
                if code == errors.no_error:
                    self.topics_to_commit.discard(topic.name)
                elif code in errors.retriable:
                    retry = True
                    self.heal_cluster = True
                elif code == errors.offset_metadata_too_large:
                    retry = True
                    adjust_metadata = True
                else:
                    log.error(
                        "Got error %s for topic %s partition %s",
                        constants.ERROR_CODES[code],
                        topic, partition.partition_id
                    )

        return (retry, adjust_metadata)

    @gen.coroutine
    def wind_down(self):
        """
        Winding down calls ``stop()`` on the allocator.
        """
        yield self.allocator.stop()


def naive_allocator(members, partitions):
    """
    Default allocator with a round robin approach.

    In this algorithm, each member of the group is cycled over and given a
    partition until there are no partitions left.  This assumes roughly equal
    capacity for each member and aims for even distribution of partition
    counts.

    Does not take into account incidental clustering of partitions within the
    same topic.
    """
    mapping = collections.defaultdict(
        lambda: collections.defaultdict(list)
    )

    for member, partition in zip(itertools.cycle(members), partitions):
        topic, partition_id = partition.split(":")
        mapping[member][topic].append(int(partition_id))

    return mapping
