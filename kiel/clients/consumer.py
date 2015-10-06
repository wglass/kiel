import collections
import logging
import json
import socket

import six
from tornado import gen

from kiel.exc import NoOffsetsError
from kiel.protocol import fetch, errors
from kiel.constants import CONSUMER_REPLICA_ID, ERROR_CODES

from .client import Client


log = logging.getLogger(__name__)


class BaseConsumer(Client):
    """
    Base class for consumers, provides `consume()` but no parition allocation.

    Allows for customizing the ``deserialier`` used.  Default is a JSON
    deserializer.
    """
    def __init__(
            self,
            brokers,
            deserializer=None,
            max_wait_time=1000,  # in milliseconds
            min_bytes=1,
            max_bytes=(1024 * 1024),
    ):
        super(BaseConsumer, self).__init__(brokers)

        self.name = ":".join([socket.gethostname(), str(id(self))])

        self.deserializer = deserializer or json.loads

        self.max_wait_time = max_wait_time
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes

        self.offsets = collections.defaultdict(
            lambda: collections.defaultdict(int)
        )
        self.synced_offsets = set()

    @property
    def allocation(self):
        """
        Property meant to denote which topics and partitions this consumer
        should be aware of.

        This is left to subclasses to implement, as it is one of the main
        behavioral differences between a single consumer and a grouped
        consumer.
        """
        raise NotImplementedError

    @gen.coroutine
    def determine_offsets(self, topic, start=None):
        """
        Subclass coroutine function for setting values in ``self.offsets``.

        Kafka offers a simple "offset" api as well as a more involved set
        of offset fetch and commit apis.  Determining which ones to use and
        how is left to the subclasses.
        """
        raise NotImplementedError

    @gen.coroutine
    def consume(self, topic, start=None):
        """
        Fetches from a given topics returns a list of deserialized values.

        If the given topic is not known to have synced offsets, a call to
        `determine_offsets()` is made first.

        If a topic is unknown entirely the cluster's ``heal()`` method is
        called and the check retried.

        Since error codes and deserialization are taken care of by
        `handle_fetch_response` this method merely yields to wait on the
        deserialized results and returns a flattened list.
        """
        if self.closing:
            return

        if topic not in self.synced_offsets:
            try:
                yield self.determine_offsets(topic, start)
            except NoOffsetsError:
                log.error("Unable to determine offsets for topic %s", topic)
                raise gen.Return([])
            self.synced_offsets.add(topic)

        if topic not in self.allocation or not self.allocation[topic]:
            log.debug("Consuming unknown topic %s, reloading metadata", topic)
            yield self.cluster.heal()

        if topic not in self.allocation or not self.allocation[topic]:
            log.error("Consuming unknown topic %s and not auto-created", topic)
            raise gen.Return([])

        ordered = collections.defaultdict(list)
        for partition_id in self.allocation[topic]:
            leader = self.cluster.get_leader(topic, partition_id)
            ordered[leader].append(partition_id)

        requests = {}
        for leader, partitions in six.iteritems(ordered):
            max_partition_bytes = int(self.max_bytes / len(partitions))
            requests[leader] = fetch.FetchRequest(
                replica_id=CONSUMER_REPLICA_ID,
                max_wait_time=self.max_wait_time,
                min_bytes=self.min_bytes,
                topics=[
                    fetch.TopicRequest(name=topic, partitions=[
                        fetch.PartitionRequest(
                            partition_id=partition_id,
                            offset=self.offsets[topic][partition_id],
                            max_bytes=max_partition_bytes,
                        )
                        for partition_id in partitions
                    ])
                ]
            )

        results = yield self.send(requests)
        raise gen.Return([
            msg for messageset in results.values() for msg in messageset
            if messageset
        ])

    def handle_fetch_response(self, response):
        """
        Handler for responses from the message "fetch" api.

        Messages returned with the "no error" code are deserialized and
        collected, the full resulting list is returned.

        A retriable error code will cause the cluster "heal" flag to be set.

        An error indicating that the offset used for the partition was out
        of range will cause the offending topic's offsets to be redetermined
        on the next call to `consume()`.

        .. note::
          This class and its subclasses assume that fetch requests are made
          on one topic at a time, so this handler only deals with the first
          topic returned.
        """
        messages = []

        # we only fetch one topic so we can assume only one comes back
        topic = response.topics[0].name
        for partition in response.topics[0].partitions:
            code = partition.error_code
            if code == errors.no_error:
                messages.extend(self.deserialize_messages(topic, partition))
            elif code in errors.retriable:
                self.heal_cluster = True
            elif code == errors.offset_out_of_range:
                log.warn("Offset out of range for topic %s", topic)
                self.synced_offsets.discard(topic)
            else:
                log.error(
                    "Got error %s for topic %s partition %s",
                    ERROR_CODES[code], topic, partition.partition_id
                )

        return messages

    def deserialize_messages(self, topic_name, partition):
        """
        Calls the ``deserializer`` on each ``Message`` value and gives the
        result.

        If an error is encountered when deserializing it is logged and the
        offending message is skipped.

        After each successful deserialization the ``self.offsets`` entry for
        the particular topic/partition pair is incremented.
        """
        messages = []
        for offset, msg in partition.message_set.messages:
            try:
                value = self.deserializer(msg.value)
            except Exception:
                log.exception(
                    "Error deserializing message: '%r'",
                    getattr(msg, "value", "No value on msg!")
                )
                continue

            messages.append(value)
            self.offsets[topic_name][partition.partition_id] = offset + 1

        return messages
