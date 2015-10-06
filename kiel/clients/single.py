import calendar
import collections
import datetime
import logging
import time

from tornado import gen

from kiel.exc import NoOffsetsError
from kiel.protocol import offset, errors
from kiel.constants import ERROR_CODES, CONSUMER_REPLICA_ID

from .consumer import BaseConsumer


EPOCH = datetime.datetime(1970, 1, 1)


log = logging.getLogger(__name__)


class SingleConsumer(BaseConsumer):
    """
    Usable consumer class for isolated-consumer use cases.

    By "isolated" consumer, that means that the consumer runs independently
    of other consumers and does not need to apportion work among others.

    Uses the basic ``offset`` api to determine topic/partition offsets.
    """
    #: special offset api value for 'beginning offset'
    BEGINNING = -2
    #: special offset api value for 'very latest offset'
    END = -1

    @property
    def allocation(self):
        """
        For single consumers the allocation is all topics and partitions.
        """
        return self.cluster.topics

    @gen.coroutine
    def determine_offsets(self, topic, start):
        """
        Sends OffsetRequests to the cluster for a given topic and start point.

        The ``start`` parameter can be any of of ``datetime.datetime``,
        ``datetime.timedelta`` or one of `SingleConsumer.BEGINNING` or
        `SingleConsumer.END`.  The value is translated into epoch seconds
        if need be and used for the "time" parameter for the offset requests.

        An offset request is sent to each of the leader brokers for the given
        topic.
        """
        log.info("Getting offsets for topic %s with start %s", topic, start)

        if start is None:
            start = self.END

        offset_time = start_to_timestamp(start)

        def request_factory():
            return offset.OffsetRequest(
                replica_id=CONSUMER_REPLICA_ID,
                topics=[
                    offset.TopicRequest(name=topic, partitions=[])
                ]
            )

        requests = collections.defaultdict(request_factory)

        for partition_id in self.allocation[topic]:
            leader = self.cluster.get_leader(topic, partition_id)
            requests[leader].topics[0].partitions.append(
                offset.PartitionRequest(
                    partition_id=partition_id,
                    time=offset_time,
                    max_offsets=1
                )
            )

        log.debug(
            "Sending offset request to %d leaders.", len(requests.keys())
        )
        yield self.send(requests)

        raise gen.Return(True)

    def handle_offset_response(self, response):
        """
        Handles responses from the offset api and sets ``self.offsets`` values.

        A succesful response will update the topic/partition pair entry in
        the ``self.offsets`` structure.

        A retriable error code response will cause the cluster's ``heal()``
        method to be called at the end of processing and the offending topic's
        offsets to be re-evaluated on the next `consume()` call.
        """
        # we only fetch one topic so we can assume only one comes back
        topic = response.topics[0].name
        for partition in response.topics[0].partitions:
            code = partition.error_code
            if code == errors.no_error:
                offset = partition.offsets[0]
                self.offsets[topic][partition.partition_id] = offset
            elif code in errors.retriable:
                self.heal_cluster = True
                self.synced_offsets.discard(topic)
            else:
                log.error(
                    "Got error %s for topic %s partition %s",
                    ERROR_CODES[code], topic, partition.partition_id
                )
                raise NoOffsetsError

    @gen.coroutine
    def wind_down(self):
        """
        The single consumer keeps little to no state so wind down is a no-op.
        """
        pass


def start_to_timestamp(start):
    """
    Helper method for translating "start" values into offset api values.

    Valid values are instances of ``datetime.datetime``, ``datetime.timedelta``
    or one of `SingleConsumer.BEGINNING` or `SingleConsumer.END`.
    """
    if isinstance(start, datetime.datetime):
        offset_time = (start - EPOCH).total_seconds()
    elif isinstance(start, datetime.timedelta):
        now = calendar.timegm(time.gmtime())
        offset_time = now - start.total_seconds()
    else:
        offset_time = start

    return offset_time
