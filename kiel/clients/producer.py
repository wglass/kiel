import collections
import logging
import json
import random

import six
from tornado import gen

from kiel.protocol import produce as produce_api, messages, errors
from kiel.constants import SUPPORTED_COMPRESSION, ERROR_CODES
from kiel.iterables import drain

from .client import Client


log = logging.getLogger(__name__)


class Producer(Client):
    """
    Client class used to "produce" messages to Kafka topics.

    Allows for customizing the ``serializer``, ``key_maker`` and
    ``partitioner`` functions.  By default a JSON serializer is used, along
    with a no-op key maker and a partitioner that chooses at random.
    """
    def __init__(
            self,
            brokers,
            serializer=None,
            key_maker=None,
            partitioner=None,
            batch_size=1,
            compression=None,
            required_acks=-1,
            ack_timeout=500,  # milliseconds
    ):
        super(Producer, self).__init__(brokers)

        if compression not in SUPPORTED_COMPRESSION:
            raise ValueError(
                "Invalid compression value %s,must be one of %s",
                compression, ", ".join(map(str, SUPPORTED_COMPRESSION))
            )
        self.compression = compression

        def json_serializer(message):
            return json.dumps(message, sort_keys=True)

        def null_key_maker(_):
            return None

        def random_partitioner(_, partitions):
            return random.choice(partitions)

        self.serializer = serializer or json_serializer
        self.key_maker = key_maker or null_key_maker
        self.partitioner = partitioner or random_partitioner

        self.batch_size = batch_size
        self.required_acks = required_acks
        self.ack_timeout = ack_timeout

        # dictionary of topic -> messages
        self.unsent = collections.defaultdict(list)
        # dictionary of correlation id -> topic -> partition -> messages
        self.sent = collections.defaultdict(
            lambda: collections.defaultdict(dict)
        )

    @property
    def unsent_count(self):
        """
        Property representing the sum total of pending messages to be sent.
        """
        return sum([len(unsent) for unsent in self.unsent.values()])

    @gen.coroutine
    def produce(self, topic, message):
        """
        Primary method that queues messages up to be flushed to the brokers.

        Performs sanity checks to make sure we're not closing and that the
        topic given is known.

        If the topic given is *not* known, the ``heal()`` method on the cluster
        is called and the check is performed again.

        Depending on the ``batch_size`` attribute this call may not actually
        send any requests and merely keeps the pending messages in the
        ``unsent`` structure.
        """
        if self.closing:
            log.warn("Producing to %s topic while closing.", topic)
            return

        if topic not in self.cluster.topics:
            log.debug("Producing to unknown topic %s, loading metadata", topic)
            yield self.cluster.heal()

        if topic not in self.cluster.topics:
            log.error("Unknown topic %s and not auto-created", topic)
            return

        self.unsent[topic].append(
            messages.Message(
                magic=0,
                attributes=0,
                key=self.key_maker(message),
                value=self.serializer(message)
            )
        )

        if not self.batch_size or self.unsent_count >= self.batch_size:
            yield self.flush()

    def queue_retries(self, topic, msgs):
        """
        Re-inserts the given messages into the ``unsent`` structure.

        This also sets the flag to denote that a cluster "heal" is necessary.
        """
        log.debug("Queueing %d messages for retry", len(msgs))
        self.unsent[topic].extend(msgs)
        self.heal_cluster = True

    @gen.coroutine
    def flush(self):
        """
        Transforms the ``unsent`` structure to produce requests and sends them.

        The first order of business is to order the pending messages in
        ``unsent`` based on partition leader.  If a message's partition leader
        is not a know broker, the message is queued up to be retried and the
        flag denoting that a cluster ``heal()`` call is needed is set.

        Once the legitimate messages are ordered, instances of ProduceRequest
        are created for each broker and sent.
        """
        if not self.unsent:
            return

        # leader -> topic -> partition -> message list
        ordered = collections.defaultdict(
            lambda: collections.defaultdict(
                lambda: collections.defaultdict(list)
            )
        )

        to_retry = collections.defaultdict(list)

        for topic, msgs in drain(self.unsent):
            for msg in msgs:
                partition = self.partitioner(
                    msg.key, self.cluster.topics[topic]
                )
                leader = self.cluster.get_leader(topic, partition)
                if leader not in self.cluster:
                    to_retry[topic].append(msg)
                    continue
                ordered[leader][topic][partition].append(msg)

        requests = {}
        for leader, topics in six.iteritems(ordered):
            requests[leader] = produce_api.ProduceRequest(
                required_acks=self.required_acks,
                timeout=self.ack_timeout,
                topics=[]
            )
            for topic, partitions in six.iteritems(topics):
                requests[leader].topics.append(
                    produce_api.TopicRequest(name=topic, partitions=[])
                )
                for partition_id, msgs in six.iteritems(partitions):
                    requests[leader].topics[-1].partitions.append(
                        produce_api.PartitionRequest(
                            partition_id=partition_id,
                            message_set=messages.MessageSet.compressed(
                                self.compression, msgs
                            )
                        )
                    )
                    self.sent[
                        requests[leader].correlation_id
                    ][topic][partition_id] = msgs

        for topic, msgs in six.iteritems(to_retry):
            self.queue_retries(topic, msgs)

        yield self.send(requests)

    def handle_produce_response(self, response):
        """
        Handler for produce api responses, discards or retries as needed.

        For the "no error" result, the corresponding messages are discarded
        from the ``sent`` structure.

        For retriable error codes the affected messages are queued up to be
        retried.

        .. warning::
          For fatal error codes the error is logged and no further action is
          taken.  The affected messages are not retried and effectively written
          over with the next call to `produce()`.
        """
        for topic in response.topics:
            for partition in topic.partitions:
                code = partition.error_code
                if code == errors.no_error:
                    pass
                elif code in errors.retriable:
                    msgs = self.sent[response.correlation_id][topic.name].pop(
                        partition.partition_id
                    )
                    self.queue_retries(topic.name, msgs)
                else:
                    log.error(
                        "Got error %s for topic %s partition %s",
                        ERROR_CODES[code], topic.name, partition.partition_id
                    )

        self.sent.pop(response.correlation_id)

    @gen.coroutine
    def wind_down(self):
        """
        Flushes the unsent messages so that none are lost when closing down.
        """
        yield self.flush()
