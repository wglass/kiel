import logging
import threading

import six
from tornado import concurrent
from kazoo import client

from kiel.zookeeper import Party, SharedSet
from kiel.events import wait_on_event


log = logging.getLogger(__name__)


class PartitionAllocator(object):
    """
    Helper class that uses Zookeeper to allocate partitions among consumers.

    Uses a ``Party`` instance to represent the group membership and a
    ``SharedSet`` instance to handle the set of partitions to be allocated.

    The ``allocator_fn`` argument is a callable that is passed a sorted list
    of members and partitions whenever change to either happens.

    .. note::

      It is *incredibly* important that the ``allocator_fn`` be stable!  All
      all of the instances of the allocator must agree on what partitions go
      where or all hell will break loose.
    """
    def __init__(
            self,
            zk_hosts,
            group_name,
            consumer_name,
            allocator_fn,
            on_rebalance=None
    ):
        self.zk_hosts = zk_hosts
        self.group_name = group_name
        self.consumer_name = consumer_name

        self.allocator_fn = allocator_fn
        self.on_rebalance = on_rebalance

        self.conn = client.KazooClient(hosts=",".join(self.zk_hosts))
        self.connected = threading.Event()

        self.members = set()
        self.members_collected = threading.Event()
        self.party = Party(
            self.conn, self.consumer_name, self.members_path,
            on_change=self.on_group_members_change
        )

        self.partitions = set()
        self.partitions_collected = threading.Event()
        self.shared_set = SharedSet(
            self.conn, self.partition_path,
            on_change=self.on_partition_change
        )

        self.mapping = {}

    @property
    def allocation(self):
        """
        Property representing the topics allocated for a specific consumer.
        """
        return self.mapping[self.consumer_name]

    @property
    def members_path(self):
        """
        Property representing the znode path of the member ``Party``.
        """
        return "/kiel/groups/%s/members" % self.group_name

    @property
    def partition_path(self):
        """
        Property representing the znode path of the ``SharedSet``.
        """
        return "/kiel/groups/%s/partitions" % self.group_name

    def start(self, seed_partitions):
        """
        Connects to zookeeper and collects member and partition data.

        Leverages the `create_attempt()` and ``wait_on_event()`` helper
        functions in order to bridge the gap between threaded async
        and tornado async.

        Returns a ``Future`` instance once done so that coroutine
        methods may yield to it.
        """
        log.info("Starting partitioner for group '%s'", self.group_name)
        f = concurrent.Future()

        attempt = create_attempter(f)

        attempt(self.connect)
        wait_on_event(self.connected)

        attempt(self.party.start)
        attempt(self.shared_set.start)
        attempt(self.party.join)
        attempt(self.add_partitions, seed_partitions)

        if f.done():
            return f

        wait_on_event(self.members_collected)
        wait_on_event(self.partitions_collected)

        f.set_result(None)

        return f

    def stop(self):
        """
        Signals the ``Party`` that this member has left and closes connections.

        This method returns a ``Future`` so that it can be yielded to in
        coroutines.
        """
        log.info("Stopping partitioner for group '%s'", self.group_name)
        f = concurrent.Future()

        attempt = create_attempter(f)

        attempt(self.party.leave)
        attempt(self.conn.stop)
        attempt(self.conn.close)

        if not f.done():
            f.set_result(None)

        return f

    def connect(self):
        """
        Establishes the kazoo connection and registers the connection handler.
        """
        self.conn.add_listener(self.handle_connection_change)
        self.conn.start_async()

    def handle_connection_change(self, state):
        """
        Handler for changes to the kazoo client's connection's state.

        Responsible for updating the ``connected`` threading event such that
        it is only set if/when the kazoo connection is live.
        """
        if state == client.KazooState.LOST:
            log.info("Zookeeper session lost!")
            self.connected.clear()
        elif state == client.KazooState.SUSPENDED:
            log.info("Zookeeper connection suspended!")
            self.connected.clear()
        else:
            log.info("Zookeeper connection (re)established.")
            self.connected.set()

    def on_group_members_change(self, new_members):
        """
        Callback for when membership of the ``Party`` changes.

        Sets the ``self.members`` attribute if membership actually
        changed, calling `rebalance()` if so.

        Sets the ``members_collected`` threading event when done.
        """
        log.info("Consumer group '%s' members changed.", self.group_name)

        new_members = set(new_members)
        if new_members != self.members:
            self.members = new_members
            self.rebalance()

        self.members_collected.set()

    def on_partition_change(self, new_partitions):
        """
        Callback for when data in the ``SharedSet`` changes.

        If ``new_partitions`` is ``None`` it means we're the first to
        use the ``SharedSet`` so we populate it with our known partitions.

        If the data has been altered in any way the ``self.partitions``
        attribute is updated and `rebalance()` called.

        Sets the `partitions_collected` threading event when done.
        """
        if new_partitions is None:
            self.conn.create(self.partition_path, value=self.partitions)
            return

        if new_partitions != self.partitions:
            self.partitions = new_partitions
            self.rebalance()

        self.partitions_collected.set()

    def add_partitions(self, partitions):
        """
        Ensures that the ``SharedSet`` contains the given partitions.

        The ``partitions`` argument should be a dictionary keyed on
        topic names who's values are lists of associated partition IDs.
        """
        new_partitions = set()
        for topic, partition_ids in six.iteritems(partitions):
            new_partitions.update(set([
                ":".join([topic, str(partition_id)])
                for partition_id in partition_ids
            ]))

        log.info(
            "Attempting to add %d partitions to consumer group '%s'",
            len(new_partitions), self.group_name
        )

        wait_on_event(self.connected)

        self.shared_set.add_items(new_partitions)

    def remove_partitions(self, old_partitions):
        """
        Ensures that the ``SharedSet`` does *not* contain the given partitions.

        The ``partitions`` argument should be a dictionary keyed on
        topic names who's values are lists of associated partition IDs.
        """
        log.info(
            "Attempting to remove %d partitions from consumer group '%s'",
            len(old_partitions), self.group_name
        )
        wait_on_event(self.connected)

        self.shared_set.remove_items(set([
            ":".join([topic, partition_id])
            for topic, partition_id in six.iteritems(old_partitions)
        ]))

    def rebalance(self):
        """
        Callback fired when membership or partition data changes.

        The ``allocator_fn`` is called on the new ``self.members`` and
        ``self.partitions`` lists to determine the mapping of members
        to partitions.

        If an ``on_rebalance`` callback is configured it is called once
        done.
        """
        log.info("Rebalancing partitions for group '%s'", self.group_name)
        members = sorted(self.members)
        partitions = sorted(self.partitions)

        self.mapping = self.allocator_fn(members, partitions)

        for topic in self.allocation:
            log.debug(
                "Allocation for topic '%s': partitions %s",
                topic, ", ".join(map(str, self.allocation[topic]))
            )

        if self.on_rebalance:
            self.on_rebalance()


def create_attempter(f):
    """
    Helper method for methods that call others and use ``Future`` directly.

    Returns a wrapper function that will set the given ``Future``'s exception
    state if the inner function call fails.
    """
    def attempt(fn, *args, **kwargs):
        if f.done():
            return

        try:
            fn(*args, **kwargs)
        except Exception as e:
            f.set_exception(e)

    return attempt
