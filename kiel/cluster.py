import collections
import logging

from tornado import gen, iostream

from kiel.protocol import metadata, errors
from kiel.constants import DEFAULT_KAFKA_PORT
from kiel.exc import ConnectionError, NoBrokersError

from .connection import Connection


log = logging.getLogger(__name__)


class Cluster(object):
    """
    Class representing a Kafka cluster.

    Handles a dict of ``Connection`` objects, one for each known broker
    and keyed off of the broker ID.

    Also keeps metadata information for topics, their partitions, and the
    partition leader brokers.
    """
    def __init__(self, bootstrap_hosts):
        self.bootstrap_hosts = bootstrap_hosts

        self.conns = {}
        self.topics = collections.defaultdict(list)
        self.leaders = collections.defaultdict(dict)

    def __getitem__(self, broker_id):
        """
        Proxies to the ``__getitem__`` of the underlying conns dictionary.

        Allows for the client to say

          ``self.cluster[broker_id].send()``

        and such.
        """
        return self.conns[broker_id]

    def __contains__(self, broker_id):
        """
        Proxies the ``__contains__`` method of the conns dictionary.

        Allows for the client to test if a broker is present via

          ``broker_id in self.cluster``
        """
        return broker_id in self.conns

    def __iter__(self):
        """
        Procies the ``__iter__`` method of the conns dictionary.

        In effect allows for iterating over known broker_id values:

          ``for broker_id in self.cluster:``
        """
        return iter(self.conns)

    def get_leader(self, topic, partition_id):
        """
        Returns the leader broker ID for a given topic/partition combo.
        """
        return self.leaders[topic][partition_id]

    @gen.coroutine
    def start(self):
        """
        Establishes connections to the brokers in a cluster as well as
        gathers topic/partition metadata.

        Cycles through each bootstrap host and attempts to send a metadata
        request.  Once a metadata request is successful the `heal()` method
        is called.
        """
        response = None

        for host in self.bootstrap_hosts:
            if ":" in host:
                host, port = host.split(":")
            else:
                port = DEFAULT_KAFKA_PORT

            conn = Connection(host, int(port))

            log.info("Using bootstrap host '%s'", host)

            try:
                yield conn.connect()
            except (iostream.StreamClosedError, ConnectionError):
                log.warn("Could not connect to bootstrap %s:%s", host, port)
                continue
            except Exception:
                log.exception("Error connecting to bootstrap host '%s'", host)
                continue

            response = yield conn.send(metadata.MetadataRequest(topics=[]))

            conn.close()
            break

        if not response:
            raise NoBrokersError

        log.info("Metadata gathered, setting up connections.")
        yield self.heal(response)

    @gen.coroutine
    def heal(self, response=None):
        """
        Syncs the state of the cluster with metadata retrieved from a broker.

        If not response argument is given, a call to `get_metatadata()` fetches
        fresh information.

        As a first step this will cull any closing/aborted connections from the
        cluster.  This is followed by repeated calls to `process_brokers()` and
        `process_topics()` until both signal that there are no missing brokers
        or topics.
        """
        if not response:
            response = yield self.get_metadata()

        broker_ids = list(self.conns.keys())
        for broker_id in broker_ids:
            if self.conns[broker_id].closing:
                log.debug(
                    "Removing %s:%s from cluster",
                    self.conns[broker_id].host, self.conns[broker_id].port
                )
                self.conns.pop(broker_id)

        missing_conns = yield self.process_brokers(response.brokers)
        missing_topics = self.process_topics(response.topics)
        while missing_conns or missing_topics:
            response = yield self.get_metadata(topics=list(missing_topics))
            missing_conns = yield self.process_brokers(response.brokers)
            missing_topics = self.process_topics(response.topics)

    @gen.coroutine
    def get_metadata(self, topics=None):
        """
        Retrieves metadata from a broker in the cluster, optionally limited
        to a set of topics.

        Each connection in the cluster is tried until one works.  If no
        connection in the cluster responds, a ``NoBrokersError`` is raised.
        """
        log.debug("Gathering metadata (topics=%s)", topics)
        if topics is None:
            topics = []

        response = None
        for conn in self.conns.values():
            try:
                response = yield conn.send(
                    metadata.MetadataRequest(topics=topics)
                )
                break
            except (iostream.StreamClosedError, ConnectionError):
                continue

        if not response:
            raise NoBrokersError

        raise gen.Return(response)

    @gen.coroutine
    def process_brokers(self, brokers):
        """
        Syncs the ``self.conn`` connection dictionary with given broker
        metadata, returning a set of broker IDs that were in the metadata but
        had failing connections.

        Known connections that are not present in the given metadata will have
        ``abort()`` called on them.
        """
        to_drop = set(self.conns.keys()) - set([b.broker_id for b in brokers])

        missing = set()

        for broker in brokers:
            if broker.broker_id in self.conns:
                continue

            try:
                conn = Connection(broker.host, broker.port)
                yield conn.connect()
                self.conns[broker.broker_id] = conn
            except iostream.StreamClosedError:
                log.warn(
                    "Could not add broker %s (%s:%s)",
                    broker.broker_id, broker.host, broker.port,
                )
                missing.add(broker.broker_id)
                continue
            except Exception:
                log.exception(
                    "Error adding broker %s (%s:%s)",
                    broker.broker_id, broker.host, broker.port,
                )
                missing.add(broker.broker_id)
                continue

        for broker_id in to_drop:
            self.conns[broker_id].abort()

        raise gen.Return(missing)

    def process_topics(self, response_topics):
        """
        Syncs the cluster's topic/partition metadata with a given response.
        Returns a set of topic names that were either missing data or had
        unknown leader IDs.

        Works by iterating over the topic metadatas and their partitions,
        checking for error codes and a connection matching the leader ID.

        Once complete the ``self.topics`` and ``self.leaders`` dictonaries are
        set with the newly validated information.
        """
        topics = collections.defaultdict(list)
        leaders = collections.defaultdict(dict)

        missing = set()

        for topic in response_topics:
            if topic.error_code == errors.unknown_topic_or_partition:
                log.error("Unknown topic %s", topic.name)
                continue
            if topic.error_code == errors.replica_not_available:
                missing.add(topic.name)
                continue

            for partition in topic.partitions:
                if partition.error_code == errors.leader_not_available:
                    log.warn(
                        "Leader not available for %s|%s, election in progress",
                        topic.name, partition.partition_id
                    )
                    missing.add(topic.name)
                    continue
                if partition.leader not in self.conns:
                    log.warn(
                        "Leader for %s|%s not in current connections.",
                        topic.name, partition.partition_id
                    )
                    missing.add(topic.name)
                    continue

                topics[topic.name].append(partition.partition_id)
                leaders[topic.name][partition.partition_id] = partition.leader

        self.topics = topics
        self.leaders = leaders

        return missing

    def stop(self):
        """
        Simple method that calls ``close()`` on each connection.
        """
        for conn in self.conns.values():
            conn.close()
