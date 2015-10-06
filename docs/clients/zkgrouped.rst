===========================
The Grouped Consumer Client
===========================

  The ``GroupedConsumer`` client class is used in cases where a set of consumers
  must coordinate which partitions to consume amongst themselves.  The class
  uses an "allocator" function to dole out partitions and Zookeeper_ to store
  the resulting allocation.  Like the other client classes it's importable via
  ``kiel.clients``.

.. code-block:: python

  from kiel import clients
  from tornado import gen

  consumer = clients.GroupedConsumer(
      ["kafka01", "kafka02"],
      "my-consumer-group",
      ["zookeeper01", "zookeeper02", "zookeeper03"],
      deserializer=None,
      partition_allocator=None,
      autocommit=True,
      max_wait_time=1000,  # in milliseconds
      min_bytes=1,
      max_bytes=(1024 * 1024),
  )

  @gen.coroutine
  def run():
      yield consumer.connect()
      msgs = yield consumer.consume("example.task.queue")
      for msg in msgs:
          process(msg)


The bootstrap broker hosts, group name and zookeeper ensemble hosts are all
required constructor arguments.

.. note::

   The list of Zookeeper hosts should include *all* of the hosts in the
   ensemble.  The Kafka brokers relay data about other brokers whereas zookeeper
   hosts do not.


Allocation
----------

Allocation works via the "partition allocator function", customizable via the
``partition_allocator`` constructor argument.  The allocator will assign *all*
known partitions, regardless of planned use.  This is so that we don't need to
checks for needed redistributions whenever a new topic is consumed.

.. warning::

   During a re-allocation it is entirely possible for a message to be consumed
   twice, this is known as "`at most once`_" delivery semantics.  If using the
   client as a job queue worker, make sure to either design the jobs to be
   idempotent or to track completion state in a separate data store.

The Default Naive Function
~~~~~~~~~~~~~~~~~~~~~~~~~~

The default allocator function uses a simple round-robin algorithm.  Each member
in the group is cycled over and given a single partition until there are no
partitions left.

This ensures a relatively even number of partitions spread over the group, but
does not account for some members having more capacity than others.  It also
does not account for differences in partition counts between topics.  It is
entirely possible for a group member to inadvertently wind up with all of the
partitions of a certain topic and other members to not.

Customizing
~~~~~~~~~~~

The allocator can be customized to be any stable function that meets the following
requirements:

* takes two arguments: a sorted list of member names and a sorted list of strings
  denoting partitions with the format ``<topic>:<partition_id>``:

.. code-block:: python

  members = ["client01:434533", "client02:12345"]
  partitions = [
      "example.topic.1:0",
      "example.topic.1:1",
      "example.topic.1:2",
      "example.topic.1:3",
      "example.topic.2:0",
      "example.topic.2:1",
  ]

* returns a dictionary keyed on member name, with nested dictionarys as values
  keyed on topic name with a list of partition ids as values:

.. code-block:: python

  {
      "client01:434533": {
          "example.topic.1": [0, 3],
          "example.topic.2": [1],
      },
      "client02:12345":
          "example.topic.1": [1, 2],
          "example.topic.2": [0]
      }
  }


Some examples would be to account for CPU count or available memory so that
more powerful members take on more work.

.. note::

   It is very important that any allocation function be *stable*.  That is, each
   member should always get the same result from the function if the same
   argument values are given.

The Deserializer
----------------

The JSON Default
~~~~~~~~~~~~~~~~

By default ``json.dumps`` is used as a deserializer.  This works in conjunction
with the default serializer on the ``Producer`` class:

.. code-block:: python

  import random

  from kiel import clients
  from tornado import gen

  producer = clients.Producer(["kafka01"])
  consumer = clients.GroupedConsumer(
      ["kafka01"], "work-group", ["zk01", "zk02", "zk03"]
  )

  @gen.coroutine
  def produce():
      yield producer.connect()
      while True:
          yield producer.produce(
              "example.colors", {"color": random.choice(["blue", "red"])}
          )

  @gen.coroutine
  def consume():
      yield consumer.connect()
      while True:
          msgs = yield consumer.consume("example.colors")
          for msg in msgs:
              print(msg["color"])

Customizing
~~~~~~~~~~~

Deserializing can be customized via the ``deserializer`` constructor parameter.
The given callable will be passed a message's value as a single argument.

A trivial example where messages are rot-13 encoded:

.. code-block:: python

  import codecs

  from kiel import clients
  from tornado import gen


  def deserialize(value):
      return codecs.decode(value, "rot_13")

  consumer = clients.GroupedConsumer(
      ["kafka01"], "work-group", ["zk01", "zk02", "zk03"],
      deserializer=deserialize
  )

  @gen.coroutine
  def consume():
      yield consumer.connect()
      while True:
          msgs = yield consumer.consume("example.colors")
          for msg in msgs:
              print(msg["color"])


Limiting Responses
------------------

Max and Min Bytes
~~~~~~~~~~~~~~~~~

The size window of responses can be controlled via the ``min_bytes`` and
``max_bytes`` constructor arguments.  These direct the Kafka brokers to
not respond until *at least* ``min_bytes`` of data is present and to
construct responses *no greater* ``max_bytes``.

.. note::

   The ``max_bytes`` directive isn't *exact* as it only limits the data in
   the partition clauses of responses, there will still be other overhead.
   The Kafka protocol does not recognize an overal "max bytes" setting but
   has a *per partition* maximum, which the consumer calculates as
   ``max_bytes`` / number of partitions.

This can be helpful for consumers starting from the beginning of a large topic
and must throttle the otherwise-massive initial responses.

.. code-block:: python

  from kiel import clients
  from tornado import gen

  consumer = clients.GroupedConsumer(
      ["kafka01"], "work-group", ["zk01", "zk02", "zk03"],
      min_bytes=1024,
      max_bytes=(10 * 1024 * 1024)
  )

  @gen.coroutine
  def start_from_beginning():
      yield consumer.connect()

      msgs = yield consumer.consume("example.topic")
      while msgs:
          # process msgs, etc.
          msgs = yield consumer.consume("example.topic")

Response Wait Time
~~~~~~~~~~~~~~~~~~

The ``max_wait_time`` constructor argument can be used to tell brokers how long
the consumer is willing to wait for data.  If the ``max_wait_time`` is reached
before data is available the broker will respond with a retriable "timeout" error
code and the ``consume()`` call will return with an empty list.


Compression
-----------

Kafka bakes compression into the wire protocol itself so the consumer classes
take care of decompression for you.

.. warning::

   Naturally, if you're using compression schemes with external dependencies
   (i.e. non-gzip schemes) when producing messages your consumers must *also*
   have those dependencies installed!


.. _Zookeeper: https://zookeeper.apache.org/
.. _`at most once`: http://kafka.apache.org/documentation.html#semantics
