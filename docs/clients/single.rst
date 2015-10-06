==========================
The Single Consumer Client
==========================

  The ``SingleConsumer`` client class is used when you want to consume messages
  but don't need to coordinate consumer instances amongst themselves.  It's
  importable via the ``kiel.clients`` module and provides a ``consume()``
  method capable of starting at the beginning or end of a topic *or* a given
  ``datetime`` or ``timedelta``.

.. code-block:: python

  from kiel import clients
  from tornado import gen

  consumer = clients.SingleConsumer(
      ["kafka01", "kafka02"],
      deserializer=None,
      max_wait_time=1000,  # in milliseconds
      min_bytes=1,
      max_bytes=(1024 * 1024),
  )

  @gen.coroutine
  def run():
      yield consumer.connect()
      msgs = yield consumer.consume("example.topic")
      for msg in msgs:
          print(msg)


The only *required* constructor parameter is the list of bootstrap broker
hosts.


Where to Start
--------------

Other than the topic to consume, the ``consume()`` method also takes an optional
parameter of where in the topic's history to start.

.. note::

   The ``start`` parameter is honored in only two cases

   * when consuming from a topic for the first time
   * an "offset out of range" error is encountered.

There are four different possible kinds of values:

* ``SingleConsumer.END`` **(default)**

  This denotes the tail end of the topic, the ``consume()`` call will return
  messages once some are available.

*  ``SingleConsumer.BEGINNING``

   The very beginning of a topic (often 0).  Useful for re-processing topics.

*  ``datetime``

   Starts consuming a topic at roughly the point it was at a given time (in
   UTC).

*  ``timedelta``

   Starts consuming a topic at roughly the point it was at a *reliative*
   time.


.. warning::

   The time-based options rely on epoch seconds and are vulnerable to clock
   skew between brokers and client servers.


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
  consumer = clients.SingleConsumer(["kafka01"])

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

  consumer = clients.SingleConsumer(["kafka01"], deserializer=deserialize)

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

  consumer = clients.SingleConsumer(
      ["kafka01"],
      min_bytes=1024,
      max_bytes=(10 * 1024 * 1024)
  )

  @gen.coroutine
  def start_from_beginning():
      yield consumer.connect()

      msgs = yield consumer.consume("example.topic", start=consumer.BEGINNING)
      while msgs:
          # process msgs, etc.
          msgs = yield consumer.consume("example.topic", start=consumer.BEGINNING)

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
