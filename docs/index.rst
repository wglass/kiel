.. title:: Kiel: Kafka Tornado Client

==========================
Kiel: Kafka Tornado Client
==========================

  Kiel is a pure python Kafka_ client library for use with Tornado_
  applications, built with ease-of-use in mind.

  There are three client classes available:

  *  :doc:`Producer <clients/producer>`
  *  :doc:`SingleConsumer <clients/single>`
  *  :doc:`GroupedConsumer <clients/zkgrouped>`


Installation
------------

Pip
~~~

Kiel is available via PyPI_, installation is as easy as::

  pip install kiel


Manual
~~~~~~

To install manually, first download and unzip the :current_tarball:`z`, then:

.. parsed-literal::

    tar -zxvf kiel-|version|.tar.gz
    cd kiel-|version|
    python setup.py install


Examples
--------

Example scripts can be found in the `examples directory`_ in the repo.

Quick Consumer Example
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from kiel import clients
    from tornado import gen, ioloop


    @gen.coroutine
    def consume():
        c = clients.SingleConsumer(brokers=["localhost"])

        yield c.connect()

        while True:
            msgs = yield c.consume("examples.colors")
            for msg in msgs:
                print(msg["color"])


    def run():
        loop = ioloop.IOloop.instance()

        loop.add_callback(consume)

        try:
            loop.start()
        except KeyboardInterrupt:
            loop.stop()


Development
-----------

The code is hosted on GitHub_

To file a bug or possible enhancement see the `Issue Tracker`_, also found
on GitHub.


License
-------

Kiel is licensed under the terms of the Apache License (2.0).  See the LICENSE_
file for more details.


.. _Kafka: http://kafka.apache.org/
.. _Tornado: http://tornadoweb.org/
.. _`current zipfile`: https://github.com/wglass/kiel/archive/master.zip
.. _PyPI: http://pypi.python.org/pypi/kiel
.. _`examples directory`: https://github.com/wglass/kiel/tree/master/examples
.. _GitHub: https://github.com/wglass/kiel
.. _`Issue Tracker`: https://github.com/wglass/kiel/issues
.. _LICENSE: https://github.com/wglass/kiel/blob/master/LICENSE


.. toctree::
   :hidden:
   :titlesonly:
   :maxdepth: 3

   clients
   releases
   source_docs
