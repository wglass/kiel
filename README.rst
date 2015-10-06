==========================
Kiel: Kafka Tornado Client
==========================

.. image::
    https://img.shields.io/pypi/v/kiel.svg
    :target: http://pypi.python.org/pypi/kiel
    :alt: Python Package Version
.. image::
    https://readthedocs.org/projects/kiel/badge/?version=latest
    :alt: Documentation Status
    :target: http://kiel.readthedocs.org/en/latest/
.. image::
    https://travis-ci.org/wglass/kiel.svg?branch=master
    :alt: Build Status
    :target: https://travis-ci.org/wglass/kiel
.. image::
    https://codeclimate.com/github/wglass/kiel/badges/gpa.svg
    :alt: Code Climate
    :target: https://codeclimate.com/github/wglass/kiel
.. image::
    https://codecov.io/github/wglass/kiel/coverage.svg?branch=master
    :alt: Codecov.io
    :target: https://codecov.io/github/wglass/kiel?branch=master


Kiel is a pure python Kafka_ client library for use with Tornado_
applications.


Installation
------------

Pip
~~~

Kiel is available via PyPI_, installation is as easy as::

  pip install kiel


Manual
~~~~~~

To install manually, first clone this here repo and:

.. parsed-literal::

    cd kiel
    python setup.py install


Documentation
-------------

More detailed information can be found on `Read The Docs`_.


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
~~~~~~~~~~~

The code is hosted on GitHub_

To file a bug or possible enhancement see the `Issue Tracker`_, also found
on GitHub.


License
~~~~~~~
\(c\) 2015-2016 William Glass

Kiel is licensed under the terms of the Apache License (2.0).  See the LICENSE_
file for more details.


.. _Kafka: http://kafka.apache.org/
.. _Tornado: http://tornadoweb.org/
.. _PyPI: http://pypi.python.org/pypi/kiel
.. _`Read The Docs`: http://kiel.readthedocs.org/
.. _GitHub: https://github.com/wglass/kiel
.. _`Issue Tracker`: https://github.com/wglass/kiel/issues
.. _LICENSE: https://github.com/wglass/kiel/blob/master/LICENSE
