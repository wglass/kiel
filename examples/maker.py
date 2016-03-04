#!/usr/bin/env python

import argparse
import logging
import random
import time

from tornado import gen, ioloop

from kiel.clients import Producer


log = logging.getLogger()


parser = argparse.ArgumentParser(
    description="Example script that produces messages to a given topic."
)
parser.add_argument(
    "brokers", type=lambda v: v.split(","),
    help="Comma-separated list of bootstrap broker servers"
)
parser.add_argument(
    "topic", type=str,
    help="Topic to publish to"
)
parser.add_argument(
    "--compression", type=str, default=None,
    choices=(None, "gzip", "snappy"),
    help="Which compression to use for messages (gzip, snappy or None)"
)
parser.add_argument(
    "--batch_size", type=int, default=1,
    help="Number of messages to batch into single server requests."
)
parser.add_argument(
    "--status_interval", type=int, default=5,
    help="Interval (in seconds) to print the current status."
)
parser.add_argument(
    "--debug", type=bool, default=False,
    help="Sets the logging level to DEBUG"
)


counter = 0
last_count = 0
last_status = 0
colors = [
    "red", "green", "blue", "yellow", "orange", "purple", "white", "black"
]


def key_maker(msg):
    return msg["counter"]


def partitioner(key, partitions):
    return partitions[key % len(partitions)]


@gen.coroutine
def run(p, args):
    global counter

    yield p.connect()

    while True:
        counter += 1

        yield p.produce(
            args.topic, {"counter": counter, "color": random.choice(colors)}
        )


def show_status():
    global last_count
    global last_status

    now = time.time()

    if not last_status:
        last_status = now

    count_since_last_status = counter - last_count
    time_since_last_status = (now - last_status) or 1

    print(
        "%s events (%s/sec)" % (
            count_since_last_status,
            count_since_last_status / time_since_last_status
        )
    )

    last_count = counter
    last_status = now


def main():
    args = parser.parse_args()
    loop = ioloop.IOLoop.instance()

    if args.debug:
        log.setLevel(logging.DEBUG)

    p = Producer(
        brokers=args.brokers,
        key_maker=key_maker,
        partitioner=partitioner,
        batch_size=args.batch_size,
        compression=args.compression
    )

    loop.add_callback(run, p, args)
    status_callback = ioloop.PeriodicCallback(
        show_status, args.status_interval * 1000
    )

    def stop_loop(_):
        status_callback.stop()
        loop.stop()

    try:
        status_callback.start()
        loop.start()
    except KeyboardInterrupt:
        p.close().add_done_callback(stop_loop)


if __name__ == "__main__":
    main()
