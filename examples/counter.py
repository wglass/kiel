#!/usr/bin/env python

import argparse
import collections
import logging

from tornado import gen, ioloop

from kiel.clients import SingleConsumer


log = logging.getLogger()


parser = argparse.ArgumentParser(
    description="Example script that consumes messages of a given topic."
)
parser.add_argument(
    "brokers", type=lambda v: v.split(","),
    help="Comma-separated list of bootstrap broker servers"
)
parser.add_argument(
    "topic", type=str,
    help="Topic to consume"
)
parser.add_argument(
    "--status_interval", type=int, default=5,
    help="Interval (in seconds) to print the current status."
)
parser.add_argument(
    "--debug", type=bool, default=False,
    help="Sets the logging level to DEBUG"
)


color_counter = collections.Counter()


@gen.coroutine
def run(c, args):
    yield c.connect()

    while True:
        msgs = yield c.consume(args.topic)

        color_counter.update([msg["color"] for msg in msgs])


def show_status():
    print (
        "counts: \n%s" % "\n".join([
            "\t%s: %s" % (color, count)
            for color, count in color_counter.most_common()
        ])
    )


def main():
    args = parser.parse_args()
    loop = ioloop.IOLoop.instance()

    if args.debug:
        log.setLevel(logging.DEBUG)

    c = SingleConsumer(brokers=args.brokers)

    loop.add_callback(run, c, args)
    status_callback = ioloop.PeriodicCallback(
        show_status, args.status_interval * 1000
    )

    def wind_down(f):
        status_callback.stop()
        loop.stop()

    try:
        status_callback.start()
        loop.start()
    except KeyboardInterrupt:
        c.close().add_done_callback(wind_down)


if __name__ == "__main__":
    main()
