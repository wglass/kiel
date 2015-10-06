#!/usr/bin/env python

import argparse
import logging

from tornado import gen, ioloop

from kiel.clients import GroupedConsumer


log = logging.getLogger()


parser = argparse.ArgumentParser(
    description="Example grouped consumer that prints out messages it gets."
)
parser.add_argument(
    "brokers", type=lambda v: v.split(","),
    help="Comma-separated list of bootstrap broker servers"
)
parser.add_argument(
    "zk_hosts", type=lambda v: v.split(","),
    help="Comma-separated list of zookeeper servers."
)
parser.add_argument(
    "topic", type=str,
    help="Topic to publish to"
)
parser.add_argument(
    "--debug", type=bool, default=False,
    help="Sets the logging level to DEBUG"
)


def process_message(msg):
    print(msg)


@gen.coroutine
def run(c, args):
    yield c.connect()

    while True:
        msgs = yield c.consume(args.topic)

        for msg in msgs:
            process_message(msg)

        if msgs:
            c.commit_offsets()


if __name__ == "__main__":
    args = parser.parse_args()
    loop = ioloop.IOLoop.instance()

    if args.debug:
        log.setLevel(logging.DEBUG)

    c = GroupedConsumer(
        brokers=args.brokers,
        group="worker-group",
        zk_hosts=args.zk_hosts,
        autocommit=False
    )

    loop.add_callback(run, c, args)

    try:
        loop.start()
    except KeyboardInterrupt:
        c.close().add_done_callback(lambda f: loop.stop())
