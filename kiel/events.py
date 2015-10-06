import sys

import six


def wait_on_event(event, timeout=None):
    """
    Waits on a single threading Event, with an optional timeout.

    This is here for compatibility reasons as python 2 can't reliably wait
    on an event without a timeout and python 3 doesn't define a ``maxint``.
    """
    if timeout is not None:
        event.wait(timeout)
        return

    if six.PY2:
        # Thanks to a bug in python 2's threading lib, we can't simply call
        # .wait() with no timeout since it would wind up ignoring signals.
        while not event.is_set():
            event.wait(sys.maxint)
    else:
        event.wait()
