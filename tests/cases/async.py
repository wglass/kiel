from tornado import testing, concurrent


class AsyncTestCase(testing.AsyncTestCase):

    def future_value(self, value):
        f = concurrent.Future()
        f.set_result(value)
        return f

    def future_error(self, exc, instance=None, tb=None):
        f = concurrent.Future()
        if instance is None or tb is None:
            f.set_exception(exc)
        else:
            f.set_exc_info((exc, instance, tb))

        return f
