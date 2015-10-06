import inspect
import re

import kiel.clients.client
import kiel.clients.consumer
import kiel.clients.grouped
import kiel.clients.producer
import kiel.clients.single
import kiel.cluster
import kiel.compression.gzip
import kiel.compression.snappy
import kiel.connection
import kiel.constants
import kiel.events
import kiel.exc
import kiel.iterables
import kiel.protocol.consumer_metadata
import kiel.protocol.fetch
import kiel.protocol.messages
import kiel.protocol.metadata
import kiel.protocol.offset
import kiel.protocol.offset_commit
import kiel.protocol.offset_fetch
import kiel.protocol.part
import kiel.protocol.primitives
import kiel.protocol.produce
import kiel.protocol.request
import kiel.protocol.response
import kiel.zookeeper.allocator
import kiel.zookeeper.party
import kiel.zookeeper.shared_set


modules_to_test = (
    kiel.clients.client,
    kiel.clients.consumer,
    kiel.clients.grouped,
    kiel.clients.producer,
    kiel.clients.single,
    kiel.cluster,
    kiel.compression.gzip,
    kiel.compression.snappy,
    kiel.connection,
    kiel.constants,
    kiel.events,
    kiel.exc,
    kiel.iterables,
    kiel.protocol.consumer_metadata,
    kiel.protocol.fetch,
    kiel.protocol.messages,
    kiel.protocol.metadata,
    kiel.protocol.offset,
    kiel.protocol.offset_commit,
    kiel.protocol.offset_fetch,
    kiel.protocol.part,
    kiel.protocol.primitives,
    kiel.protocol.produce,
    kiel.protocol.request,
    kiel.protocol.response,
    kiel.zookeeper.allocator,
    kiel.zookeeper.party,
    kiel.zookeeper.shared_set,
)


def test_docstrings():
    for module in modules_to_test:
        for path, thing in get_module_things(module):
            yield create_docstring_assert(path, thing)


def get_module_things(module):
    module_name = module.__name__

    for func_name, func in get_module_functions(module):
        if inspect.getmodule(func) != module:
            continue
        yield (module_name + "." + func_name, func)

    for class_name, klass in get_module_classes(module):
        if inspect.getmodule(klass) != module:
            continue
        yield (module_name + "." + class_name, klass)

        for method_name, method in get_class_methods(klass):
            if method_name not in klass.__dict__:
                continue
            yield (module_name + "." + class_name + ":" + method_name, method)


def get_module_classes(module):
    for name, klass in inspect.getmembers(module, predicate=inspect.isclass):
        yield (name, klass)


def get_module_functions(module):
    for name, func in inspect.getmembers(module, predicate=inspect.isfunction):
        yield (name, func)


def get_class_methods(klass):
    for name, method in inspect.getmembers(klass, predicate=inspect.ismethod):
        yield (name, method)


def create_docstring_assert(path, thing):

    def test_function():
        assert_docstring_present(thing, path)
        # TODO(wglass): uncomment this assert and fill out the param info
        # for methods and functions
        # assert_docstring_includes_param_metadata(thing, path)

    test_name = "test_docstring__%s" % de_camelcase(path)
    test_function.__name__ = test_name
    test_function.description = test_name

    return test_function


# TODO(wglass): remove __init__ from this when the param metadata assert is
# re-enabled
skipped_special_methods = ("__init__", "__str__", "__repr__")


def assert_docstring_present(thing, path):
    if any([path.endswith(special) for special in skipped_special_methods]):
        return

    docstring = inspect.getdoc(thing)
    if not docstring or not docstring.strip():
        raise AssertionError("No docstring present for %s" % path)


def assert_docstring_includes_param_metadata(thing, path):
    if inspect.isclass(thing):
        return

    docstring = inspect.getdoc(thing)
    if not docstring:
        return

    for arg_name in inspect.getargspec(thing).args:
        if arg_name in ("self", "cls"):
            continue

        if ":param %s:" % arg_name not in docstring:
            raise AssertionError(
                "Missing :param: for arg %s of %s" % (arg_name, path)
            )
        if ":type %s:" % arg_name not in docstring:
            raise AssertionError(
                "Missing :type: for arg %s of %s" % (arg_name, path)
            )


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def de_camelcase(name):
    return all_cap_re.sub(
        r'\1_\2',
        first_cap_re.sub(r'\1_\2', name)
    ).lower()
