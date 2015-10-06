import os

import flake8.main
import six


MAX_COMPLEXITY = 11


def test_style():
    for path in ("kiel", "tests"):
        python_files = list(get_python_files(path))
        yield create_style_assert(path, python_files)


def get_python_files(path):
    path = os.path.join(os.path.dirname(__file__), "../", path)
    for root, dirs, files in os.walk(path):
        for filename in files:
            if not filename.endswith(".py"):
                continue
            yield os.path.join(root, filename)


def create_style_assert(path, python_files):

    def test_function():
        assert_conforms_to_style(python_files)

    test_name = "test_style__%s" % path
    test_function.__name__ = test_name
    test_function.description = test_name

    return test_function


def assert_conforms_to_style(python_files):
    checker = flake8.main.get_style_guide(
        paths=python_files, max_complexity=MAX_COMPLEXITY
    )
    checker.options.jobs = 1
    checker.options.verbose = True
    result = checker.check_files()

    assert not result.messages, "\n".join([
        "%s: %s" % (code, message)
        for code, message in six.iteritems(result.messages)
    ])
