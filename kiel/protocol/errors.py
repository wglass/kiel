import six

from kiel.constants import ERROR_CODES, RETRIABLE_CODES


class Errors(object):
    pass


errors = Errors()
errors.retriable = set()

for code, error in six.iteritems(ERROR_CODES):
    setattr(errors, error, code)
    if error in RETRIABLE_CODES:
        errors.retriable.add(code)
