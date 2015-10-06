class KielError(Exception):
    """
    Base exception for all Kiel-specific errors.
    """
    pass


class NoBrokersError(KielError):
    """
    Error raised when a ``Cluster`` has no available connections.
    """
    pass


class NoOffsetsError(KielError):
    """
    Error raised when requests fetching offsets fail fatally.
    """
    pass


class ConnectionError(KielError):
    """
    This error is raised when a single broker ``Connection`` goes bad.
    """
    def __init__(self, host, port, broker_id=None):
        self.host = host
        self.port = port
        self.broker_id = broker_id

    def __str__(self):
        return "Error connecting to %s:%s" % (self.host, self.port)


class UnhandledResponseError(KielError):
    """
    Error raised when a client recieves a response but has no handler method.

    Any client that sends a request for an api is expected to define a
    corresponding ``handle_<api>_response`` method.
    """
    def __init__(self, api):
        self.api = api

    def __str__(self):
        return "No handler method for '%s' api" % self.api
