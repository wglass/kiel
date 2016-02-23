DEFAULT_KAFKA_PORT = 9092

#: Compression flag value denoting ``gzip`` was used
GZIP = 1
#: Compression flag value denoting ``snappy`` was used
SNAPPY = 2
#: This set denotes the compression schemes currently supported by Kiel
SUPPORTED_COMPRESSION = (None, GZIP, SNAPPY)

CLIENT_ID = "kiel"

#: The "api version" value sent over the wire.  Currently always 0
API_VERSION = 0
#: Mapping of response api codes and their names
API_KEYS = {
    "produce": 0,
    "fetch": 1,
    "offset": 2,
    "metadata": 3,
    "offset_commit": 8,
    "offset_fetch": 9,
    "group_coordinator": 10,
}


#: All consumers use replica id -1, other values are meant to be
#: used by Kafka itself.
CONSUMER_REPLICA_ID = -1

#: A mapping of known error codes to their string values
ERROR_CODES = {
    0: "no_error",
    -1: "unknown",
    1: "offset_out_of_range",
    2: "invalid_message",
    3: "unknown_topic_or_partition",
    4: "invalid_message_size",
    5: "leader_not_available",
    6: "not_partition_leader",
    7: "request_timed_out",
    8: "broker_not_available",
    9: "replica_not_available",
    10: "message_size_too_large",
    11: "stale_controller_epoch",
    12: "offset_metadata_too_large",
    14: "offsets_load_in_progress",
    15: "coordinator_not_available",
    16: "not_coordinator",
}
#: Set of error codes marked "retryable" by the Kafka docs.
RETRIABLE_CODES = set([
    "invalid_message",
    "unknown_topic_or_partition",
    "leader_not_available",
    "not_partition_leader",
    "request_timed_out",
    "offsets_load_in_progress",
    "coordinator_not_available",
    "not_coordinator",
])
