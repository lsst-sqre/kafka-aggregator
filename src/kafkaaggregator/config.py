"""Configuration definition."""

__all__ = ["Configuration"]

import os
from dataclasses import dataclass


@dataclass
class Configuration:
    """Configuration for kafkaaggregator."""

    broker: str = os.getenv("KAFKA_BROKER_URL", "kafka://localhost:9092")
    """The Kafka broker URL.

    Currently, the only supported production transport is kafka://.
    This uses the aiokafka client under the hood, for consuming and producing
    messages.
    """

    registry_url: str = os.getenv(
        "SCHEMA_REGISTRY_URL", "http://localhost:8081"
    )
    """The Confluent Schema Registry URL.

    Used in conjunction with faust-avro to register Avro schemas.
    """

    store: str = os.getenv("STORE", "memory://")
    """The backend used for table storage.

    Tables are stored in-memory by default. In production, a persistent table
    store, such as rocksdb:// is preferred.
    """

    window_size: float = float(os.getenv("WINDOW_SIZE", "1"))
    """Size of the tumbling window in seconds used to aggregate messages.

    See https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing
    """

    window_expires: float = float(os.getenv("WINDOW_EXPIRES", "1"))
    """Window expiration time in seconds. This parameter controls when the
    callback function to process the expired window(s) is called.

    The default value is set to the window size, which
    means that at least two tumbling windows will be filled up with messages
    before the callback function is called to process the expired window(s).

    Note that if the worker (or the producer) stops, the next time the callback
    is called it might process windows from previous executions as messages
    from the stream are persisted by Faust.
    """

    topic_partitions: int = int(os.getenv("TOPIC_PARTITIONS", "4"))
    """Default number of partitions for new topics.

    This defines the maximum number of workers we could use to distribute the
    workload of the application.
    """

    src_topic: str = os.getenv("SOURCE_TOPIC", "kafkaaggregator-src-topic")
    """Name of the source topic used in the kafkaaggregator example.
    """

    agg_topic: str = os.getenv(
        "AGGREGATION_TOPIC", "kafkaaggregator-agg-topic"
    )
    """Name of the aggregation topic used in the kafkaaggregator example."""
