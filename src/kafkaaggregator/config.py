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

    store: str = os.getenv("STORE", "memory://")
    """The backend used for table storage.

    Tables are stored in-memory by default. In production, a persistent table
    store, such as rocksdb:// is preferred.
    """
