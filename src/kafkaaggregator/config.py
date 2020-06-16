"""Configuration definition."""

__all__ = ["Configuration"]

import os
from dataclasses import dataclass, field
from typing import List


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

    Schema Registry used to read source topic schemas.
    """

    internal_registry_url: str = os.getenv(
        "INTERNAL_SCHEMA_REGISTRY_URL", "http://localhost:28081"
    )
    """Internal Confluent Schema Registry URL.

    Used in conjunction with faust-avro to register aggregated topic schemas.
    Depending on your Kafka setup you can use this internal Schema Registry to
    separate the aggregated topic schemas from other schemas and avoid
    Schema ID conflicts.
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

    source_topic_name: str = os.getenv(
        "SOURCE_TOPIC", "kafkaaggregator-example"
    )
    """Name of the source topic used in the kafkaaggregator example."""

    topic_rename_format: str = os.getenv(
        "TOPIC_RENAME_FORMAT", "{source_topic_name}-aggregated"
    )
    """A format string for the aggregation topic name, which must contain
    ``{source_topic_name}`` as a placeholder for the source topic name."""

    excluded_field_names: List[str] = field(default_factory=list)
    """List of field names to exclude from aggregation."""

    agents_output_dir: str = os.getenv("AGENTS_OUTPUT_DIR", "agents")
    """Name of output directoty for the agents code."""

    agents_template_file: str = os.getenv("AGENTS_TEMPLATE_FILE", "agents.j2")
    """Name of the agents Jinja2 template file."""

    def __post_init__(self) -> None:
        """Post config initialization steps."""
        # Validate topic_rename_format
        if "{source_topic_name}" not in self.topic_rename_format:
            raise ValueError(
                "config.topic_rename_format must contain the "
                "{source_topic_name} string."
            )

        # Set default value for excluded_field_names.
        # By default we exclude the field names ``time``, ``window_size``, and
        # ``count`` that are special as they are added by the aggregator.
        self.excluded_field_names = self._strtolist(
            os.getenv("EXCLUDED_FIELD_NAMES", "time, window_size, count")
        )

    def _strtolist(self, s: str) -> List[str]:
        """Convert comma separated values to a list of strings.

        Parameters
        ----------
        s : `str`
            Comma separated values

        Returns
        -------
        slist : `list`
        """
        slist = s.replace(" ", "").split(",")
        return slist
