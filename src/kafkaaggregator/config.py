"""Configuration definition."""

__all__ = ["Configuration"]

import os
import sys
from dataclasses import dataclass, field
from os.path import abspath, dirname, isdir
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
        "INTERNAL_SCHEMA_REGISTRY_URL", "http://localhost:8081"
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

    See also `Faust's windowing feature
    <https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing>`_
    documentation.
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

    min_sample_size: int = int(os.getenv("MIN_SAMPLE_SIZE", "2"))
    """Minimum sample size to compute statistics.

    Given the size of the tumbling window and the frequency of incoming
    messages, this parameter sets the minimum sample size to compute
    statistics. The Faust tumbling window will always contain at least one
    message. If the number messages in the tumbling window is smaller than
    min_sample_size the values of the first message are used instead.

    The default value min_sample_size=2 make sure we can compute stdev.
    """

    topic_partitions: int = int(os.getenv("TOPIC_PARTITIONS", "4"))
    """Default number of partitions for new topics.

    This defines the maximum number of workers we could use to distribute the
    workload of the application.
    """

    source_topic_name_prefix: str = os.getenv(
        "SOURCE_TOPIC_NAME_PREFIX", "example"
    )
    """Prefix for the source topic name used in the aggregation example."""

    ntopics: int = int(os.getenv("NTOPICS", "10"))
    """Number of source topics used in the aggregation example."""

    nfields: int = int(os.getenv("NFIELDS", "10"))
    """Number of fields for source topics used in the aggregation example."""

    frequency: float = float(os.getenv("FREQUENCY", "10"))
    """The frequency in Hz in which messages are produced for the
    example topics.
    """

    max_messages: int = int(os.getenv("MAX_MESSAGES", "10"))
    """The maximum number of messages to produce. Set max_messages to a number
    smaller than 1 to produce an indefinite number of messages.
    """

    topic_regex: str = os.getenv("TOPIC_REGEX", "^example-[0-9][0-9][0-9]?$")
    """Regex to select source topics to aggregate."""

    excluded_topics: List[str] = field(default_factory=list)
    """Topics excluded from aggregation."""

    topic_rename_format: str = os.getenv(
        "TOPIC_RENAME_FORMAT", "{source_topic_name}-aggregated"
    )
    """A format string for the aggregation topic name, which must contain
    ``{source_topic_name}`` as a placeholder for the source topic name.
    """

    excluded_field_names: List[str] = field(default_factory=list)
    """List of field names to exclude from being aggregated."""

    agents_output_dir: str = os.getenv("AGENTS_OUTPUT_DIR", "agents")
    """Name of output directory for the agents' code."""

    agent_template_file: str = os.getenv("AGENT_TEMPLATE_FILE", "agent.j2")
    """Name of the agent Jinja2 template file."""

    def __post_init__(self) -> None:
        """Post config initialization steps."""
        # Set default value for excluded_topics
        self.excluded_topics = self._strtolist(
            os.getenv("EXCLUDED_TOPICS", "")
        )

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

        # Make sure agents_output_dir exists and update syspath to enable
        # agents autodiscover
        if not isdir(self.agents_output_dir):
            os.makedirs(self.agents_output_dir)

        sys.path.append(abspath(dirname(self.agents_output_dir)))

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
