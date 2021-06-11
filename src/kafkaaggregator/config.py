"""Configuration definition."""

__all__ = ["Configuration", "ExampleConfiguration"]

import os
import sys
from dataclasses import dataclass
from os.path import abspath, dirname, isdir
from typing import List

from kafkaaggregator.operations import Operation


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

    default_window_expiration_seconds: float = float(
        os.getenv("WINDOW_EXPIRATION_SECONDS", "0")
    )
    """Default Window expiration time in seconds. This parameter controls when the
    callback function to process the expired window(s) is called.
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

    agents_output_dir: str = os.getenv("AGENTS_OUTPUT_DIR", "agents")
    """Name of output directory for the agents' code."""

    agent_template_file: str = os.getenv("AGENT_TEMPLATE_FILE", "agent.j2")
    """Name of the agent Jinja2 template file."""

    def __post_init__(self) -> None:
        """Post config initialization steps."""
        # Validate operations
        self.operations = self._strtolist(
            os.getenv("OPERATIONS", "min, q1, mean, median, stdev, q3, max")
        )

        for operation in self.operations:
            if operation not in Operation.values():
                raise ValueError(
                    f"Invalid operation '{operation}' in config.operations. "
                    f"Allowed values are: {', '.join(Operation.values())}."
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


@dataclass
class ExampleConfiguration:
    """Configuration for the Kafkaaggregator example."""

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

    source_topic_name_prefix: str = os.getenv(
        "SOURCE_TOPIC_NAME_PRFIX", "example"
    )
    """The prefix for source topic names to use with the aggregator example.
    """
