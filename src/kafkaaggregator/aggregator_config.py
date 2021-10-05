"""Pydantic models for the aggregator configuration."""

from pathlib import Path
from typing import Any, List, Mapping, Optional, Set

import yaml
from pydantic import BaseModel, validator

from kafkaaggregator.operations import Operation

__all__ = [
    "SourceTopic",
    "Filter",
    "WindowAggregation",
    "AggregatedTopic",
    "AggregatorConfig",
]


class SourceTopic(BaseModel):
    """Describe a source topic."""

    name: str
    """Source topic name."""

    fields: List[str]
    """List of fields to keep from the source topic."""

    # TODO: add value transformation

    map: Optional[Mapping[str, str]] = None
    """Map transformation on fields.

    For example, to rename a field use ``field: new_field``.
    """

    # TODO: add validation for Source topic name and fields

    @validator("map")
    def validate_map(
        cls, field_value: str, values: Mapping[str, Any], **kwargs: Any
    ) -> str:
        """Validate the mapping parameter."""
        if field_value:
            for k in field_value:
                if k not in values["fields"]:
                    raise ValueError(
                        f"Invalid field name '{k}' in mapping specification."
                    )
        return field_value


class Filter(BaseModel):
    """Filter transformation on source topics."""

    source_topics: List[SourceTopic]
    """List of source topics to keep in the source stream."""


class WindowAggregation(BaseModel):
    """Specify window aggregation configuration."""

    window_size_seconds: int = 1
    """Size of the tumbling window in seconds used to aggregate messages."""

    window_expiration_seconds: int = 0
    """Default Window expiration time in seconds. This parameter controls when the
    callback function to process the expired window(s) is called.
    """

    min_sample_size: int = 2
    """Minimum sample size to compute statistics.

    Given the size of the tumbling window and the frequency of incoming
    messages, this parameter sets the minimum sample size to compute
    statistics. The Faust tumbling window will always contain at least one
    message. If the number of messages in the tumbling window is smaller than
    min_sample_size, no statistics are computed and the values for the first
    message are used instead.

    The default value ``min_sample_size=2`` make sure we can compute stdev.
    """

    operations: List[str] = ["mean"]
    """Window aggregation operations to perform."""

    @validator("operations")
    def validate_operations(cls, operations: List) -> List:
        """Validate the operations parameter."""
        for operation in operations:
            if operation not in Operation.values():
                raise ValueError(
                    f"Invalid operation '{operation}'. "
                    f"Allowed values are: {', '.join(Operation.values())}."
                )
        return operations


class AggregatedTopic(BaseModel):
    """Describe an aggregated topic."""

    name: str
    """Aggregated topic name."""

    filter: Filter
    """Data filtering."""

    window_aggregation: WindowAggregation
    """Window aggregation."""

    @property
    def source_topics(self) -> List:
        """List source topic names."""
        source_topics: List = []
        for topic in self.filter.source_topics:
            source_topics.append(topic.name)

        return source_topics

    def get(self, source_topic: str) -> SourceTopic:
        """Get source topic object by its name."""
        for topic in self.filter.source_topics:
            if topic.name == source_topic:
                break
        return topic


class AggregatedTopics(BaseModel):
    """Describe the configuration for all the aggregated topics."""

    aggregated_topics: List[AggregatedTopic]
    """List of aggregated topics."""


class AggregatorConfig:
    """A representation of the aggregator configuration."""

    def __init__(self, configfile: Path) -> None:
        self._configfile = configfile
        self._config = self._parse()

    def _parse(self) -> AggregatedTopics:
        """Parse aggregator configuration file."""
        f = open(self._configfile)
        yaml_data = yaml.safe_load(f)
        config = AggregatedTopics.parse_obj(yaml_data)
        return config

    @property
    def config(self) -> AggregatedTopics:
        """Return the configuration object."""
        return self._config

    @property
    def aggregated_topics(self) -> List:
        """List aggregated topic names."""
        aggregated_topic: List = []
        for topic in self._config.aggregated_topics:
            aggregated_topic.append(topic.name)
        return aggregated_topic

    @property
    def source_topics(self) -> Set:
        """Return all source topics in the aggregator config."""
        source_topics: Set = set()
        for topic in self._config.aggregated_topics:
            source_topics.update(topic.source_topics)
        return source_topics

    def get(self, aggregated_topic: str) -> AggregatedTopic:
        """Get aggregated topic object by its name."""
        for topic in self._config.aggregated_topics:
            if topic.name == aggregated_topic:
                break
        return topic
