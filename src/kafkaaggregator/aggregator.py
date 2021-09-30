"""Create the aggregation model and compute summary statistics.

Given a source topic and a list of field names to exclude from aggregation
create the aggregation model and compute summary statistics.

kafka-aggregator adds the aggregation fields `time`, `window_size`, and
`count` and computes `min`, `mean`, `stdev`, `median`, and `max` statistics
for every numeric field in the source topic.
"""


__all__ = ["Aggregator"]

import asyncio
import json
import logging
from pathlib import Path
from statistics import StatisticsError
from typing import Any, List

from faust_avro import Record

from kafkaaggregator.aggregator_config import AggregatorConfig
from kafkaaggregator.fields import Field
from kafkaaggregator.models import create_record
from kafkaaggregator.operations import (  # noqa: F401
    mean,
    median,
    q1,
    q3,
    stdev,
)
from kafkaaggregator.topics import AggregatedTopic, SourceTopic

logger = logging.getLogger("kafkaaggregator")


class Aggregator:
    """Create the aggregation model and compute summary statistics.

    Given a source topic and a list of field names to exclude from aggregation
    create the aggregation model and compute summary statistics.

    kafka-aggregator adds the aggregation fields `time`, `window_size`, and
    `count` and computes `min`, `mean`, `stdev`, `median`, and `max` statistics
    for every numeric field in the source topic.

    Parameters
    ----------
    aggregated_topic : `str`
        Name of the aggregated topic.
    config : `AggregatorConfig`
        Aggregator configuration
    """

    logger = logger

    def __init__(self, configfile: Path, aggregated_topic: str) -> None:

        self._aggregated_topic = AggregatedTopic(name=aggregated_topic)

        config = AggregatorConfig(configfile).get(aggregated_topic)
        self._operations = config.window_aggregation.operations
        self._window_size_secods = (
            config.window_aggregation.window_size_seconds
        )
        self._min_sample_size = config.window_aggregation.min_sample_size

        # Supports the 1 source topic -> 1 aggregated topic case for the moment
        source_topic = config.source_topics[0]

        self._source_topic = SourceTopic(name=source_topic)
        self._fields = config.get(source_topic).fields
        self._create_record = create_record

        self._aggregated_fields: List[Field] = []
        self._record: Record = None

    @staticmethod
    def _create_aggregated_fields(
        fields: List[Field],
        operations: List[str],
    ) -> List[Field]:
        """Create aggregated topic fields based on the source topic fields.

        Add the fields `time`, `window_size`, and `count`.
        For each numeric field in the source topic add new fields for each
        configured operation.

        Parameters
        ----------
        fields : `list` [`Field`]
            List of fields to aggregate.
        operations : `list`
            List of operations to perform.

        Returns
        -------
        aggregation_fields : `list` [`Field`]
            List of aggregation fields.
        """
        time = Field(name="time", type=float)
        window_size = Field(name="window_size", type=float)
        count = Field(name="count", type=int)

        aggregated_fields = [time, window_size, count]

        for field in fields:
            # Only numeric fields are aggregated
            if field.type in (int, float):
                for operation in operations:
                    f = Field(
                        name=f"{operation}_{field.name}",
                        type=float,
                        source_field_name=field.name,
                        operation=operation,
                    )
                    aggregated_fields.append(f)

        return aggregated_fields

    async def create_record(self) -> Record:
        """Create a Faust-avro Record class for the aggregation topic.

        Returns
        -------
        record : `Record`
            Faust-avro Record class for the aggreated topic.
        """
        aggregated_topic_name = self._aggregated_topic.name
        logger.info(f"Create Faust record for topic {aggregated_topic_name}.")

        cls_name = aggregated_topic_name.title().replace("-", "")

        # TODO: add ability to filter fields
        source_fields = await self._source_topic.get_fields()

        self._aggregated_fields = self._create_aggregated_fields(
            source_fields, self._operations
        )

        self._record = self._create_record(
            cls_name=cls_name,
            fields=self._aggregated_fields,
            doc=f"Faust record for topic {aggregated_topic_name}.",
        )

        await self._register(self._record)

        return self._record

    def async_create_record(self) -> Record:
        """Sync call to ``async create_record()``.

        Get the current event loop and call the async ``create_record()``
        method.

        Returns
        -------
        record : `Record`
            Faust-avro Record class for the aggreation topic.
        """
        loop = asyncio.get_event_loop()
        record = loop.run_until_complete(self.create_record())
        return record

    async def _register(self, record: Record) -> None:
        """Register the Avro schema for the aggregation topic.

        Parameters
        ----------
        record: `Record`
            Faust-avro Record for the aggregation model.
        """
        topic_name = self._aggregated_topic.name
        logger.info(f"Register Avro schema for topic {topic_name}.")
        schema = record.to_avro(registry=self._aggregated_topic._registry)

        await self._aggregated_topic.register(schema=json.dumps(schema))

    def compute(
        self,
        time: float,
        messages: List[Any],
    ) -> Record:
        """Compute summary statistics for a list of messages.

        Parameters
        ----------
        time: `float`
            The timestamp of the aggregated message, typically the midpoint
            of the aggregation window.
        messages: `list`
            List of messages from which to compute the summary statistics

        Returns
        -------
        aggregated_message: `Record`
            Aggregated message.
        """
        if not self._record:
            msg = (
                "Use Aggregator.create_record() to created the Faust record "
                "for the aggregated topic first."
            )
            raise RuntimeError(msg)

        count = len(messages)

        aggregated_values = {
            "count": count,
            "time": time,
            "window_size": self._window_size_secods,
        }

        for aggregated_field in self._aggregated_fields:

            if aggregated_field.operation:

                source_field_name = aggregated_field.source_field_name
                values = [message[source_field_name] for message in messages]

                try:
                    operation = aggregated_field.operation
                    # Make sure there are enough values to compute statistics
                    if len(values) >= self._min_sample_size:
                        aggregated_value = eval(operation)(values)
                    else:
                        # use the first value instead
                        aggregated_value = values[0]
                except Exception:
                    msg = f"Error computing {operation} of {values}."
                    raise StatisticsError(msg)

                aggregated_values.update(
                    {aggregated_field.name: aggregated_value}
                )

        aggregated_message = self._record(**aggregated_values)

        return aggregated_message
