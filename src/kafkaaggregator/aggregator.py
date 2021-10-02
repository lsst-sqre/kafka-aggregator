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
from statistics import StatisticsError
from typing import Any, List

from faust_avro import Record

from kafkaaggregator.aggregator_config import AggregatedTopic
from kafkaaggregator.fields import Field
from kafkaaggregator.models import create_record
from kafkaaggregator.operations import (  # noqa: F401
    mean,
    median,
    q1,
    q3,
    stdev,
)
from kafkaaggregator.topic_schema import (
    AggregatedTopicSchema,
    SourceTopicSchema,
)

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

    def __init__(self, aggregated_topic: AggregatedTopic) -> None:

        self._name = aggregated_topic.name

        self._aggregated_topic_schema = AggregatedTopicSchema(name=self._name)

        self._window_size_secods = (
            aggregated_topic.window_aggregation.window_size_seconds
        )
        self._operations = aggregated_topic.window_aggregation.operations
        self._min_sample_size = (
            aggregated_topic.window_aggregation.min_sample_size
        )

        # Supports the 1 source topic -> 1 aggregated topic case for the moment
        source_topic_name = aggregated_topic.source_topics[0]

        self._source_topic_schema = SourceTopicSchema(name=source_topic_name)

        # TODO: return field names prefixed by source topic
        self._fields = aggregated_topic.get(source_topic_name).fields

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

    async def create_record_and_register(self) -> Record:
        """Create a Faust Record and scehma for the aggregation topic."""
        logger.info(f"Create Faust record for topic {self._name}.")

        cls_name = self._name.title().replace("-", "")

        # TODO: add ability to filter fields (use self._fields)
        # The source fields are already validated when the
        # AggregatedTopic object is created

        fields = await self._source_topic_schema.get_fields()

        self._aggregated_fields = self._create_aggregated_fields(
            fields, self._operations
        )

        # Create Faust Record
        self._record = self._create_record(
            cls_name=cls_name,
            fields=self._aggregated_fields,
            doc=f"Faust record for topic {self._name}.",
        )

        logger.info(f"Register Avro schema for topic {self._name}.")

        # Convert Faust Record to Avro Schema
        avro_schema = self._record.to_avro(
            registry=self._aggregated_topic_schema._registry
        )

        # Register Avro Schema with Schema Registry
        await self._aggregated_topic_schema.register(
            schema=json.dumps(avro_schema)
        )

        return self._record

    def async_create_record_and_register(self) -> None:
        """Sync call to ``async create_record()``.

        Get the current event loop and call the async
        ``create_record_and_register()``
        method.
        """
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.create_record_and_register())

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
