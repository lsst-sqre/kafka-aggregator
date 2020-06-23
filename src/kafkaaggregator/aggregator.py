"""Create the aggregation model and compute summary statistics.

Given a source topic and a list field names to exclude from the source
topic creates the model for the aggregation topic and compute summary
statistics.

The aggregator add the aggregation fields `time`, `window_size`, and
`count` and computes the following statistics: `min`, `mean`, `stdev`,
`median`, and `max` statistics for every numeric field in the source topic.
"""


__all__ = ["Aggregator"]

import asyncio
import json
import logging
from statistics import StatisticsError, mean, median, stdev  # noqa: F401
from typing import Any, List

from faust_avro import Record

from kafkaaggregator.fields import Field, Operation
from kafkaaggregator.models import make_record
from kafkaaggregator.topics import AggregationTopic, SourceTopic

logger = logging.getLogger("kafkaaggregator")


class Aggregator:
    """Create the aggregation model and compute summary statistics.

    Given a source topic, creates the model for the aggregation topic and
    compute summary statistics.

    Parameters
    ----------
    source_topic : `str`
        Name of the source kafka topic.
    aggregation_topic : `str`
        Name of the kafka topic with the aggregated data.
    excluded_field_names: `list` ['str']
        List of field names to exclude from aggregation.
    """

    logger = logger

    def __init__(
        self,
        source_topic_name: str,
        aggregation_topic_name: str,
        excluded_field_names: List[str],
    ) -> None:

        self._source_topic = SourceTopic(name=source_topic_name)
        self._aggregation_topic = AggregationTopic(name=aggregation_topic_name)
        self._excluded_field_names = excluded_field_names
        self._make_record = make_record

    @staticmethod
    def _create_aggregation_fields(
        fields: List[Field], excluded_field_names: List
    ) -> List[Field]:
        """Create the aggregation topic fields based on the source topic fields.

        Add the fields `time`, `window_size`, and `count` and fields for the
        `min`, `mean`, `stdev`, `median`, and `max` statistics for every
        numeric field in the source topic.

        Fields in `excluded_field_names` are excluded from aggregation.

        Parameters
        ----------
        fields : `list` [`Field`]
            List of fields to aggregate.
        excluded_field_names : `list`
            List of fields excluded from aggregation.

        Returns
        -------
        aggregation_fields : `list` [`Field`]
            List of aggregation fields.
        """
        time = Field(name="time", type=float)
        window_size = Field(name="window_size", type=float)
        count = Field(name="count", type=int)

        aggregation_fields = [time, window_size, count]

        for field in fields:
            if field.name in excluded_field_names:
                logger.info(f"Excluding field {field.name}.")
                continue
            # Only numeric fields are aggregated
            if field.type in (int, float):
                for operation in Operation:
                    f = Field(
                        name=f"{operation.value}_{field.name}",
                        type=float,
                        source_field_name=field.name,
                        operation=operation,
                    )
                    aggregation_fields.append(f)

        return aggregation_fields

    async def create_record(self) -> Record:
        """Create the Faust Record for the aggregation topic.

        Returns
        -------
        record : `Record`
            Faust-avro Record for the aggreation topic.
        """
        logger.info(f"Make Faust record for topic {self._source_topic.name}.")

        cls_name = self._source_topic.name.title().replace("-", "")

        fields = await self._source_topic.get_fields()

        self._aggregation_fields = self._create_aggregation_fields(
            fields, self._excluded_field_names
        )

        self._record = self._make_record(
            cls_name=cls_name,
            fields=self._aggregation_fields,
            doc=f"Faust record for topic {self._source_topic.name}.",
        )

        await self._register(self._record)

        return self._record

    def async_create_record(self) -> Record:
        """Async call to create record.

        Get the current event loop and call the async `create_record()`
        method.

        Returns
        -------
        record : `Record`
            Faust-avro Record for the aggreation topic.
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
        logger.info(
            f"Register Avro schema for topic {self._aggregation_topic.name}."
        )
        schema = record.to_avro(registry=self._aggregation_topic._registry)

        await self._aggregation_topic.register(schema=json.dumps(schema))

    def compute(
        self, time: float, window_size: float, messages: List[Any]
    ) -> Record:
        """Compute summary statistics for a list of messages.

        Parameters
        ----------
        time: `float`
            The timestamp of the aggregated message, typically the midpoint
            of the aggregation window.
        window_size: `float`
            Size of the aggregation window.
        messages: `list`
            List of messages from which to compute the summary statistics

        Returns
        -------
        aggregated_message: `Record`
            Aggregated message.
        """
        if not self._record:
            msg = (
                "Use Aggregator.record() to created the Faust record for the "
                "aggregation topic first."
            )
            raise RuntimeError(msg)

        count = len(messages)

        aggregated_values = {
            "count": count,
            "time": time,
            "window_size": window_size,
        }

        for aggregation_field in self._aggregation_fields:

            if aggregation_field.operation:

                source_field_name = aggregation_field.source_field_name
                values = [message[source_field_name] for message in messages]

                try:
                    operation = aggregation_field.operation.value
                    aggregated_value = eval(operation)(values)
                except Exception:
                    msg = f"Error computing {operation} of {values}."
                    raise StatisticsError(msg)

                aggregated_values.update(
                    {aggregation_field.name: aggregated_value}
                )

        aggregated_message = self._record(**aggregated_values)

        return aggregated_message
