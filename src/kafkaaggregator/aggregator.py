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

from kafkaaggregator.fields import Field
from kafkaaggregator.models import make_record
from kafkaaggregator.topics import AggregationTopic, SourceTopic

logger = logging.getLogger("kafkaaggregator")


def _strtolist(s: str) -> List[str]:
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
    excluded_field_names: `str`
        Comma separated list of field names to exclude from aggregation.
    """

    logger = logger

    def __init__(
        self,
        source_topic: str,
        aggregation_topic: str,
        excluded_field_names: str,
    ) -> None:

        self._source_topic = SourceTopic(source_topic)
        self._aggregation_topic = AggregationTopic(aggregation_topic)
        self._excluded_field_names = _strtolist(excluded_field_names)
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
        time = Field("time", float)
        window_size = Field("window_size", float)
        count = Field("count", int)

        aggregation_fields = [time, window_size, count]

        for field in fields:
            if field.name in excluded_field_names:
                logger.info(f"Excluding field {field.name}.")
                continue
            # Only numeric fields are aggregated
            if (field.type is int) or (field.type is float):
                for operation in ("min", "mean", "stdev", "median", "max"):
                    metadata = {
                        "source_field": field.name,
                        "operation": operation,
                    }
                    f = Field(f"{operation}_{field.name}", float, metadata)
                    aggregation_fields.append(f)

        return aggregation_fields

    async def create_record(self) -> Record:
        """Create the Faust Record for the aggregation topic.

        Returns
        -------
        record : `Record`
            Faust-avro Record for the aggreation topic.
        """
        logger.info(f"Make Faust record for topic {self._source_topic.topic}.")

        fields = await self._source_topic.get_fields()

        self._aggregation_fields = self._create_aggregation_fields(
            fields, self._excluded_field_names
        )

        self._record = self._make_record(
            cls_name="AggregationRecord",
            fields=self._aggregation_fields,
            doc=f"Faust record for topic {self._source_topic.topic}.",
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
        subject = f"{self._aggregation_topic.topic}-value"
        logger.info(f"Register Avro schema for subject {subject}.")
        schema = record.to_avro(registry=self._aggregation_topic._registry)

        await self._aggregation_topic.register(
            subject=subject, schema=json.dumps(schema)
        )

    def compute(self) -> None:
        pass
