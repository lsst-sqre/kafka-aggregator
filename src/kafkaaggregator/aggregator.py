__all__ = ["Aggregator"]

import asyncio
import json
import logging
from typing import List

from faust_avro import Record

from kafkaaggregator.config import Configuration
from kafkaaggregator.fields import Field
from kafkaaggregator.models import make_record
from kafkaaggregator.topics import AggregationTopic, SourceTopic

logger = logging.getLogger("kafkaaggregator")


def _strtolist(s: str) -> List[str]:
    """Convert comma separated values to a list of strings."""
    return s.replace(" ", "").split(",")


class Aggregator:
    """Given a source topic, creates the model for the aggregation topic and
    compute summary statistics.

    Parameters
    ----------
    config: `kafkaaggregator.config.Configuration`
        kafkaaggregator configuration
    source_topic : `str`
        Name of the source kafka topic.
    aggregation_topic : `str`
        Name of the kafka topic with the aggregated data.
    """

    logger = logger

    def __init__(
        self, config: Configuration, source_topic: str, aggregation_topic: str,
    ) -> None:

        self._config = config
        self._source_topic = SourceTopic(source_topic)
        self._aggregation_topic = AggregationTopic(aggregation_topic)
        self._excluded_field_names = _strtolist(
            self._config.excluded_field_names
        )
        self._make_record = make_record

    @staticmethod
    def _create_aggregation_fields(
        fields: List[Field], excluded_field_names: List
    ) -> List[Field]:
        """Create the aggregation topic fields based on the source topic fields.

        Add the fields `time`, `window_size`, and `count` and fields for the
        `min`, `mean`, `median`, and `max` statistics for every numeric
        field in the source topic.

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
            # Only fields representing numbers are aggregated
            if (field.type is int) or (field.type is float):
                for agg in ("min", "mean", "median", "max"):
                    f = Field(f"{agg}_{field.name}", float)
                    aggregation_fields.append(f)

        return aggregation_fields

    async def record(self) -> Record:
        """Create the Faust Record for the aggregation topic.

        Returns
        -------
        record : `Record`
            Faust-avro Record for the aggreation topic.
        """
        logger.info(f"Make Faust record for topic {self._source_topic.topic}.")

        fields = await self._source_topic.get_fields()

        aggregation_fields = self._create_aggregation_fields(
            fields, self._excluded_field_names
        )

        record = self._make_record(
            cls_name="AggregationRecord",
            fields=aggregation_fields,
            doc=f"Faust record for topic {self._source_topic.topic}.",
        )

        await self._register(record)

        return record

    def async_record(self) -> Record:
        """Get the current event loop and call the async `record()`
        method.

        Returns
        -------
        record : `Record`
            Faust-avro Record for the aggreation topic.
        """
        loop = asyncio.get_event_loop()
        record = loop.run_until_complete(self.record())
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
