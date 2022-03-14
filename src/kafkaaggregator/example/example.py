"""Aggregation example."""


__all__ = ["AggregationExample"]

import asyncio
import json
import logging
import random
from pathlib import Path
from time import time
from typing import List

import faust_avro
from faust_avro import Record

from kafkaaggregator.aggregator_config import AggregatorConfig
from kafkaaggregator.config import ExampleConfiguration
from kafkaaggregator.fields import Field
from kafkaaggregator.models import create_record
from kafkaaggregator.topic_schema import SourceTopicSchema

AvroSchemaT = str

logger = logging.getLogger("kafkaaggregator")

example_config = ExampleConfiguration()


class AggregationExample:
    """Initialize topics and produce messages for the aggregation example.

    The aggregation example can be used to evaluate the performance of the
    kafka-aggregator application.
    """

    MAX_NFIELDS = 999

    def __init__(self, config_file: Path) -> None:
        self._nfields = min(
            example_config.nfields, AggregationExample.MAX_NFIELDS
        )
        self._create_record = create_record
        aggregator_config = AggregatorConfig(config_file)
        self._source_topics = aggregator_config.source_topics

    def _make_fields(self) -> List[Field]:
        """Make fields for the example topics.

        Returns
        -------
        fields : `list`
            A list of fields mapping field name and type.
        """
        # A source topic needs a timestamp field
        time = Field(name="time", type=float)
        fields = [time]
        for n in range(self._nfields):
            fields.append(Field(name=f"value{n}", type=float))
        return fields

    async def init_topics(self, app: faust_avro.App) -> None:
        """Initialize source topics for the kafka-aggregator example.

        Create the faust_avro.Record for the source topics, create
        and register their Avro schemas with the Schema Registry.
        Create the topics in Kafka

        Parameters
        ----------
        app : `faust_avro.App`
            Faust application
        """
        # All source topics in the kafka-aggregator example have
        # the same fields
        fields = self._make_fields()
        topics = []
        for topic in self._source_topics:
            # Create the faust_avro.Record
            record = self._create_record(topic, fields)
            # Create and register the Avro schema
            source_topic = SourceTopicSchema(topic)
            avro_schema = record.to_avro(registry=source_topic._registry)
            await source_topic.register(schema=json.dumps(avro_schema))
            # Create topic in Kafka, internal means that the number of
            # partitions is configured by Faust
            _topic = app.topic(topic, value_type=record, internal=True)
            await _topic.declare()
            topics.append(_topic)

    async def produce(
        self, app: faust_avro.App, frequency: float, max_messages: int
    ) -> None:
        """Produce messages for the source topics in the aggregation example.

        In the aggregation example we can specify the frequency in which the
        messages are produced, the maximum number of messages for each source
        topic and the number of fields in every message.

        Parameters
        ----------
        app : `faust_avro.App`
            Faust application
        frequency : `float`
            The frequency in Hz in wich messages are produced.
        max_messages : `int`
            The maximum number of messages to produce.
        """
        logger.info(
            f"Producing message(s) at {frequency} Hz for each source topic."
        )
        count = 0
        send_count = 0

        while True:
            message = {"time": time()}
            for n in range(self._nfields):
                value = random.random()
                message.update({f"value{n}": value})
            # The same message is sent for all source topics
            for topic in self._faust_topics:
                await faust_topic.send(value=message)
                send_count += 1

            await asyncio.sleep(1 / frequency)
            # Allow for an indefinite loop if max_messages is a number
            # smaller than 1
            count += 1
            if count > max_messages:
                logger.info(f"{send_count} messages sent.")
                break
