"""Aggregation example."""


__all__ = ["AggregationExample"]

import asyncio
import json
import logging
import random
from time import time
from typing import List

import faust_avro
from faust_avro import Record

from kafkaaggregator.config import ExampleConfiguration
from kafkaaggregator.fields import Field
from kafkaaggregator.models import create_record
from kafkaaggregator.topic_schema import SourceTopicSchema

AvroSchemaT = str

logger = logging.getLogger("kafkaaggregator")

config = ExampleConfiguration()


class AggregationExample:
    """Initialize topics and produce messages for the aggregation example.

    The aggregation example can be used to evaluate the performance of the
    kafka-aggregator application.
    """

    MAX_NTOPICS = 999
    MAX_NFIELDS = 999

    def __init__(self) -> None:
        self._ntopics = min(config.ntopics, AggregationExample.MAX_NTOPICS)
        self._nfields = min(config.nfields, AggregationExample.MAX_NFIELDS)
        self._source_topic_names: List = []
        self._create_record = create_record

    def make_fields(self) -> List[Field]:
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

    def create_record(self, name: str) -> Record:
        """Create a Faust-avro Record class for the source topic.

        With a Faust-avro Record for the source topic it is possible
        to produce messages in Avro format for the aggregation example,
        instead of using ``value_type=bytes``.

        Returns
        -------
        record : `Record`
            Faust-avro Record class for the source topic.
        """
        logger.info(f"Make Faust record for topic {name}.")
        cls_name = name.title().replace("-", "")
        fields = self.make_fields()
        self._record = self._create_record(
            cls_name=cls_name,
            fields=fields,
            doc=f"Faust record for topic {name}.",
        )
        return self._record

    async def initialize(self, app: faust_avro.App) -> None:
        """Initialize source topics for the aggregation example.

        To initialize the topic, its schema needs to be registered in
        the Schema Registry and the topic itself needs to be created in Kafka.

        Parameters
        ----------
        app : `faust_avro.App`
            Faust application
        """
        for n in range(self._ntopics):
            source_topic_name = f"{config.source_topic_name_prefix}-{n:03d}"
            source_topic = SourceTopicSchema(name=source_topic_name)
            record = self.create_record(name=source_topic_name)
            schema = record.to_avro(registry=source_topic._registry)
            await source_topic.register(schema=json.dumps(schema))
            # Declare the source topic as an internal topic in Faust.
            internal_topic = app.topic(
                source_topic_name, value_type=record, internal=True
            )
            await internal_topic.declare()
            self._source_topic_names.append(source_topic_name)

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
            for source_topic_name in self._source_topic_names:
                source_topic = app.topic(source_topic_name)
                await source_topic.send(value=message)
                send_count += 1

            await asyncio.sleep(1 / frequency)
            # Allow for an indefinite loop if max_messages is a number
            # smaller than 1
            count += 1
            if count == max_messages:
                logger.info(f"{send_count} messages sent.")
                break
