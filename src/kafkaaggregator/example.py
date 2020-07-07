"""Aggregation example."""


__all__ = ["AggregationExample", "UnexpectedNumberOfTopicsError"]

import asyncio
import json
import logging
import random
from time import time

import faust_avro

from kafkaaggregator.app import config
from kafkaaggregator.topics import SourceTopic

AvroSchemaT = str

logger = logging.getLogger("kafkaaggregator")


class UnexpectedNumberOfTopicsError(RuntimeError):
    """Raised when the number of source topics is unnexpected.

    The number of source topics in Kafka must match the number of topics
    initialized by the example.
    """


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

    def create_schema(self, name: str) -> AvroSchemaT:
        """Create Avro schema for the source topic.

        Parameters
        ----------
        source_topic_name : `str`
            Name of the source topic.
        """
        fields = []
        # A source topic needs a timestamp field
        fields.append(dict(name="time", type="double"))

        for n in range(self._nfields):
            fields.append(dict(name=f"value{n}", type="double"))

        schema = json.dumps(
            dict(
                type="record",
                name=name.replace("-", "_"),
                doc="Source topic with raw values.",
                fields=fields,
            )
        )
        return schema

    async def initialize(self, app: faust_avro.App) -> None:
        """Initialize source topics for the aggregation example.

        The source topic is not managed by Faust, it is an external
        topic. To initialize the topic, its schema needs to registered in
        the Schema Registry and the topic itself needs to be created in Kafka.

        Parameters
        ----------
        app : `faust_avro.App`
            Faust application
        """
        for n in range(self._ntopics):
            source_topic_name = f"{config.source_topic_name_prefix}-{n:03d}"
            source_topic = SourceTopic(name=source_topic_name)
            schema = self.create_schema(name=source_topic_name)
            await source_topic.register(schema=schema)
            # Declare the source topic as an external topic in Faust.
            # External topics do not have a corresponding model, thus
            # value_type=bytes.
            external_topic = app.topic(
                source_topic_name, value_type=bytes, internal=False
            )
            await external_topic.declare()

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
        source_topic_names = SourceTopic.names()
        if len(source_topic_names) != self._ntopics:
            msg = (
                "Unexpected number of source topics from Kafka. Hint: "
                "verify if the topic_regex configuration matches the "
                "source topic names."
            )
            raise UnexpectedNumberOfTopicsError(msg)
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
            for source_topic_name in source_topic_names:
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
