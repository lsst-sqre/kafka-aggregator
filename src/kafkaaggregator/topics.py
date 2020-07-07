"""Implements Topic, SourceTopic and AggregationTopic classes.

The Topic class has methods to retrieve the topic schema from the Schema
Registry and a parsed list of fields from the Avro schema with Python types.

The child classes SourceTopic and AggregationTopic set the right Schema
Registry URL to be used with each topic type.
"""
__all__ = ["SchemaException", "Topic", "SourceTopic", "AggregationTopic"]

import json
import logging
import re
from typing import List, Set, Union

from faust_avro.asyncio import ConfluentSchemaRegistryClient
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from kafkaaggregator.app import config
from kafkaaggregator.fields import Field

logger = logging.getLogger("kafkaaggregator")

AvroSchemaT = str


class SchemaException(Exception):
    """A generic schema registry client exception."""


class Topic:
    """
    Topic schema and interaction with the Schema Registry.

    Parameters
    ----------
    name : `str`
        Name of a kafka topic.
    registry_url : `str`
        Schema Registry URL.
    """

    logger = logger

    def __init__(self, name: str, registry_url: str) -> None:

        self.name = name
        self._subject = f"{self.name}-value"
        self._client = ConfluentSchemaRegistryClient(url=registry_url)
        self._registry = self._client.registry
        self._parse = self._client.registry.parse

    async def get_schema(self) -> AvroSchemaT:
        """Retrieve topic schema from the Schema Registry.

        Returns
        -------
        schema : `str`
            Avro schema.
        """
        schema = None
        try:
            schema = await self._client.schema_by_topic(self._subject)
        except Exception:
            msg = f"Could not retrieve schema for subject {self._subject}."
            raise SchemaException(msg)

        return schema

    async def get_fields(self) -> List[Field]:
        """Get topic fields.

        Parses the topic Avro schema and returns a list of fields with
        Python types.

        Returns
        -------
        fields : `list` [`Field`]
            List of topic fields.
        """
        schema = await self.get_schema()
        fields = []
        if schema:
            # The faust-avro parser expects a json-parsed avro schema
            # https://github.com/masterysystems/faust-avro/blob/master/faust_avro/parsers/avro.py#L20
            parsed_schema = self._parse(json.loads(schema))
            for field in parsed_schema.fields:
                fields.append(Field(field.name, field.type.python_type))

        return fields

    async def register(self, schema: AvroSchemaT) -> Union[int, None]:
        """Register an Avro schema with the Schema Registry.

        If the schema is already register for this subject it does nothing.

        Parameters
        ----------
        subject : `str`
            Name of the topic subject.
        schema : `str`
            Avro schema.

        Returns
        -------
        schema_id : `int` or `None`
            Schema ID from the Schema Registry or `None` if it is already
            registered.
        """
        logger.info(f"Register schema for subject {self._subject}.")

        is_registered = False
        try:
            is_registered = await self._client.is_registered(
                self._subject, schema
            )
        except Exception:
            msg = "Could not connect to Schema Registry."
            raise SchemaException(msg)

        schema_id = None
        if not is_registered:
            try:
                schema_id = await self._client.register(self._subject, schema)
            except Exception:
                msg = f"Could not register schema for subject {self._subject}."
                raise SchemaException(msg)
        return schema_id


class SourceTopic(Topic):
    """Represents source topics.

    Sets the right Schema Registry URL for source topics.

    Parameters
    ----------
    name: `str`
        Name of the source topic in Kafka.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name=name, registry_url=config.registry_url)

    @staticmethod
    def names() -> Set[str]:
        """Return a set of source topic names from Kafka.

        Use the `topic_regex` and `excluded_topics` configuration settings to
        select topics from kafka.
        """
        logger.info("Discovering source topics...")
        bootstrap_servers = [config.broker.replace("kafka://", "")]
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers, enable_auto_commit=False
            )
            names: Set[str] = consumer.topics()
        except KafkaError as e:
            logger.error("Error retrieving topics from Kafka.")
            raise e

        if config.topic_regex:
            pattern = re.compile(config.topic_regex)
            names = {name for name in names if pattern.match(name)}

        if config.excluded_topics:
            excluded_topics = set(config.excluded_topics)
            names = names - excluded_topics

        n = len(names)
        s = ", ".join(sorted(names))
        logger.info(f"Found {n} source topic(s): {s}")

        return names


class AggregationTopic(Topic):
    """Represents aggregation topics.

    Sets the right Schema Registry URL for aggregation topics.

    Parameters
    ----------
    name: `str`
        Name of the aggregation topic in Kafka.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name=name, registry_url=config.internal_registry_url)
