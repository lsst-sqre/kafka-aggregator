"""Implements TopicSchema, SourceTopicSchema and AggregationTopicSchema.

The TopicSchema class has methods to retrieve the topic schema from the Schema
Registry and a parsed list of fields from the Avro schema with Python types.

The child classes SourceTopic and AggregationTopic set the right Schema
Registry URL to be used with each topic type.
"""
__all__ = [
    "SchemaException",
    "TopicSchema",
    "SourceTopicSchema",
    "AggregatedTopicSchema",
]

import json
import logging
from typing import List, Union

from faust_avro.asyncio import ConfluentSchemaRegistryClient

from kafkaaggregator.app import config
from kafkaaggregator.fields import Field

logger = logging.getLogger("kafkaaggregator")

AvroSchemaT = str


class SchemaException(Exception):
    """A generic schema registry client exception."""


class TopicSchema:
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

    def __init__(
        self, name: str, registry_url: str = config.registry_url
    ) -> None:

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

    async def get_fields(self, filter: List[str]) -> List[Field]:
        """Get topic fields.

        Parses the topic Avro schema and returns a list of fields with
        Python types.

        Parameters
        ----------
        filter : `list` [`str`]
            List of fields names to return.

        Returns
        -------
        fields : `list` [`Field`]
            List of field names and Python types.
        """
        schema = await self.get_schema()
        fields = []
        if schema:
            # The faust-avro parser expects a json-parsed avro schema
            # https://github.com/masterysystems/faust-avro/blob/master/faust_avro/parsers/avro.py#L20
            parsed_schema = self._parse(json.loads(schema))
            for field in parsed_schema.fields:
                if field.name in filter:
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


class SourceTopicSchema(TopicSchema):
    """Represents source topic schema.

    Sets the right Schema Registry URL for source topics.

    Parameters
    ----------
    name: `str`
        Name of the source topic in Kafka.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name=name, registry_url=config.registry_url)


class AggregatedTopicSchema(TopicSchema):
    """Represents aggregated topic schema.

    Sets the right Schema Registry URL for aggregated topics.

    Parameters
    ----------
    name: `str`
        Name of the aggregated topic in Kafka.
    """

    def __init__(self, name: str) -> None:
        super().__init__(name=name, registry_url=config.internal_registry_url)
