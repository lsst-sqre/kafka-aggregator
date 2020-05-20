__all__ = ["SchemaException", "Topic", "SourceTopic", "AggregationTopic"]

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


class Topic:
    """
    Topic schema and interaction with the Schema Registry.

    Parameters
    ----------
    topic : `str`
        Name of a kafka topic.
    registry_url : `str`
        Schema Registry URL.
    """

    logger = logger

    def __init__(self, topic: str, registry_url: str) -> None:

        self.topic = topic
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
        subject = f"{self.topic}-value"
        schema = None
        try:
            schema = await self._client.schema_by_topic(subject)
        except Exception:
            msg = f"Could not retrieve schema for subject {subject}."
            raise SchemaException(msg)

        return schema

    async def get_fields(self) -> List[Field]:
        """Parses the topic Avro schema and returns a list of fields with
        faust-avro types.

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

    async def register(
        self, subject: str, schema: AvroSchemaT
    ) -> Union[int, None]:
        """Register an Avro schema with the Schema Registry. If the schema is
        already register for this subject it does nothing.

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
        logger.info(f"Register schema for subject {subject}.")

        is_registered = False
        try:
            is_registered = await self._client.is_registered(subject, schema)
        except Exception:
            msg = f"Could not connect to Schema Registry."
            raise SchemaException(msg)

        schema_id = None
        if not is_registered:
            try:
                schema_id = await self._client.register(subject, schema)
            except Exception:
                msg = f"Could not register schema for subject {subject}."
                raise SchemaException(msg)
        return schema_id


class SourceTopic(Topic):
    """This child class sets the right Schema Registry URL for source topics.

    Parameters
    ----------
    topic: `str`
        Name of the source topic in Kafka.
    """

    def __init__(self, topic: str) -> None:
        super().__init__(topic, registry_url=config.registry_url)


class AggregationTopic(Topic):
    """This child class sets the right Schema Registry URL for the aggreation
    topics.

    Parameters
    ----------
    topic: `str`
        Name of the aggregation topic in Kafka.
    """

    def __init__(self, topic: str) -> None:
        super().__init__(
            topic=topic, registry_url=config.internal_registry_url
        )
