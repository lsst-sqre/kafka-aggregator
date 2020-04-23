"""Initialize the kafkaaggregator source topic.

In the aggregation example, the source topic is not managed by
Faust. It is an external topic in the sense that there's no model for it
in Faut and that agents assume it already exists.

We use the `on_worker_init` signal to initialize the source topic in kafka and
register the schema with the Schema Registry before the Faust workers start.
"""

__all__ = ["init_external_topic", "on_worker_init"]

import asyncio
import json
import logging
from typing import Any

import faust_avro
from aiohttp.client_exceptions import ClientConnectorError
from faust_avro.asyncio import (
    ConfluentSchemaRegistryClient,
    SchemaNotFound,
    SubjectNotFound,
)

from kafkaaggregator.app import app, config

SchemaT = str

logger = logging.getLogger("kafkaaggregator")

SCHEMA = json.dumps(
    dict(
        type="record",
        name="SourceTopic",
        doc="Source topic with raw values.",
        fields=[
            dict(name="time", type="double"),
            dict(name="value", type="double"),
        ],
    )
)


async def init_external_topic(topic: str, schema: SchemaT) -> None:
    """Initialize a topic not managed by Faust.

    Create the topic on kafka and register the schema under the corresponding
    subject on the Schema Registry.

    Parameters
    ----------
        topic: `str`
            Name of the topic.
        schema: `str`
            Avro schema.
    """
    subject = f"{topic}-value"
    client = ConfluentSchemaRegistryClient()
    schema_id = None
    try:
        schema_id = await client.sync(subject, schema)
    except SchemaNotFound:
        logger.info(f"Schema for subject {subject} not found.")
    except SubjectNotFound:
        logger.info(f"Subject {subject} not found.")
    except ClientConnectorError:
        logger.error(
            f"Could not connect to Schema Registry at {config.registry_url}."
        )

    if not schema_id:
        logger.info(f"Register schema for subject {subject}.")
        await client.register(subject, schema)

    # An external topic does not have a corresponding model in Faust,
    # thus value_type=bytes.
    external_topic = app.topic(topic, value_type=bytes, internal=False)
    await external_topic.declare()


@app.on_worker_init.connect
def on_worker_init(app: faust_avro.App, **kwargs: Any) -> None:
    """Ensure the topic is initialized before the Faust workers start.
    """

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        init_external_topic(topic=config.src_topic, schema=SCHEMA)
    )
