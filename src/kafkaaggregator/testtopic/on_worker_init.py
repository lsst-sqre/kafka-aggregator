"""Initialize the test topic for the aggregation example.

In this example the test topic is not managed by Faust. We use the
`on_worker_init` signal to create the test topic on kafka and register the
schema with the Schema Registry before a Faust worker start.
"""

__all__ = ["init_external_topic", "on_worker_init"]

import asyncio
import json
import logging

import faust_avro
from aiohttp.client_exceptions import ClientConnectorError
from faust_avro.asyncio import (
    ConfluentSchemaRegistryClient,
    SchemaNotFound,
    SubjectNotFound,
)

from kafkaaggregator.app import app, config

Schema = str

logger = logging.getLogger("kafkaaggregator")

TOPIC = "test-topic"

SCHEMA = json.dumps(
    dict(
        type="record",
        name="TestTopic",
        doc="Test topic with raw values.",
        fields=[
            dict(name="time", type="double"),
            dict(name="value", type="double"),
        ],
    )
)


async def init_external_topic(topic: str, schema: Schema) -> None:
    """Initialize a topic not managed by Faust.

    Create the topic on kafka and register the schema under the corresponding
    subject on the Schema Registry.

    Parameters
    ==========
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
    test_topic = app.topic(topic, value_type=bytes, internal=False)
    await test_topic.declare()


@app.on_worker_init.connect
def on_worker_init(app: faust_avro.App, **kwargs: int) -> None:
    """Ensure the topic is initialized when the worker starts.
    """

    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_external_topic(topic=TOPIC, schema=SCHEMA))
