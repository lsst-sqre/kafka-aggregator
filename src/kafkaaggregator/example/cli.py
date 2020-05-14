"""Command-line interface for the aggregation example."""

__all__ = ["produce", "init_example"]

import asyncio
import json
import logging
import random
from time import time

from aiohttp.client_exceptions import ClientConnectorError
from faust.cli import AppCommand, option
from faust_avro.asyncio import (
    ConfluentSchemaRegistryClient,
    SchemaNotFound,
    SubjectNotFound,
)

from kafkaaggregator.app import app, config

logger = logging.getLogger("kafkaaggregator")


@app.command(
    option(
        "--frequency",
        type=float,
        default=10,
        help="The frequency in Hz in wich messages are produced.",
        show_default=True,
    ),
    option(
        "--max-messages",
        type=int,
        default=600,
        help="The maximum number of messages to produce.",
        show_default=True,
    ),
)
async def produce(
    self: AppCommand, frequency: float, max_messages: int
) -> None:
    """Produce messages for the kafkaaggregator source topic.

    Parameters
    ----------
    frequency: `float`
        The frequency in Hz in wich messages are produced
    max_messages: `int`
        The maximum number of messages to produce.
    """
    # Assume the source topic exists
    src_topic = app.topic(config.src_topic)

    logger.info(
        f"Producing {max_messages} message(s) for {config.src_topic} at "
        f"{frequency} Hz."
    )

    for i in range(max_messages):
        value = random.random()
        await src_topic.send(value=dict(time=time(), value=value))
        await asyncio.sleep(1 / frequency)


@app.command()
async def init_example(self: AppCommand) -> None:
    """Initialize the source topic used in the aggregation example.

    The source topic has a fixed schema with a timestamp and a value. It is
    used produce messages for the aggregation example. This topic is not
    managed by Faust, it represents an external topic and thus needs to be
    created in Kafka and its schema needs to registered in the Schema Registry.
    """
    topic = config.src_topic

    subject = f"{topic}-value"

    schema = json.dumps(
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

    client = ConfluentSchemaRegistryClient(url=config.registry_url)

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
