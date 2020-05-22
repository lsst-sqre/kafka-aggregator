"""Command-line interface for the aggregation example."""

__all__ = ["produce", "init_example"]

import asyncio
import json
import logging
import random
from time import time

from faust.cli import AppCommand, option

from kafkaaggregator.app import app, config
from kafkaaggregator.topics import SourceTopic

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
    """Produce messages for the kafkaaggregator source topic

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
    src_topic = SourceTopic(topic=config.src_topic)

    subject = f"{src_topic.topic}-value"
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

    # Register source topic schema
    await src_topic.register(subject=subject, schema=schema)

    # Declare the source topic as an external topic in Faust. External topics
    # do not have a corresponding model in Faust, thus value_type=bytes.
    external_topic = app.topic(
        src_topic.topic, value_type=bytes, internal=False
    )
    await external_topic.declare()
