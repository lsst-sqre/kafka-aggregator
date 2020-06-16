"""Command-line interface for kafkaaggregator."""

__all__ = ["main", "produce", "init_example"]

import asyncio
import json
import logging
import random
from time import time

from faust.cli import AppCommand, option

from kafkaaggregator.app import app, config
from kafkaaggregator.generator import AgentGenerator
from kafkaaggregator.topics import SourceTopic

logger = logging.getLogger("kafkaaggregator")


def main() -> None:
    """Entrypoint for Faust CLI."""
    app.main()


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
    src_topic = app.topic(config.source_topic_name)

    logger.info(
        f"Producing {max_messages} message(s) for {config.source_topic_name} "
        f"at {frequency} Hz."
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
    source_topic = SourceTopic(name=config.source_topic_name)

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
    await source_topic.register(schema=schema)

    # Declare the source topic as an external topic in Faust. External topics
    # do not have a corresponding model in Faust, thus value_type=bytes.
    external_topic = app.topic(
        source_topic.name, value_type=bytes, internal=False
    )
    await external_topic.declare()

    # Generate Faust agent for the aggregation example
    agent_generator = AgentGenerator(
        source_topic_names=[config.source_topic_name]
    )
    await agent_generator.run()
