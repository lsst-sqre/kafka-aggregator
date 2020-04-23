"""Command-line interface for the aggregation example."""

__all__ = ["produce"]

import asyncio
import logging
import random
from time import time

from faust.cli import AppCommand, option

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
    """Produce messages for the kafkaaggregator source topic
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
