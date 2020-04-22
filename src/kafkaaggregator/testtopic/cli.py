"""Command-line interface for the test topic aggregation example."""

__all__ = ["produce"]

import asyncio
import logging
import random
from time import time

from faust.cli import AppCommand, option

from kafkaaggregator.app import app

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
    """Produce messages for the kafkaaggregator test-topic
    """

    # Assume the test topic exists
    test_topic = app.topic("test-topic")

    logger.info(
        f"Producing {max_messages} message(s) for test-topic at "
        f"{frequency} Hz."
    )

    for i in range(max_messages):
        value = random.random()
        await test_topic.send(value=dict(time=time(), value=value))
        await asyncio.sleep(1 / frequency)
