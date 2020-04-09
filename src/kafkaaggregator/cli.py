"""Administrative command-line interface."""

__all__ = ["main", "produce"]

import asyncio
import random
from datetime import datetime

from faust.cli import AppCommand, option

from kafkaaggregator.app import app
from kafkaaggregator.models import TestTopic


def main() -> None:
    """kafkaaggregator

    Administrative command-line interface for kafkaaggregator.
    """
    app.main()


@app.command(
    option(
        "--frequency",
        type=float,
        default=1,
        help="The frequency in Hz in wich messages are produced.",
        show_default=True,
    ),
    option(
        "--max-messages",
        type=int,
        default=100,
        help="The maximum number of messages to produce.",
        show_default=True,
    ),
)
async def produce(
    self: AppCommand, frequency: float, max_messages: int
) -> None:
    """Produce messages for the kafkaaggregator test-topic
    """
    test_topic = app.topic("test-topic", value_type=TestTopic)

    for i in range(max_messages):
        value = random.random()
        await test_topic.send(
            value=TestTopic(time=datetime.now(), value=value)
        )
        await asyncio.sleep(1 / frequency)
