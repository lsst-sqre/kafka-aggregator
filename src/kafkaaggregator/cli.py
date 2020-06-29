"""Command-line interface for kafkaaggregator."""

__all__ = ["main", "produce", "init_example"]

import logging

from faust.cli import AppCommand, option

from kafkaaggregator.app import app, config
from kafkaaggregator.example import (
    AggregationExample,
    UnexpectedNumberOfTopicsError,
)
from kafkaaggregator.generator import AgentGenerator

logger = logging.getLogger("kafkaaggregator")


def main() -> None:
    """Entrypoint for Faust CLI."""
    app.main()


@app.command(
    option(
        "--frequency",
        type=float,
        default=config.frequency,
        help="The frequency in Hz in wich messages are produced.",
        show_default=True,
    ),
    option(
        "--max-messages",
        type=int,
        default=config.max_messages,
        help="The maximum number of messages to produce.",
        show_default=True,
    ),
)
async def produce(
    self: AppCommand, frequency: float, max_messages: int
) -> None:
    """Produce messages for the aggregation example."""
    example = AggregationExample()

    try:
        await example.produce(
            app=app, frequency=frequency, max_messages=max_messages
        )
    except UnexpectedNumberOfTopicsError as e:
        logger.error(e)


@app.command()
async def init_example(self: AppCommand) -> None:
    """Initialize the source topic used in the aggregation example."""
    example = AggregationExample()
    await example.initialize(app=app)


@app.command()
async def generate_agents(self: AppCommand) -> None:
    """Generate Faust agents' code."""
    agent_generator = AgentGenerator()
    await agent_generator.run()
