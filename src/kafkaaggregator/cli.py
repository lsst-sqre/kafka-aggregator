"""Command-line interface for kafkaaggregator."""

__all__ = ["main", "produce", "init_example"]

import logging
from pathlib import Path

from faust.cli import AppCommand, option

from kafkaaggregator.app import app
from kafkaaggregator.config import Configuration, ExampleConfiguration
from kafkaaggregator.example.example import AggregationExample
from kafkaaggregator.generator import AgentGenerator

logger = logging.getLogger("kafkaaggregator")

config = Configuration()
example_config = ExampleConfiguration()


def main() -> None:
    """Entrypoint for Faust CLI."""
    app.main()


@app.command(
    option(
        "--frequency",
        type=float,
        default=example_config.frequency,
        help="The frequency in Hz in wich messages are produced.",
        show_default=True,
    ),
    option(
        "--max-messages",
        type=int,
        default=example_config.max_messages,
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
    except Exception as e:
        logger.error(e)


@app.command()
async def init_example(self: AppCommand) -> None:
    """Initialize the source topic used in the aggregation example."""
    example = AggregationExample()
    await example.initialize(app=app)


@app.command(
    option(
        "--config-file",
        type=str,
        default=config.aggregator_config_file,
        help="Aggregator configuration file.",
        show_default=True,
    ),
    option(
        "--aggregated-topic",
        type=str,
        help=(
            "The aggregated topic to generate the agent for. If not specified "
            "generate agents for all aggregated topics in the configuration."
        ),
    ),
)
async def generate_agents(
    self: AppCommand, config_file: str, aggregated_topic: str
) -> None:
    """Generate Faust agents' code."""
    agent_generator = AgentGenerator(Path(config_file), aggregated_topic)
    await agent_generator.run()
