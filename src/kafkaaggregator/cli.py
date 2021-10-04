"""Command-line interface for kafkaaggregator."""

__all__ = ["main", "produce", "init_example", "generate_agents"]

import logging
from pathlib import Path

from faust.cli import AppCommand, option

from kafkaaggregator.aggregator_config import AggregatorConfig
from kafkaaggregator.app import app, config
from kafkaaggregator.config import ExampleConfiguration
from kafkaaggregator.example.example import AggregationExample
from kafkaaggregator.generator import AgentGenerator

logger = logging.getLogger("kafkaaggregator")

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
        "--template-file",
        type=str,
        default=config.agent_template_file,
        help="Name of the agent Jinja2 template file.",
        show_default=True,
    ),
    option(
        "--output-dir",
        type=str,
        default=config.agents_output_dir,
        help="Name of output directory for the agents' code.",
        show_default=True,
    ),
    option(
        "--config-file",
        type=str,
        default=config.aggregator_config_file,
        help="Aggregator configuration file.",
        show_default=True,
    ),
)
async def generate_agents(
    self: AppCommand,
    config_file: str,
    template_file: str,
    output_dir: str,
) -> None:
    """Generate Faust agents' code."""
    config_file_path = Path(__file__).parent.joinpath(config_file)
    aggregator_config = AggregatorConfig(config_file_path)

    for aggregated_topic in aggregator_config.aggregated_topics:

        agent_generator = AgentGenerator(
            config_file_path, aggregated_topic, template_file, output_dir
        )
        await agent_generator.run()
