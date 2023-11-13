"""Generates Faust agents (stream processors) using the agent.j2 template."""

__all__ = ["AgentGenerator"]

import logging
import os
from pathlib import Path
from typing import Any, Mapping

import aiofiles
from jinja2 import Environment, PackageLoader, Template, TemplateError

logger = logging.getLogger("kafkaaggregator")


class AgentGenerator:
    """Generate a Faust agent for an aggregated topic.

    Creates the context and renders the agents code template.

    Parameters
    ----------
    config_file : `Path`
        Aggregator configuration file.
    aggregated_topic: str
        Name of the aggregated topic.
    template_file: str
        Name of the agent Jinja2 template file.
    output_dir: str
        Name of output directory for the agents' code.
    """

    logger = logger

    def __init__(
        self,
        config_file: Path,
        aggregated_topic: str,
        template_file: str,
        output_dir: str,
    ) -> None:

        self._config_file = config_file
        self._aggregated_topic = aggregated_topic
        self._template_file = template_file
        self._output_dir = output_dir

    def _create_context(self) -> Mapping[str, Any]:
        """Create the template context.

        The template context stores the values passed to the template.

        Returns
        -------
        context : `dict`
            A dictionary with values passed to the template.
        """
        cls_name = self._aggregated_topic.title().replace("-", "")

        context = dict(
            aggregated_topic=self._aggregated_topic,
            config_file=self._config_file,
            cls_name=cls_name,
        )

        return context

    def _load_template(self) -> Template:
        """Load the agent template file."""
        env = Environment(
            loader=PackageLoader("kafkaaggregator"), keep_trailing_newline=True
        )
        try:
            template = env.get_template(self._template_file)
        except TemplateError as e:
            logger.error("Error loading the agent template file.")
            raise e
        return template

    async def run(self) -> None:
        """Run agents code generation."""
        logger.info(f"Generating agent code for {self._aggregated_topic}.")

        template = self._load_template()
        context = self._create_context()

        output_file = os.path.join(
            self._output_dir, f"{self._aggregated_topic}.py"
        )

        async with aiofiles.open(output_file, "w") as f:
            await f.write(template.render(**context))
