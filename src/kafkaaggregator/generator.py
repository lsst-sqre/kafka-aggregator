"""Generates Faust agents based on the agent.j2 template."""

__all__ = ["AgentGenerator"]

import logging
import os
from typing import Any, Mapping

import aiofiles
from jinja2 import Environment, PackageLoader, Template, TemplateError

from kafkaaggregator.app import config
from kafkaaggregator.topics import SourceTopic

logger = logging.getLogger("kafkaaggregator")


class AgentGenerator:
    """Generate Faust agents from a list of source topics.

    Creates the context and renders the agents code template.

    Parameters
    ----------
    source_topic_names : `list`
        List of source topic names.
    """

    logger = logger

    def __init__(self) -> None:
        self._source_topic_names = SourceTopic.names()
        self._template: Template = self._load_template()

    @property
    def template(self) -> Template:
        """Get the agent template."""
        return self._template

    @staticmethod
    def _create_filepath(source_topic_name: str) -> str:
        """Return the file path for the agent.

        The directory name comes from the agents_output_dir configuration
        parameter and the file name is based on source topic name.

        Parameters
        ----------
        source_topic_name : `str`
            Name of the source topic to aggregate.
        """
        agents_output_dir = config.agents_output_dir

        filepath = os.path.join(agents_output_dir, f"{source_topic_name}.py")

        return filepath

    @staticmethod
    def _create_context(source_topic_name: str) -> Mapping[str, Any]:
        """Create the template context.

        The template context stores the values passed to the template.

        Parameters
        ----------
        source_topic_name : `str`
            Name of the source topic to aggregate

        Returns
        -------
        context : `dict`
            A dictionary with values passed to the template.
        """
        topic_rename_format = config.topic_rename_format

        aggregation_topic_name = topic_rename_format.format(
            source_topic_name=source_topic_name
        )

        context = dict(
            source_topic_name=source_topic_name,
            aggregation_topic_name=aggregation_topic_name,
        )

        return context

    @staticmethod
    def _load_template() -> Template:
        """Load the agent template file."""
        agent_template_file = config.agent_template_file

        env = Environment(
            loader=PackageLoader("kafkaaggregator"), keep_trailing_newline=True
        )
        try:
            template = env.get_template(agent_template_file)
        except TemplateError as e:
            logger.error("Error loading the agent template file.")
            raise e

        return template

    async def run(self) -> None:
        """Run agents code generation."""
        for source_topic_name in self._source_topic_names:
            logger.info(f"Generating agent code for {source_topic_name}.")
            filepath = self._create_filepath(source_topic_name)
            context = self._create_context(source_topic_name)

            async with aiofiles.open(filepath, "w") as file:
                await file.write(self._template.render(**context))
