"""Generates Faust agents based on the agent.j2 template."""

__all__ = ["AgentGenerator"]

import logging
import os
import re
from typing import Any, Mapping, Set

import aiofiles
from jinja2 import Environment, PackageLoader, Template, TemplateError
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from kafkaaggregator.app import config

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
        self._source_topic_names = self._get_source_topic_names()
        self._template: Template = self._load_template()

    @property
    def template(self) -> Template:
        """Get the agent template."""
        return self._template

    @staticmethod
    def _get_source_topic_names() -> Set[str]:
        """Get a set of source topics to aggregate from Kafka.

        Retrieve source topics based on the topic_regex and the excluded_topics
        configuration parameters.

        Returns
        -------
        source_topic_names : `set`
            Set of source topics to aggregate.
        """
        logger.info("Discovering source topics...")
        bootstrap_servers = [config.broker.replace("kafka://", "")]
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers, enable_auto_commit=False
            )
            source_topic_names: Set[str] = consumer.topics()
        except KafkaError as e:
            logger.error("Error retrieving topics from Kafka.")
            raise e

        if config.topic_regex:
            pattern = re.compile(config.topic_regex)
            source_topic_names = {
                name for name in source_topic_names if pattern.match(name)
            }

        if config.excluded_topics:
            excluded_topics = set(config.excluded_topics)
            source_topic_names = source_topic_names - excluded_topics

        n = len(source_topic_names)
        s = ", ".join(source_topic_names)
        logger.info(f"Found {n} topic(s): {s}")

        return source_topic_names

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
