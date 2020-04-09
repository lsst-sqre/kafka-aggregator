"""The main application definition for kafkaaggregator."""

__all__ = ["create_app"]

import faust

from kafkaaggregator.config import Configuration


def create_app() -> faust.App:
    """Create and configure the Faust application."""
    config = Configuration()

    app = faust.App(
        id="kafkaaggregator",
        broker=config.broker,
        store=config.store,
        autodiscover=True,
        origin="kafkaaggregator",
        topic_partitions=config.topic_partitions,
    )

    return app


app: faust.App = create_app()
