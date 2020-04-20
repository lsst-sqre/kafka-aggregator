"""The main application definition for kafkaaggregator."""

__all__ = ["create_app"]

import faust

from kafkaaggregator.config import Configuration


def create_app(config: Configuration = None) -> faust.App:
    """Create and configure the Faust application.

    Parameters
    ----------
    config : `Configuration`, optional
        The configuration to use.  If not provided, the default Configuration
        will be used.
    """

    if not config:
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


# The default configuration can also be imported from this module
config = Configuration()

app: faust.App = create_app(config)
