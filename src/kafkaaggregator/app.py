"""The main application definition for kafkaaggregator."""

__all__ = ["create_app"]

import faust_avro

from kafkaaggregator.config import Configuration


def create_app(config: Configuration = None) -> faust_avro.App:
    """Create and configure the Faust application.

    Parameters
    ----------
    config : `Configuration`, optional
        The configuration to use.  If not provided, the default Configuration
        will be used.
    """

    if not config:
        config = Configuration()

    app = faust_avro.App(
        id="kafkaaggregator",
        broker=config.broker,
        registry_url=config.registry_url,
        store=config.store,
        autodiscover=True,
        origin="kafkaaggregator",
        topic_partitions=config.topic_partitions,
    )

    return app


# The default configuration can also be imported from this module
config = Configuration()

app: faust_avro.App = create_app(config)
