"""Configure a kafka-aggregator test application."""

from pathlib import Path

import faust_avro
import pytest
from yarl import URL

from kafkaaggregator.app import create_app


@pytest.fixture()
def test_app() -> faust_avro.App:
    """Creates test app.

    Returns
    -------
    app : `Faust_avro.App`
        Faust Avro application.
    """
    app = create_app()
    # Ensure memory store is used for tests
    app.finalize()
    app.conf.store = URL("memory://")
    app.flow_control.resume()
    return app


@pytest.fixture
def config_dir() -> Path:
    """Directory containing test configuration data."""
    return Path(__file__).parent.joinpath("data/config")
