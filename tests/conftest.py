import faust_avro
import pytest
from yarl import URL

from kafkaaggregator.app import create_app


@pytest.fixture()
def test_app() -> faust_avro.App:
    """Creates test app """
    app = create_app()
    # Ensure memory store is used for tests
    app.finalize()
    app.conf.store = URL("memory://")
    app.flow_control.resume()
    return app
