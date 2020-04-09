from datetime import datetime

import faust


class TestTopic(faust.Record, serializer="json"):
    """Test topic with raw values."""

    time: datetime
    value: float
