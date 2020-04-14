import faust


class TestTopic(faust.Record, serializer="json"):
    """Test topic with raw values."""

    __test__ = False
    time: float
    value: float


class AggTestTopic(faust.Record, serializer="json"):
    """Test topic with aggregated values."""

    time: float
    count: int
    min: float
    mean: float
    max: float
