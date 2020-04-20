import faust_avro


class TestTopic(faust_avro.Record):
    """Test topic with raw values."""

    __test__ = False
    time: float
    value: float


class AggTestTopic(faust_avro.Record):
    """Test topic with aggregated values."""

    time: float
    count: int
    min: float
    mean: float
    max: float
