"""Models for the internal topics managed by Faust."""

import faust_avro


class AggTestTopic(faust_avro.Record):
    """Test topic with aggregated values."""

    time: float
    count: int
    min: float
    mean: float
    max: float
