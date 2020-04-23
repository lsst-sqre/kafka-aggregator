"""Models for the internal topics managed by Faust."""

import faust_avro


class AggTopic(faust_avro.Record):
    """Topic with aggregated values."""

    time: float
    count: int
    min: float
    mean: float
    max: float
