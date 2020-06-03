"""Test the Kafka-aggregator method for computing summary statistics."""

from typing import Any, List, Mapping

import pytest

from kafkaaggregator.aggregator import Aggregator
from kafkaaggregator.fields import Field, Operation
from kafkaaggregator.models import make_record


@pytest.fixture
def incoming_messages():
    """Mock incoming messages."""
    messages = [
        {"time": 0, "value": 1.0},
        {"time": 1, "value": 2.0},
        {"time": 2, "value": 3.0},
    ]
    return messages


@pytest.fixture
def aggregation_fields():
    """Mock aggregation fields."""
    fields = [
        Field("time", int),
        Field("count", int),
        Field("window_size", float),
        Field("min_value", float, "value", Operation.MIN),
        Field("mean_value", float, "value", Operation.MEAN),
        Field("median_value", float, "value", Operation.MEDIAN),
        Field("stdev_value", float, "value", Operation.STDEV,),
        Field("max_value", float, "value", Operation.MAX),
    ]
    return fields


@pytest.fixture
def expected_result():
    """Return test expected result."""
    result = {
        "count": 3,
        "min_value": 1.0,
        "time": 1.0,
        "window_size": 1.0,
        "max_value": 3.0,
        "mean_value": 2.0,
        "median_value": 2.0,
        "stdev_value": 1.0,
    }
    return result


def test_compute(
    incoming_messages: List[Any],
    aggregation_fields: List[Field],
    expected_result: Mapping[str, Any],
) -> None:
    """Test the Aggregator compute method.

    Parameters
    ----------
    incoming_messages: `list`
        Mock list of incoming messages
    aggregation_fields:  `list` [`Field`]
        List of fields to aggregate.
    expected_result: `dict`
        Dictionary with the expected result for the aggregated_message
    """
    Agg = Aggregator(
        source_topic_name="test-source-topic",
        aggregation_topic_name="test-aggregation-topic",
        # If these fields are present in the incoming message they are excluded
        # as they are used by the aggregator
        excluded_field_names="time, count, window_size",
    )

    # Mock the creation of the aggregation fields
    Agg._aggregation_fields = aggregation_fields
    # Mock the creation of the Faust Record for the aggregation topic
    Agg._record = make_record(
        cls_name="AggregationRecord",
        fields=aggregation_fields,
        doc="Faust record for topic test-source-topic.",
    )
    aggregated_message = Agg.compute(
        time=1.0, window_size=1.0, messages=incoming_messages
    )
    assert aggregated_message.is_valid()
    assert aggregated_message.asdict() == expected_result
