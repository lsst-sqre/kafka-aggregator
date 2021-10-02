"""Test the Kafka-aggregator method for computing summary statistics."""

from pathlib import Path
from typing import Any, List, Mapping

import pytest

from kafkaaggregator.aggregator import Aggregator
from kafkaaggregator.aggregator_config import AggregatorConfig
from kafkaaggregator.fields import Field
from kafkaaggregator.models import create_record


@pytest.fixture
def incoming_messages() -> List[Any]:
    """Mock incoming messages."""
    messages = [
        {"time": 0, "value": 1.0},
        {"time": 1, "value": 2.0},
        {"time": 2, "value": 3.0},
    ]
    return messages


@pytest.fixture
def aggregated_fields() -> List[Field]:
    """Mock aggregation fields."""
    fields = [
        Field("time", int),
        Field("count", int),
        Field("window_size", float),
        Field("min_value", float, "value", "min"),
        Field("mean_value", float, "value", "mean"),
        Field("median_value", float, "value", "median"),
        Field("stdev_value", float, "value", "stdev"),
        Field("max_value", float, "value", "max"),
    ]
    return fields


@pytest.fixture
def expected_result() -> Mapping[str, Any]:
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


@pytest.fixture
def first_message_value(incoming_messages: List[Any]) -> Mapping[str, Any]:
    """Return the value of the first message instead of computing statistics.

    That's the expected result if the number of messages in the aggregation
    window is smaller than the min_sample_size.
    """
    result = {
        "count": 3,
        "min_value": incoming_messages[0]["value"],
        "time": 1.0,  # timestamp of the aggregated message
        "window_size": 1.0,
        "max_value": incoming_messages[0]["value"],
        "mean_value": incoming_messages[0]["value"],
        "median_value": incoming_messages[0]["value"],
        "stdev_value": incoming_messages[0]["value"],
    }
    return result


def test_compute(
    config_dir: Path,
    incoming_messages: List[Any],
    aggregated_fields: List[Field],
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
    config_file = config_dir.joinpath("aggregator_config.yaml")

    aggregated_topic = AggregatorConfig(config_file).get("aggregated_example0")

    Agg = Aggregator(aggregated_topic)

    # Mock the creation of the aggregated fields
    Agg._aggregated_fields = aggregated_fields

    # Mock the creation of the Faust Record for the aggregation topic
    Agg._record = create_record(
        cls_name="AggregationRecord",
        fields=aggregated_fields,
        doc="Faust record for topic test-source-topic.",
    )
    aggregated_message = Agg.compute(
        time=1.0,
        messages=incoming_messages,
    )
    assert aggregated_message.is_valid()
    assert aggregated_message.asdict() == expected_result


def test_compute_min_sample_size(
    config_dir: Path,
    incoming_messages: List[Any],
    aggregated_fields: List[Field],
    first_message_value: Mapping[str, Any],
) -> None:
    """Test the Aggregator compute method.

    Test the case where the min_sample_size is larger than the number
    of messages in the aggregation window.

    Parameters
    ----------
    incoming_messages: `list`
        Mock list of incoming messages
    aggregation_fields:  `list` [`Field`]
        List of fields to aggregate.
    expected_result: `dict`
        Dictionary with the expected result for the aggregated_message
    """
    config_file = config_dir.joinpath("aggregator_config_min_sample_size.yaml")

    aggregated_topic = AggregatorConfig(config_file).get("aggregated_example0")

    Agg = Aggregator(aggregated_topic)

    # Mock the creation of the aggregated fields
    Agg._aggregated_fields = aggregated_fields

    # Mock the creation of the Faust Record for the aggregation topic
    Agg._record = create_record(
        cls_name="AggregationRecord",
        fields=aggregated_fields,
        doc="Faust record for topic test-source-topic.",
    )

    aggregated_message = Agg.compute(
        time=1.0,
        messages=incoming_messages,
    )
    assert aggregated_message.asdict() == first_message_value
