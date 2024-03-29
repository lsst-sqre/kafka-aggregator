"""Tests for the aggregator module."""

from typing import List

import pytest

from kafkaaggregator.aggregator import Aggregator
from kafkaaggregator.fields import Field
from kafkaaggregator.models import create_record


@pytest.fixture
def source_topic_fields() -> List[Field]:
    """Mock source topic fields."""
    fields = [
        Field("time", int),
        Field("value", float),
        Field("excluded", int),
        Field("nonnumeric", bool),
        Field("inttype", int),
    ]
    return fields


@pytest.fixture
def excluded_field_names() -> List[str]:
    """Mock excluded field names."""
    return ["time", "excluded"]


@pytest.fixture
def operations() -> List[str]:
    """Mock list of opertions."""
    return ["min", "mean", "median", "stdev", "max"]


def test_aggregation_fields(
    source_topic_fields: List[Field],
    excluded_field_names: List[str],
    operations: List[str],
) -> None:
    """Test aggregation fields creation."""
    aggregation_fields = Aggregator._create_aggregated_fields(
        source_topic_fields, operations
    )
    # `time`, `count` and `window_size` are added by the aggregator
    assert Field("time", float) in aggregation_fields
    assert Field("count", int) in aggregation_fields
    assert Field("window_size", float) in aggregation_fields
    # if there's `time` field in the source topic it is replaced
    assert Field("time", int) not in aggregation_fields
    # summary statistic fields added based on the the `value` field
    assert Field("min_value", float) in aggregation_fields
    assert Field("mean_value", float) in aggregation_fields
    assert Field("median_value", float) in aggregation_fields
    assert Field("max_value", float) in aggregation_fields
    # field names added to the excluded_field_names list are not aggregated
    assert Field("excluded", float) not in aggregation_fields
    # non numeric fields are excluded
    assert Field("nonnumeric", bool) not in aggregation_fields
    assert Field("min_nonnumeric", float) not in aggregation_fields
    # int type is aggregated as float
    assert Field("min_inttype", float) in aggregation_fields


def test_record_class() -> None:
    """Test Faust Record creation."""
    # make a simple Faust Record
    Foo = create_record(
        cls_name="Foo",
        fields=[Field("bar", int)],
        doc="Test record",
    )
    f = Foo(bar=0)
    assert f.is_valid()
    assert f.asdict() == {"bar": 0}
