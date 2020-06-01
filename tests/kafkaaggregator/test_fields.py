"""Tests for the fields module."""

from kafkaaggregator.fields import Field


def test_hash() -> None:
    """Test if Field is hashable.

    A Field must be hashable to be used with Faust.
    """
    assert hash(Field("field", int, metadata={"operation": "mean"}))
