"""Tests for the fields module."""
import pytest

from kafkaaggregator.fields import Field


def test_invalid_operation() -> None:
    """Test for invalid operation."""
    with pytest.raises(RuntimeError):
        Field(
            "field", int, source_field_name="source_field", operation="maximum"
        )
