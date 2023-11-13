"""Test Faust tumbling window."""

import pytest
from faust.windows import TumblingWindow


@pytest.mark.asyncio
async def test_tumbling_window_ranges() -> None:
    """Test Faust tumbling window ranges."""
    size = 1.0
    window = TumblingWindow(size)
    # Test if a timestamp in the extremes of the window falls in the expected
    # window
    start = 0
    end = 0.9
    assert window.ranges(start) == [(start, end)]
    assert window.ranges(end) == [(start, end)]
    assert window.ranges(start + size) == [(start + size, end + size)]
    # Test if a timestamp between (start + end) and (start + size) falls
    # in the expected window
    assert window.ranges(start + end + 0.05) == [(start, end)]
