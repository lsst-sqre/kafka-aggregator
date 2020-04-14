import pytest
from faust.windows import TumblingWindow

from kafkaaggregator.testtopic.agents import count, process_stream
from kafkaaggregator.testtopic.models import TestTopic


@pytest.mark.asyncio
async def test_tumbling_window_ranges():
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


@pytest.mark.asyncio
async def test_count_table(test_app):
    """Test if the count table is updated every time the agent processes a new
    message
    """
    async with process_stream.test_context() as agent:
        test_topic_1 = TestTopic(time=0.0, value=0.5)
        await agent.put(test_topic_1)
        assert count["test_topic"] == 1
        await agent.put(test_topic_1)
        assert count["test_topic"] == 2


@pytest.mark.asyncio
async def test_count_table_persistence(test_app):
    """Test if count table is persisted after restarting the app"""
    async with process_stream.test_context() as agent:
        test_topic_1 = TestTopic(time=0.0, value=0.5)
        await agent.put(test_topic_1)
        assert count["test_topic"] == 3
