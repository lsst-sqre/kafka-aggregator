import pytest
from faust import Record
from faust.windows import TumblingWindow

from kafkaaggregator.config import Configuration
from kafkaaggregator.example.agents import count, process_src_topic

config = Configuration()


class TestTopic(Record):

    __test__ = False
    time: float
    value: float


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
    message and if counts are persisted.
    """
    test_topic = TestTopic(time=0.0, value=0.5)
    async with process_src_topic.test_context() as agent:
        await agent.put(test_topic)
        assert count[config.src_topic] == 1

    async with process_src_topic.test_context() as agent:
        await agent.put(test_topic)
        assert count[config.src_topic] == 2
