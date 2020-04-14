"""Faust agent for the test topic aggregation example.

The Faust agent persists the messages for the test topic in tumbling windows.
See https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing

The window_size parameter controls the number of messages to aggregate,
if the test topic is produced at a constant frequency, the number of messages
aggregated in each window is n = window_size * frequency. The window range is
relative to the time field in the test topic.

The window_expires parameter controls when the callback function to
process the expired window(s) is called. When a window is processed, a new
message for the aggregated test topic is produced with the aggregated results.
"""

__all__ = ["aggregate", "process_window", "process_stream"]


import logging
from statistics import mean
from typing import AsyncGenerator, List, Tuple

from faust import web
from faust.types import StreamT

from kafkaaggregator.app import app
from kafkaaggregator.config import Configuration
from kafkaaggregator.testtopic.models import AggTestTopic, TestTopic

logger = logging.getLogger("kafkaaggregator")

config = Configuration()

test_topic = app.topic("test-topic", value_type=TestTopic, internal=True)

aggregated_test_topic = app.topic(
    "agg-test-topic", value_type=AggTestTopic, internal=True
)


def aggregate(timestamp: float, messages: List[TestTopic]) -> AggTestTopic:
    """Return an aggregated message from a list of messages for the test
    topic and a timestamp.

    Parameters
    ----------

    timestamp: `float`
        A timestamp for the aggregated message, for example, the mid point of
        the window that contains the messages.
    messages: `List[TestTopic]`
        List of test topic messages to aggregate.

    Returns
    -------
    aggregated_message: `AggTestTopic`
        Aggregated message.
    """
    count = len(messages)

    values = [message.value for message in messages]
    minimum = min(values)
    average = mean(values)
    maximum = max(values)

    aggregated_message = AggTestTopic(
        time=timestamp, count=count, min=minimum, mean=average, max=maximum
    )

    return aggregated_message


def process_window(key: Tuple, value: List[TestTopic]) -> None:
    """Process a window and send an aggregated message.

    Parameters
    ----------

    key: `Tuple`
        key for the current window in the WindowSet associated to
        ``Table[k]``. The key contains the window range, for example,
        key = (k, (start, end))

    value: `List[TestTopic]`
        List of messages in the current window.
    """

    start, end = key[1]

    # Faust defines the window range as (start,  start + size - 0.1)
    # https://github.com/robinhood/faust/blob/master/faust/types/windows.py#L16
    # To compute the midpoint of the window we have to correct the window range
    # by 0.1. Note that, despite of this definition, messages with timestamps
    # between (start + size - 0.1) and (start + size) are correctly added to
    # the current window.
    window_timestamp = (start + end + 0.1) / 2

    aggregated_message = aggregate(window_timestamp, value)

    aggregated_test_topic.send_soon(value=aggregated_message)

    logger.info(
        f"{aggregated_message.count:5d} messages aggregated on window "
        f"({start}, {end + 0.1})."
    )


# Tumbling window to persist test topic messages, the process_window
# callback is called when the window expires. Window range is relative to the
# time field in the messages.
table = (
    app.Table(
        "tumbling-window",
        default=list,
        on_window_close=process_window,
        help=f"Persit messages from test-topic in windows of "
        f"{config.window_size}s.",
    )
    .tumbling(config.window_size, expires=config.window_expires)
    .relative_to_field(TestTopic.time)
)

count = app.Table(
    "count",
    default=int,
    help="Number of test_topic messages processed by the worker.",
)


@app.agent(test_topic)
async def process_stream(stream: StreamT) -> AsyncGenerator:
    """The agent updates the table with incoming messages from the test topic.

    Parameters
    ----------

    stream: `StreamT`
        The incoming stream of events (messages)
    """
    async for message in stream:
        count["test_topic"] += 1
        messages = table["test_topic"].value()
        messages.append(message)
        table["test_topic"] = messages

        yield message


@app.page("/test_topic/")
async def get_count(self: web.View, request: web.Request) -> web.Response:
    """Handle ``GET /test_topic/`` request.

    This endpoint returns the number of test topic messages
    processed by the worker.
    """
    response = self.json({"count": count["test_topic"] - 1})
    return response
