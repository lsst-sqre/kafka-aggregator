"""Faust agent for the source topic aggregation example.

The Faust agent persists the messages for the source topic in tumbling windows.
See https://faust.readthedocs.io/en/latest/userguide/tables.html#windowing

The window_size parameter controls the number of messages to aggregate,
if the source topic is produced at a constant frequency, the number of messages
aggregated in each window is n = window_size * frequency. The window range is
relative to the timestamp in the kafka stream.

The window_expires parameter controls when the callback function that
processes the expired window(s) is called. When a window is processed, a new
message is produced with the aggregated results.
"""

__all__ = [
    "process_window",
    "process_src_topic",
    "process_agg_topic",
]


import logging
from typing import Any, AsyncGenerator, List, Tuple

from faust import web
from faust.types import StreamT

from kafkaaggregator.aggregator import Aggregator
from kafkaaggregator.app import app, config

logger = logging.getLogger("kafkaaggregator")


# Topic names for the example are obtained from the configuration
Agg = Aggregator(
    source_topic=config.src_topic,
    aggregation_topic=config.agg_topic,
    excluded_field_names=config.excluded_field_names,
)

# The Faust Record for the aggregation topic is created at runtime
AggregationRecord = Agg.async_create_record()

src_topic = app.topic(config.src_topic)

agg_topic = app.topic(
    config.agg_topic, value_type=AggregationRecord, internal=True
)


def process_window(key: Tuple, value: List[Any]) -> None:
    """Process a window and send an aggregated message.

    Parameters
    ----------
    key: `Tuple`
        key for the current window in the WindowSet associated to
        ``Table[k]``. The key contains the window range.
        Example: ``key = (k, (start, end))``

    value: `list`
        List of messages in the current window.
    """
    start, end = key[1]

    # Faust defines the window range as (start,  start + size - 0.1)
    # https://github.com/robinhood/faust/blob/master/faust/types/windows.py#L16
    # To compute the midpoint of the window we have to correct the window range
    # by 0.1. Note that, despite of this definition, messages with timestamps
    # between (start + size - 0.1) and (start + size) are correctly added to
    # the current window.
    time = (start + end + 0.1) / 2

    agg_message = Agg.compute(
        time=time, window_size=config.window_size, messages=value
    )

    agg_topic.send_soon(value=agg_message)

    logger.info(
        f"{agg_message.count:5d} messages aggregated on window "
        f"({start}, {end + 0.1})."
    )


# Tumbling window to persist source topic messages, the process_window
# callback is called when the window expires. Note that we don't have a model
# for the source topic to specify the time field to use. An alternative is to
# construct the window ranges relative to the timestamp added by Kafka.
table = (
    app.Table(
        "tumbling-window",
        default=list,
        on_window_close=process_window,
        help=f"Persit messages in windows of " f"{config.window_size}s.",
    )
    .tumbling(config.window_size, expires=config.window_expires)
    .relative_to_stream()
)

count = app.Table(
    "count", default=int, help="Number of messages processed by the worker.",
)


@app.agent(src_topic)
async def process_src_topic(stream: StreamT) -> AsyncGenerator:
    """Process source topic messages.

    Update the tumbling window with messages from the
    source topic. It also counts the number or incoming messages.

    Parameters
    ----------
    stream: `StreamT`
        The incoming stream of events (messages)
    """
    async for message in stream:
        count[config.src_topic] += 1
        messages = table[config.src_topic].value()
        messages.append(message)
        table[config.src_topic] = messages

        yield message


@app.agent(agg_topic)
async def process_agg_topic(stream: StreamT) -> AsyncGenerator:
    """Count the number of aggragated messages.

    Parameters
    ----------
    stream: `StreamT`
        The incoming stream of events (messages)
    """
    async for agg_message in stream:
        count[config.agg_topic] += 1

        yield agg_message


@app.page("/count/")
async def get_count(self: web.View, request: web.Request) -> web.Response:
    """Handle ``GET /count/`` request.

    Get the number of messages and aggregated messages from the count table.
    """
    response = self.json(
        {
            config.src_topic: count[config.src_topic],
            config.agg_topic: count[config.agg_topic],
        }
    )
    return response
