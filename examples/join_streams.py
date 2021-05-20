"""Faust agent to demonstrate joining two streams."""

__all__ = [
    "process_window",
    "process_stream",
]

import logging
from typing import Any, AsyncGenerator, List, Tuple

from faust.types import StreamT

from kafkaaggregator.app import app, config

logger = logging.getLogger("kafkaaggregator")

# Consume from two source topics
source_topics = app.topic("example-000", "example-001")


def process_window(key: Tuple, value: List[Any]) -> None:
    """Process the tumbling window.

    Parameters
    ----------
    key: `Tuple`
        key for the current window in the WindowSet associated to
        ``Table[k]``. The key contains the window range.
        Example: ``key = (k, (start, end))``

    value: `list`
        List of messages in the current window.
    """
    # The resulting stream joins fields from the two source topics
    time = key[0]
    print(time, value)


# Persist the joined stream in a tumbling window table
# .relative_to_stream() means that the window range is relative to the
# timestamp added by kafka
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


@app.agent(source_topics)
async def process_stream(stream: StreamT) -> AsyncGenerator:
    """Process incoming events from the source topics."""
    async for event in stream.events():
        # The timestamp (key) comes from the "time" field in the stream
        timestamp = event.value["time"]
        # Get the current value for this key
        record = table[timestamp].value()
        # Append the new value
        record.append({f"{event.message.topic}.value": event.value["value0"]})
        # upsert table
        table[timestamp] = record

        yield event
