from collections.abc import Iterator
from datetime import datetime, timedelta
from schemas import Interval


def get_intervals(start_ts: datetime, stop_ts: datetime, interval: timedelta) -> Iterator[Interval]:
    """
    Generate intervals between start_ts and stop_ts with the specified interval.
    """
    current = start_ts
    while current < stop_ts:
        next_interval = min(current + interval, stop_ts)
        yield  Interval(start_ts=current, end_ts=next_interval)
        current = next_interval

