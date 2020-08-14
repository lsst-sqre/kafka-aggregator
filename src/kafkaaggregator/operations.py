"""Possible statistical operations on fields."""

__all__ = ["Operation", "q1", "q3"]

from enum import Enum
from statistics import mean, median, quantiles, stdev  # noqa: F401
from typing import List


class Operation(Enum):
    """Possible statistical operations on fields."""

    MIN = "min"
    Q1 = "q1"
    MEAN = "mean"
    MEDIAN = "median"
    Q3 = "q3"
    STDEV = "stdev"
    MAX = "max"

    @staticmethod
    def values() -> List[str]:
        """Return list of possible operations."""
        return list(map(lambda op: op.value, Operation))


def q1(data: List[float]) -> float:
    """Compute the data first quartile.

    Parameters
    ----------
    data: `list`
        List of values to compute the statistics from.

    Returns
    -------
    value: `float`
        The data first quartile.
    """
    quartiles = quantiles(data, n=4)
    return quartiles[0]


def q3(data: List[float]) -> float:
    """Compute the daa third quartile.

    Parameters
    ----------
    data: `list`
        List of values to compute the statistics from.

    Returns
    -------
    value: `float`
        The data first quartile.
    """
    quartiles = quantiles(data, n=4)
    return quartiles[2]
