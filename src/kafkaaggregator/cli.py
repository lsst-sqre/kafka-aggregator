"""Administrative command-line interface."""

__all__ = ["main"]

from kafkaaggregator.app import app


def main() -> None:
    """kafkaaggregator

    Administrative command-line interface for kafkaaggregator.
    """
    app.main()
