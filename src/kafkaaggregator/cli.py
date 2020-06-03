"""Entrypoint for Faust CLI."""

__all__ = ["main"]

from kafkaaggregator.app import app


def main() -> None:
    """Entrypoint for Faust CLI."""
    app.main()
