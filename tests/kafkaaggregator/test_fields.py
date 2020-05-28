from kafkaaggregator.fields import Field


def test_hash() -> None:
    assert hash(Field("field", int, metadata={"operation": "mean"}))
