"""Tests for the topics module."""

import json

import faust_avro
import pytest

from kafkaaggregator.fields import Field
from kafkaaggregator.topic_schema import TopicSchema


@pytest.fixture
def avro_schema() -> str:
    """Mock avro schema to test primitive data types."""
    schema = json.dumps(
        dict(
            type="record",
            name="test",
            doc="Test Avro primitive data types",
            fields=[
                dict(name="int_field", type="int"),
                dict(name="long_field", type="long"),
                dict(name="float_field", type="float"),
                dict(name="double_field", type="double"),
                dict(name="bytes_field", type="bytes"),
                dict(name="string_field", type="string"),
            ],
        )
    )
    return schema


@pytest.mark.asyncio
@pytest.mark.vcr
async def test_register(avro_schema: str) -> None:
    """Test topic schema registration."""
    topic_schema = TopicSchema(
        name="test-avro-schema", registry_url="http://localhost:8081"
    )
    schema_id = await topic_schema.register(schema=avro_schema)
    assert schema_id == 1


# https://github.com/masterysystems/faust-avro/blob/master/faust_avro/types.py
@pytest.mark.asyncio
@pytest.mark.vcr
async def test_get_fields(avro_schema: str) -> None:
    """Test `topic.get_fields()` method returning faust-avro types."""
    topic_schema = TopicSchema(
        name="test-avro-schema", registry_url="http://localhost:8081"
    )
    await topic_schema.register(schema=avro_schema)
    fields = await topic_schema.get_fields()

    assert Field("int_field", faust_avro.types.int32) in fields
    assert Field("long_field", int) in fields
    assert Field("float_field", faust_avro.types.float32) in fields
    assert Field("double_field", float) in fields
    assert Field("bytes_field", bytes) in fields
    assert Field("string_field", str) in fields
