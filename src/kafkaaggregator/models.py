"""Dynamic creation of Faust-avro Records."""

__all__ = ["create_record"]

from typing import Any, List, Mapping, Optional

from faust_avro import Record

from kafkaaggregator.fields import Field


def create_record(
    topic_name: str, fields: List[Field], doc: Optional[str] = None
) -> Record:
    """Create a Faust-avro Record class during runtime.

    Parameters
    ----------
    topic_name: `str`
        Name of the topic to create the faust_avro.Record for.
    fields: `list` [`Field`]
        List of tuples mapping field names and types for the faust_avro.Record.
    doc: `str`
        Docstring for the faust_avro.Record, if not provided create one.

    Returns
    -------
    cls: `Record`
        A faust_avro.Record class.

    Examples
    --------
    >>> from kafkaaggregator.fields import Field
    >>> from kafkaaggregator.models import create_record
    >>> Foo = create_record('Foo', [Field('bar', int)])
    >>> f = Foo(bar=0)
    >>> f.bar
    0
    >>> f.dumps()
    {'bar': 0, '__faust': {'ns': '__main__.Foo'}}

    """
    cls_name = topic_name.title().replace("-", "")
    _fields: Mapping[str, Any] = dict([f.astuple() for f in fields])

    if doc is None:
        doc = f"Faust Record for topic {topic_name}"

    cls_attrs = dict(
        __annotations__=_fields,
        __doc__=doc,
    )

    return type(cls_name, (Record,), cls_attrs)
