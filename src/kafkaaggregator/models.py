"""Dynamic creation of Faust-avro Records."""

__all__ = ["create_record"]

from typing import Any, List, Mapping

from faust_avro import Record

from kafkaaggregator.fields import Field


def create_record(
    cls_name: str, fields: List[Field], doc: str = None
) -> Record:
    """Create a Faust-avro Record class during runtime.

    Parameters
    ----------
    cls_name: `str`
        Name of the new class to create.
    fields: `list` [`Field`]
        List of tuples mapping field names and types for the Faust-avro Record.
    doc: `str`
        Docstring for the new class.

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
    _fields: Mapping[str, Any] = dict([f.astuple() for f in fields])

    cls_attrs = dict(
        __annotations__=_fields,
        __doc__=doc,
    )

    return type(cls_name, (Record,), cls_attrs)
