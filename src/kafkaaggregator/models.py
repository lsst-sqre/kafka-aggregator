"""A class factory for Faust Records."""

__all__ = ["make_record"]

from typing import Any, List, Mapping

import faust_avro

from kafkaaggregator.fields import Field


def make_record(
    cls_name: str, fields: List[Field], doc: str = None
) -> faust_avro.Record:
    """Create a Faust Record class.

    Examples
    --------
    >>> from kafkaaggregator.fields import Field
    >>> from kafkaaggregator.models import make_record
    >>> Foo = make_record('Foo', [Field('bar', int)])
    >>> f = Foo(bar=0)
    >>> f.bar
    0
    >>> f.dumps()
    {'bar': 0, '__faust': {'ns': '__main__.Foo'}}

    Parameters
    ----------
    cls_name: `str`
        Name of the new class to create.
    fields: `list` [`tuple`]
        List of tuples mapping field names and types for the Faust Record.
    doc: `str`
        Docstring for the new class.

    Returns
    -------
    cls: `faust_avro.Record`
        A faust_avro.Record subclass.

    """
    _fields: Mapping[str, Any] = dict([f.astuple() for f in fields])

    cls_attrs = dict(__annotations__=_fields, __doc__=doc,)

    return type(cls_name, (faust_avro.Record,), cls_attrs)
