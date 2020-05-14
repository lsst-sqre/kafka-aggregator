"""A class factory for Faust Records."""

__all__ = ["make_record"]

from typing import Any, Mapping

import faust_avro


def make_record(
    cls_name: str, fields: Mapping[str, Any], doc: str = None
) -> faust_avro.Record:
    """Create a Faust Record class.

    Examples
    --------
    >>> Foo = make_record('Foo', {'bar': int})
    >>> f = Foo(bar=0)
    >>> f.bar
    0
    >>> f.dumps()
    {'bar': 0, '__faust': {'ns': '__main__.Foo'}}

    Parameters
    ----------
    cls_name: `str`
        Name of the new class to create.
    fields: `dict`
        Dictionary mapping of field names and types for the Faust Record.
    doc: `str`
        Docstring for the new class.

    Returns
    -------
    cls: `faust_avro.Record`
        A faust_avro.Record subclass.

    """
    cls_attrs = dict(__annotations__=fields, __doc__=doc,)

    return type(cls_name, (faust_avro.Record,), cls_attrs)
