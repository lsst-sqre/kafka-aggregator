"""Aggregated field class.

The Field has a numerical type by construction. It holds the name of
the source field being aggregated and the operation performed.
"""

__all__ = ["Field"]

from typing import Mapping, Optional, Tuple, Type, Union

from kafkaaggregator.operations import Operation

BasicType = Union[Type[int], Type[float], Type[bytes], Type[str]]


class Field:
    """Represents an aggregated field of numeric type.

    Parameters
    ----------
    name : `str`
        Field name.
    type : `int` or `float` or `bytes` or `str`
        Field data type.
    source_field_name : `str`, optional
        Source field name.
    operation : `str`, optional
    """

    def __init__(
        self,
        name: str,
        type: BasicType,
        source_field_name: Optional[str] = None,
        operation: Optional[str] = None,
    ) -> None:
        self.name = name
        self.type = type
        self.source_field_name = source_field_name
        self.operation = operation
        if operation:
            if operation not in Operation.values():
                raise RuntimeError(
                    f"Invalid operation '{operation}'. "
                    f"Allowed values are: {', '.join(Operation.values())}."
                )

    def __repr__(self) -> str:
        """Field representation."""
        return "Field(" f"name={self.name!r}," f"type={self.type!r})"

    def __eq__(self, other: object) -> bool:
        """Field equal to opetator."""
        if not isinstance(other, Field):
            return NotImplemented
        return self.astuple() == other.astuple()

    def __hash__(self) -> int:
        """Field needs to be hashable to work with Faust."""
        return object.__hash__(self)

    def astuple(self) -> Tuple[str, BasicType]:
        """Convert field to tuple."""
        _field = (self.name, self.type)
        return _field

    def asdict(self) -> Mapping[str, BasicType]:
        """Convert field to dict."""
        _field = {self.name: self.type}
        return _field
