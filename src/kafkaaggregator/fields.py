"""Field represents a field in the aggregation topic.

The Field type is numeric by construction (only numeric fields are aggregated).
It also allows for metadata about the field such as the name of the source
field to aggregate and the aggregation operation.
"""

__all__ = ["Field"]

from typing import Any, Mapping, Optional, Tuple, Type, Union


class Field:
    """Represents an aggregation field with numeric types.

    Parameters
    ----------
    name: `str`
        Field name.
    type: `int` or `float`
        Field type.
    metadata: `dict`, optional
        Field metadata.
    """

    def __init__(
        self,
        name: str,
        type: Union[Type[int], Type[float]],
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.name = name
        self.type = type
        self.metadata = metadata

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

    def astuple(self) -> Tuple[str, Union[Type[int], Type[float]]]:
        """Convert field to tuple."""
        _field = (self.name, self.type)
        return _field

    def asdict(self) -> Mapping[str, Union[Type[int], Type[float]]]:
        """Convert field to dict."""
        _field = {self.name: self.type}
        return _field
