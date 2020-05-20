__all__ = ["Field"]

from typing import Mapping, Tuple, Type, Union


class Field:
    """Represents an aggregation field with numeric types.

    Parameters
    ----------
    name: `str`
        Field name.
    type: `int` or `float`
        Field type.
    """

    def __init__(self, name: str, type: Union[Type[int], Type[float]]) -> None:
        self.name = name
        self.type = type

    def __repr__(self) -> str:
        return "Field(" f"name={self.name!r}," f"type={self.type!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Field):
            return NotImplemented
        return self.astuple() == other.astuple()

    def astuple(self) -> Tuple[str, Union[Type[int], Type[float]]]:
        _field = (self.name, self.type)
        return _field

    def asdict(self) -> Mapping[str, Union[Type[int], Type[float]]]:
        _field = {self.name: self.type}
        return _field
