import sys
from typing import (
    Any,
    Callable,
    ForwardRef,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

from loguru import logger

__all__ = ["logger", "find_first"]


Item = TypeVar("Item")


def find_first(
    func: Callable[[Item], bool], seq: Sequence[Item]
) -> Optional[Item]:  # pragma: no cover
    result = filter(func, seq)
    try:
        return next(result)
    except StopIteration:
        return None


def is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")


def foreign_key_forward_ref(
    model: Type[Any], fk: Union[str, Type[Any]], **localns: Any
) -> Type[Any]:  # pragma: no cover
    if not isinstance(fk, str):
        return fk

    if model.__module__ in sys.modules:
        globalns = sys.modules[model.__module__].__dict__.copy()
    else:  # pragma: no cover
        globalns = {}

    globalns.setdefault(model.__name__, model)

    ref: ForwardRef = ForwardRef(fk)

    return cast(Any, ref)._evaluate(globalns, localns, set())
