from typing import (
    Callable,
    Optional,
    Sequence,
    TypeVar,
)

from loguru import logger

__all__ = ["logger", "find_first"]


Item = TypeVar("Item")


def find_first(func: Callable[[Item], bool], seq: Sequence[Item]) -> Optional[Item]:  # pragma: no cover
    result = filter(func, seq)
    try:
        return next(result)
    except StopIteration:
        return None


def is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")
