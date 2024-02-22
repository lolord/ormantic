from contextvars import ContextVar
from typing import Callable, Optional, Sequence, TypeVar
from weakref import WeakValueDictionary

from loguru import logger

orm_cache_var: ContextVar[WeakValueDictionary] = ContextVar("orm_cache")
orm_cache_var.set(WeakValueDictionary())


__all__ = ["logger", "orm_cache_var", "find_first"]


Item = TypeVar("Item")


def find_first(func: Callable[[Item], bool], seq: Sequence[Item]) -> Optional[Item]:
    result = filter(func, seq)
    try:
        return next(result)
    except StopIteration:
        return None


def is_dunder(name: str) -> bool:
    return name.startswith("__") and name.endswith("__")
