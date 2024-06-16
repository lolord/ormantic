from types import TracebackType
from typing import Any, Optional, Protocol, Type


class CursorProto(Protocol):
    def execute(self, operation: str, parameters: Any = None) -> Any:
        ...

    def executemany(self, operation: str, seq_of_parameters: Any = None) -> Any:
        ...

    def fetchone(self) -> Any:
        ...

    def fetchall(self) -> Any:
        ...

    @property
    def lastrowid(self) -> int:
        ...

    @property
    def rowcount(self) -> int:
        ...

    def close(self) -> None:
        ...

    def __enter__(self) -> "CursorProto":
        ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        ...


class ConnectionProto(Protocol):
    def cursor(self) -> CursorProto:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    def close(self) -> None:
        ...

    def __enter__(self) -> "ConnectionProto":
        ...

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        ...


class DBAPIProto(Protocol):
    def connect(self, *args: Any, **kwargs: Any) -> ConnectionProto:
        ...
