from abc import abstractmethod
from types import NoneType
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Generic,
    Mapping,
    Optional,
    Protocol,
    TypeVar,
    Union,
    get_args,
    get_origin,
    runtime_checkable,
)

NoArgAnyCallable = Callable[[], Any]
AbstractSetIntStr = AbstractSet[Union[int, str]]
MappingIntStrAny = Mapping[Union[int, str], Any]
DictStrAny = Dict[str, Any]


def is_nullable(_type: Any) -> bool:
    return get_origin(_type) is Union and NoneType in get_args(_type)


class Named:
    @classmethod
    @abstractmethod
    def __pos__(cls) -> str:
        raise NotImplementedError


class ABCField(Named):
    table: "ABCTable"


FieldType = TypeVar("FieldType", bound=ABCField)


# Dict[str, ABCField] is not compatible with Dict[str, FieldType]
# so use FieldDict instead
class FieldDict(dict[str, FieldType]):
    ...


@runtime_checkable
class ABCTable(Protocol):
    def get_fields(self) -> FieldDict:
        raise NotImplementedError

    def get_field(self, field_name: str) -> Optional[ABCField]:
        return self.get_fields().get(field_name)

    def __pos__(self) -> str:
        raise NotImplementedError


ModelType = TypeVar("ModelType", bound=ABCTable)


class ABCQuery(ABCTable, Generic[ModelType]):
    table: ModelType

    @abstractmethod
    def __pos__(self):
        raise NotImplementedError

    def get_fields(self) -> Dict[str, ABCField]:
        return self.table.get_fields()


ModelType = TypeVar("ModelType", bound=ABCTable)
