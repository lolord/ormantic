import sys
from abc import abstractmethod
from types import NoneType
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    ForwardRef,
    Generic,
    Mapping,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
    cast,
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


def foreign_key_forward_ref(
    model: Type[Any], fk: Union[str, Type[Any]], **localns: Any
) -> Type[Any]:
    if not isinstance(fk, str):
        return fk

    if model.__module__ in sys.modules:
        globalns = sys.modules[model.__module__].__dict__.copy()
    else:  # pragma: no cover
        globalns = {}

    globalns.setdefault(model.__name__, model)

    ref: ForwardRef = ForwardRef(fk)

    return cast(Any, ref)._evaluate(globalns, localns, set())


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
        ...

    def get_field(self, field_name: str) -> Optional[ABCField]:
        return self.get_fields().get(field_name)

    def __pos__(self) -> str:
        ...


ModelType = TypeVar("ModelType", bound=ABCTable)


class ABCQuery(ABCTable, Generic[ModelType]):
    table: ModelType

    def __pos__(self):
        return self.__pos__()

    def get_fields(self) -> Dict[str, ABCField]:
        return self.table.get_fields()
