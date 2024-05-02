from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Literal,
    Optional,
    Self,
    Sequence,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
)

from ormantic.errors import FieldNotFoundError
from ormantic.express import Predicate
from ormantic.fields import CountField, DistinctField, FakeField, SupportSort, asc, desc
from ormantic.typing import ABCField, ABCQuery, ModelType

QueryResultItemType = TypeVar("QueryResultItemType")

SupportExpress: TypeAlias = List[SupportSort]


class SelectMixin(ABCQuery[ModelType], Generic[ModelType]):
    if TYPE_CHECKING:
        # table: ModelType
        fields: List[ABCField]

    def select(self, *fields: Union[ABCField, str]) -> Self:
        self.fields = []
        for field in fields:
            if isinstance(field, ABCField):
                self.fields.append(field)
            else:
                field_ = self.table.get_field(field)
                if field_ is None:
                    raise FieldNotFoundError(field_)
                self.fields.append(field_)

        return self


class FilterMixin(ABCQuery[ModelType], Generic[ModelType]):
    if TYPE_CHECKING:
        # table: ABCTable
        filters: List[Predicate]

    def filter(self, *args: Union[Predicate, Dict, bool], **kwargs: Any) -> Self:
        if getattr(self, "filters", None) is None:  # pragma: no cover
            self.filters = []
        for item in args:
            if isinstance(item, Predicate):
                self.filters.append(item)
            elif isinstance(item, Dict):
                kwargs.update(item)
            else:  # pragma: no cover
                raise ValueError(f"Error Expression: {item}")

        if kwargs:
            for name, value in kwargs.items():
                field = self.table.get_field(name)
                if field is None:
                    raise FieldNotFoundError(field)
                self.filters.append(field == value)
        return self

    where = filter


class OrderByMixin(ABCQuery[ModelType], Generic[ModelType]):
    if TYPE_CHECKING:
        sorts: SupportExpress
        # table: ABCTable

    def order_by(self, *args: Tuple[SupportSort, bool], **kwargs: bool) -> Self:
        self.sorts += args
        for name, v in kwargs.items():
            field = self.table.get_field(name)
            if field is None:
                raise FieldNotFoundError(field)
            self.sorts.append(asc(field) if v else desc(field))
        return self


class LimitMixin:
    MAXROWS: ClassVar[int] = 2147483647

    if TYPE_CHECKING:
        offset: int | None
        rows: int | None

    def limit(self, offset: int | None = None, rows: int | None = None) -> Self:
        self.offset = offset
        self.rows = rows
        return self

    def first(self) -> Self:
        return self.limit(None, 1)

    def all(self) -> Self:
        return self.limit(None, None)


class PaginateMixin(LimitMixin):
    def paginate(self, page: int = 1, page_size: int = 1000) -> Self:
        return self.limit((page - 1) * page_size, page_size)


class CountDistinct(SelectMixin[ModelType], Generic[ModelType]):
    # table: ABCTable

    def count(self, field: Union[ABCField, str, Literal[1, "*"]] = "*") -> Self:
        """如果列为主键,count(列名)效率优于count(1)
        如果列不为主键,count(1)效率优于count(列名)
        如果表中存在主键,count(主键列名)效率最优
        如果表中只有一列,则count(*)效率最优
        如果表有多列,且不存在主键,则count(1)效率优于count(*)"""
        if isinstance(field, ABCField):
            return self.select(CountField(field))
        else:
            field = str(field)
            _field = self.table.get_field(field)
            if _field:
                return self.select(CountField(_field))
            else:
                return self.select(FakeField(str(field), table=self.table).count())

    def distinct(self, field: Union[ABCField, str]) -> Self:
        if isinstance(field, ABCField):
            _field = DistinctField(field)
        else:
            _field = FakeField(str(field), table=self.table).distinct()
        self.fields = [_field]
        return self

    def count_distinct(self, field: Union[ABCField, str]) -> Self:
        self.distinct(field)
        _field = self.fields.pop()
        self.count(_field)
        return self


class Query(
    FilterMixin[ModelType],
    CountDistinct[ModelType],
    PaginateMixin,
    OrderByMixin[ModelType],
    Generic[ModelType],
):
    def __init__(
        self,
        table: ModelType,
        fields: Sequence[Union[str, ABCField]] = [],
        filters: Sequence[Union[Predicate, Dict, bool]] = [],
        offset: int | None = None,
        rows: int | None = None,
        sorts: SupportExpress = [],
    ):
        self.table = table
        self.fields = []
        self.filters = []
        self.offset = 0
        self.rows = 0
        self.sorts = []

        if not fields:  # pragma: no cover
            fields = tuple(table.get_fields().values())
        self.select(*fields)
        self.filter(*filters)
        self.order_by(*sorts)
        self.limit(offset, rows)

    def orm_name(self) -> str:
        return self.table.orm_name()


class Update(FilterMixin[ModelType]):
    def __init__(
        self,
        table: ModelType,
        filters: Sequence[Union[Predicate, Dict]] = [],
        value: Optional[Dict[str, Any]] = None,
    ):
        self.table = table

        self.value = {} if value is None else value
        self.filter(*filters)

    def update(self, **value: Any) -> Self:
        self.value.update(value)
        return self

    def orm_name(self) -> str:
        return self.table.orm_name()


class Delete(FilterMixin[ModelType]):
    def __init__(
        self,
        table: ModelType,
        filters: Sequence[Union[Predicate, Dict]] = [],
    ):
        self.table = table
        self.filter(*filters)

    def orm_name(self) -> str:
        return self.table.orm_name()


class Insert(ABCQuery[ModelType]):
    values: List[ModelType]

    def __init__(self, table: ModelType, values: List[ModelType]):
        self.table = table
        self.values = [] if values is None else values

    def add(self, value: ModelType) -> Self:
        self.values.append(value)
        return self

    def orm_name(self) -> str:
        return ""
