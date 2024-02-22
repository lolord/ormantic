import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
)

from .express import BoolExpression
from .fields import FakeField, SupportSort, asc, desc
from .typing import ABCField, ABCQuery, ABCTable, ModelType

QueryResultItemType = TypeVar("QueryResultItemType")

SupportExpress: TypeAlias = List[SupportSort]


class SelectMixin:
    if TYPE_CHECKING:
        table: ABCTable
        fields: List[ABCField]

    def select(self, *fields: Union[ABCField, Any]):
        for field in fields:
            if not isinstance(field, ABCField):
                field = self.table.get_field(field)
                if field is None:
                    field = FakeField(name=str(field), table=self.table)

            self.fields.append(field)

        return self


class FilterMixin:
    if TYPE_CHECKING:
        table: ABCTable
        filters: List[Union[BoolExpression, Dict]]

    def filter(self, *args: Union[BoolExpression, Dict], **kwargs: Any):
        if getattr(self, "filters", None) is None:  # pragma: no cover
            self.filters = []
        for item in args:
            if isinstance(item, BoolExpression):
                self.filters.append(item)
            elif isinstance(item, Dict):
                kwargs.update(item)
            else:  # pragma: no cover
                raise TypeError(f"{type(item)} is not BaseExpression")

        if kwargs:
            for name, value in kwargs.items():
                field = self.table.get_field(name)
                if field is not None:
                    self.filters.append(field == value)
                else:  # pragma: no cover
                    raise ValueError(f"{name} not find in {self.table}")
        return self

    where = filter


class OrderByMixin:
    if TYPE_CHECKING:
        sorts: SupportExpress
        table: ABCTable

    def order_by(self, *args: Tuple[SupportSort, bool], **kwargs: bool):
        self.sorts += args
        for name, v in kwargs.items():
            field = self.table.get_field(name)
            if field is None:
                warnings.warn(f"This field may not exist:{name}", RuntimeWarning)
            else:
                self.sorts.append(asc(field) if v else desc(field))
        return self


class LimitMixin:
    MAXROWS: ClassVar[int] = 2147483647

    if TYPE_CHECKING:
        offset: int | None
        rows: int | None

    def limit(self, offset: int | None = None, rows: int | None = None):
        self.offset = offset
        self.rows = rows
        return self

    def first(self):
        return self.limit(rows=1)

    def all(self):
        return self.limit(None, None)


class PaginateMixin(LimitMixin):
    def paginate(self, page: int = 1, page_size: int = 1000):
        return self.limit((page - 1) * page_size, page_size)


class DistinctMixin:
    table: ABCTable
    is_distinct: bool

    def distinct(self, field: Union[ABCField, str]):
        self.is_distinct = True
        if isinstance(field, ABCField):
            self.fields = [field]
        else:
            field = FakeField(str(field), table=self.table)
        return self


class CountMixin:
    """如果列为主键,count(列名)效率优于count(1)
    如果列不为主键,count(1)效率优于count(列名)
    如果表中存在主键,count(主键列名)效率最优
    如果表中只有一列,则count(*)效率最优
    如果表有多列,且不存在主键,则count(1)效率优于count(*)"""

    is_count: bool
    table: ABCTable

    def count(self, field: Union[ABCField, str]):
        self.is_count = True
        if isinstance(field, ABCField):
            self.fields = [field]
        else:
            field = FakeField(str(field), table=self.table)
        return self


class BaseQuery(
    ABCQuery,
    SelectMixin,
    FilterMixin,
    CountMixin,
    DistinctMixin,
    PaginateMixin,
    OrderByMixin,
    Generic[ModelType],
):
    def __init__(
        self,
        table: ModelType,
        fields: Sequence[Union[str, ABCField]] = [],
        filters: Sequence[Union[BoolExpression, Dict]] = [],
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

        if not fields:
            fields = tuple(table.get_fields().values())
        self.select(*fields)
        self.filter(*filters)
        self.order_by(*sorts)
        self.limit(offset, rows)


class BaseUpdate(ABCQuery, FilterMixin):
    def __init__(
        self,
        table: ABCTable,
        filters: Sequence[Union[BoolExpression, Dict]] = [],
        value: Optional[Dict[str, Any]] = None,
    ):
        self.table = table

        self.value = {} if value is None else value
        self.filter(*filters)

    def update(self, **value: Any):
        self.value.update(value)
        return self


class BaseDelete(ABCQuery[ABCTable], FilterMixin):
    def __init__(
        self,
        table: ABCTable,
        filters: Sequence[Union[BoolExpression, Dict]] = [],
    ):
        self.table = table
        self.filter(*filters)


class BaseInsert(ABCQuery[ABCTable]):
    values: List[Dict]

    def __init__(self, table: ABCTable, values: List[Dict]):
        self.table = table
        self.values = values

    def add_values(self, values: Dict):
        self.values.append(values)
        return self
