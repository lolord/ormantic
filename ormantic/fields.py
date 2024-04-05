from typing import Any, Callable, Optional, Self, Tuple, TypeVar

from pydantic.fields import FieldInfo, ModelField, Undefined

from ormantic.errors import FieldAttributeConflictError
from ormantic.express import ArithmeticMixin, LoginMixin
from ormantic.typing import ABCField, ABCTable, is_nullable

SupportSort = TypeVar("SupportSort", bound=ABCField, covariant=True)


def Field(
    default: Any = Undefined,
    *,
    name: Optional[str] = None,
    primary: bool = False,
    autoincrement: bool = False,
    update_factory: Optional[Callable] = None,
    default_factory: Optional[Callable] = None,
    alias: Optional[str] = None,
    title: Optional[str] = None,
    description: Optional[str] = None,
    const: Optional[bool] = None,
    gt: Optional[float] = None,
    ge: Optional[float] = None,
    lt: Optional[float] = None,
    le: Optional[float] = None,
    multiple_of: Optional[float] = None,
    min_items: Optional[int] = None,
    max_items: Optional[int] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    allow_mutation: Optional[bool] = True,
    regex: Optional[str] = None,
    **extra: Any,
) -> Any:
    """Used to provide extra information about a field, either for the model schema or
    complex validation. Some arguments apply only to number fields (``int``, ``float``,
     ``Decimal``) and some apply only to ``str``.

    Tip:
        The main additions of ORMantic to the regular pydantic `Field` are the
        `name` and the `primary` options.

    Warning:
        If both `default` and `default_factory` are set, an error is raised.

    Args:
        default: since this is replacing the field’s default, its first argument is
            used to set the default, use ellipsis (``...``) to indicate the field has no
            default value
        name: the name to use in the the mongo document structure
        primary: this field should be considered as a primary key.
        autoincrement: autoincrement.
        update_factory: update_factory.
        default_factory: callable that will be called when a default value is needed
            for this field.
        alias: table field name.
        title: can be any string, used in the schema
        description: can be any string, used in the schema
        const: this field is required and *must* take it's default value
        gt: only applies to numbers, requires the field to be "greater than". The
            schema will have an ``exclusiveMinimum`` validation keyword
        ge: only applies to numbers, requires the field to be "greater than or equal
            to". The schema will have a ``minimum`` validation keyword
        lt: only applies to numbers, requires the field to be "less than". The schema
            will have an ``exclusiveMaximum`` validation keyword
        le: only applies to numbers, requires the field to be "less than or equal to"
            . The schema will have a ``maximum`` validation keyword
        multiple_of: only applies to numbers, requires the field to be "a multiple of
            ". The schema will have a ``multipleOf`` validation keyword
        min_items: only applies to sequences, requires the field to have a minimum
            item count.
        max_items: only applies to sequences, requires the field to have a maximum
            item count.
        min_length: only applies to strings, requires the field to have a minimum
            length. The schema will have a ``maximum`` validation keyword
        max_length: only applies to strings, requires the field to have a maximum
            length. The schema will have a ``maxLength`` validation keyword
        regex: only applies to strings, requires the field match against a regular
            expression pattern string. The schema will have a ``pattern`` validation
            keyword
        **extra: any additional keyword arguments will be added as is to the schema

    <!---
    # noqa: DAR201
    # noqa: DAR003
    # noqa: DAR401
    # noqa: DAR101
    -->
    """
    # Perform casts on optional fields to avoid incompatibility due to the strict
    # optional mypy setting

    # autoincrement && (default_factory || default) It's not allowed
    if autoincrement and (default_factory is not None or default is not Undefined):
        raise FieldAttributeConflictError(
            "cannot specify both autoincrement and default or default_factory"
        )

    field = FieldInfo(
        default=None if default == Ellipsis else default,
        name=name,
        primary=primary,
        autoincrement=autoincrement,
        update_factory=update_factory,
        default_factory=default_factory,
        alias=alias,
        title=title,
        description=description,
        const=const,
        gt=gt,
        ge=ge,
        lt=lt,
        le=le,
        multiple_of=multiple_of,
        min_items=min_items,
        max_items=max_items,
        min_length=min_length,
        max_length=max_length,
        allow_mutation=allow_mutation,
        regex=regex,
        **extra,
    )
    field._validate()
    return field


def asc(field: SupportSort) -> Tuple[SupportSort, bool]:
    """Sort by ascending `field`."""
    return (field, True)


def desc(field: SupportSort) -> Tuple[SupportSort, bool]:
    """Sort by descending `field`."""
    return (field, False)


class SortMixin(ABCField):
    @property
    def asc(self) -> Tuple[Self, bool]:
        return asc(self)

    @property
    def desc(self) -> Tuple[Self, bool]:
        return desc(self)


class CountField(ABCField):
    def __init__(self, field: ABCField) -> None:
        self.field = field

    def __pos__(self) -> str:
        return f"count({+self.field})"

    def __str__(self) -> str:
        return f"count({+self.field})"


class CountFieldMixin(ABCField):
    def count(self) -> CountField:
        return CountField(self)


class DistinctField(CountFieldMixin, ABCField):
    def __init__(self, field: ABCField) -> None:
        self.field = field

    def __pos__(self) -> str:
        return f"distinct {self.field}"


class DistinctFieldMixin(ABCField):
    def distinct(self) -> ABCField:
        return DistinctField(self)


class FakeField(
    LoginMixin,
    ArithmeticMixin,
    SortMixin,
    CountFieldMixin,
    DistinctFieldMixin,
    ABCField,
):
    name: str

    def __init__(self, name: str, table: ABCTable) -> None:
        self.name = name
        self.table = table

    def __pos__(self) -> str:
        return self.name


class FieldProxy(
    LoginMixin,
    ArithmeticMixin,
    SortMixin,
    CountFieldMixin,
    DistinctFieldMixin,
    ABCField,
):
    def __init__(self, pydantic_field: ModelField, orm_model: ABCTable) -> None:
        self.pydantic_field = pydantic_field
        self.table = orm_model

    def __pos__(self) -> str:
        return self.pydantic_field.alias or self.pydantic_field.name

    @property
    def orm_model(self) -> ABCTable:
        return self.table

    @property
    def required(self):
        return self.pydantic_field.required

    @property
    def nullable(self) -> bool:
        """from annotation get `nullable`

        Returns:
            Optional[Callable[..., Any]]: nullable
        """
        _type = self.table.__annotations__.get(self.pydantic_field.name)
        return is_nullable(_type)

    @property
    def primary(self) -> bool:
        """from pydantic.ModelField.FieldInfo.extra get `primary`

        Returns:
            Optional[Callable[..., Any]]: primary
        """
        return self.pydantic_field.field_info.extra.get("primary", False)

    @property
    def autoincrement(self) -> bool:
        """from pydantic.ModelField.FieldInfo.extra get `autoincrement`

        Returns:
            Optional[Callable[..., Any]]: autoincrement
        """
        return self.pydantic_field.field_info.extra.get("autoincrement", False)

    @property
    def update_factory(self) -> Optional[Callable[..., Any]]:
        """from pydantic.ModelField.FieldInfo.extra get `update_factory`

        Returns:
            Optional[Callable[..., Any]]: update_factory
        """
        return self.pydantic_field.field_info.extra.get("update_factory", None)

    @property
    def default_factory(self):
        return self.pydantic_field.default_factory

    def __str__(self) -> str:
        return f"{+self.orm_model}.{+self}"

    def __repr__(self) -> str:
        return f"FieldProxy(name='{+self}', table='{+self.orm_model}')"

    def __hash__(self) -> int:
        return hash(str(self))
