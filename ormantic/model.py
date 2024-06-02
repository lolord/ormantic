from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Optional,
    Self,
    Type,
    TypeVar,
    Union,
    cast,
    dataclass_transform,
    no_type_check,
)

import pydantic

from ormantic.errors import AutoIncrementFieldExists, PrimaryKeyMissingError, PrimaryKeyModifyError
from ormantic.fields import Field, FieldProxy
from ormantic.typing import ABCTable, AbstractSetIntStr, DictStrAny, FieldDict, MappingIntStrAny
from ormantic.utils import is_dunder

_is_base_model_class_defined = False


@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))
class ModelMetaclass(pydantic.main.ModelMetaclass, ABCTable):
    @staticmethod
    def _prepare_fields(
        namespace: DictStrAny,
    ) -> tuple[set[str], DictStrAny, DictStrAny]:
        hot_fields: set[str] = set()
        annotations: dict[str, Any] = {}
        relations: dict[str, Any] = {}

        base_annotations: dict[str, Any] = namespace.get("__annotations__", {})

        for field_name, field_type in base_annotations.items():
            if is_dunder(field_name):
                continue

            if field_name in namespace:
                default = namespace.get(field_name)
                if default is None:
                    annotations[field_name] = Optional[field_type]

                else:
                    assert isinstance(default, pydantic.fields.FieldInfo)
                    if default.extra.get("update_factory") is not None:
                        hot_fields.add(field_name)
                    if default.extra.get("autoincrement") is True:
                        annotations[field_name] = Optional[field_type]

            if field_name not in annotations:
                annotations[field_name] = field_type

        # for r in relations:
        #     annotations.pop(r)
        #     namespace.pop(r)
        return hot_fields, annotations, relations

    @no_type_check
    def __new__(  # noqa C901
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ):
        table: str = namespace.get("__table__", name.lower())
        abstract = namespace.get("__abstract__", False)

        hot_fields, annotations, relations = ModelMetaclass._prepare_fields(namespace)

        namespace["__table__"] = table
        namespace["__orm_fields__"] = {}
        namespace["__annotations__"] = {
            **namespace.get("__annotations__", {}),
            **annotations,
        }
        namespace["__hot_fields__"] = tuple(hot_fields)
        namespace["__primary_keys__"] = {}
        namespace["__petty_keys__"] = {}
        namespace["__inc_field__"] = None
        namespace["__relation_infos__"] = {k: v for k, v in relations.items()}

        cls: Type[Model] = super().__new__(mcs, name, bases, namespace, **kwargs)

        for field_name in cls.__fields__:
            pydantic_field = cls.__fields__[field_name]
            field = FieldProxy(pydantic_field=pydantic_field, table=cls)
            setattr(cls, field_name, field)
            cls.__orm_fields__[field_name] = field

            if field.autoincrement:
                if cls.__inc_field__ is None:
                    cls.__inc_field__ = field_name
                else:
                    raise AutoIncrementFieldExists(field_name)

            if field.primary:
                cls.__primary_keys__[field_name] = field
            else:
                cls.__petty_keys__[field_name] = field

        if not abstract:  # pragma: no cover
            if not cls.__primary_keys__ and _is_base_model_class_defined:
                raise PrimaryKeyMissingError(f"{name} not find primary key")

        return cls

    def orm_name(cls) -> str:
        return cast(str, getattr(cls, "__table__"))


class BaseORMModel(pydantic.BaseModel):
    if TYPE_CHECKING:
        __table__: ClassVar[str]
        __orm_fields__: ClassVar[FieldDict[FieldProxy]]
        __petty_keys__: ClassVar[dict[str, FieldProxy]]
        __primary_keys__: ClassVar[dict[str, FieldProxy]]
        __hot_fields__: ClassVar[tuple[str, ...]]
        __inc_field__: ClassVar[Optional[str]]

    @classmethod
    def get_fields(cls) -> FieldDict:
        return copy(cls.__orm_fields__)

    @classmethod
    def get_field(cls, field_name: str) -> Optional[FieldProxy]:
        return cast(Optional[FieldProxy], cls.get_fields().get(field_name))

    @classmethod
    def orm_name(cls) -> str:
        return cls.__table__

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__fields__:
            if name in self.__primary_keys__ and getattr(self, name, None) is not None:
                raise PrimaryKeyModifyError
            proxy = self.__orm_fields__[name]

            if not proxy.nullable and value is None:
                raise ValueError(f"{self}.{name} is not null")

            old_value = self.__dict__.get(name, None)
            if name in self.__dict__ and old_value == value:
                return

            super().__setattr__(name, value)

            if name not in self.__hot_fields__:
                for t in self.__hot_fields__:
                    field = self.__orm_fields__[t]
                    assert field.update_factory is not None
                    super().__setattr__(t, field.update_factory())
        else:
            object.__setattr__(self, name, value)

    # TODO
    # def __getattr__(self, name: str) -> Any:
    #     if name in self.__relation_infos__:
    #         return self._relations_[name]
    #     return super().__getattribute__(name)

    def __getattribute__(self, name: str) -> Any:
        attr = super().__getattribute__(name)
        if isinstance(attr, FieldProxy):  # pragma: no cover
            return None
        return super().__getattribute__(name)

    def __init__(self, **data: Any):
        # The default value obtained through object signature may be None.
        # If there is a factory method, this field should be popped.

        unsets = set()
        for name, field in self.__orm_fields__.items():
            if field.nullable:
                if name not in data:
                    data[name] = None
                    unsets.add(name)
            elif data.get(name) is None:
                if field.default_factory:
                    data[name] = field.default_factory()
                if field.update_factory:
                    data[name] = field.update_factory()

        try:
            super(BaseORMModel, self).__init__(**data)
            for i in unsets:
                self.__fields_set__.remove(i)
        except TypeError as e:  # pragma: no cover
            raise TypeError(
                "Model values must be a dict; you may not have returned a dictionary from a root validator"
            ) from e

    def dict(
        self,
        *,
        include: Union["AbstractSetIntStr", "MappingIntStrAny", None] = None,
        exclude: Union["AbstractSetIntStr", "MappingIntStrAny", None] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        primary_keys: Optional[bool] = None,
        petty_keys: Optional[bool] = None,
    ) -> "DictStrAny":
        _, _, validation_error = pydantic.main.validate_model(self.__class__, self.__dict__)

        if validation_error:  # pragma: no cover
            raise validation_error

        include = set() if include is None else set(include)
        exclude = set() if exclude is None else set(exclude)

        if primary_keys is True:
            include.update(self.__primary_keys__)
        elif primary_keys is False:
            exclude.update(self.__primary_keys__)
        if petty_keys is True:
            include.update(self.__petty_keys__)
        elif petty_keys is False:
            exclude.update(self.__petty_keys__)

        include = include if include else None
        exclude = exclude if exclude else None

        data = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        return data

    def set_auto_increment(self, inc_id: int) -> bool:
        if self.__inc_field__ and getattr(self, self.__inc_field__) is None:
            setattr(self, self.__inc_field__, inc_id)
            return True
        return False

    @classmethod
    def validate_row(cls, row: Dict) -> Self:
        val = cls.validate(row)
        val.__fields_set__.clear()
        return val


class Model(BaseORMModel, metaclass=ModelMetaclass):
    """Object Relational Mapping Model"""


_is_base_model_class_defined = True

ModelType = TypeVar("ModelType", bound=Model)
