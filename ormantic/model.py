from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    dataclass_transform,
    no_type_check,
)

import pydantic

from .errors import PrimaryKeyMissingError, PrimaryKeyModifyError
from .fields import Field, FieldProxy
from .typing import FieldDict
from .utils import is_dunder

if TYPE_CHECKING:
    from .typing import AbstractSetIntStr, DictStrAny, MappingIntStrAny


_is_base_model_class_defined = False


@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))
class ModelMetaclass(pydantic.main.ModelMetaclass):
    @staticmethod
    def _prepare_fields(namespace: Dict[str, Any]):
        hot_fields = set()
        annotations = {}
        relations = {}
        base_annotations: Dict[str, Any] = namespace.get("__annotations__", {})

        for field_name, field_type in base_annotations.items():
            if is_dunder(field_name):
                continue

            if field_name in namespace:
                default = namespace.get(field_name)
                if isinstance(default, pydantic.fields.FieldInfo):
                    if default.extra.get("update_factory") is not None:
                        hot_fields.add(field_name)
                    if default.extra.get("autoincrement") is True:
                        annotations[field_name] = Optional[field_type]
                elif default is None:
                    annotations[field_name] = Optional[field_type]

                if field_name not in annotations:
                    annotations[field_name] = field_type

        for r in relations:
            annotations.pop(r)
            namespace.pop(r)
        return hot_fields, annotations, relations

    @no_type_check
    def __new__(  # noqa C901
        mcs,
        name: str,
        bases: Tuple[type, ...],
        namespace: Dict[str, Any],
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

        namespace["__relation_infos__"] = {k: v for k, v in relations.items()}

        cls: Type[Model] = super().__new__(mcs, name, bases, namespace, **kwargs)

        for field_name in cls.__fields__:
            pydantic_field = cls.__fields__[field_name]
            field = FieldProxy(pydantic_field=pydantic_field, orm_model=cls)
            setattr(cls, field_name, field)
            cls.__orm_fields__[field_name] = field

            if field.autoincrement:
                inc_field = getattr(cls, "__inc_field__", None)
                if inc_field is None:
                    cls.__inc_field__ = field_name
                else:
                    raise ValueError(
                        f"Table fields can only be one: {inc_field}, {field_name}"
                    )

            if field.primary:
                cls.__primary_keys__[field_name] = field
            else:
                cls.__petty_keys__[field_name] = field

        if not abstract:
            if not cls.__primary_keys__ and _is_base_model_class_defined:
                raise PrimaryKeyMissingError(f"{name} not find primary key")

        return cls

    def __pos__(cls) -> str:
        return getattr(cls, "__table__")


class BaseORMModel(pydantic.BaseModel):
    __slots__ = "__fields_modified__"
    if TYPE_CHECKING:
        __table__: ClassVar[str]
        __orm_fields__: ClassVar[FieldDict[FieldProxy]]
        __petty_keys__: ClassVar[Dict[str, FieldProxy]]
        __primary_keys__: ClassVar[Dict[str, FieldProxy]]
        __hot_fields__: ClassVar[Tuple[str, ...]]
        __inc_field__: ClassVar[Optional[str]]

    @classmethod
    def get_fields(cls) -> FieldDict:
        return cls.__orm_fields__

    @classmethod
    def get_field(cls, field_name: str) -> Optional[FieldProxy]:
        return cast(Optional[FieldProxy], cls.get_fields().get(field_name))

    @classmethod
    def __pos__(cls) -> str:
        return cls.__table__

    def __setattr__(self, name: str, value: Any) -> None:
        if name in self.__fields__:
            if name in self.__primary_keys__ and getattr(self, name, None) is not None:
                raise PrimaryKeyModifyError()
            proxy = cast(FieldProxy, self.__orm_fields__[name])

            if not proxy.nullable and value is None:
                raise ValueError(f"{self}.{name} is not null")

            old_value = self.__dict__.get(name, None)
            if name in self.__dict__ and old_value == value:
                return

            super().__setattr__(name, value)

            self.__fields_modified__.add(name)

            if name not in self.__hot_fields__:
                for t in self.__hot_fields__:
                    field = self.__orm_fields__.get(t)
                    if field is not None and field.update_factory is not None:
                        setattr(self, t, field.update_factory())
                        self.__fields_modified__.add(name)
        else:
            object.__setattr__(self, name, value)

    def _key_tuple(self):
        return tuple(getattr(self, k) for k in self.__primary_keys__.keys())

    def __getattr__(self, name: str) -> Any:
        if name in self.__relation_infos__:
            return self._relations_[name]
        return super().__getattribute__(name)

    def __getattribute__(self, name: str) -> Any:
        attr = super().__getattribute__(name)
        if isinstance(attr, FieldProxy):
            return None
        return super().__getattribute__(name)

    def __init__(self, **data: Any):
        # The default value obtained through object signature may be None.
        # If there is a factory method, this field should be popped.
        for k, v in list(data.items()):
            if v is None and k in self.__orm_fields__:
                field = self.__orm_fields__[k]
                if field.default_factory and not field.nullable:
                    data.pop(k)

        values, fields_set, validation_error = pydantic.main.validate_model(
            self.__class__, data
        )

        if validation_error:
            raise validation_error
        try:
            pydantic.main.object_setattr(self, "__dict__", values)
        except TypeError as e:  # pragma: no cover
            raise TypeError(
                "Model values must be a dict; you may not have returned a dictionary from a root validator"
            ) from e
        pydantic.main.object_setattr(self, "__fields_set__", fields_set)
        self._init_private_attributes()

        object.__setattr__(self, "__fields_modified__", set(self.__orm_fields__.keys()))

    def __repr_args__(self) -> Sequence[Tuple[Optional[str], Any]]:
        return ((k, v) for k, v in self.__dict__.items() if k in self.__fields__)  # type: ignore

    @classmethod
    def encode(
        cls,
        value: Any,
        encoder: Optional[Callable[[Any], Any]] = None,
        ensure_ascii=False,
        **dumps_kwargs: Any,
    ):
        encoder = cast(Callable[[Any], Any], encoder or cls.__json_encoder__)
        return cls.__config__.json_dumps(
            value, default=encoder, ensure_ascii=ensure_ascii, **dumps_kwargs
        )

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
        exclude_unmod: bool = False,
        primary_keys: Optional[bool] = None,
        petty_keys: Optional[bool] = None,
    ) -> "DictStrAny":
        _, _, validation_error = pydantic.main.validate_model(
            self.__class__, self.__dict__
        )

        if validation_error is not None:
            raise validation_error

        include = set() if include is None else set(include)
        exclude = set() if exclude is None else set(exclude)
        if exclude_unmod:
            include.update(self.__fields_modified__)

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

    def json(
        self,
        *,
        include: Union["AbstractSetIntStr", "MappingIntStrAny", None] = None,
        exclude: Union["AbstractSetIntStr", "MappingIntStrAny", None] = None,
        by_alias: bool = False,
        skip_defaults: Optional[bool] = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        **dumps_kwargs: Any,
    ) -> str:
        _, _, validation_error = pydantic.main.validate_model(
            self.__class__, self.__dict__
        )
        if validation_error:
            raise validation_error

        return super().json(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            **dumps_kwargs,
        )

    def set_inc_id(self, inc_id: int):
        inc_field = self.__inc_field__
        if inc_field is not None:
            if getattr(self, inc_field, None) is None:
                setattr(self, inc_field, inc_id)
        else:
            raise ValueError("not find autoincrement field")


class Model(BaseORMModel, metaclass=ModelMetaclass):
    """Object Relational Mapping Model"""


_is_base_model_class_defined = True

ModelType = TypeVar("ModelType", bound=Model)
