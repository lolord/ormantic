import time
from datetime import datetime, timedelta
from typing import Optional, cast

import pytest

from ormantic import (
    Field,
    FieldProxy,
    Model,
    PrimaryKeyMissingError,
    PrimaryKeyModifyError,
)
from ormantic.errors import AutoIncrementFieldExists, FieldAttributeConflictError


def test_model_pk():
    with pytest.raises(PrimaryKeyMissingError):

        class Foo(Model):
            id: int


def test_model_pk_type():
    class Foo(Model):
        id: int = Field(primary=True, autoincrement=True)

    assert Foo.__fields__["id"].annotation == Optional[int]

    class Bar(Model):
        name: str = Field(primary=True)

    assert Bar.__fields__["name"].annotation == str

    class FooBar(Model):
        id: Optional[int] = Field(primary=True, autoincrement=True)
        name: str = Field(primary=True)

    assert FooBar.__fields__["id"].annotation == Optional[int]
    assert FooBar.__fields__["name"].annotation == str


class User(Model):
    __table__: str = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str
    c_at: datetime = Field(default_factory=datetime.now)
    u_at: datetime = Field(None, update_factory=datetime.now)


USER_ID = cast(FieldProxy, User.id)
USER_NAME = cast(FieldProxy, User.name)
USER_CAT = cast(FieldProxy, User.c_at)
USER_UAT = cast(FieldProxy, User.u_at)


def test_model_name():
    assert +User == "user"
    assert User.__table__ == "user"


def test_model_annotations():
    assert User.__annotations__["id"] == Optional[int]
    assert User.__annotations__["name"] == str
    assert User.__annotations__["c_at"] == datetime
    assert User.__annotations__["u_at"] == datetime


def test_model_fields():
    assert isinstance(User.id, FieldProxy)
    assert isinstance(User.name, FieldProxy)


def test_auto_increment():
    stu = User(name="test")  # type: ignore
    assert getattr(User, "__inc_field__") == "id"
    assert stu.id is None
    assert stu.set_auto_increment(1) is True
    assert stu.id == 1
    assert stu.set_auto_increment(1) is False
    with pytest.raises(PrimaryKeyModifyError):
        stu.id = 1

    class Foo(Model):
        id: int = Field(primary=True)

    assert hasattr(Foo, "__inc_field__") is False


def test_auto_increment_error():
    with pytest.raises(AutoIncrementFieldExists):

        class Case1(Model):
            id: int = Field(primary=True, autoincrement=True)
            inc: int = Field(autoincrement=True)

    with pytest.raises(FieldAttributeConflictError):

        class Case2(Model):
            id: int = Field(primary=True, autoincrement=True, default_factory=int)

    with pytest.raises(FieldAttributeConflictError):

        class Case3(Model):
            id: int = Field(0, primary=True, autoincrement=True)


def test_model_dict():
    stu = User(name="test")  # type: ignore

    assert stu.c_at is not None
    assert stu.u_at is not None
    assert stu.dict() == {
        "id": None,
        "name": "test",
        "c_at": stu.c_at,
        "u_at": stu.u_at,
    }

    stu.id = 1
    assert stu.u_at is not None
    assert stu.dict() == {
        "id": 1,
        "name": "test",
        "c_at": stu.c_at,
        "u_at": stu.u_at,
    }
    assert stu.dict(primary_keys=True) == {"id": 1}
    assert stu.dict(primary_keys=True) == stu.dict(petty_keys=False)

    assert stu.dict(petty_keys=True) == {
        "name": "test",
        "c_at": stu.c_at,
        "u_at": stu.u_at,
    }
    assert stu.dict(petty_keys=True) == stu.dict(primary_keys=False)

    assert User(**stu.dict()) == stu


def test_model_hot_field():
    stu = User(id=1, name="test")  # type: ignore
    u_at = stu.u_at
    stu.age = 1
    assert stu.age == 1
    assert u_at == stu.u_at
    time.sleep(0.001)
    stu.name = ""
    assert u_at != stu.u_at

    with pytest.raises(ValueError):
        stu.name = None  # type: ignore

    u_at = stu.u_at
    time.sleep(0.001)
    stu.name = ""
    assert u_at == stu.u_at

    u_at = stu.u_at
    stu.u_at = stu.u_at + timedelta(days=1)
    assert stu.u_at == u_at + timedelta(days=1)
