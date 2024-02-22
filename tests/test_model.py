from typing import Optional

import pytest

from ormantic.errors import PrimaryKeyMissingError, PrimaryKeyModifyError
from ormantic.fields import Field, FieldProxy
from ormantic.model import Model


def test_model_pk():
    with pytest.raises(PrimaryKeyMissingError):

        class Foo(Model):
            id: int


def test_model_pk_type():
    class Foo(Model):
        id: int = Field(primary=True, autoincrement=True)

    assert Foo.__fields__["id"].annotation == Optional[int]

    class Bar(Model):
        id: Optional[int] = Field(primary=True, autoincrement=True)

    assert Bar.__fields__["id"].annotation == Optional[int]


class Student(Model):
    id: int = Field(primary=True, autoincrement=True)
    name: str = Field(...)


def test_model_name():
    assert +Student == "student"
    assert Student.__table__ == "student"


def test_model_annotations():
    assert Student.__annotations__ == {"id": Optional[int], "name": str}


def test_model_fields():
    assert isinstance(Student.id, FieldProxy)
    assert isinstance(Student.name, FieldProxy)


def test_model_init():
    print(Student.__fields__)
    print(Student.__orm_fields__)
    stu = Student(name="test")
    Student.validate
    print(stu.__dict__)
    assert stu.dict() == {"id": None, "name": "test"}

    stu.id = 1
    assert stu.dict() == {"id": 1, "name": "test"}

    with pytest.raises(PrimaryKeyModifyError):
        stu.id = 2


test_model_init()
