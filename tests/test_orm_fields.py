from datetime import datetime
from typing import cast

from ormantic import Field, FieldProxy, Model


class User(Model):
    __table__ = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str = Field(alias="Name")
    age: int = None  # type: ignore
    c_at: datetime = Field(default_factory=datetime.now)
    u_at: datetime = Field(None, update_factory=datetime.now)


USER_ID = cast(FieldProxy, User.id)
USER_NAME = cast(FieldProxy, User.name)
USER_AGE = cast(FieldProxy, User.age)
USER_CAT = cast(FieldProxy, User.c_at)
USER_UAT = cast(FieldProxy, User.u_at)


def test_field():
    assert USER_ID.required is False
    assert USER_NAME.required is True
    assert USER_AGE.required is False
    assert USER_CAT.required is False
    assert USER_UAT.required is False

    assert USER_ID.orm_name() == "id"
    assert USER_NAME.orm_name() == "Name"

    assert USER_ID.autoincrement is True

    assert USER_CAT.default_factory == datetime.now
    assert USER_UAT.update_factory == datetime.now
