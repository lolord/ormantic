from typing import cast

import pytest

from ormantic import Field, Model, Query
from ormantic.errors import FieldNotFoundError
from ormantic.fields import FieldProxy
from ormantic.query import Delete, Insert, Update


class User(Model):
    __table__ = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str


User_ID = cast(FieldProxy, User.id)
User_Name = cast(FieldProxy, User.name)


def test_query_attr():
    query = Query(User)
    assert query.table is User
    assert query.get_field("id") is User_ID
    assert query.fields == list(User.__fields__.values())


def test_filter():
    query = Query(User)
    assert query.filters == []

    query = Query(User).filter(User.id == 1)
    assert query.filters == [User.id == 1]

    query = Query(User).filter(id=1)
    assert query.filters == [User.id == 1]

    query = Query(User).filter({"id": 1})
    assert query.filters == [User.id == 1]

    with pytest.raises(FieldNotFoundError):
        Query(User).filter(email="email")


def test_select():
    query = Query(User).select("id")
    assert query.fields == [User_ID]

    query = Query(User).select("id", User_Name)
    assert query.fields == [User_ID, User_Name]

    with pytest.raises(FieldNotFoundError):
        query = Query(User).select("email")


def test_count_distinct():
    query = Query(User).count()
    assert +query.fields[0] == "count(*)"

    query = Query(User).count("id")
    assert query.fields == [User_ID]
    query = Query(User).count(User_ID)
    assert query.fields == [User_ID]

    query = Query(User).distinct("name")
    assert query.fields == [User_Name]

    query = Query(User).distinct(User_Name)
    assert query.fields == [User_Name]

    query = Query(User).count_distinct(User_Name)
    assert query.fields == [User_ID]


def test_order():
    query = Query(User).order_by(User_ID.asc, name=False)
    assert query.sorts == [User_ID.asc, User_Name.desc]

    with pytest.raises(FieldNotFoundError):
        Query(User).order_by(email=True)


def test_query_limit():
    query = Query(User).first()

    assert query.offset is None
    assert query.rows == 1

    query = Query(User).all()

    assert query.offset is None
    assert query.rows is None

    query = Query(User).paginate()

    assert query.offset == 0
    assert query.rows == 1000

    query = Query(User).paginate(2, 10)

    assert query.offset == 10
    assert query.rows == 10


def test_insert():
    users = [User(id=1, name="test"), User(id=2, name="test")]
    query = Insert(User, [users[0]])
    query.add(users[1])
    assert query.values == users


def test_update():
    query = Update(User).filter(User_ID == 1).update(name="test")
    assert query.table is User
    assert query.filters == [User_ID == 1]
    assert query.value == {"name": "test"}


def test_delete():
    query = Delete(User).filter(User_ID == 1)
    assert query.table is User
    assert query.filters == [User_ID == 1]
