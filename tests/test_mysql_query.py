from ormantic.dialects.mysql.query import Query
from ormantic.fields import Field
from ormantic.model import Model


class User(Model):
    __table__ = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str


def test_fields():
    query = Query(User)
    assert list(query.fields) == ["id", "name"]
    assert query.sql_params() == ("select `user`.`id`, `user`.`name` from `user`", ())


def test_order_by():
    query = Query(User).order_by(User.id.asc)  # type: ignore
    assert query.sql_params() == (
        "select `user`.`id`, `user`.`name` from `user` order by `user`.`id` asc",
        (),
    )

    query = Query(User).order_by(id=True)  # type: ignore
    assert query.sql_params() == (
        "select `user`.`id`, `user`.`name` from `user` order by `user`.`id` asc",
        (),
    )

    query = Query(User).order_by(User.id.asc, User.name.desc)  # type: ignore
    assert query.sql_params() == (
        "select `user`.`id`, `user`.`name` from `user` order by `user`.`id` asc, `user`.`name` desc",
        (),
    )


def test_limit():
    query = Query(User).limit(offset=5, rows=10)  # type: ignore
    assert query.sql_params() == (
        "select `user`.`id`, `user`.`name` from `user` limit 5, 10",
        (),
    )

    query = Query(User).first()  # type: ignore
    assert query.sql_params() == (
        "select `user`.`id`, `user`.`name` from `user` limit 1",
        (),
    )

    query = Query(User).all()  # type: ignore
    assert query.sql_params() == ("select `user`.`id`, `user`.`name` from `user`", ())


def test_query():
    query = (
        Query(User)
        .filter(User.id == 1, User.name == "Tom")  # type: ignore
        .order_by(User.id.asc, User.name.desc)  # type: ignore
        .limit(offset=5, rows=10)
    )
    assert query.sql_params() == (
        "select `user`.`id`, `user`.`name` from `user` where `id` = %s and `name` = %s order by `user`.`id` asc, `user`.`name` desc limit 5, 10",
        (1, "Tom"),
    )
