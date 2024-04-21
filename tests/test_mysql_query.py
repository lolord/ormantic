from ormantic import Delete, Field, Model, Query
from ormantic.dialects.mysql.query import sql_params


class User(Model):
    __table__ = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str


def test_fields():
    query = Query(User)
    assert list(query.fields) == ["id", "name"]
    assert sql_params(query) == ("select `user`.`id`, `user`.`name` from `user`", ())


def test_order_by():
    query = Query(User).order_by(User.id.asc)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name` from `user` order by `user`.`id` asc",
        (),
    )

    query = Query(User).order_by(id=True)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name` from `user` order by `user`.`id` asc",
        (),
    )

    query = Query(User).order_by(User.id.asc, User.name.desc)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name` from `user` order by `user`.`id` asc, `user`.`name` desc",
        (),
    )


def test_limit():
    query = Query(User).limit(offset=5, rows=10)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name` from `user` limit 5, 10",
        (),
    )

    query = Query(User).first()  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name` from `user` limit 1",
        (),
    )

    query = Query(User).all()  # type: ignore
    assert sql_params(query) == ("select `user`.`id`, `user`.`name` from `user`", ())


def test_query():
    query = (
        Query(User)
        .filter(User.id == 1, User.name == "Tom")  # type: ignore
        .order_by(User.id.asc, User.name.desc)  # type: ignore
        .limit(offset=5, rows=10)
    )
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name` from `user` where `user`.`id` = %s and `user`.`name` = %s order by `user`.`id` asc, `user`.`name` desc limit 5, 10",
        (1, "Tom"),
    )


def test_count():
    query = Query(User).count()
    assert sql_params(query) == ("select count(*) from `user`", ())

    query = Query(User).count("*")
    assert sql_params(query) == ("select count(*) from `user`", ())

    query = Query(User).count(1)
    assert sql_params(query) == ("select count(1) from `user`", ())

    query = Query(User).count(User.id)  # type: ignore
    assert sql_params(query) == ("select count(`user`.`id`) from `user`", ())

    query = Query(User).filter(User.id == 1).count(User.id)  # type: ignore
    assert sql_params(query) == (
        "select count(`user`.`id`) from `user` where `user`.`id` = %s",
        (1,),
    )


def test_distinct():
    query = Query(User).distinct(User.id)  # type: ignore
    assert sql_params(query) == ("select distinct `user`.`id` from `user`", ())

    query = Query(User).filter(User.name == "test").distinct(User.id)  # type: ignore
    assert sql_params(query) == (
        "select distinct `user`.`id` from `user` where `user`.`name` = %s",
        ("test",),
    )


def test_count_distinct():
    query = Query(User).filter(User.name == "test").count_distinct(User.id)  # type: ignore
    assert sql_params(query) == (
        "select count(distinct `user`.`id`) from `user` where `user`.`name` = %s",
        ("test",),
    )

    query = Query(User).filter(User.name == "test").count_distinct("id")  # type: ignore
    assert sql_params(query) == (
        "select count(distinct `user`.`id`) from `user` where `user`.`name` = %s",
        ("test",),
    )


def test_delete():
    query = Delete(User)
    assert sql_params(query) == ("delete from `user`", ())

    query.filter(User.id == 1)
    assert sql_params(query) == ("delete from `user` where `user`.`id` = %s", (1,))
