from ormantic import Delete, Field, Insert, Model, Query, Update
from ormantic.dialects.mysql.query import sql_params


class User(Model):
    __table__ = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str
    age: int


def test_fields():
    query = Query(User)
    assert list(query.fields) == ["id", "name", "age"]
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user`",
        (),
    )


def test_order_by():
    query = Query(User).order_by(User.id.asc)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user` order by `user`.`id` asc",
        (),
    )

    query = Query(User).order_by(id=True)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user` order by `user`.`id` asc",
        (),
    )

    query = Query(User).order_by(User.id.asc, User.name.desc)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user` order by `user`.`id` asc, `user`.`name` desc",
        (),
    )


def test_limit():
    query = Query(User).limit(offset=5, rows=10)  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user` limit 5, 10",
        (),
    )

    query = Query(User).first()  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user` limit 1",
        (),
    )

    query = Query(User).all()  # type: ignore
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` from `user`",
        (),
    )


def test_query():
    query = (
        Query(User)
        .filter(User.id == 1, User.name == "Tom")  # type: ignore
        .order_by(User.id.asc, User.name.desc)  # type: ignore
        .limit(offset=5, rows=10)
    )
    assert sql_params(query) == (
        "select `user`.`id`, `user`.`name`, `user`.`age` "
        + "from `user` "
        + "where `user`.`id` = %s and `user`.`name` = %s "
        + "order by `user`.`id` asc, `user`.`name` desc "
        + "limit 5, 10",
        (1, "Tom"),
    )

    query = (
        Query(User)
        .filter((User.id > 1) & (User.id < 10))
        .filter((User.name == "tom") | (User.name == "Tom"))
        .filter((User.age > 20) & (User.age == 30))
        .order_by(User.id.asc, User.name.desc)  # type: ignore
        .limit(offset=5, rows=10)
    )

    assert sql_params(query) == (
        (
            "select `user`.`id`, `user`.`name`, `user`.`age` "
            + "from `user` "
            + "where `user`.`id` > %s and `user`.`id` < %s "
            + "and ( `user`.`name` = %s or `user`.`name` = %s ) "
            + "and `user`.`age` > %s and `user`.`age` = %s "
            + "order by `user`.`id` asc, `user`.`name` desc "
            + "limit 5, 10"
        ),
        (1, 10, "tom", "Tom", 20, 30),
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


def test_insert():
    query = Insert(User, [User(id=1, name="test1", age=20), User(id=2, name="test2", age=30)])

    assert sql_params(query) == (
        "insert into `user` (`user`.`id`, `user`.`name`, `user`.`age`) values (%s, %s, %s)",
        ((1, "test1", 20), (2, "test2", 30)),
    )


def test_update():
    query = Update(User).update(name="test")
    assert sql_params(query) == (
        "update `user` set `user`.`name` = %s",
        ("test",),
    )

    query.filter(User.id == 1)

    assert sql_params(query) == (
        "update `user` set `user`.`name` = %s where `user`.`id` = %s",
        ("test", 1),
    )


def test_delete():
    query = Delete(User)
    assert sql_params(query) == ("delete from `user`", ())

    query.filter(User.id == 1)
    assert sql_params(query) == ("delete from `user` where `user`.`id` = %s", (1,))


def test_sql():
    assert sql_params("select 1") == ("select 1", ())
