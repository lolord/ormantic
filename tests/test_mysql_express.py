from ormantic.dialects.mysql.query import mysql_predicate_sql
from ormantic.express import encode


def test_mysql_eq():
    expr = encode(
        {"name": "Tom"},
    )

    assert mysql_predicate_sql(expr) == ("`name` = %s", ["Tom"])

    expr = encode(
        {"name": {"$eq": "Tom"}},
    )

    assert mysql_predicate_sql(expr) == ("`name` = %s", ["Tom"])


def test_mysql_ne():
    expr = encode(
        {"name": {"$ne": "Tom"}},
    )

    assert mysql_predicate_sql(expr) == ("`name` != %s", ["Tom"])


def test_mysql_gt():
    expr = encode(
        {"id": {"$gt": 1}},
    )

    assert mysql_predicate_sql(expr) == ("`id` > %s", [1])

    expr = encode(
        {"id": {"$gte": 1}},
    )

    assert mysql_predicate_sql(expr) == ("`id` >= %s", [1])


def test_mysql_lt():
    expr = encode(
        {"id": {"$lt": 1}},
    )

    assert mysql_predicate_sql(expr) == ("`id` < %s", [1])

    expr = encode(
        {"id": {"$lte": 1}},
    )

    assert mysql_predicate_sql(expr) == ("`id` <= %s", [1])


def test_mysql_like():
    expr = encode(
        [
            {"name": {"$like": "_%"}},
            {"name": {"$not_like": "%_"}},
        ]
    )

    assert mysql_predicate_sql(expr) == (
        "`name` like %s and `name` not like %s",
        ["_%", "%_"],
    )


def test_mysql_regex():
    expr = encode(
        {"name": {"$regex": "Tom"}},
    )

    assert mysql_predicate_sql(expr) == ("`name` REGEXP %s", ["Tom"])


def test_mysql_and():
    expr = encode(
        {
            "$and": [
                {"id": {"$gt": 1}},
                {"name": {"$eq": "test"}},
            ],
            "id": {"$lt": 3},
        }
    )

    assert mysql_predicate_sql(expr) == (
        "`id` > %s and `name` = %s and `id` < %s",
        [1, "test", 3],
    )


def test_mysql_or():
    expr = encode(
        {
            "$or": [
                {"id": {"$lt": 1}},
                {"id": {"$gt": 10}},
            ]
        }
    )

    assert mysql_predicate_sql(expr) == ("`id` < %s or `id` > %s", [1, 10])


def test_mysql_arithmetic():
    expr = encode(
        {"price": {"$add": [1, 2]}},
    )

    assert mysql_predicate_sql(expr) == ("`price` + %s = %s", [1, 2])

    expr = encode(
        {"price": {"$sub": [1, 2]}},
    )

    assert mysql_predicate_sql(expr) == (
        "`price` - %s = %s",
        [1, 2],
    ), mysql_predicate_sql(expr)

    expr = encode(
        {"price": {"$mul": [1, 2]}},
    )
    assert mysql_predicate_sql(expr) == (
        "`price` * %s = %s",
        [1, 2],
    ), mysql_predicate_sql(expr)

    expr = encode(
        {"price": {"$floordiv": [2, 1]}},
    )
    assert mysql_predicate_sql(expr) == (
        "`price` / %s = %s",
        [2, 1],
    ), mysql_predicate_sql(expr)

    expr = encode(
        {"price": {"$truediv": [2, 1]}},
    )
    assert mysql_predicate_sql(expr) == (
        "`price` div %s = %s",
        [2, 1],
    ), mysql_predicate_sql(expr)

    expr = encode(
        {"price": {"$mod": [2, 1]}},
    )
    assert mysql_predicate_sql(expr) == (
        "`price` % %s = %s",
        [2, 1],
    ), mysql_predicate_sql(expr)


def test_table_field():
    expr = encode(
        {"user.name": "Tom"},
    )
    assert mysql_predicate_sql(expr) == ("`user`.`name` = %s", ["Tom"])
