from ormantic.dialects.mysql.query import mysql_queries
from ormantic.express import encode_expression


def test_mysql_eq():
    expr = encode_expression(
        {"name": "Tom"},
    )

    assert mysql_queries(expr) == ("`name` = %s", ["Tom"])

    expr = encode_expression(
        {"name": {"$eq": "Tom"}},
    )

    assert mysql_queries(expr) == ("`name` = %s", ["Tom"])


def test_mysql_ne():
    expr = encode_expression(
        {"name": {"$ne": "Tom"}},
    )

    assert mysql_queries(expr) == ("`name` != %s", ["Tom"])


def test_mysql_gt():
    expr = encode_expression(
        {"id": {"$gt": 1}},
    )

    assert mysql_queries(expr) == ("`id` > %s", [1])

    expr = encode_expression(
        {"id": {"$gte": 1}},
    )

    assert mysql_queries(expr) == ("`id` >= %s", [1])


def test_mysql_lt():
    expr = encode_expression(
        {"id": {"$lt": 1}},
    )

    assert mysql_queries(expr) == ("`id` < %s", [1])

    expr = encode_expression(
        {"id": {"$lte": 1}},
    )

    assert mysql_queries(expr) == ("`id` <= %s", [1])


def test_mysql_like():
    expr = encode_expression(
        [
            {"name": {"$like": "_%"}},
            {"name": {"$not_like": "%_"}},
        ]
    )

    assert mysql_queries(expr) == (
        "`name` like %s and `name` not like %s",
        ["_%", "%_"],
    )


def test_mysql_regex():
    expr = encode_expression(
        {"name": {"$regex": "Tom"}},
    )

    assert mysql_queries(expr) == ("`name` REGEXP %s", ["Tom"])


def test_mysql_and():
    expr = encode_expression(
        {
            "$and": [
                {"id": {"$gt": 1}},
                {"name": {"$eq": "test"}},
            ],
            "id": {"$lt": 3},
        }
    )

    assert mysql_queries(expr) == (
        "`id` > %s and `name` = %s and `id` < %s",
        [1, "test", 3],
    )


def test_mysql_or():
    expr = encode_expression(
        {
            "$or": [
                {"id": {"$lt": 1}},
                {"id": {"$gt": 10}},
            ]
        }
    )

    assert mysql_queries(expr) == ("`id` < %s or `id` > %s", [1, 10])


def test_mysql_arithmetic():
    expr = encode_expression(
        {"price": {"$add": [1, 2]}},
    )
    assert mysql_queries(expr) == ("`price` + %s = %s", [1, 2]), mysql_queries(expr)

    expr = encode_expression(
        {"price": {"$sub": [1, 2]}},
    )
    assert mysql_queries(expr) == ("`price` - %s = %s", [1, 2]), mysql_queries(expr)

    expr = encode_expression(
        {"price": {"$mul": [1, 2]}},
    )
    assert mysql_queries(expr) == ("`price` * %s = %s", [1, 2]), mysql_queries(expr)

    expr = encode_expression(
        {"price": {"$floordiv": [2, 1]}},
    )
    assert mysql_queries(expr) == ("`price` / %s = %s", [2, 1]), mysql_queries(expr)

    expr = encode_expression(
        {"price": {"$truediv": [2, 1]}},
    )
    assert mysql_queries(expr) == ("`price` div %s = %s", [2, 1]), mysql_queries(expr)

    expr = encode_expression(
        {"price": {"$mod": [2, 1]}},
    )
    assert mysql_queries(expr) == ("`price` % %s = %s", [2, 1]), mysql_queries(expr)


def test_table_field():
    expr = encode_expression(
        {"user.name": "Tom"},
    )
    assert mysql_queries(expr) == ("`user`.`name` = %s", ["Tom"])
