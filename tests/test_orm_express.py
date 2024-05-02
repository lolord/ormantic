from typing import cast

from ormantic import Field, FieldProxy, Model
from ormantic.express import Func, Predicate, encode


class User(Model):
    id: int = Field(primary=True, autoincrement=True)
    name: str


USER_ID = cast(FieldProxy, User.id)
X, Y, Z = 1, 2, 3


def test_arith_express():
    expr: Predicate

    expr = USER_ID + X
    assert expr.dict() == {"$add": [USER_ID, X]}
    expr = USER_ID - X
    assert expr.dict() == {"$sub": [USER_ID, X]}
    expr = USER_ID * X
    assert expr.dict() == {"$mul": [USER_ID, X]}
    expr = USER_ID / X
    assert expr.dict() == {"$truediv": [USER_ID, X]}
    expr = USER_ID // X
    assert expr.dict() == {"$floordiv": [USER_ID, X]}
    expr = USER_ID % X
    assert expr.dict() == {"$mod": [USER_ID, X]}

    expr = USER_ID + X
    assert expr.dict() == {"$add": [USER_ID, X]}
    expr = USER_ID - X
    assert expr.dict() == {"$sub": [USER_ID, X]}
    expr = USER_ID * X
    assert expr.dict() == {"$mul": [USER_ID, X]}
    expr = USER_ID / X
    assert expr.dict() == {"$truediv": [USER_ID, X]}
    expr = USER_ID // X
    assert expr.dict() == {"$floordiv": [USER_ID, X]}
    expr = USER_ID % X
    assert expr.dict() == {"$mod": [USER_ID, X]}


def test_bool_express():
    expr: Predicate

    expr = USER_ID > X
    assert expr.dict() == {"$gt": [USER_ID, X]}
    expr = USER_ID >= X
    assert expr.dict() == {"$gte": [USER_ID, X]}
    expr = USER_ID < X
    assert expr.dict() == {"$lt": [USER_ID, X]}
    expr = USER_ID <= X
    assert expr.dict() == {"$lte": [USER_ID, X]}
    expr = USER_ID == X
    assert expr.dict() == {"$eq": [USER_ID, X]}
    expr = USER_ID != X
    assert expr.dict() == {"$ne": [USER_ID, X]}

    expr = USER_ID.in_((X, Y, Z))
    assert expr.dict() == {"$in": [USER_ID, (X, Y, Z)]}
    expr = USER_ID.nin((X, Y, Z))
    assert expr.dict() == {"$nin": [USER_ID, (X, Y, Z)]}
    expr = USER_ID.or_(X)
    assert expr.dict() == {"$or": [USER_ID, X]}
    expr = USER_ID.and_(X)
    assert expr.dict() == {"$and": [USER_ID, X]}


def test_decode():
    expr: Predicate

    expr = (USER_ID + X) == Y
    assert expr.dict() == {"$eq": [{"$add": [USER_ID, X]}, Y]}
    expr = (USER_ID - X) == Y
    assert expr.dict() == {"$eq": [{"$sub": [USER_ID, X]}, Y]}
    expr = (USER_ID * X) == Y
    assert expr.dict() == {"$eq": [{"$mul": [USER_ID, X]}, Y]}
    expr = (USER_ID / X) == Y
    assert expr.dict() == {"$eq": [{"$truediv": [USER_ID, X]}, Y]}
    expr = (USER_ID // X) == Y
    assert expr.dict() == {"$eq": [{"$floordiv": [USER_ID, X]}, Y]}
    expr = (USER_ID % X) == Y
    assert expr.dict() == {"$eq": [{"$mod": [USER_ID, X]}, Y]}

    expr = (USER_ID + X) != Y
    assert expr.dict() == {"$ne": [{"$add": [USER_ID, X]}, Y]}
    expr = (USER_ID - X) != Y
    assert expr.dict() == {"$ne": [{"$sub": [USER_ID, X]}, Y]}
    expr = (USER_ID * X) != Y
    assert expr.dict() == {"$ne": [{"$mul": [USER_ID, X]}, Y]}
    expr = (USER_ID / X) != Y
    assert expr.dict() == {"$ne": [{"$truediv": [USER_ID, X]}, Y]}
    expr = (USER_ID // X) != Y
    assert expr.dict() == {"$ne": [{"$floordiv": [USER_ID, X]}, Y]}
    expr = (USER_ID % X) != Y
    assert expr.dict() == {"$ne": [{"$mod": [USER_ID, X]}, Y]}

    expr = (USER_ID > X) & (USER_ID == Y) & (USER_ID < Z)

    assert expr.dict() == {
        "$and": [
            {"$gt": [USER_ID, X]},
            {"$eq": [USER_ID, Y]},
            {"$lt": [USER_ID, Z]},
        ]
    }

    expr = (USER_ID > X) | (USER_ID == Y) & (USER_ID < Z)

    assert expr.dict() == {
        "$or": [
            {"$gt": [USER_ID, X]},
            {
                "$and": [
                    {"$eq": [USER_ID, Y]},
                    {"$lt": [USER_ID, Z]},
                ]
            },
        ]
    }

    expr = ((USER_ID > X) | (USER_ID == Y)) & (USER_ID < Z)

    assert expr.dict() == {
        "$and": [
            {
                "$or": [
                    {"$gt": [USER_ID, X]},
                    {"$eq": [USER_ID, Y]},
                ]
            },
            {"$lt": [USER_ID, Z]},
        ]
    }

    expr = (USER_ID > X) & (USER_ID == Y) | (USER_ID < Z)

    assert expr.dict() == {
        "$or": [
            {
                "$and": [
                    {"$gt": [USER_ID, X]},
                    {"$eq": [USER_ID, Y]},
                ]
            },
            {"$lt": [USER_ID, Z]},
        ]
    }

    expr = (USER_ID > X) | (USER_ID == Y) | (USER_ID < Z)

    assert expr.dict() == {
        "$or": [
            {"$gt": [USER_ID, X]},
            {"$eq": [USER_ID, Y]},
            {"$lt": [USER_ID, Z]},
        ]
    }


def test_encode():
    expr: Predicate

    expr = encode({"$eq": X})
    assert expr.dict() == {"$eq": [X]}

    expr = encode({"user.id": X})
    assert expr.dict() == {"$eq": ["user.id", X]}
    assert expr == encode(expr)

    expr = encode({"user.id": {"$mod": X}})

    assert expr.dict() == {"$mod": ["user.id", X]}

    expr = encode({"user.id": {"$mod": [X, Y]}})
    assert expr.dict() == {"$eq": [{"$mod": ["user.id", X]}, Y]}

    expr = encode({"$eq": [{"user.id": {"$mod": X}}, X]})
    assert expr.dict() == {"$eq": [{"$mod": ["user.id", X]}, X]}
    assert encode({"$eq": [{"$mod": ["user.id", X]}, X]}).dict() == {"$eq": [{"$mod": ["user.id", X]}, X]}

    expr = encode({"user.id": {"$eq": {"$add": ["user.id", 1]}}})
    assert expr.dict() == {"$eq": ["user.id", {"$add": ["user.id", 1]}]}

    expr = encode({"user.json": {"x": X}})
    assert expr.dict() == {"$eq": ["user.json", {"x": X}]}

    expr = encode({"user.id": {"$gt": X, "$lt": Y}, "user.name": {"$eq": "test"}})

    assert expr.dict() == {
        "$and": [
            {"$gt": ["user.id", X]},
            {"$lt": ["user.id", Y]},
            {"$eq": ["user.name", "test"]},
        ]
    }

    expr = encode(
        [
            {"user.id": {"$gt": X}},
            {"user.id": {"$lt": Y}},
            {
                "user.name": {"$eq": "test"},
            },
        ]
    )

    assert expr.dict() == {
        "$and": [
            {"$gt": ["user.id", X]},
            {"$lt": ["user.id", Y]},
            {"$eq": ["user.name", "test"]},
        ]
    }

    expr = encode(
        {
            "$and": [
                {"$and": [{"user.id": {"$gt": X}}, {"user.name": {"$eq": "test"}}]},
                {"user.id": {"$lt": Y}},
            ]
        }
    )

    assert expr.dict() == {
        "$and": [
            {"$gt": ["user.id", X]},
            {"$eq": ["user.name", "test"]},
            {"$lt": ["user.id", Y]},
        ]
    }

    expr = encode(
        {
            "$and": [
                {"user.id": {"$gt": X}},
                {"user.name": {"$eq": "test"}},
            ],
            "user.id": {"$lt": Y},
        }
    )

    assert expr.dict() == {
        "$and": [
            {"$gt": ["user.id", X]},
            {"$eq": ["user.name", "test"]},
            {"$lt": ["user.id", Y]},
        ]
    }

    expr = encode({"user.id": {"$gt": X, "$eq": Y, "$lt": Z}})

    assert expr.dict() == {
        "$and": [
            {"$gt": ["user.id", X]},
            {"$eq": ["user.id", Y]},
            {"$lt": ["user.id", Z]},
        ]
    }

    expr = encode(
        {
            "$or": [{"user.id": {"$gt": X}}, {"user.id": {"$eq": Y}}],
            "user.id": {"$lt": Z},
        }
    )
    assert expr.dict() == {
        "$and": [
            {"$or": [{"$gt": ["user.id", X]}, {"$eq": ["user.id", Y]}]},
            {"$lt": ["user.id", Z]},
        ]
    }

    expr = encode({"$or": [{"user.id": {"$gt": X, "$eq": Y}}, {"user.id": {"$lt": Z}}]})
    assert expr.dict() == {
        "$or": [
            {"$and": [{"$gt": ["user.id", X]}, {"$eq": ["user.id", Y]}]},
            {"$lt": ["user.id", Z]},
        ]
    }

    expr = encode(
        {
            "$or": [
                {"user.id": {"$gt": X}},
                {"user.id": {"$eq": Y}},
                {"user.id": {"$lt": Z}},
            ]
        }
    )
    assert expr.dict() == {
        "$or": [
            {"$gt": ["user.id", X]},
            {"$eq": ["user.id", Y]},
            {"$lt": ["user.id", Z]},
        ]
    }


def test_func_method():
    assert Func.eq(USER_ID, X).dict() == {"$eq": [USER_ID, X]}
    assert Func.ne(USER_ID, X).dict() == {"$ne": [USER_ID, X]}
    assert Func.gt(USER_ID, X).dict() == {"$gt": [USER_ID, X]}
    assert Func.gte(USER_ID, X).dict() == {"$gte": [USER_ID, X]}
    assert Func.lt(USER_ID, X).dict() == {"$lt": [USER_ID, X]}
    assert Func.lte(USER_ID, X).dict() == {"$lte": [USER_ID, X]}
    assert Func.in_(USER_ID, (X, Y, Y)).dict() == {"$in": [USER_ID, (X, Y, Y)]}
    assert Func.nin(USER_ID, (X, Y, Y)).dict() == {"$nin": [USER_ID, (X, Y, Y)]}
    assert Func.match(User.name, "test_%").dict() == {"$regex": [User.name, "test_%"]}

    assert Func.and_(Func.gt(USER_ID, X), Func.lt(USER_ID, Y)).dict() == {
        "$and": [{"$gt": [USER_ID, X]}, {"$lt": [USER_ID, Y]}]
    }
    assert Func.or_(Func.eq(USER_ID, X), Func.eq(USER_ID, Y), Func.eq(USER_ID, Z)).dict() == {
        "$or": [
            {"$eq": [USER_ID, X]},
            {"$eq": [USER_ID, Y]},
            {"$eq": [USER_ID, Z]},
        ]
    }

    assert Func.add(USER_ID, X).dict() == {"$add": [USER_ID, X]}
    assert Func.sub(USER_ID, X).dict() == {"$sub": [USER_ID, X]}
    assert Func.mul(USER_ID, X).dict() == {"$mul": [USER_ID, X]}
    assert Func.truediv(USER_ID, X).dict() == {"$truediv": [USER_ID, X]}
    assert Func.floordiv(USER_ID, X).dict() == {"$floordiv": [USER_ID, X]}
    assert Func.mod(USER_ID, X).dict() == {"$mod": [USER_ID, X]}
