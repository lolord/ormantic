from ormantic.express import BoolExpression, encode_expression
from ormantic.fields import Field
from ormantic.model import Model


class User(Model):
    __table__ = "user"
    id: int = Field(primary=True, autoincrement=True)
    name: str


def test_express():
    expr: BoolExpression

    expr = User.id + 2
    print(type(expr))
    print(expr.dict())
    assert expr.dict() == {"user.id": {"$add": 2}}
    expr = User.id - 2
    assert expr.dict() == {"user.id": {"$sub": 2}}
    expr = User.id * 2
    assert expr.dict() == {"user.id": {"$mul": 2}}
    expr = User.id / 2
    assert expr.dict() == {"user.id": {"$truediv": 2}}
    expr = User.id // 2
    assert expr.dict() == {"user.id": {"$floordiv": 2}}
    expr = User.id % 2
    assert expr.dict() == {"user.id": {"$mod": 2}}

    expr = (User.id + 2) == 1
    assert expr.dict() == {"user.id": {"$add": [2, 1]}}
    expr = (User.id - 2) == 1
    assert expr.dict() == {"user.id": {"$sub": [2, 1]}}
    expr = (User.id * 2) == 1
    assert expr.dict() == {"user.id": {"$mul": [2, 1]}}
    expr = (User.id / 2) == 1
    assert expr.dict() == {"user.id": {"$truediv": [2, 1]}}
    expr = (User.id // 2) == 1
    assert expr.dict() == {"user.id": {"$floordiv": [2, 1]}}
    expr = (User.id % 2) == 1
    assert expr.dict() == {"user.id": {"$mod": [2, 1]}}

    expr = (User.id > 1) & (User.id == 2) & (User.id < 3)
    assert expr.dict() == {"user.id": {"$gt": 1, "$eq": 2, "$lt": 3}}

    expr = ((User.id > 1) | (User.id == 2)) & (User.id < 3)
    assert expr.dict() == {
        "$or": [{"user.id": {"$gt": 1}}, {"user.id": {"$eq": 2}}],
        "user.id": {"$lt": 3},
    }

    expr = (User.id > 1) & (User.id == 2) | (User.id < 3)
    assert expr.dict() == {
        "$or": [{"user.id": {"$gt": 1, "$eq": 2}}, {"user.id": {"$lt": 3}}]
    }

    expr = (User.id > 1) | (User.id == 2) | (User.id < 3)
    assert expr.dict() == {
        "$or": [
            {"user.id": {"$gt": 1}},
            {"user.id": {"$eq": 2}},
            {"user.id": {"$lt": 3}},
        ]
    }


def test_encode_expression():
    expr = encode_expression({"user.id": {"$mod": 2}})
    assert expr.dict() == {"user.id": {"$mod": 2}}

    expr = encode_expression({"user.id": {"$mod": [2, 1]}})
    assert expr.dict() == {"user.id": {"$mod": [2, 1]}}

    simplified = {"user.id": {"$gt": "1", "$lt": "3"}, "user.name": {"$eq": "test"}}

    expr = encode_expression(simplified)
    assert simplified == expr.dict()

    expr = encode_expression(
        {
            "$and": [
                {"$and": [{"user.id": {"$gt": "1"}}, {"user.name": {"$eq": "test"}}]},
                {"user.id": {"$lt": "3"}},
            ]
        }
    )
    assert simplified == expr.dict()

    expr = encode_expression(
        {
            "$and": [
                {"user.id": {"$gt": "1"}},
                {"user.name": {"$eq": "test"}},
            ],
            "user.id": {"$lt": "3"},
        }
    )
    assert simplified == expr.dict()

    expr = encode_expression(
        [
            {"user.id": {"$gt": "1"}, "user.name": {"$eq": "test"}},
            {"user.id": {"$lt": "3"}},
        ]
    )

    assert simplified == expr.dict()

    expr = {"user.id": {"$gt": 1, "$eq": 2, "$lt": 3}}
    assert encode_expression(expr).dict() == expr

    expr = {
        "$or": [{"user.id": {"$gt": 1}}, {"user.id": {"$eq": 2}}],
        "user.id": {"$lt": 3},
    }

    assert encode_expression(expr).dict() == expr

    expr = {"$or": [{"user.id": {"$gt": 1, "$eq": 2}}, {"user.id": {"$lt": 3}}]}
    assert encode_expression(expr).dict() == expr

    expr = {
        "$or": [
            {"user.id": {"$gt": 1}},
            {"user.id": {"$eq": 2}},
            {"user.id": {"$lt": 3}},
        ]
    }
    assert encode_expression(expr).dict() == expr


# test_express()
# test_encode_expression()
