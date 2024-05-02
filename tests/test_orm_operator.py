import pytest

from ormantic.operators import ArithmeticOperator, LogicOperator, Operator


def test_operator():
    assert len(list(Operator)) >= len(list(LogicOperator)) + len(list(ArithmeticOperator))
    assert LogicOperator.eq in LogicOperator
    assert LogicOperator.eq in Operator
    assert LogicOperator.eq == Operator("$eq")
    assert "$eq" in Operator  # type: ignore
    assert "$eq" == Operator("$eq")

    assert LogicOperator["$eq"] == LogicOperator.eq
    assert LogicOperator["eq"] == LogicOperator.eq

    assert Operator("$ne") == "$ne"
    assert -Operator("$eq") == Operator("$ne")

    xxx = Operator("$xxx")
    with pytest.raises(ValueError):
        -xxx  # type: ignore

    assert LogicOperator.eq != LogicOperator.ne
    assert ("$eq" == LogicOperator.ne) is False
