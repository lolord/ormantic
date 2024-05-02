import re
from typing import Any, Iterable, TypeAlias, Union, final

from ormantic.errors import PredicateEncodeError
from ormantic.operators import ArithmeticOperator, LogicOperator, Operator

PredicateAny: TypeAlias = Union["Predicate", Any]


@final
class Func:
    @staticmethod
    def and_(*elements: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.AND, elements)

    @staticmethod
    def or_(*elements: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.OR, elements)

    @staticmethod
    def eq(expr: PredicateAny, value: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.eq, [expr, value])

    @staticmethod
    def ne(expr: PredicateAny, value: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.ne, [expr, value])

    @staticmethod
    def gt(expr: PredicateAny, value: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.gt, [expr, value])

    @staticmethod
    def gte(expr: PredicateAny, value: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.gte, [expr, value])

    @staticmethod
    def lt(expr: PredicateAny, value: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.lt, [expr, value])

    @staticmethod
    def lte(expr: PredicateAny, value: PredicateAny) -> "Predicate":
        return Predicate(LogicOperator.lte, [expr, value])

    @staticmethod
    def in_(expr: PredicateAny, sequence: Iterable) -> "Predicate":
        return Predicate(LogicOperator.IN, [expr, tuple(sequence)])

    @staticmethod
    def nin(expr: PredicateAny, sequence: Iterable) -> "Predicate":
        return Predicate(LogicOperator.NIN, [expr, tuple(sequence)])

    @staticmethod
    def match(expr: PredicateAny, pattern: Union[re.Pattern, str]) -> "Predicate":
        return Predicate(LogicOperator.REGEX, [expr, pattern])

    @staticmethod
    def add(left: PredicateAny, right: PredicateAny) -> "Predicate":
        return Predicate(ArithmeticOperator.add, [left, right])

    @staticmethod
    def sub(left: PredicateAny, right: PredicateAny) -> "Predicate":
        return Predicate(ArithmeticOperator.sub, [left, right])

    @staticmethod
    def mul(left: PredicateAny, right: PredicateAny) -> "Predicate":
        return Predicate(ArithmeticOperator.mul, [left, right])

    @staticmethod
    def truediv(left: PredicateAny, right: PredicateAny) -> "Predicate":
        return Predicate(ArithmeticOperator.truediv, [left, right])

    @staticmethod
    def floordiv(left: PredicateAny, right: PredicateAny) -> "Predicate":
        return Predicate(ArithmeticOperator.floordiv, [left, right])

    @staticmethod
    def mod(left: PredicateAny, right: PredicateAny) -> "Predicate":
        return Predicate(ArithmeticOperator.mod, [left, right])


class ArithmeticMixin:
    def __add__(self, other: Any) -> "Predicate":
        return self.add(other)

    def __sub__(self, other: Any) -> "Predicate":
        return self.sub(other)

    def __mul__(self, other: Any) -> "Predicate":
        return self.mul(other)

    def __truediv__(self, other: Any) -> "Predicate":
        return self.truediv(other)

    def __floordiv__(self, other: Any) -> "Predicate":
        return self.floordiv(other)

    def __mod__(self, other: Any) -> "Predicate":
        return self.mod(other)

    def add(self, other: Any) -> "Predicate":
        return Func.add(self, other)

    def sub(self, other: Any) -> "Predicate":
        return Func.sub(self, other)

    def mul(self, other: Any) -> "Predicate":
        return Func.mul(self, other)

    def truediv(self, other: Any) -> "Predicate":
        return Func.truediv(self, other)

    def floordiv(self, other: Any) -> "Predicate":
        return Func.floordiv(self, other)

    def mod(self, other: Any) -> "Predicate":
        return Func.mod(self, other)


class LogicMixin:
    def __gt__(self, value: Any) -> "Predicate":
        return self.gt(value)

    def __ge__(self, value: Any) -> "Predicate":
        return self.gte(value)

    def __lt__(self, value: Any) -> "Predicate":
        return self.lt(value)

    def __le__(self, value: Any) -> "Predicate":
        return self.lte(value)

    def __eq__(self, value: Any) -> "Predicate":  # type: ignore
        return self.eq(value)

    def __ne__(self, value: Any) -> "Predicate":  # type: ignore
        return self.ne(value)

    def __or__(self, other: Any) -> "Predicate":
        return self.or_(other)

    def __and__(self, other: Any) -> "Predicate":
        return self.and_(other)

    def gt(self, value: Any) -> "Predicate":
        return Func.gt(self, value)

    def gte(self, value: Any) -> "Predicate":
        return Func.gte(self, value)

    def lte(self, value: Any) -> "Predicate":
        return Func.lte(self, value)

    def lt(self, value: Any) -> "Predicate":
        return Func.lt(self, value)

    def eq(self, value: Any) -> "Predicate":
        return Func.eq(self, value)

    def ne(self, value: Any) -> "Predicate":
        return Func.ne(self, value)

    def in_(self, value: Iterable) -> "Predicate":
        return Func.in_(self, value)

    def nin(self, value: Iterable) -> "Predicate":
        return Func.nin(self, value)

    def or_(self, value: Any) -> "Predicate":
        return Func.or_(self, value)

    def and_(self, value: Any) -> "Predicate":
        return Func.and_(self, value)


class Predicate(LogicMixin, ArithmeticMixin):
    def __init__(self, operator: Operator, values: list | tuple) -> None:
        _values = []

        for i in values:
            if isinstance(i, Predicate) and operator == i.operator and len(i.values) > 1:
                _values.extend(i.values)
            else:
                _values.append(i)
        self.operator: Operator = operator
        self.values: list = _values

    def dict(self) -> dict:
        return {str(self.operator): [v.dict() if isinstance(v, Predicate) else v for v in self.values]}

    def __str__(self) -> str:
        return f"Predicate({self.operator},{str(self.values)})"


def check_predicate(obj: Any) -> bool:
    if isinstance(obj, dict):
        if all((i in Operator) for i in obj):
            return True
        elif all(check_predicate(v) for v in obj.values()):
            return True
    return False


def validate_predicate_value(v: Any) -> Predicate | Any:
    return encode(v) if check_predicate(v) else v


def encode(obj: Any) -> Predicate:
    if isinstance(obj, Predicate):
        return obj

    if isinstance(obj, (list, tuple)):
        return Func.and_(*[encode(i) for i in obj])

    if isinstance(obj, dict):
        if len(obj) > 1:
            return encode(list(map(dict, zip(obj.items()))))

        express: list[Predicate] = []

        field, value = obj.popitem()
        op = Operator.validate(field)
        if op:
            if op is LogicOperator.AND:  # v is sequence
                express.extend(encode(i) for i in value)
            elif op is LogicOperator.OR:  # v is sequence
                express.append(Func.or_(*[encode(i) for i in value]))
            elif op in (LogicOperator.IN, LogicOperator.NIN):  # v is sequence
                express.append(Predicate(op, [value[0], tuple(value[1])]))
            elif op in ArithmeticOperator:
                f, v = value
                if isinstance(v, list):
                    assert len(v) == 2
                    e = Predicate(op, [f, validate_predicate_value(v[0])])
                    express.append(Predicate(LogicOperator.eq, [e, validate_predicate_value(v[1])]))
                else:
                    express.append(Predicate(op, [f, validate_predicate_value(v)]))
            else:
                if isinstance(value, list):
                    express.append(Predicate(op, [validate_predicate_value(i) for i in value]))
                else:
                    express.append(
                        Predicate(
                            op,
                            [validate_predicate_value(value)],
                        )
                    )

        elif check_predicate(value):
            # {field: {$: {$: x}}}
            for k, v in value.items():
                e = encode({k: [field, v]})
                express.append(e)
        else:
            express.append(Predicate(LogicOperator.eq, [field, value]))

        return express[0] if len(express) == 1 else Func.and_(*express)

    raise PredicateEncodeError(str(obj))  # pragma: no cover
