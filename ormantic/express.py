import re
from abc import abstractmethod
from typing import Any, Dict, Iterable, List, Union, cast

from .operators import BooleanOperator, LogicOperator, Operator


class Predicate:
    def __init__(self, operator: Operator, value: Union["Predicate", Any]) -> None:
        self.operator = operator
        self.value = value

    def dict(self) -> dict:
        return decode(self)

    def __or__(self, other: "BoolExpression") -> "Predicate":
        return Predicate(LogicOperator.OR, [self, other])

    def __and__(self, other: "BoolExpression") -> "Predicate":
        return Predicate(LogicOperator.AND, [self, other])

    def __str__(self) -> str:
        return f"Predicate({self.operator},{str(self.value)})"


class BaseExpression:
    __slots__ = ("left", "predicates")

    predicates: List[Predicate]

    def __init__(self, /, left=None, operator=None, value=None, *, predicates=None):
        self.left = left
        self.predicates = []
        if operator:
            self.predicates.append(Predicate(operator, value))
        if predicates:
            self.predicates.extend(predicates)

    @abstractmethod
    def dict(self) -> dict:
        ...

    def __hash__(self):
        return id(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.left},{self.predicates})"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.left},{self.predicates})"


class BoolExpression(BaseExpression):
    def dict(self) -> dict:
        return decode(self)

    def __or__(self, other: "BoolExpression") -> Predicate:
        return Predicate(LogicOperator.OR, [self, other])

    def __and__(self, other: "BoolExpression") -> Predicate:
        return Predicate(LogicOperator.AND, [self, other])


QueryExpression = Union[BoolExpression, Predicate, Dict]


def check_predicate(obj: dict):
    return isinstance(obj, dict) and any((i in Operator) for i in obj)


def encode_expression(obj: Any) -> Union[BoolExpression, Predicate]:
    if isinstance(obj, Predicate):
        return obj
    if isinstance(obj, BoolExpression):
        return obj

    if isinstance(obj, (list, tuple)):
        return and_(*[encode_expression(i) for i in obj])

    if isinstance(obj, dict):
        express = []
        for k, v in obj.items():
            op = Operator.validate(k)
            if op:
                if op is LogicOperator.AND:  # v is sequence
                    express.extend(encode_expression(i) for i in v)
                elif op is LogicOperator.OR:  # v is sequence
                    express.append(or_(*[encode_expression(i) for i in v]))
                else:
                    # express.append(BoolExpression(k, op, v))
                    raise ValueError(f"{obj}")

            elif check_predicate(v):
                # {field: {$not: {$gt: 1}}}
                expr = BoolExpression(k)
                for op, right in v.items():
                    op = Operator(op)
                    assert op not in LogicOperator
                    if check_predicate(right):
                        expr.predicates.append(Predicate(op, encode_expression(right)))
                    else:
                        expr.predicates.append(Predicate(op, right))

                express.append(expr)
            else:
                express.append(BoolExpression(k, BooleanOperator.eq, v))

        return express[0] if len(express) == 1 else and_(*express)

    raise ValueError(f"{obj} is not BoolExpression")


def decode(expr: Union[BoolExpression, Predicate]) -> dict[str, Any]:
    if isinstance(expr, BoolExpression):
        predicates = {}
        for p in expr.predicates:
            predicates.update(decode(p))
        return {str(expr.left): predicates}

    elif isinstance(expr, Predicate):
        if expr.operator is LogicOperator.AND:
            data = {}
            for sub in cast(list[BoolExpression], expr.value):
                for k, v in decode(sub).items():
                    if k in data:
                        data[k].update(v)
                    else:
                        data[k] = v
            return data
        elif expr.operator is LogicOperator.OR:
            values = []
            for sub in cast(list[BoolExpression], expr.value):
                if isinstance(sub, Predicate) and sub.operator is LogicOperator.OR:
                    values.extend(decode(sub)[str(LogicOperator.OR)])
                else:
                    values.append(decode(sub))
            return {str(expr.operator): values}
        else:
            if isinstance(expr.value, Predicate):
                return {str(expr.operator): decode(expr.value)}
            else:
                return {str(expr.operator): expr.value}

    raise TypeError(f"{type(expr)} is not in [BoolExpression, Predicate]")


def and_(*elements: QueryExpression) -> Predicate:
    """Logical **AND** operation between multiple `BoolExpression` objects."""
    return Predicate(LogicOperator.AND, elements)


def or_(*elements: QueryExpression) -> Predicate:
    """Logical **OR** operation between multiple `BoolExpression` objects."""
    return Predicate(LogicOperator.OR, elements)


def eq(expr: BaseExpression, value: Any) -> BoolExpression:
    """Equality comparison operator."""
    return BoolExpression(expr, BooleanOperator.eq, value)


def ne(expr: BaseExpression, value: Any) -> BoolExpression:
    """Inequality comparison operator (includes documents not containing the expr)."""
    return BoolExpression(expr, BooleanOperator.ne, value)


def gt(expr: BaseExpression, value: Any) -> BoolExpression:
    """Greater than (strict) comparison operator (i.e. >)."""
    return BoolExpression(expr, BooleanOperator.gt, value)


def gte(expr: BaseExpression, value: Any) -> BoolExpression:
    """Greater than or equal comparison operator (i.e. >=)."""
    return BoolExpression(expr, BooleanOperator.gte, value)


def lt(expr: BaseExpression, value: Any) -> BoolExpression:
    """Less than (strict) comparison operator (i.e. <)."""
    return BoolExpression(expr, BooleanOperator.lt, value)


def lte(expr: BaseExpression, value: Any) -> BoolExpression:
    """Less than or equal comparison operator (i.e. <=)."""
    return BoolExpression(expr, BooleanOperator.lte, value)


def in_(expr: Any, sequence: Iterable) -> BoolExpression:
    """Select instances where `expr` is contained in `sequence`."""
    return BoolExpression(expr, BooleanOperator.IN, sequence)


def nin(expr: Any, sequence: Iterable) -> BoolExpression:
    """Select instances where `expr` is **not** contained in `sequence`."""
    return BoolExpression(expr, BooleanOperator.NIN, sequence)


def match(expr: BaseExpression, pattern: Union[re.Pattern, str]) -> BoolExpression:
    if isinstance(pattern, str):
        r = re.compile(pattern)
    else:
        r = pattern
    return BoolExpression({expr: r})


class FilterMixin:
    def __gt__(self, value: Any) -> BoolExpression:
        return self.gt(value)

    def gt(self, value: Any) -> BoolExpression:
        return BoolExpression(self, BooleanOperator.gt, value)

    def gte(self, value: Any) -> BoolExpression:
        return BoolExpression(self, BooleanOperator.gte, value)

    def __ge__(self, value: Any) -> BoolExpression:
        return self.gte(value)

    def lt(self, value: Any) -> BoolExpression:
        return BoolExpression(self, BooleanOperator.lt, value)

    def __lt__(self, value: Any) -> BoolExpression:
        return self.lt(value)

    def lte(self, value: Any) -> BoolExpression:
        return BoolExpression(self, BooleanOperator.lte, value)

    def __le__(self, value: Any) -> BoolExpression:
        return self.lte(value)

    def eq(self, value: Any) -> BoolExpression:
        return BoolExpression(self, BooleanOperator.eq, value)

    def __eq__(self, value: Any) -> BoolExpression:
        return self.eq(value)

    def ne(self, value: Any) -> BoolExpression:
        return BoolExpression(self, BooleanOperator.ne, value)

    def __ne__(self, value: Any) -> BoolExpression:  # type, ignore
        return self.ne(value)

    def in_(self, value: Iterable, strict=False) -> BoolExpression:
        return in_(self, value)

    def nin(self, value: Iterable, strict=False) -> BoolExpression:
        return nin(self, value)
