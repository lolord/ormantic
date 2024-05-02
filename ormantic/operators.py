from typing import ClassVar, Iterator, Optional, no_type_check

OPERATOR_PREFIX = "$"


class OperatorMeta(type):
    __operators__: ClassVar[dict[str, "Operator"]] = {}

    def __contains__(self, obj: "Operator") -> bool:
        return obj in self.__operators__

    def __iter__(self) -> Iterator["Operator"]:
        return (self.__operators__[name] for name in self.__operators__)

    def __call__(self, operator: str) -> "Operator":
        ops: dict[str, "Operator"] = getattr(self, "__operators__")
        if operator in ops:
            return ops[operator]
        else:
            ops[operator] = super().__call__(operator)
            return ops[operator]


class Operator(metaclass=OperatorMeta):
    def __new__(cls: type, operator: str) -> "Operator":
        if not operator.startswith(OPERATOR_PREFIX):
            raise ValueError(f"operator must start with `{OPERATOR_PREFIX}`:{operator}")

        instance: Operator = super().__new__(cls)  # type: ignore
        Operator.__operators__[operator] = instance
        return Operator.__operators__[operator]

    def __init__(self, operator: str):
        self.operator = operator

    def __str__(self) -> str:
        return self.operator

    def __repr__(self) -> str:
        return f'Operator("{self.operator}")'

    def __hash__(self) -> int:
        return hash(self.operator)

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, Operator):
            return self.operator == __o.operator
        return self.operator == __o

    @staticmethod
    def validate(operator: str) -> Optional["Operator"]:
        try:
            return Operator(operator)
        except ValueError:
            return None

    def __neg__(self) -> "Operator":
        global negative
        if self in negative:
            return negative[self]
        raise ValueError(f"Not exists neg:{self}")


class OperatorGroup(type):
    __operators__: dict[str, Operator]

    def __contains__(cls, obj: Operator) -> bool:
        return obj in cls.__operators__

    def __iter__(cls) -> Iterator[Operator]:
        return (cls.__operators__[name] for name in cls.__operators__)

    @no_type_check
    def __new__(metacls, cls, bases, classdict, **kwds):
        new_cls = super().__new__(metacls, cls, bases, classdict, **kwds)
        operators = {str(v): v for v in classdict.values() if isinstance(v, Operator)}
        setattr(new_cls, "__operators__", operators)
        return new_cls

    def __getitem__(cls, name: str) -> Operator:  # pragma: no cover
        name = name if name.startswith(OPERATOR_PREFIX) else OPERATOR_PREFIX + name
        return cls.__operators__[name]

    def registers(cls, *operators: Operator) -> None:
        for operator in operators:
            if operator not in cls.__operators__:
                cls.__operators__[str(operator)] = operator
            else:  # pragma: no cover
                raise ValueError(f"{operator} already exists in the {cls}")


class ArithmeticOperator(metaclass=OperatorGroup):
    add = Operator("$add")
    sub = Operator("$sub")
    mul = Operator("$mul")
    truediv = Operator("$truediv")
    floordiv = Operator("$floordiv")
    mod = Operator("$mod")


class LogicOperator(metaclass=OperatorGroup):
    gt = Operator("$gt")
    gte = Operator("$gte")
    lt = Operator("$lt")
    lte = Operator("$lte")
    eq = Operator("$eq")
    ne = Operator("$ne")
    IN = Operator("$in")
    NIN = Operator("$nin")
    REGEX = Operator("$regex")
    AND = Operator("$and")
    OR = Operator("$or")


negative = {
    LogicOperator.gt: LogicOperator.lte,
    LogicOperator.gte: LogicOperator.lt,
    LogicOperator.lt: LogicOperator.gte,
    LogicOperator.lte: LogicOperator.gt,
    LogicOperator.eq: LogicOperator.ne,
    LogicOperator.ne: LogicOperator.eq,
    LogicOperator.NIN: LogicOperator.IN,
    LogicOperator.IN: LogicOperator.NIN,
}


operators = {str(i): i for i in LogicOperator}
operators.update((str(i), i) for i in ArithmeticOperator)
