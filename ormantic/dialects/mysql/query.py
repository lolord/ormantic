from typing import Any, Generic, List, Tuple, cast

from ormantic.express import BoolExpression, Predicate
from ormantic.fields import FieldProxy
from ormantic.model import Model
from ormantic.operators import (
    ArithmeticOperator,
    BooleanOperator,
    LogicOperator,
    Operator,
)
from ormantic.query import (
    BaseDelete,
    BaseInsert,
    BaseQuery,
    BaseUpdate,
    ModelType,
)

like, not_like = Operator("$like"), Operator("$not_like")
BooleanOperator.registers(like, not_like)


class ParamStyle:
    """PEP249 ParamStyle"""

    Qmark = "?"  # Question mark style, e.g. ...WHERE name=?
    # Numeric, positional style, e.g. ...WHERE name=:1
    # named	Named style, e.g. ...WHERE name=:name
    Format = "%s"  # ANSI C printf format codes, e.g. ...WHERE name=%s
    # pyformat	Python extended format codes, e.g. ...WHERE name=%(name)s


class Config:
    DEFAULT_CHARSET = "utf8mb4"

    PARAMSTYLE = ParamStyle.Format


symbols = {
    ArithmeticOperator.add: "+",
    ArithmeticOperator.sub: "-",
    ArithmeticOperator.mul: "*",
    ArithmeticOperator.truediv: "div",
    ArithmeticOperator.floordiv: "/",
    ArithmeticOperator.mod: "%",
    BooleanOperator.gt: ">",
    BooleanOperator.gte: ">=",
    BooleanOperator.lt: "<",
    BooleanOperator.lte: "<=",
    BooleanOperator.eq: "=",
    BooleanOperator.ne: "!=",
    BooleanOperator.NIN: "nin",
    BooleanOperator.IN: "in",
    BooleanOperator.REGEX: "REGEXP",
    LogicOperator.OR: "or",
    LogicOperator.AND: "and",
    like: "like",
    not_like: "not like",
}


def _mysql_queries(expr: Any, sql: List[str], params: List):
    # print('expr', expr)
    if isinstance(expr, str):
        sql.append(".".join(f"`{i}`" for i in expr.split(".")))
    # elif isinstance(expr, (tuple, list)):

    elif isinstance(expr, FieldProxy):
        # quote name
        # print("proxy", expr, +expr)
        sql.append(f"`{+expr}`")

    elif isinstance(expr, BoolExpression):
        _mysql_queries(expr.left, sql, params)
        for p in expr.predicates:
            _mysql_queries(p, sql, params)

    elif isinstance(expr, Predicate):
        if expr.operator in LogicOperator:
            predicates = cast(List[Predicate], expr.value)
            for i in predicates:
                bracket = isinstance(i, LogicOperator) and i.operator != expr.operator
                if bracket:
                    sql.append("(")
                _mysql_queries(i, sql, params)
                if bracket:
                    sql.append(")")
                sql.append(symbols[expr.operator])
            sql.pop()
        elif expr.operator in ArithmeticOperator:
            sql.append(symbols[expr.operator])
            sql.append(Config.PARAMSTYLE)
            sql.append("=")
            sql.append(Config.PARAMSTYLE)
            params.extend(cast(list, expr.value))

        elif expr.operator == BooleanOperator.eq and expr.value is None:
            sql.append("is null")
        elif expr.operator == BooleanOperator.ne and expr.value is None:
            sql.append("is not null")
        else:
            sql.append(symbols[expr.operator])
            sql.append(Config.PARAMSTYLE)
            params.append(expr.value)
    else:
        raise ValueError()


def mysql_queries(expr: Any) -> Tuple[str, List]:
    sql = []
    params = []
    _mysql_queries(expr, sql, params)
    return " ".join(sql), params


class MysqlMixin:
    def sql_params(self) -> tuple[str, tuple[Any, ...]]:
        ...


class Query(BaseQuery, MysqlMixin, Generic[ModelType]):
    def sql_params(self) -> tuple[str, tuple[Any, ...]]:
        sql = []
        params = []
        sql.append("select")
        for field in self.fields:
            sql.append(f"`{+self.table}`.`{+field}`,")
        # remove tail comma
        sql.append(sql.pop()[:-1])
        sql.append("from")
        sql.append(f"`{+self.table}`")
        if self.filters:
            sql.append("where")
            _mysql_queries(Predicate(LogicOperator.AND, self.filters), sql, params)
        if self.sorts:
            sql.append("order")
            sql.append("by")
            for field, sort in self.sorts:
                sql.append(f"`{+self.table}`.`{+field}`")
                sql.append("asc," if sort else "desc,")
            # remove tail comma
            sql.append(sql.pop()[:-1])
        if self.rows:
            sql.append("limit")
            if self.offset:
                sql.append(f"{self.offset}, {self.rows}")
            else:
                sql.append(f"{self.rows}")
        return " ".join(sql), tuple(params)


class Delete(BaseDelete, MysqlMixin):
    def sql_params(self) -> tuple[str, tuple[Any, ...]]:
        sql = []
        params = []
        sql.append("delete")
        sql.append("from")
        sql.append(f"`{+self.table}`")

        if self.filters:
            sql.append("where")
            _mysql_queries(Predicate(LogicOperator.AND, self.filters), sql, params)

        return " ".join(sql), tuple(params)


class Update(BaseUpdate, MysqlMixin):
    def sql_params(self) -> tuple[str, tuple[Any, ...]]:
        sql = []
        params = []
        sql.append("update")
        sql.append(f"`{+self.table}`")
        sql.append("set")

        for k, v in self.value.items():
            field = self.table.get_field(k)
            sql.append(f"`{+self.table}`.`{+field}`")
            sql.append("=")
            sql.append(f"{Config.PARAMSTYLE},")
            params.append(v)

        # remove tail comma
        sql.append(sql.pop()[:-1])

        if self.filters:
            sql.append("where")
            _mysql_queries(Predicate(LogicOperator.AND, self.filters), sql, params)

        return " ".join(sql), tuple(params)


class Insert(BaseInsert, MysqlMixin):
    def sql_params(self) -> tuple[str, tuple[Any, ...]]:
        sql = []
        params = []
        sql.append("insert")
        sql.append("into")
        sql.append(f"`{+self.table}`")
        sql.append("(")

        field_names = []
        for field_name, field in self.table.get_fields().items():
            field_names.append(field_name)
            sql.append(f"`{+self.table}`.`{+field}`,")

        # remove tail comma
        sql.append(sql.pop()[:-1])
        sql.append(")")

        sql.append("VALUES")

        sql.append(f"({Config.PARAMSTYLE})")

        for value in self.values:
            params.append(tuple(value[i] for i in field_names))

        # remove tail comma
        sql.append(sql.pop()[:-1])

        return " ".join(sql), tuple(params)


class UpInsert(MysqlMixin):
    def __init__(self, value: Model):
        self.value = value

    def sql_params(self) -> tuple[str, tuple[Any, ...]]:
        sql = []
        params = []
        table = type(self.value)
        sql.append("insert")
        sql.append("into")
        sql.append(f"`{+table}`")
        sql.append("(")

        field_names = []
        for field_name, field in table.get_fields().items():
            field_names.append(field_name)
            sql.append(f"`{+table}`.`{+field}`,")

        # remove tail comma
        sql.append(sql.pop()[:-1])
        sql.append(")")

        sql.append("VALUES")

        sql.append("(")
        for i in field_names:
            sql.append(f"{Config.PARAMSTYLE},")
            params.append(getattr(self.value, i))
        # remove tail comma
        sql.append(sql.pop()[:-1])
        sql.append(")")

        sql.append("ON")
        sql.append("DUPLICATE")
        sql.append("KEY")
        sql.append("UPDATE")

        for i, field in table.__petty_keys__.items():
            sql.append(f"`{+table}`.`{+field}`")
            sql.append("=")
            sql.append(f"{Config.PARAMSTYLE},")
            params.append(getattr(self.value, i))
        sql.append(sql.pop()[:-1])

        return " ".join(sql), tuple(params)
