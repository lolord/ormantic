from typing import Any, List, cast

from ormantic.errors import OperatorError
from ormantic.express import Predicate
from ormantic.fields import CountField, DistinctField, FieldProxy
from ormantic.model import Model
from ormantic.operators import ArithmeticOperator, LogicOperator, Operator
from ormantic.query import Delete, Insert, Query, Update
from ormantic.typing import ABCField, ABCTable, ModelType

like, not_like = Operator("$like"), Operator("$not_like")
LogicOperator.registers(like, not_like)


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
    LogicOperator.gt: ">",
    LogicOperator.gte: ">=",
    LogicOperator.lt: "<",
    LogicOperator.lte: "<=",
    LogicOperator.eq: "=",
    LogicOperator.ne: "!=",
    LogicOperator.NIN: "nin",
    LogicOperator.IN: "in",
    LogicOperator.REGEX: "REGEXP",
    LogicOperator.OR: "or",
    LogicOperator.AND: "and",
    like: "like",
    not_like: "not like",
}


def mysql_predicate_tokens(expr: Any, sql: list[str], params: List) -> None:
    if isinstance(expr, str):
        sql.append(".".join(f"`{i}`" for i in expr.split(".")))

    elif isinstance(expr, FieldProxy):
        # quote name
        sql.append(f"`{expr.table.orm_name()}`.`{expr.orm_name()}`")

    elif isinstance(expr, Predicate):
        if expr.operator in LogicOperator:
            if expr.operator in (LogicOperator.AND, LogicOperator.OR):
                predicates = cast(list[Predicate], expr.values)
                for i in predicates:
                    bracket = i.operator in (LogicOperator.AND, LogicOperator.OR) and i.operator != expr.operator
                    # TODO
                    if bracket:
                        sql.append("(")
                    mysql_predicate_tokens(i, sql, params)
                    if bracket:
                        sql.append(")")
                    sql.append(symbols[expr.operator])
                sql.pop()
            elif (
                expr.operator in (LogicOperator.eq, LogicOperator.ne)
                and len(expr.values) == 2
                and expr.values[1] is None
            ):
                mysql_predicate_tokens(expr.values[0], sql, params)
                if expr.operator == LogicOperator.eq:
                    sql.append("is null")
                else:
                    sql.append("is not null")
            else:
                mysql_predicate_tokens(expr.values[0], sql, params)
                sql.append(symbols[expr.operator])
                sql.append(Config.PARAMSTYLE)
                params.append(expr.values[1])

        elif expr.operator in ArithmeticOperator:
            field, value = expr.values
            mysql_predicate_tokens(field, sql, params)
            sql.append(symbols[expr.operator])
            sql.append(Config.PARAMSTYLE)

            params.append(value)

        else:  # pragma: no cover
            raise OperatorError(expr.operator)
    else:  # pragma: no cover
        raise ValueError(f"Expression not supported: {expr}")


def predicate_sql_params(expr: Predicate) -> tuple[str, List]:
    sql: list[str] = []
    params: list[Any] = []
    mysql_predicate_tokens(expr, sql, params)
    return " ".join(sql), params


def mysql_token(value: Any) -> str:
    if isinstance(value, ABCField):
        if isinstance(value, DistinctField):
            return f"distinct {mysql_token(value.field)}"
        if isinstance(value, CountField):
            return f"count({mysql_token(value.field)})"
        else:
            name = value.orm_name()
            if name in ("1", "*"):
                return name
            return f"{mysql_token(value.table)}.`{name}`"
    elif isinstance(value, ABCTable):
        return f"`{value.orm_name()}`"
    else:  # pragma: no cover
        raise ValueError(value)


def sql_params(value: Any) -> tuple[str, tuple[Any, ...]]:
    if isinstance(value, Query):
        return query_sql_params(value)
    elif isinstance(value, Delete):
        return delete_sql_params(value)
    elif isinstance(value, Update):
        return update_sql_params(value)
    elif isinstance(value, Insert):
        return insert_sql_params(value)
    elif isinstance(value, Model):
        return upsert_sql_params(value)
    elif isinstance(value, str):
        return (value, ())
    else:  # pragma: no cover
        raise ValueError(value)


def query_sql_params(query: Query[ModelType]) -> tuple[str, tuple[Any, ...]]:
    sql: list[str] = []
    params: list[Any] = []
    sql.append("select")
    sql.append(", ".join(mysql_token(field) for field in query.fields))

    sql.append("from")
    sql.append(f"`{query.table.orm_name()}`")

    if query.filters:
        sql.append("where")

        mysql_predicate_tokens(Predicate(LogicOperator.AND, query.filters), sql, params)
    if query.sorts:
        sql.append("order")
        sql.append("by")
        for field, sort in query.sorts:
            sql.append(mysql_token(field))
            sql.append("asc," if sort else "desc,")
        # remove tail comma
        sql.append(sql.pop()[:-1])
    if query.rows:
        sql.append("limit")
        if query.offset:
            sql.append(f"{query.offset}, {query.rows}")
        else:
            sql.append(f"{query.rows}")
    return " ".join(sql), tuple(params)


def delete_sql_params(query: Delete[ModelType]) -> tuple[str, tuple[Any, ...]]:
    sql: list[str] = []
    params: list[Any] = []
    sql.append("delete")
    sql.append("from")
    sql.append(f"`{query.table.orm_name()}`")

    if query.filters:
        sql.append("where")
        mysql_predicate_tokens(Predicate(LogicOperator.AND, query.filters), sql, params)

    return " ".join(sql), tuple(params)


def update_sql_params(query: Update[ModelType]) -> tuple[str, tuple[Any, ...]]:
    sql: list[str] = []
    params: list[Any] = []
    sql.append("update")
    sql.append(f"`{query.table.orm_name()}`")
    sql.append("set")

    for k, v in query.value.items():
        field = query.table.get_field(k)
        sql.append(mysql_token(field))
        sql.append("=")
        sql.append(f"{Config.PARAMSTYLE},")
        params.append(v)

    # remove tail comma
    sql.append(sql.pop()[:-1])

    if query.filters:
        sql.append("where")
        mysql_predicate_tokens(Predicate(LogicOperator.AND, query.filters), sql, params)

    return " ".join(sql), tuple(params)


def insert_sql_params(query: Insert[ModelType]) -> tuple[str, tuple[Any, ...]]:
    fields = query.table.get_fields()

    sql = []
    sql.append("insert")
    sql.append("into")
    sql.append(f"`{query.table.orm_name()}`")
    sql.append(f"({', '.join(mysql_token(field) for field in fields.values())})")
    sql.append("values")
    sql.append(f"({', '.join(Config.PARAMSTYLE for _ in fields)})")

    params = []
    for value in query.values:
        params.append(tuple(getattr(value, i) for i in fields))

    return " ".join(sql), tuple(params)


def upsert_sql_params(value: Model) -> tuple[str, tuple[Any, ...]]:
    table = type(value)
    fields = table.get_fields()

    sql: list[str] = []
    params: list[Any] = []

    sql.append("insert")
    sql.append("into")
    sql.append(f"`{table.orm_name()}`")
    sql.append(f"({', '.join(mysql_token(field) for field in fields.values())})")
    sql.append("values")

    sql.append("(")
    for i in fields:
        sql.append(f"{Config.PARAMSTYLE},")
        params.append(getattr(value, i))
    # remove tail comma
    sql.append(sql.pop()[:-1])
    sql.append(")")

    sql.append("ON")
    sql.append("DUPLICATE")
    sql.append("KEY")
    sql.append("UPDATE")

    for i, field in table.__petty_keys__.items():
        sql.append(mysql_token(field))
        sql.append("=")
        sql.append(f"{Config.PARAMSTYLE},")
        params.append(getattr(value, i))
    sql.append(sql.pop()[:-1])

    return " ".join(sql), tuple(params)
