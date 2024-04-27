from typing import Any, List, Tuple, cast

from ormantic.errors import OperatorUnregisteredError
from ormantic.express import Predicate
from ormantic.fields import CountField, DistinctField, FieldProxy
from ormantic.model import Model
from ormantic.operators import ArithmeticOperator, LogicOperator, Operator
from ormantic.query import Delete, Insert, Query, Update
from ormantic.typing import ABCField, ABCTable

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


def mysql_predicate_tokens(expr: Any, sql: List[str], params: List):
    if isinstance(expr, str):
        sql.append(".".join(f"`{i}`" for i in expr.split(".")))

    elif isinstance(expr, FieldProxy):
        # quote name
        sql.append(f"`{+expr.table}`.`{+expr}`")

    elif isinstance(expr, Predicate):
        if expr.operator in LogicOperator:
            if expr.operator in (LogicOperator.AND, LogicOperator.OR):
                predicates = cast(List[Predicate], expr.values)
                for i in predicates:
                    bracket = (
                        isinstance(i, LogicOperator) and i.operator != expr.operator
                    )
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

        else:
            raise OperatorUnregisteredError(expr.operator)
    else:
        raise ValueError(f"Expression not supported: {expr}")


def predicate_sql_params(expr: Predicate) -> Tuple[str, List]:
    sql = []
    params = []
    mysql_predicate_tokens(expr, sql, params)
    return " ".join(sql), params


def mysql_token(value: Any) -> str:
    if isinstance(value, ABCField):
        if isinstance(value, DistinctField):
            return f"distinct {mysql_token(value.field)}"
        if isinstance(value, CountField):
            return f"count({mysql_token(value.field)})"
        else:
            name = +value
            if name in ("1", "*"):
                return name
            return f"{mysql_token(value.table)}.`{name}`"
    elif isinstance(value, ABCTable):
        return f"`{+value}`"
    raise ValueError(value)


def sql_params(value) -> tuple[str, tuple[Any, ...]]:
    if isinstance(value, Query):
        return query_sql_params(value)
    if isinstance(value, Delete):
        return delete_sql_params(value)
    if isinstance(value, Update):
        return update_sql_params(value)
    if isinstance(value, Insert):
        return insert_sql_params(value)
    if isinstance(value, Model):
        return upsert_sql_params(value)
    if isinstance(value, str):
        return (value, ())
    raise ValueError(value)


def query_sql_params(query: Query) -> tuple[str, tuple[Any, ...]]:
    sql = []
    params = []
    sql.append("select")
    for field in query.fields:
        sql.append(mysql_token(field) + ",")
    # remove tail comma
    sql.append(sql.pop()[:-1])
    sql.append("from")
    sql.append(f"`{+query.table}`")

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


def delete_sql_params(query: Delete) -> tuple[str, tuple[Any, ...]]:
    sql = []
    params = []
    sql.append("delete")
    sql.append("from")
    sql.append(f"`{+query.table}`")

    if query.filters:
        sql.append("where")
        mysql_predicate_tokens(Predicate(LogicOperator.AND, query.filters), sql, params)

    return " ".join(sql), tuple(params)


def update_sql_params(query: Update) -> tuple[str, tuple[Any, ...]]:
    sql = []
    params = []
    sql.append("update")
    sql.append(f"`{+query.table}`")
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


def insert_sql_params(query: Insert) -> tuple[str, tuple[Any, ...]]:
    sql = []
    params = []
    sql.append("insert")
    sql.append("into")
    sql.append(f"`{+query.table}`")
    sql.append("(")

    field_names = []
    for field_name, field in query.table.get_fields().items():
        field_names.append(field_name)
        sql.append(mysql_token(field) + ",")

    # remove tail comma
    sql.append(sql.pop()[:-1])
    sql.append(")")

    sql.append("VALUES")

    sql.append(f"({','.join(Config.PARAMSTYLE for _ in field_names)})")

    print("query.values", query.values)
    for value in query.values:
        params.append(tuple(getattr(value, i) for i in field_names))

    return " ".join(sql), tuple(params)


def upsert_sql_params(value: Model) -> tuple[str, tuple[Any, ...]]:
    sql = []
    params = []
    table = type(value)
    sql.append("insert")
    sql.append("into")
    sql.append(f"`{+table}`")
    sql.append("(")

    field_names = []
    for field_name, field in table.get_fields().items():
        field_names.append(field_name)
        sql.append(mysql_token(field) + ",")

    # remove tail comma
    sql.append(sql.pop()[:-1])
    sql.append(")")

    sql.append("VALUES")

    sql.append("(")
    for i in field_names:
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
