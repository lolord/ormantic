from types import TracebackType
from typing import Any, ContextManager, Literal, Optional, Type, TypeAlias, Union, cast

import psycopg2
from dbutils.pooled_db import PooledDB, PooledDedicatedDBConnection

from ormantic.dialects.pg.query import sql_params
from ormantic.errors import RowNotFoundError
from ormantic.express import Predicate
from ormantic.fields import FieldProxy, SortedItems
from ormantic.model import ModelType
from ormantic.query import Delete, Insert, Query, Update
from ormantic.typing import ABCField, ABCQuery
from ormantic.utils import logger

Connection: TypeAlias = PooledDedicatedDBConnection


class ConnectCreator:
    dbapi = psycopg2

    def __init__(
        self,
        *,
        user: str = "",
        password: str = "",
        host: str = "127.0.0.1",
        database: Optional[str] = None,
        port: int = 5432,
        **kwargs: Any,
    ):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.kwargs = kwargs

    def __call__(
        self,
    ) -> psycopg2.extensions.connection:
        c = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            port=self.port,
            **self.kwargs,
        )
        return cast(psycopg2.extensions.connection, c)


class Client(ContextManager["Client"]):
    def __init__(
        self,
        db: ConnectCreator,
        mincached: int = 0,
        maxcached: int = 0,
        maxshared: int = 0,
        maxconnections: int = 0,
        blocking: bool = False,
        maxusage: int = 0,
        setsession: Any = None,
        reset: bool = True,
        failures: Any = None,
        ping: int = 1,
        **kwargs: Any,
    ):
        self.pool = PooledDB(
            db,
            mincached=mincached,
            maxcached=maxcached,
            maxshared=maxshared,
            maxconnections=maxconnections,
            blocking=blocking,
            maxusage=maxusage,
            setsession=setsession,
            reset=reset,
            failures=failures,
            ping=ping,
            **kwargs,
        )

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        self.pool.close()

    def session(self) -> "Session":
        conn = cast(Connection, self.pool.dedicated_connection())
        return Session(conn)


class Session(ContextManager["Session"]):
    def __init__(self, connection: Connection) -> None:
        self.connection: Connection = connection

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def close(self) -> None:
        self.commit()
        self.connection.close()

    def find(
        self,
        model: Type[ModelType],
        *filters: Predicate | bool,
        sorts: SortedItems = (),
        offset: Optional[int] = None,
        rows: Optional[int] = None,
    ) -> list[ModelType]:
        query = Query(model, filters=filters, offset=offset, rows=rows, sorts=sorts)
        results = []
        cursor = self.execute(query)
        for row in cursor.fetchall():
            print("row", row)
            results.append(model.validate(zip(model.__fields__.keys(), row)))
        return results

    def find_one(
        self,
        model: Type[ModelType],
        *filters: Predicate | bool,
        sorts: SortedItems = (),
    ) -> Optional[ModelType]:
        """Search for a Model instance matching the query filter provided

        Args:
            model: orm model class
            *filters: query filters
            sorts: sort expression

        Raises:
            RowParsingError: unable to parse the resulting row

        Returns:
            the fetched instance if found otherwise None

        """
        for i in self.find(model, *filters, sorts=sorts, offset=0, rows=1):
            return i
        return None

    def count(
        self,
        model: Type[ModelType],
        field: Union[ABCField, Literal[1, "*"]] = "*",
        *filters: Predicate,
    ) -> int:
        """Get the count of rows matching a query

        Args:
            model: orm model class
            field: selected field
            *filters: query filters

        Returns:
            int: number of row matching the query
        """
        query = Query(model, filters=filters).count(field)
        cursor = self.execute(query)
        (result,) = cursor.fetchone()
        return cast(int, result)

    def distinct(
        self,
        model: Type[ModelType],
        field: FieldProxy,
        *filters: Predicate,
        sorts: SortedItems = (),
    ) -> list[Any]:
        """Get the field of rows matching a query

        Args:
            model (Type[ModelType]): orm model class
            field (FieldProxy): selected field
            *filters: query filters

        Returns:
            list[Any]: field list
        """
        query = Query(model, filters=filters, sorts=sorts).distinct(field)
        cursor = self.execute(query)
        data = cursor.fetchall()
        return [i for (i,) in data]

    def save(self, instance: ModelType) -> ModelType:
        """Persist an instance to the database

        Args:
            instance (ModelType): instance to persist
        """
        if instance.__fields_set__:
            query: ABCQuery
            if (instance.__fields_set__ & instance.__primary_keys__.keys()) or not all(
                instance.dict(primary_keys=True).values()
            ):
                query = Insert(type(instance), [instance])
            else:
                query = (
                    Update(type(instance))
                    .filter(instance.dict(primary_keys=True))
                    .update(**instance.dict(exclude_unset=True))
                )

            cursor = self.execute(query)
            id = cast(int, cursor.lastrowid)
            logger.debug(f"lastrowid: {id}")
            instance.set_auto_increment(id)
            instance.__fields_set__.clear()
        return instance

    def save_all(self, instances: list[ModelType]) -> list[ModelType]:
        for i in instances:
            self.save(i)
        return instances

    def delete(self, query: Delete) -> int:
        cursor = self.execute(query)
        return cast(int, cursor.rowcount)

    def remove(self, instance: ModelType) -> ModelType:
        """Remove an instance from the database

        Args:
            instance: the instance to delete

        Raises:
            RowNotFoundError: the instance has not been persisted to the database
        """

        cursor = self.execute(Delete(type(instance), [instance.dict(primary_keys=True)]))

        if cursor.rowcount == 0:
            raise RowNotFoundError
        return instance

    def execute(self, query: ABCQuery | str) -> Any:
        sql, params = sql_params(query)
        logger.debug(f"sql_params: {sql}, {params}")
        cur = self.connection.cursor()

        if isinstance(query, Insert):
            cur.executemany(sql, params)
        else:
            cur.execute(sql, params)
        return cur

    def commit(self) -> None:
        try:
            self.connection.commit()
        finally:
            self.connection.rollback()
