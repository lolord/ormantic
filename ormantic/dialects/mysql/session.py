from typing import (
    Any,
    ContextManager,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeAlias,
    Union,
    cast,
)

import pymysql
from dbutils.pooled_db import PooledDB, PooledDedicatedDBConnection
from pymysql import Connect as PyMySQLConnection
from pymysql.cursors import Cursor, DictCursor

from ormantic.dialects.mysql.query import sql_params
from ormantic.errors import RowNotFoundError
from ormantic.express import Predicate
from ormantic.fields import FieldProxy, SupportSort
from ormantic.model import ModelType
from ormantic.query import Delete, Query
from ormantic.typing import ABCField, ABCQuery
from ormantic.utils import logger

Connection: TypeAlias = PooledDedicatedDBConnection


class ConnectFactory:
    dbapi = pymysql

    def __init__(
        self,
        user=None,  # The first four arguments is based on DB-API 2.0 recommendation.
        password="",
        host=None,
        database=None,
        port=3306,
        **kwargs,
    ) -> None:
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.kwargs = kwargs

    def __call__(
        self,
    ) -> PyMySQLConnection:
        return PyMySQLConnection(
            user=self.user,
            password=self.password,
            host=self.host,
            database=self.database,
            port=self.port,
            **self.kwargs,
        )


class Client(ContextManager):
    def __init__(
        self,
        db: ConnectFactory,
        mincached=0,
        maxcached=0,
        maxshared=0,
        maxconnections=0,
        blocking=False,
        maxusage=None,
        setsession=None,
        reset=True,
        failures=None,
        ping=1,
        *args,
        **kwargs,
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
            *args,
            **kwargs,
        )

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self.pool.close()

    def session(self) -> "Session":
        conn = cast(Connection, self.pool.dedicated_connection())
        return Session(conn)


class Session(ContextManager):
    """The Session object is responsible for handling database operations with MySQL
    in an asynchronous way using aiomysql.
    """

    def __init__(self, connection: Connection) -> None:
        self.connection: Connection = connection

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def close(self):
        self.connection.close()

    def find(
        self,
        model: Type[ModelType],
        *filters: Predicate | bool,
        sorts: List[Tuple[SupportSort, bool]] = [],
        offset: Optional[int] = None,
        rows: Optional[int] = None,
    ) -> List[ModelType]:
        query = Query(model, filters=filters, offset=offset, rows=rows, sorts=sorts)
        results = []
        cursor = self.execute(query, DictCursor)
        for row in cursor.fetchall():
            results.append(model.validate(row))
        return results

    def find_one(
        self,
        model: Type[ModelType],
        *filters: Predicate | bool,
        sorts: List[Tuple[SupportSort, bool]] = [],
    ) -> Optional[ModelType]:
        """Search for a Model instance matching the query filter provided

        Args:
            model: orm model
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
            model: orm model
            *filters: query filters
            field: selected field

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
        sorts: List[Tuple[SupportSort, bool]] = [],
    ) -> List[Any]:
        """_summary_

        Args:
            model (Type[ModelType]): _description_
            field (FieldProxy): _description_

        Returns:
            List[Any]: _description_
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
        if not instance.__fields_set__:
            return instance
        cursor = self.execute(instance)
        id = cursor.lastrowid
        if id is not None:
            logger.info(f"lastrowid: {id}")
            instance.set_auto_increment(id)
        return instance

    def save_all(self, instances: List[ModelType]):
        for i in instances:
            self.save(i)
        return instances

    def delete(self, query: Delete) -> int:
        cursor = self.execute(query)
        return cursor.rowcount

    def remove(self, instance: ModelType) -> ModelType:
        """Remove an instance from the database

        Args:
            instance: the instance to delete

        Raises:
            RowNotFoundError: the instance has not been persisted to the database
        """

        count = self.execute(Delete(type(instance), [instance.dict(primary_keys=True)]))
        if count == 0:
            raise RowNotFoundError
        return instance

    def execute(self, query: ABCQuery | ModelType | str, *cursors: Type[Cursor]) -> Any:  # type: ignore
        sql, params = sql_params(query)
        logger.info(f"sql_params: {sql}, {params}")
        cur = self.connection.cursor(*cursors)
        cur.execute(sql, params)
        return cur
        # return cursor.fetchall()

    def commit(self) -> None:
        try:
            self.connection.commit()
        finally:
            self.connection.rollback()
