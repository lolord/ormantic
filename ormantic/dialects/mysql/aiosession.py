from asyncio.events import AbstractEventLoop
from contextlib import AbstractAsyncContextManager
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Generator,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import aiomysql
from aiomysql import Connection, Pool
from aiomysql.cursors import Cursor, DictCursor

from ormantic.dialects.mysql.query import sql_params
from ormantic.errors import RowNotFoundError
from ormantic.express import Predicate
from ormantic.fields import FieldProxy, SupportSort
from ormantic.model import ModelType
from ormantic.query import Delete, Query
from ormantic.typing import ABCField, ABCQuery
from ormantic.utils import logger


async def create_client(
    *,
    host: str = "127.0.0.1",
    port: int = 3306,
    user: Optional[str] = None,
    password: str = "",
    db: Optional[str] = None,
    minsize=1,
    maxsize=10,
    echo=False,
    pool_recycle=-1,
    loop: Optional[AbstractEventLoop] = None,
    autocommit=False,
    connect_timeout: Optional[float] = None,
    charset: str = "",
    **kwargs: Any,
):
    pool = await aiomysql.create_pool(
        host=host,
        port=port,
        user=user,
        password=password,
        db=db,
        minsize=minsize,
        maxsize=maxsize,
        echo=echo,
        pool_recycle=pool_recycle,
        loop=loop,
        autocommit=autocommit,
        connect_timeout=connect_timeout,
        charset=charset,
        **kwargs,
    )
    return AIOClient(pool)


class AIOClient(AbstractAsyncContextManager):
    def __init__(self, pool: Pool):
        self.pool = pool

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.close()

    async def close(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def release(self, conn: Connection):
        await self.pool.release(conn)

    def session(self) -> "AIOSession":
        return AIOSession(self)

    async def connection(self):
        return await self.pool.acquire()


class AIOCursor(Awaitable[List[ModelType]], AsyncIterable[ModelType]):
    """This AIOCursor object support multiple async operations:

    - **async for**: asynchronously iterate over the query results
    - **await** : when awaited it will return a list of the fetched models
    """

    def __init__(
        self, session: "AIOSession", model: Type[ModelType], query: Query[ModelType]
    ):
        super().__init__()
        self.session = session
        self.model = model
        self.cursor: Optional[DictCursor] = None
        self.query: Query[ModelType] = query
        self.results: Optional[List[ModelType]] = None

    def __await__(self) -> Generator[None, None, List[ModelType]]:
        if self.results is not None:  # pragma: no cover
            return self.results

        self.cursor = yield from self.session.execute(
            self.query, DictCursor
        ).__await__()
        rows = yield from self.cursor.fetchall().__await__()
        instances = []
        for row in rows:
            instances.append(self.model.validate_row(row))
            yield
        self.results = instances
        return instances

    async def __aiter__(self) -> AsyncGenerator[ModelType, None]:
        if self.results is not None:  # pragma: no cover
            for res in self.results:
                yield res
            return

        results = []
        self.cursor = await self.session.execute(self.query, DictCursor)
        rows = await self.cursor.fetchall()
        for row in rows:
            instance = self.model.validate_row(row)
            results.append(instance)
            yield instance
        self.results = results


class AIOSession(AbstractAsyncContextManager, Awaitable["AIOSession"]):
    """The Session object is responsible for handling database operations with MySQL
    in an asynchronous way using aiomysql.
    """

    def __init__(self, client: AIOClient, autocommit: bool = False) -> None:
        self._connection: Connection | None = None
        self.client: AIOClient = client
        self.autocommit = autocommit

    @property
    def connection(self) -> Connection:
        return cast(Connection, self._connection)

    def __await__(self) -> Generator[Any, Any, "AIOSession"]:
        yield from self.__aenter__().__await__()
        return self

    async def __aenter__(self):
        if self._connection is None:
            self._connection = await self.client.connection()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.autocommit is True:
            await self.commit()
        await self.close()

    async def close(self):
        try:
            await self.client.release(self.connection)
        finally:
            self.pool = None  # type: ignore
            self._connection = None  # type: ignore

    def find(
        self,
        model: Type[ModelType],
        *filters: Union[Predicate, bool],
        sorts: List[Tuple[SupportSort, bool]] = [],
        offset: Optional[int] = None,
        rows: Optional[int] = None,
    ) -> AIOCursor[ModelType]:
        """Search for Model instances matching the query filter provided

        Args:
            model: orm model
            *filters: query filters
            sorts: sort expression
            offset: number of row to skip
            rows: maximum number of instance fetched

        Raises:
            RowParsingError: unable to parse the resulting row

        Returns:
            [ormantic.session.AIOCursor][] of the query

        """
        query = Query(model, filters=filters, offset=offset, rows=rows, sorts=sorts)
        return AIOCursor(self, model, query)

    async def find_one(
        self,
        model: Type[ModelType],
        *filters: Union[Predicate, bool],
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
        async for i in self.find(model, *filters, sorts=sorts, offset=0, rows=1):
            return i
        return None

    async def count(
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
        cursor = await self.execute(query)
        (result,) = await cursor.fetchone()
        return cast(int, result)

    async def distinct(
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
        cursor = await self.execute(query)
        data = await cursor.fetchall()
        return [i for (i,) in data]

    async def save(self, instance: ModelType) -> ModelType:
        """Persist an instance to the database

        Args:
            instance (ModelType): instance to persist
        """
        if not instance.__fields_set__:
            return instance
        cursor = await self.execute(instance)
        id = cursor.lastrowid
        if id is not None:
            logger.info(f"lastrowid: {id}")
            instance.set_auto_increment(id)
        return instance

    async def save_all(self, instances: List[ModelType]):
        for i in instances:
            await self.save(i)
        return instances

    async def delete(self, query: Delete) -> int:
        cursor = await self.execute(query)
        return cursor.rowcount

    async def remove(self, instance: ModelType) -> ModelType:
        """Remove an instance from the database

        Args:
            instance: the instance to delete

        Raises:
            RowNotFoundError: the instance has not been persisted to the database
        """

        count = await self.execute(
            Delete(type(instance), [instance.dict(primary_keys=True)])
        )
        if count == 0:  # pragma: no cover
            raise RowNotFoundError
        return instance

    async def execute(
        self,
        query: ABCQuery | ModelType | str,  # type: ignore
        *cursors: Type[Cursor],
    ) -> Any:
        sql, params = sql_params(query)
        logger.info(f"sql_params: {sql}, {params}")
        async with self.connection.cursor(*cursors) as cur:
            await cur.execute(sql, params)
            return cur
        # return await cursor.fetchall()

    async def commit(self) -> None:
        try:
            await self.connection.commit()
        finally:
            await self.connection.rollback()
