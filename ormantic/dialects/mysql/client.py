from asyncio.events import AbstractEventLoop
from contextlib import AbstractAsyncContextManager
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Optional,
    Type,
    TypeVar,
)

import aiomysql
from aiomysql import Connection
from aiomysql import Cursor as AIOMySQLCursor

from ormantic.dialects.mysql.query import Insert, MysqlMixin, Query, UpInsert
from ormantic.model import Model
from ormantic.utils import logger

AIOCursor = TypeVar("AIOCursor", bound=AIOMySQLCursor)


class AsyncClient(AbstractAsyncContextManager):
    if TYPE_CHECKING:
        connection: Connection
        connected: bool

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: Optional[str] = None,
        password: str = "",
        db: Optional[str] = None,
        loop: Optional[AbstractEventLoop] = None,
        autocommit=False,
        connect_timeout: Optional[float] = None,
        charset: str = "",
        **kwargs: Any,
    ):
        self.connection = Connection(
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
            loop=loop,
            autocommit=autocommit,
            connect_timeout=connect_timeout,
            charset=charset,
            **kwargs,
        )
        self.connected = False

    async def connect(self):
        if not self.connected:
            await self.connection._connect()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self.connection.close()
        else:
            await self.connection.ensure_closed()

    async def cursor(self, *cursor_class: Type[AIOCursor]) -> AIOCursor:
        await self.connect()
        return await self.connection.cursor(*cursor_class)

    async def execute(self, query: MysqlMixin, commit=False) -> List[Dict]:
        cursor = await self.cursor(aiomysql.DictCursor)
        sql, params = query.sql_params()
        logger.info(f"sql_params: {sql}, {params}")
        await cursor.execute(sql, params)
        if commit:
            await self.connection.commit()

        data: List[Dict] = await cursor.fetchall()
        return data

    async def commit(self, *queries: MysqlMixin) -> None:
        await self.connect()
        try:
            async with self.connection.cursor() as cursor:
                for query in queries:
                    sql, params = query.sql_params()
                    logger.info(f"sql_params: {sql}, {params}")
                    if isinstance(query, Insert):
                        if len(params) == 1:
                            await cursor.execute(sql, params[0])
                        else:
                            await cursor.executemany(sql, params)
                    else:
                        await cursor.execute(sql, params)
                await self.connection.commit()
        except Exception:  # pragma: no cover
            await self.connection.rollback()
            raise

    async def commit_model(self, *models: Model) -> None:
        await self.connect()
        try:
            async with self.connection.cursor() as cursor:
                for model in models:
                    sql, params = UpInsert(model).sql_params()
                    logger.info(f"sql_params: {sql}, {params}")
                    await cursor.execute(sql, params)
                    inc_id = cursor.lastrowid
                    print("inc_id", inc_id)
                    model.set_inc_id(inc_id)
            await self.connection.commit()
        except Exception:
            await self.connection.rollback()
            raise

    async def count(self, query: Query) -> int:
        if getattr(query, "is_count", None) is None:
            # TODO
            raise ValueError("query is not count")

        cursor = await self.cursor()
        sql, params = query.sql_params()
        logger.info(sql)
        await cursor.execute(sql, params)
        (result,) = await cursor.fetchone()
        return result

    async def distinct(self, query: Query) -> List[Any]:
        if getattr(query, "is_distinct", None) is None:
            # TODO
            raise ValueError("query is not distinct")

        cursor = await self.cursor()
        sql, params = query.sql_params()
        logger.info(sql)
        await cursor.execute(sql, params)
        data = await cursor.fetchall()
        return [i for (i, _) in data]
