from typing import (
    Any,
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    Generator,
    List,
    Optional,
    Tuple,
    Type,
)

from aiomysql.cursors import DictCursor

from ormantic.express import BoolExpression
from ormantic.fields import FieldProxy, SupportSort
from ormantic.model import ModelType
from ormantic.typing import ABCField

from .client import AsyncClient
from .query import Delete, Query


class AIOCursor(Awaitable[List[ModelType]], AsyncIterable[ModelType]):
    """This AIOCursor object support multiple async operations:

    - **async for**: asynchronously iterate over the query results
    - **await** : when awaited it will return a list of the fetched models
    """

    def __init__(
        self, engine: "AIOEngine", model: Type[ModelType], query: Query[ModelType]
    ):
        super().__init__()
        self.engine = engine
        self.model = model
        self.cursor: Optional[DictCursor] = None
        self.query: Query[ModelType] = query
        self.results: Optional[List[ModelType]] = None

    def __await__(self) -> Generator[None, None, List[ModelType]]:
        if self.results is not None:
            return self.results
        if self.cursor is None:
            self.cursor = yield from self.engine._client.cursor(DictCursor).__await__()

        sql, params = self.query.sql_params()

        yield from self.cursor.execute(sql, params).__await__()
        rows = yield from self.cursor.fetchall().__await__()
        instances = []
        for row in rows:
            instances.append(self.model.validate(row))
            yield
        self.results = instances
        return instances

    async def __aiter__(self) -> AsyncGenerator[ModelType, None]:
        if self.results is not None:
            for res in self.results:
                yield res
            return
        if self.cursor is None:  # pragma: no cover
            self.cursor = await self.engine._client.cursor(DictCursor)
        results = []

        sql, params = self.query.sql_params()
        await self.cursor.execute(sql, params)
        rows = await self.cursor.fetchall()
        for row in rows:
            instance = self.model.validate(row)
            results.append(instance)
            yield instance
        self.results = results


class AIOEngine:
    """The AIOEngine object is responsible for handling database operations with MySQL
    in an asynchronous way using aiomysql.
    """

    def __init__(self, client: AsyncClient) -> None:
        self._client = client

    def find(
        self,
        model: Type[ModelType],
        *filters: BoolExpression,
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
            [ormantic.engine.AIOCursor][] of the query

        """
        query = Query(model, filters=filters, offset=offset, rows=rows, sorts=sorts)
        return AIOCursor(self, model, query)

    find_many = find

    async def find_one(
        self,
        model: Type[ModelType],
        *filters: BoolExpression,
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
        field: ABCField,
        *filters: BoolExpression,
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
        return await self._client.count(query)

    async def distinct(
        self,
        model: Type[ModelType],
        field: FieldProxy,
        *filters: BoolExpression,
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
        return await self._client.distinct(query)

    async def save(self, instance: ModelType) -> ModelType:
        """Persist an instance to the database

        Args:
            instance (ModelType): instance to persist
        """
        await self._client.commit_model(instance)
        return instance

    async def save_all(self, instances: List[ModelType]):
        """Persist instances to the database

        This method behaves as multiple 'upsert' operations. If one of the row
        already exists with the same primary key, it will be overwritten.

        Args:
            instances (List[ModelType]): instances to persist
        """
        await self._client.commit_model(*instances)

    async def delete(self, instance: ModelType) -> ModelType:
        """Delete an instance from the database

        Args:
            instance: the instance to delete

        Raises:
            RowNotFoundError: the instance has not been persisted to the database
        """

        await self._client.execute(
            Delete(type(instance), [instance.dict(primary_keys=True)]), commit=True
        )
        return instance

    async def delete_all(self, instances: List[ModelType]) -> List[ModelType]:
        """Delete instances from the database

        Args:
            instances: the instances to delete

        Raises:
            RowNotFoundError: the instance has not been persisted to the database
        """
        for instance in instances:
            await self.delete(instance)
        return instances

    async def close(self):
        await self._client.__aexit__(None, None, None)
