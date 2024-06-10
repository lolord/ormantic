from typing import TypeAlias, cast

import psycopg2
from dbutils.pooled_db import PooledDedicatedDBConnection

from ormantic.dialects.bases import BaseClient, BaseConnectCreator, BaseSyncSession
from ormantic.dialects.postgresql.query import sql_params

Connection: TypeAlias = PooledDedicatedDBConnection


class ConnectCreator(BaseConnectCreator):
    dbapi = psycopg2


class Client(BaseClient["Client"]):
    def session(self, autocommit: bool = False) -> "Session":
        conn = cast(Connection, self.pool.dedicated_connection())
        return Session(conn, autocommit=autocommit)


class Session(BaseSyncSession["Session"]):
    """Session object handles Postgresql database operations in a synchronized manner"""

    sql_params = sql_params
