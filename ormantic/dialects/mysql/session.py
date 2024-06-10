from typing import TypeAlias, cast

import pymysql
from dbutils.pooled_db import PooledDedicatedDBConnection

from ormantic.dialects.bases import BaseClient, BaseConnectCreator, BaseSyncSession
from ormantic.dialects.mysql.query import sql_params

Connection: TypeAlias = PooledDedicatedDBConnection


class ConnectCreator(BaseConnectCreator):
    dbapi = pymysql


class Client(BaseClient["Client"]):
    def session(self, autocommit: bool = False) -> "Session":
        conn = cast(Connection, self.pool.dedicated_connection())
        return Session(conn, autocommit)


class Session(BaseSyncSession["Session"]):
    """Session object handles MySQL database operations in a synchronized manner"""

    sql_params = sql_params
