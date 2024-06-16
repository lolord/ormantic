from typing import cast

import psycopg2

from ormantic.dialects.bases import BaseClient, BaseConnectCreator, BaseSyncSession
from ormantic.dialects.dbapi import ConnectionProto, DBAPIProto
from ormantic.dialects.postgresql.query import sql_params


class ConnectCreator(BaseConnectCreator):
    dbapi = cast(DBAPIProto, psycopg2)


class Client(BaseClient["Client"]):
    def session(self, autocommit: bool = False) -> "Session":
        conn = cast(ConnectionProto, self.pool.dedicated_connection())
        return Session(conn, autocommit=autocommit)


class Session(BaseSyncSession["Session"]):
    """Session object handles Postgresql database operations in a synchronized manner"""

    sql_params = sql_params
