from typing import cast

import pymysql

from ormantic.dialects.bases import BaseClient, BaseConnectCreator, BaseSyncSession
from ormantic.dialects.dbapi import ConnectionProto, DBAPIProto
from ormantic.dialects.mysql.query import sql_params


class ConnectCreator(BaseConnectCreator):
    dbapi = cast(DBAPIProto, pymysql)


class Client(BaseClient["Client"]):
    def session(self, autocommit: bool = False) -> "Session":
        conn = cast(ConnectionProto, self.pool.dedicated_connection())
        return Session(conn, autocommit)


class Session(BaseSyncSession["Session"]):
    """Session object handles MySQL database operations in a synchronized manner"""

    sql_params = sql_params
