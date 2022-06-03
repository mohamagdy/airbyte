#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Mapping, Any

from psycopg2._psycopg import connection as Connection
from psycopg2.pool import SimpleConnectionPool


class ConnectionPool:
    def __init__(self, config: Mapping[str, Any]):
        self.config = config

        self.connection_pool = None

    def create_pool(self):
        self.connection_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=self.config.get("max_connections"),
            host=self.config.get("host"),
            port=self.config.get("port"),
            database=self.config.get("database"),
            user=self.config.get("username"),
            password=self.config.get("password")
        )

    def get_connection(self, autocommit: bool = False) -> Connection:
        connection = self.connection_pool.getconn()
        connection.autocommit = autocommit

        return connection

    def put_connection(self, connection: Connection):
        self.connection_pool.putconn(connection)
