#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase
from unittest.mock import MagicMock

from psycopg2.pool import SimpleConnectionPool

from destination_redshift_no_dbt import ConnectionPool


class TestConnectionPool(TestCase):
    def setUp(self) -> None:
        config_mock = {"max_connections": 1}
        self.pool = ConnectionPool(config=config_mock)

    def test_create_pool(self):
        assert self.pool.connection_pool is None

        self.pool.create_pool()

        assert self.pool.connection_pool is not None
        assert isinstance(self.pool.connection_pool, SimpleConnectionPool)

    def test_get_connection_with_default_autocommit(self):
        self.pool.connection_pool = MagicMock()

        connection = self.pool.get_connection()

        self.pool.connection_pool.getconn.assert_called_once()
        assert not connection.autocommit

    def test_get_connection_with_autocommit(self):
        self.pool.connection_pool = MagicMock()

        autocommit = True
        connection = self.pool.get_connection(autocommit=autocommit)

        self.pool.connection_pool.getconn.assert_called_once()
        assert connection.autocommit == autocommit

    def test_get_connection_without_autocommit(self):
        self.pool.connection_pool = MagicMock()

        autocommit = False
        connection = self.pool.get_connection(autocommit=autocommit)

        self.pool.connection_pool.getconn.assert_called_once()
        assert connection.autocommit == autocommit
