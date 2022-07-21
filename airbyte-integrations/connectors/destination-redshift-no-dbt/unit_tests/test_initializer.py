#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from copy import deepcopy
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from airbyte_cdk.models import DestinationSyncMode
from destination_redshift_no_dbt.table import Table

from destination_redshift_no_dbt.stream import Stream

from destination_redshift_no_dbt.initializer import Initializer


class TestInitializer(TestCase):
    def setUp(self) -> None:
        self.namespace_mock = "namespace"
        self.stream_name_mock = "stream"
        self.stream_mock = MagicMock(namespace=self.namespace_mock)
        self.stream_mock.name = self.stream_name_mock

        self.sync_mode_mock = DestinationSyncMode.append_dedup
        configured_stream_mock = MagicMock(stream=self.stream_mock, destination_sync_mode=self.sync_mode_mock, primary_key=[["id"]])
        configured_catalog_mock = MagicMock(streams=[configured_stream_mock])

        self.connection_pool_mock = MagicMock()

        self.initializer = Initializer(
            configured_catalog=configured_catalog_mock,
            connection_pool=self.connection_pool_mock
        )

    @patch("destination_redshift_no_dbt.initializer.JsonSchemaToTables")
    def test_streams(self, json_to_tables_mock):
        json_to_tables_instance_mock = json_to_tables_mock.return_value

        table_mock = Table(schema=self.namespace_mock, name=self.stream_name_mock)
        table_mock.name = "table"
        json_to_tables_instance_mock.tables = {table_mock.name: table_mock}

        expected_stream = Stream(
            namespace=self.namespace_mock,
            name=self.stream_name_mock,
            destination_sync_mode=self.sync_mode_mock,
            final_tables=json_to_tables_instance_mock.tables
        )

        expected_streams = {self.stream_name_mock: expected_stream}

        actual_streams = self.initializer.streams()

        assert actual_streams == expected_streams
        assert actual_streams[self.stream_name_mock].destination_sync_mode == expected_streams[self.stream_name_mock].destination_sync_mode

    def test_create_final_tables_without_overwrite_sync_mode(self):
        table_mock = Table(schema=self.namespace_mock, name=self.stream_name_mock)
        table_mock.name = "table"

        stream_mock = Stream(
            namespace=self.namespace_mock,
            name=self.stream_name_mock,
            destination_sync_mode=self.sync_mode_mock,
            final_tables={table_mock.name: table_mock}
        )

        cursor_mock = MagicMock()
        self.connection_pool_mock.get_connection.return_value.cursor.return_value = cursor_mock

        self.initializer.create_final_tables({self.stream_name_mock: stream_mock})

        self.connection_pool_mock.get_connection.assert_called_once_with(autocommit=True)
        cursor_mock.execute.assert_called_once_with(table_mock.create_statement())

    def test_create_final_tables_with_overwrite_sync_mode(self):
        table_mock = Table(schema=self.namespace_mock, name=self.stream_name_mock)
        table_mock.name = "table"

        stream_mock = Stream(
            namespace=self.namespace_mock,
            name=self.stream_name_mock,
            destination_sync_mode=DestinationSyncMode.overwrite,
            final_tables={table_mock.name: table_mock}
        )

        cursor_mock = MagicMock()
        self.connection_pool_mock.get_connection.return_value.cursor.return_value = cursor_mock

        self.initializer.create_final_tables({self.stream_name_mock: stream_mock})

        self.connection_pool_mock.get_connection.assert_called_once_with(autocommit=True)
        cursor_mock.execute.assert_has_calls([
            call(table_mock.create_statement()),
            call(table_mock.truncate_statement())
        ])

    def test_create_staging_tables(self):
        table_mock = Table(schema=self.namespace_mock, name=self.stream_name_mock)
        table_mock.name = "table"

        staging_table_mock = deepcopy(table_mock)
        staging_table_mock.schema = f"_airbyte_{table_mock.schema}"

        stream_mock = Stream(
            namespace=self.namespace_mock,
            name=self.stream_name_mock,
            destination_sync_mode=self.sync_mode_mock,
            final_tables={table_mock.name: table_mock}
        )

        cursor_mock = MagicMock()
        self.connection_pool_mock.get_connection.return_value.cursor.return_value = cursor_mock

        self.initializer.create_staging_tables({self.stream_name_mock: stream_mock})

        self.connection_pool_mock.get_connection.assert_called_once_with(autocommit=True)
        cursor_mock.execute.assert_has_calls([
            call(f"CREATE SCHEMA IF NOT EXISTS {staging_table_mock.schema}"),
            call(staging_table_mock.create_statement(staging=True))
        ])

    def test_skip_creating_staging_tables(self):
        table_mock = Table(schema=self.namespace_mock, name=self.stream_name_mock)
        table_mock.name = "table"

        staging_table_mock = deepcopy(table_mock)
        staging_table_mock.schema = f"_airbyte_{table_mock.schema}"

        stream_mock = Stream(
            namespace=self.namespace_mock,
            name=self.stream_name_mock,
            destination_sync_mode=DestinationSyncMode.overwrite,
            final_tables={table_mock.name: table_mock}
        )

        cursor_mock = MagicMock()
        self.connection_pool_mock.get_connection.return_value.cursor.return_value = cursor_mock

        self.initializer.create_staging_tables({self.stream_name_mock: stream_mock})

        self.connection_pool_mock.get_connection.assert_called_once_with(autocommit=True)
        cursor_mock.execute.assert_not_called()

    @patch("destination_redshift_no_dbt.initializer.CSVWriter")
    def test_create_csv_writers(self, csv_writer_mock):
        csv_writer_instance_mock = csv_writer_mock.return_value

        table_mock = Table(schema=self.namespace_mock, name=self.stream_name_mock)
        table_mock.name = "table"

        stream_mock = Stream(
            namespace=self.namespace_mock,
            name=self.stream_name_mock,
            destination_sync_mode=DestinationSyncMode.overwrite,
            final_tables={table_mock.name: table_mock}
        )

        actual_csv_writers = Initializer.csv_writers({self.stream_name_mock: stream_mock})

        csv_writer_mock.assert_called_once_with(table=table_mock)
        csv_writer_instance_mock.initialize_writer.assert_called_once()
        assert actual_csv_writers == {table_mock.name: csv_writer_instance_mock}
