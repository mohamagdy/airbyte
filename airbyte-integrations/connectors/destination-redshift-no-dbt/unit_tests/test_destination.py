#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from argparse import Namespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from airbyte_cdk.models import Status, AirbyteConnectionStatus, Type

from destination_redshift_no_dbt import DestinationRedshiftNoDbt


@patch("destination_redshift_no_dbt.destination.ConnectionPool")
class TestDestination(TestCase):
    def setUp(self) -> None:
        self.config_mock = {"max_connections": 1}
        args = {"command": "check", "config": self.config_mock}

        self.parsed_args = Namespace(**args)

        self.destination = DestinationRedshiftNoDbt()

        self.destination.read_config = MagicMock(return_value=self.config_mock)

    def test_run_check_creates_connection_pool(self, connection_pool_mock):
        connection_pool_instance_mock = connection_pool_mock.return_value

        self.destination.run_cmd(self.parsed_args)

        connection_pool_instance_mock.create_pool.assert_called_once()

    def test_successful_check(self, connection_pool_mock):
        connection_pool_instance_mock = connection_pool_mock.return_value

        cursor_mock = MagicMock()
        connection_pool_instance_mock.get_connection.return_value.cursor.return_value = cursor_mock

        self.destination.run_cmd(self.parsed_args)

        logger_mock = MagicMock()
        config_mock = MagicMock()
        check_result = self.destination.check(logger=logger_mock, config=config_mock)

        connection_pool_instance_mock.get_connection.assert_called_with(autocommit=True)
        cursor_mock.execute.assert_called_with("SELECT 1")

        assert check_result == AirbyteConnectionStatus(status=Status.SUCCEEDED)

    def test_failing_check(self, connection_pool_mock):
        connection_pool_instance_mock = connection_pool_mock.return_value

        cursor_mock = MagicMock()
        cursor_mock.execute.side_effect = Exception()
        connection_pool_instance_mock.get_connection.return_value.cursor.return_value = cursor_mock

        self.destination.run_cmd(self.parsed_args)

        logger_mock = MagicMock()
        config_mock = MagicMock()

        check_result = self.destination.check(logger=logger_mock, config=config_mock)

        assert check_result.status == Status.FAILED

    @patch("destination_redshift_no_dbt.destination.ConfiguredAirbyteCatalog")
    def test_run_write_creates_connection_pool(self, configured_airbyte_catalog_mock, connection_pool_mock):
        connection_pool_instance_mock = connection_pool_mock.return_value

        configured_catalog_mock = MagicMock()
        configured_airbyte_catalog_mock.parse_file.return_value = configured_catalog_mock

        self.destination.run_cmd(self.parsed_args)

        connection_pool_instance_mock.create_pool.assert_called_once()

    @patch("destination_redshift_no_dbt.destination.ConfiguredAirbyteCatalog")
    @patch("destination_redshift_no_dbt.destination.RecordsWriter")
    def test_write_with_state_message_yield_message(self, records_writer_mock, configured_airbyte_catalog_mock, connection_pool_mock):
        records_writer_instance_mock = records_writer_mock.return_value
        connection_pool_instance_mock = connection_pool_mock.return_value

        configured_catalog_mock = MagicMock()
        configured_airbyte_catalog_mock.parse_file.return_value = configured_catalog_mock

        self.destination.run_cmd(self.parsed_args)

        config_mock = MagicMock()

        state_json_mock = MagicMock()
        state_mock = MagicMock(json=state_json_mock)

        message_mock = MagicMock(type=Type.STATE, state=state_mock)

        input_messages_mock = MagicMock()
        input_messages_mock.__iter__.return_value = [message_mock]

        flush_mock = MagicMock()
        records_writer_instance_mock.flush = flush_mock

        self.destination.last_flushed_state = MagicMock()

        messages = self.destination.write(
            config=config_mock,
            configured_catalog=configured_catalog_mock,
            input_messages=input_messages_mock
        )

        assert len(list(messages)) == 1
        assert records_writer_instance_mock.flush.call_count == 2
        connection_pool_instance_mock.closeall.assert_called_once()

    @patch("destination_redshift_no_dbt.destination.ConfiguredAirbyteCatalog")
    @patch("destination_redshift_no_dbt.destination.RecordsWriter")
    def test_write_with_state_message_not_yield_message(self, records_writer_mock, configured_airbyte_catalog_mock, connection_pool_mock):
        records_writer_instance_mock = records_writer_mock.return_value
        connection_pool_instance_mock = connection_pool_mock.return_value

        configured_catalog_mock = MagicMock()
        configured_airbyte_catalog_mock.parse_file.return_value = configured_catalog_mock

        self.destination.run_cmd(self.parsed_args)

        config_mock = MagicMock()

        state_json_mock = MagicMock()
        state_mock = MagicMock(json=state_json_mock)

        message_mock = MagicMock(type=Type.STATE, state=state_mock)

        input_messages_mock = MagicMock()
        input_messages_mock.__iter__.return_value = [message_mock]

        flush_mock = MagicMock()
        records_writer_instance_mock.flush = flush_mock

        self.destination.last_flushed_state = state_json_mock.return_value

        messages = self.destination.write(
            config=config_mock,
            configured_catalog=configured_catalog_mock,
            input_messages=input_messages_mock
        )

        assert len(list(messages)) == 0
        assert records_writer_instance_mock.flush.call_count == 2
        connection_pool_instance_mock.closeall.assert_called_once()

    @patch("destination_redshift_no_dbt.destination.ConfiguredAirbyteCatalog")
    @patch("destination_redshift_no_dbt.destination.RecordsWriter")
    def test_write_with_record_message(self, records_writer_mock, configured_airbyte_catalog_mock, connection_pool_mock):
        records_writer_instance_mock = records_writer_mock.return_value
        connection_pool_instance_mock = connection_pool_mock.return_value

        configured_catalog_mock = MagicMock()
        configured_airbyte_catalog_mock.parse_file.return_value = configured_catalog_mock

        self.destination.run_cmd(self.parsed_args)

        config_mock = MagicMock()

        state_json_mock = MagicMock()
        state_mock = MagicMock(json=state_json_mock)

        message_mock = MagicMock(type=Type.RECORD, state=state_mock)

        input_messages_mock = MagicMock()
        input_messages_mock.__iter__.return_value = [message_mock]

        flush_mock = MagicMock()
        records_writer_instance_mock.flush = flush_mock

        self.destination.last_flushed_state = state_json_mock.return_value

        messages = self.destination.write(
            config=config_mock,
            configured_catalog=configured_catalog_mock,
            input_messages=input_messages_mock
        )

        assert len(list(messages)) == 0
        records_writer_instance_mock.write.assert_called_once_with(message=message_mock)
        assert records_writer_instance_mock.flush.call_count == 1
        connection_pool_instance_mock.closeall.assert_called_once()
