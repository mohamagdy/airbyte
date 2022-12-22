#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from argparse import Namespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

import singlestoredb
from airbyte_cdk.models import Status, AirbyteConnectionStatus, Type

from destination_singlestore import DestinationSinglestore


class TestDestination(TestCase):
    def setUp(self) -> None:
        self.config_mock = {}
        args = {"command": "check", "config": self.config_mock}

        self.parsed_args = Namespace(**args)

        self.destination = DestinationSinglestore()

        self.destination.read_config = MagicMock(return_value=self.config_mock)

    @patch("singlestoredb.connect")
    def test_successful_check(self, connect_mock):
        connection_mock = connect_mock.return_value

        cursor_mock = MagicMock()
        connection_mock.cursor.return_value = cursor_mock

        self.destination.run_cmd(self.parsed_args)

        logger_mock = MagicMock()
        config_mock = MagicMock()
        check_result = self.destination.check(logger=logger_mock, config=config_mock)

        cursor_mock.execute.assert_called_with("SELECT 1")

        cursor_mock.close.assert_called_once()
        connection_mock.close.assert_called_once()

        assert check_result == AirbyteConnectionStatus(status=Status.SUCCEEDED)

    @patch("singlestoredb.connect")
    def test_failing_check(self, connect_mock):
        connection_mock = connect_mock.return_value

        cursor_mock = MagicMock()
        cursor_mock.execute.side_effect = Exception()
        connection_mock.cursor.return_value = cursor_mock

        self.destination.run_cmd(self.parsed_args)

        logger_mock = MagicMock()
        config_mock = MagicMock()

        check_result = self.destination.check(logger=logger_mock, config=config_mock)

        assert check_result.status == Status.FAILED

    @patch("destination_singlestore.destination.ConfiguredAirbyteCatalog")
    def test_run_write_creates_connection(self, configured_airbyte_catalog_mock):
        connect_mock = MagicMock()

        singlestoredb.connect = connect_mock

        configured_catalog_mock = MagicMock()
        configured_airbyte_catalog_mock.parse_file.return_value = configured_catalog_mock

        self.destination.run_cmd(self.parsed_args)

        connect_mock.assert_called_once()

    @patch("singlestoredb.connect")
    @patch("destination_singlestore.destination.ConfiguredAirbyteCatalog")
    @patch("destination_singlestore.destination.RecordsWriter")
    def test_write_with_state_message_yield_message(self, records_writer_mock, configured_airbyte_catalog_mock, connect_mock):
        records_writer_instance_mock = records_writer_mock.return_value
        connection_mock = connect_mock.return_value

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
        connection_mock.close.assert_called_once()

    @patch("singlestoredb.connect")
    @patch("destination_singlestore.destination.ConfiguredAirbyteCatalog")
    @patch("destination_singlestore.destination.RecordsWriter")
    def test_write_with_state_message_not_yield_message(self, records_writer_mock, configured_airbyte_catalog_mock, connect_mock):
        records_writer_instance_mock = records_writer_mock.return_value
        connection_mock = connect_mock.return_value

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
        connection_mock.close.assert_called_once()

    @patch("singlestoredb.connect")
    @patch("destination_singlestore.destination.ConfiguredAirbyteCatalog")
    @patch("destination_singlestore.destination.RecordsWriter")
    def test_write_with_record_message(self, records_writer_mock, configured_airbyte_catalog_mock, connect_mock):
        records_writer_instance_mock = records_writer_mock.return_value
        connection_mock = connect_mock.return_value

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
        connection_mock.close.assert_called_once()
