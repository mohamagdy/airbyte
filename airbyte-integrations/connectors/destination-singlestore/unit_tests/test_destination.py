#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from argparse import Namespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from airbyte_cdk.models import Status, AirbyteConnectionStatus

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
