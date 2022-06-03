#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase
from unittest.mock import MagicMock, patch

from destination_redshift_no_dbt.csv_writer import CSVWriter, CSV_EXTENSION, GZIP_EXTENSION


class TestCSVWriter(TestCase):
    def setUp(self) -> None:
        self.table_mock = MagicMock()

        self.csv_writer = CSVWriter(table=self.table_mock)

    @patch("destination_redshift_no_dbt.csv_writer.NamedTemporaryFile")
    @patch("destination_redshift_no_dbt.csv_writer.DictWriter")
    @patch("builtins.open")
    def test_initialize_writer(self, open_mock, dict_writer_mock, named_temporary_file_mock):
        dict_writer_instance_mock = dict_writer_mock.return_value
        temp_file_open_mock = open_mock.return_value

        self.csv_writer.initialize_writer()

        named_temporary_file_mock.assert_called_once_with(delete=True, suffix=f".{CSV_EXTENSION}")

        dict_writer_mock.assert_called_once_with(temp_file_open_mock, fieldnames=self.table_mock.field_names)
        dict_writer_instance_mock.writeheader.assert_called_once()

    def test_write_record(self):
        record = MagicMock()
        self.csv_writer._dict_writer = MagicMock()

        self.csv_writer.write(records=record)

        self.csv_writer._dict_writer.writerows.assert_called_with([record])
        assert self.csv_writer.rows_count() == 1

    def test_write_records(self):
        records = [MagicMock()]
        self.csv_writer._dict_writer = MagicMock()

        self.csv_writer.write(records=records)

        self.csv_writer._dict_writer.writerows.assert_called_with(records)
        assert self.csv_writer.rows_count() == len(records)

    @patch("gzip.open")
    @patch("destination_redshift_no_dbt.csv_writer.copyfileobj")
    def test_flush_gzipped_with_records(self, copyfileobj_mock, gzip_open_mock):
        records = [MagicMock()]
        self.csv_writer._dict_writer = MagicMock()
        self.csv_writer.initialize_writer = MagicMock()

        temporary_file_mock = MagicMock()
        temporary_file_mock.name = "temp"

        self.csv_writer._temporary_file = temporary_file_mock

        self.csv_writer.write(records=records)
        gzip_file = self.csv_writer.flush_gzipped()

        gzip_open_mock.assert_called_once_with(f"{temporary_file_mock.name}.{GZIP_EXTENSION}", "wb")
        self.csv_writer.initialize_writer.assert_called_once()
        copyfileobj_mock.assert_called_once()

        assert gzip_file == gzip_open_mock.return_value

    def test_flush_gzipped_with_no_records(self):
        assert self.csv_writer.flush_gzipped() is None

    @patch("destination_redshift_no_dbt.csv_writer.Path")
    def test_delete_gzip_file(self, path_mock):
        gzip_mock = MagicMock()

        path_instance_mock = path_mock.return_value

        CSVWriter.delete_gzip_file(gzip_mock)

        path_mock.assert_called_with(gzip_mock.name)
        path_instance_mock.unlink.asser_called_once()
