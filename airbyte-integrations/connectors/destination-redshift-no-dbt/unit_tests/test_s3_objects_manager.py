#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from destination_redshift_no_dbt.s3_objects_manager import S3ObjectsManager


class TestS3ObjectsManager(TestCase):
    def setUp(self) -> None:
        self.bucket_mock = "bucket"
        self.s3_path_mock = "path"

        self.s3_manager = S3ObjectsManager(bucket=self.bucket_mock, s3_path=self.s3_path_mock)

        self.s3_manager.session = MagicMock()

    @patch("destination_redshift_no_dbt.s3_objects_manager.Path")
    def test_upload_file_to_s3(self, path_mock):
        path_instance_mock = path_mock.return_value
        path_instance_mock.name = "path"

        file_path_mock = MagicMock()
        client_mock = self.s3_manager.session.client

        object_mock = f"{self.s3_path_mock}/file.txt"
        self.s3_manager.object_name = MagicMock(return_value=object_mock)

        self.s3_manager.check_file_exists = MagicMock(return_value=True)
        actual_s3_path = self.s3_manager.upload_file_to_s3(file_path_mock)

        path_mock.assert_called_with(file_path_mock)
        client_mock.assert_called_once_with("s3")
        client_mock.return_value.upload_file.assert_called_once()

        assert actual_s3_path == f"s3://{self.bucket_mock}/{object_mock}"

    def test_upload_file_to_s3_retry_on_exception(self):
        with patch("destination_redshift_no_dbt.s3_objects_manager.Path"):
            file_path_mock = MagicMock()
            client_mock = self.s3_manager.session.client

            client_mock.return_value.upload_file.side_effect = [
                ClientError(error_response=MagicMock(), operation_name=MagicMock()),
                MagicMock()
            ]

            object_mock = f"{self.s3_path_mock}/file.txt"
            self.s3_manager.object_name = MagicMock(return_value=object_mock)

            self.s3_manager.check_file_exists = MagicMock()

            actual_s3_path = self.s3_manager.upload_file_to_s3(file_path=file_path_mock)

            assert client_mock.return_value.upload_file.call_count == 2
            self.s3_manager.check_file_exists.assert_called_once()

            assert actual_s3_path == f"s3://{self.bucket_mock}/{object_mock}"

    @patch("destination_redshift_no_dbt.s3_objects_manager.Path")
    def test_delete_file_from_s3(self, path_mock):
        path_instance_mock = path_mock.return_value
        path_instance_mock.name = "path"

        file_path_mock = MagicMock()
        client_mock = self.s3_manager.session.client

        object_mock = f"{self.s3_path_mock}/file.txt"
        self.s3_manager.object_name = MagicMock(return_value=object_mock)

        self.s3_manager.check_file_exists = MagicMock(return_value=True)
        actual_delete_result = self.s3_manager.delete_file_from_s3(file_path_mock)

        path_mock.assert_called_with(file_path_mock)
        client_mock.assert_called_once_with("s3")
        client_mock.return_value.delete_object.assert_called_once_with(Bucket=self.bucket_mock, Key=object_mock)

        assert actual_delete_result

    def test_delete_retry_on_exception(self):
        with patch("destination_redshift_no_dbt.s3_objects_manager.Path"):
            file_path_mock = MagicMock()
            client_mock = self.s3_manager.session.client

            client_mock.return_value.delete_object.side_effect = [
                ClientError(error_response=MagicMock(), operation_name=MagicMock()),
                MagicMock()
            ]

            object_mock = f"{self.s3_path_mock}/file.txt"
            self.s3_manager.object_name = MagicMock(return_value=object_mock)

            actual_delete_result = self.s3_manager.delete_file_from_s3(file_path=file_path_mock)

            assert client_mock.return_value.delete_object.call_count == 2

            assert actual_delete_result

    def test_check_file_exists(self):
        file_name_mock = MagicMock()
        client_mock = self.s3_manager.session.client

        object_mock = f"{self.s3_path_mock}/file.txt"
        self.s3_manager.object_name = MagicMock(return_value=object_mock)

        actual_check_result = self.s3_manager.check_file_exists(file_name=file_name_mock)

        client_mock.assert_called_once_with("s3")
        client_mock.return_value.head_object.assert_called_once_with(Bucket=self.bucket_mock, Key=object_mock)

        assert actual_check_result

    def test_object_name(self):
        file_name_mock = "file.txt"
        assert self.s3_manager.object_name(file_name=file_name_mock) == f"{self.s3_path_mock}/{file_name_mock}"
