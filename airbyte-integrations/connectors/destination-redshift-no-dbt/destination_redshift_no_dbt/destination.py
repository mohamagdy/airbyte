#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import argparse
import logging
from typing import Mapping, Any, Iterable, Dict

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, ConfiguredAirbyteCatalog, AirbyteMessage, Status, Type

from destination_redshift_no_dbt.connection_pool import ConnectionPool
from destination_redshift_no_dbt.csv_writer import CSVWriter
from destination_redshift_no_dbt.initializer import Initializer
from destination_redshift_no_dbt.records_writer import RecordsWriter
from destination_redshift_no_dbt.s3_objects_manager import S3ObjectsManager
from destination_redshift_no_dbt.stream import Stream

logger = logging.getLogger("airbyte")


class DestinationRedshiftNoDbt(Destination):
    def __init__(self):
        self.streams: Dict[str, Stream] = dict()

        self.csv_writers: Dict[str, CSVWriter] = dict()

        self.last_flushed_state = None

        self.connection_pool = None

        self.s3_object_manager = None
        self.iam_role_arn = None

    def run_cmd(self, parsed_args: argparse.Namespace) -> Iterable[AirbyteMessage]:
        cmd = parsed_args.command

        if cmd in ["write", "check"]:
            config = self.read_config(config_path=parsed_args.config)
            self.connection_pool = ConnectionPool(config=config)
            self.connection_pool.create_pool()

            if cmd == "write":
                self.s3_object_manager = S3ObjectsManager(
                    bucket=config.get("s3_bucket_name"),
                    s3_path=config.get("s3_bucket_path"),
                    aws_access_key_id=config.get("access_key_id"),
                    aws_secret_access_key=config.get("secret_access_key")
                )

                self.iam_role_arn = config.get("iam_role_arn")

                initializer = Initializer(
                    configured_catalog=ConfiguredAirbyteCatalog.parse_file(parsed_args.catalog),
                    connection_pool=self.connection_pool
                )
                self.streams = initializer.streams()
                initializer.create_final_tables(streams=self.streams)
                initializer.create_staging_tables(streams=self.streams)
                self.csv_writers = Initializer.csv_writers(streams=self.streams)

        return super().run_cmd(parsed_args)

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            cursor = self.connection_pool.get_connection(autocommit=True).cursor()
            cursor.execute("SELECT 1")
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")

    def write(
            self,
            config: Mapping[str, Any],
            configured_catalog: ConfiguredAirbyteCatalog,
            input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        records_writer = RecordsWriter(
            connection_pool=self.connection_pool,
            streams=self.streams,
            csv_writers=self.csv_writers,
            s3_object_manager=self.s3_object_manager,
            iam_role_arn=self.iam_role_arn
        )

        for message in input_messages:
            if message.type == Type.STATE:
                records_writer.flush()

                current_state = message.state.json(exclude_unset=True)

                if self.last_flushed_state != current_state:
                    yield message

                self.last_flushed_state = current_state
            elif message.type == Type.RECORD:
                records_writer.write(message=message)

        records_writer.flush()
        self.connection_pool.close_all()
