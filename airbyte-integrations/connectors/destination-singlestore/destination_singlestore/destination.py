#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import argparse
from typing import Any, Iterable, Mapping, Dict

import singlestoredb
from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from destination_singlestore.csv_writer import CSVWriter

from destination_singlestore.records_writer import RecordsWriter
from destination_singlestore.initializer import Initializer
from destination_singlestore.stream import Stream


class DestinationSinglestore(Destination):
    def __init__(self):
        self.streams: Dict[str, Stream] = dict()

        self.csv_writers: Dict[str, CSVWriter] = dict()

        self.last_flushed_state = None

        self.connection = None

    def run_cmd(self, parsed_args: argparse.Namespace) -> Iterable[AirbyteMessage]:
        cmd = parsed_args.command

        if cmd in ["write", "check"]:
            config = self.read_config(config_path=parsed_args.config)

            self.connection = singlestoredb.connect(
                host=config.get("host"),
                port=config.get("port"),
                user=config.get("username"),
                password=config.get("password"),
                local_infile=True
            )

            if cmd == "write":
                initializer = Initializer(
                    configured_catalog=ConfiguredAirbyteCatalog.parse_file(parsed_args.catalog),
                    connection=self.connection
                )
                self.streams = initializer.streams()
                initializer.create_final_tables(streams=self.streams)
                initializer.create_staging_tables(streams=self.streams)
                self.csv_writers = Initializer.csv_writers(streams=self.streams)

        return super().run_cmd(parsed_args)

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")

            cursor.close()
            self.connection.close()

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")

    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        records_writer = RecordsWriter(
            connection=self.connection,
            streams=self.streams,
            csv_writers=self.csv_writers
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
        self.connection.close()
