#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import logging
from copy import deepcopy
from typing import Dict

from airbyte_cdk.models import ConfiguredAirbyteCatalog, DestinationSyncMode
from singlestoredb.connection import Connection

from destination_singlestore.csv_writer import CSVWriter
from destination_singlestore.json_schema_to_tables import JsonSchemaToTables
from destination_singlestore.stream import Stream


logger = logging.getLogger("airbyte")


class Initializer:
    def __init__(self, configured_catalog: ConfiguredAirbyteCatalog, connection: Connection):
        self.configured_catalog = configured_catalog
        self.connection = connection

    def streams(self) -> Dict[str, Stream]:
        streams = dict()

        for stream in self.configured_catalog.streams:
            schema = stream.stream.namespace
            stream_name = stream.stream.name
            primary_keys = list(map(lambda pks: [stream_name] + pks, stream.primary_key)) or [[]]

            converter = JsonSchemaToTables(schema=schema, root_table=stream_name, primary_keys=primary_keys)
            converter.convert(stream.stream.json_schema)

            sync_mode = stream.destination_sync_mode

            if schema:
                streams[stream_name] = Stream(
                    namespace=schema,
                    name=stream_name,
                    destination_sync_mode=sync_mode,
                    final_tables=converter.tables
                )
            else:
                logger.fatal(f"Stream {stream_name} doesn't have a schema set.")

        return streams

    def create_final_tables(self, streams: Dict[str, Stream]):
        cursor = self.connection.cursor()

        for stream in streams.values():
            for table in stream.final_tables.values():
                cursor.execute(table.create_statement())

                if stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    cursor.execute(table.truncate_statement())

        self.connection.commit()

    def create_staging_tables(self, streams: Dict[str, Stream]):
        cursor = self.connection.cursor()

        for stream in streams.values():
            staging_schema = f"_airbyte_{stream.namespace}"

            if stream.destination_sync_mode == DestinationSyncMode.append_dedup:
                create_schema_statement = f"CREATE SCHEMA IF NOT EXISTS {staging_schema}"
                cursor.execute(create_schema_statement)

                for key, table in stream.final_tables.items():
                    staging_table = deepcopy(table)
                    staging_table.schema = staging_schema

                    stream.staging_tables[key] = staging_table

                    cursor.execute(staging_table.create_statement(staging=True))

        self.connection.commit()

    @staticmethod
    def csv_writers(streams: Dict[str, Stream]) -> Dict[str, CSVWriter]:
        csv_writers = dict()
        for stream in streams.values():
            for table in stream.final_tables.values():
                csv_writer = CSVWriter(table=table)
                csv_writer.initialize_writer()

                csv_writers[table.name] = csv_writer

        return csv_writers
