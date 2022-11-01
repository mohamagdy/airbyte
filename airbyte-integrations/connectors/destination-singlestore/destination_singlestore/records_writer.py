#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import logging
from datetime import datetime
from functools import reduce
from hashlib import sha256
from typing import Union, List, Dict

from airbyte_cdk.models import AirbyteMessage, DestinationSyncMode
from dotmap import DotMap
from singlestoredb.connection import Connection

from destination_singlestore.csv_writer import CSVWriter
from destination_singlestore.stream import Stream
from destination_singlestore.table import AIRBYTE_ID_NAME, AIRBYTE_EMITTED_AT, Table

PARENT_CHILD_SPLITTER = "."

logger = logging.getLogger("airbyte")


class RecordsWriter:
    def __init__(self, connection: Connection, streams: Dict[str, Stream], csv_writers: Dict[str, CSVWriter]):
        self.connection = connection
        self.streams = streams
        self.csv_writers = csv_writers

    def write(self, message: AirbyteMessage):
        stream = self.streams[message.record.stream]

        nested_record = DotMap({message.record.stream: message.record.data})

        # Build a simple tree out of the dictionary and visit the parent nodes before children to assign the IDs to the parents
        # before the children so that the children can use the parent IDs in the references (foreign key).
        # Parents key always have shorter length than the children as the children combine the parent and the child keys.
        # The return value is a list of lists.
        #
        # For example, if the input is:
        #       `['orders', 'orders.account', 'orders.accounts.address']`
        # The output is:
        #       `[['orders'], ['orders', 'account'], ['orders', 'accounts', 'address']]`
        nodes = sorted(map(lambda k: k.split(PARENT_CHILD_SPLITTER), stream.final_tables.keys()), key=len)
        emitted_at = message.record.emitted_at

        for node in nodes:
            final_table = stream.final_tables[PARENT_CHILD_SPLITTER.join(node)]

            def get_records(parents: Union[DotMap, List[DotMap]], method: str) -> List[DotMap]:
                if not isinstance(parents, list):
                    parents = [parents]

                parents = list(filter(None, parents))

                children_records = []
                for parent_item in parents:
                    children = parent_item.get(method)
                    if not isinstance(children, list):
                        children = [children]

                    for child_item in children:
                        if child_item and final_table.references and method == node[-1]:
                            child_item[final_table.reference_key.name] = parent_item[AIRBYTE_ID_NAME]

                        children_records.append(child_item)

                return children_records

            records = reduce(get_records, [nested_record] + node)
            records = list(filter(None.__ne__, records))

            if records:
                # Choose primary keys (other than the Airbyte auto generated ID). If there are no primary keys (except the auto
                # generated Airbyte ID, this happens with children), then choose all the fields as hash keys to generate the ID.
                hashing_keys = [pk for pk in final_table.primary_keys if pk != AIRBYTE_ID_NAME] or final_table.field_names

                self._assign_id_and_emitted_at(records=records, hashing_keys=hashing_keys, emitted_at=emitted_at)

                # Assign only the table fields and drop other fields
                records = [
                    DotMap([field_name, record[field_name] or None] for field_name in final_table.field_names) for record in records
                ]

                csv_writer = self.csv_writers[final_table.name]
                csv_writer.write(records)

    def flush(self):
        for stream in self.streams.values():
            for key, final_table in stream.final_tables.items():
                self._flush_csv_writer_to_destination(
                    csv_writer=self.csv_writers[final_table.name],
                    final_table=final_table,
                    staging_table=stream.staging_tables.get(key),
                    mode=stream.destination_sync_mode
                )

    @staticmethod
    def _assign_id_and_emitted_at(records: List[DotMap], emitted_at: int, hashing_keys: List[str]):
        for record in records:
            RecordsWriter._assign_id(record=record, hashing_keys=hashing_keys)
            record[AIRBYTE_EMITTED_AT.name] = datetime.utcfromtimestamp(emitted_at / 1000).isoformat(timespec="seconds")

    @staticmethod
    def _assign_reference_id(records: List[DotMap], reference_key: str, reference_id: str):
        for record in records:
            record[reference_key] = reference_id

    @staticmethod
    def _assign_id(record: DotMap, hashing_keys: List[str]):
        if AIRBYTE_ID_NAME not in record:
            data = "".join([str(record[hashing_key]) for hashing_key in hashing_keys]).encode()

            record[AIRBYTE_ID_NAME] = sha256(data).hexdigest()[-32:]

    def _flush_csv_writer_to_destination(self, csv_writer: CSVWriter, final_table: Table, staging_table: Table, mode: DestinationSyncMode):
        rows_count = csv_writer.rows_count()
        temporary_gzip_file = csv_writer.flush_gzipped()

        if temporary_gzip_file:
            logger.info(f"Flushing {rows_count} to destination")

            cursor = self.connection.cursor()

            if mode in [DestinationSyncMode.append, DestinationSyncMode.overwrite]:
                copy_statement = final_table.load_csv_gzip_statement(path=temporary_gzip_file.name)
                cursor.execute(copy_statement)
            else:
                copy_statement = staging_table.load_csv_gzip_statement(path=temporary_gzip_file.name)
                cursor.execute(copy_statement)

                deduplicate_statement = staging_table.deduplicate_statement()
                cursor.execute(deduplicate_statement)

                upsert_statement = final_table.upsert_statement(staging_table=staging_table)
                cursor.execute(upsert_statement)
                cursor.execute(staging_table.truncate_statement())

            self.connection.commit()

            CSVWriter.delete_gzip_file(temporary_gzip_file)
