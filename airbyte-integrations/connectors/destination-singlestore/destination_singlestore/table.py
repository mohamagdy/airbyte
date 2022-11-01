#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import List, Optional

from destination_singlestore.data_type_converter import VARCHAR, TIMESTAMP
from destination_singlestore.field import Field, DataType

AIRBYTE_ID_NAME = "_airbyte_ab_id"
AIRBYTE_EMITTED_AT_NAME = "_airbyte_emitted_at"

AIRBYTE_KEY_MAX_LENGTH = "32"
AIRBYTE_KEY_DATA_TYPE = DataType(name=VARCHAR, length=AIRBYTE_KEY_MAX_LENGTH)
AIRBYTE_AB_ID = Field(name=AIRBYTE_ID_NAME, data_type=AIRBYTE_KEY_DATA_TYPE)
AIRBYTE_EMITTED_AT = Field(name=AIRBYTE_EMITTED_AT_NAME, data_type=DataType(name=TIMESTAMP))


class Table:
    def __init__(self, schema: str, name: str, fields: List[Field] = None, primary_keys: List[str] = None, references: "Table" = None):
        self.schema = schema
        self.name = name
        self._fields = fields or list()

        self.primary_keys = [AIRBYTE_AB_ID.name, *(primary_keys or list())]

        self.references = references

        self._reference_key = None

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.name}"

    @property
    def reference_key(self) -> Optional[Field]:
        if self._reference_key_name and not self._reference_key:
            self._reference_key = Field(name=self._reference_key_name, data_type=AIRBYTE_KEY_DATA_TYPE)
        return self._reference_key

    @property
    def fields(self) -> List[Field]:
        return list(filter(None, [AIRBYTE_AB_ID, AIRBYTE_EMITTED_AT, *self._fields, self.reference_key]))

    def add_field(self, field: Field):
        self._fields.append(field)

    @property
    def field_names(self) -> List[str]:
        return list(map(lambda field: field.name, self.fields))

    def create_statement(self, staging: bool = False) -> str:
        primary_keys = f", PRIMARY KEY({', '.join(self.primary_keys)})"

        fields = ", ".join(map(lambda field: str(field), self.fields))

        sort_key = f", SORT KEY({AIRBYTE_EMITTED_AT_NAME})"

        return f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.name} (
                {fields}{primary_keys}{sort_key}
            )
            AUTOSTATS_ENABLED = {'TRUE' if not staging else 'FALSE'};
        """

    def truncate_statement(self) -> str:
        return f"TRUNCATE TABLE {self.full_name}"

    def load_csv_gzip_statement(self, path: str) -> str:
        return f"""
            LOAD DATA LOCAL INFILE '{path}' COMPRESSION GZIP
            INTO TABLE {self.schema}.{self.name}
            FIELDS TERMINATED BY ','
            IGNORE 1 LINES
        """

    def upsert_statement(self, staging_table: "Table") -> str:
        fields = ", ".join(
            [f"{field_name} = VALUES({field_name})" for field_name in self.field_names]
        )

        return f"""INSERT INTO {self.full_name} SELECT * FROM {staging_table.full_name} ON DUPLICATE KEY UPDATE {fields};"""

    def deduplicate_statement(self) -> str:
        return f"""
                WITH duplicates AS (
                    SELECT *, row_number() OVER (PARTITION BY {AIRBYTE_ID_NAME} ORDER BY {AIRBYTE_EMITTED_AT_NAME} DESC) as rn
                    FROM {self.schema}.{self.name}
                )
                DELETE FROM {self.schema}.{self.name} WHERE {AIRBYTE_ID_NAME} IN (SELECT {AIRBYTE_ID_NAME} FROM duplicates WHERE rn > 1)
            """

    @property
    def _reference_key_name(self) -> Optional[str]:
        if self.references:
            return f"_airbyte_{self.references.name}_id"
