#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import List, Optional

from destination_redshift_no_dbt.data_type_converter import VARCHAR, TIMESTAMP_WITHOUT_TIME_ZONE
from destination_redshift_no_dbt.field import Field, DataType

AIRBYTE_ID_NAME = "_airbyte_ab_id"
AIRBYTE_EMITTED_AT_NAME = "_airbyte_emitted_at"

AIRBYTE_KEY_MAX_LENGTH = "32"
AIRBYTE_KEY_DATA_TYPE = DataType(name=VARCHAR, length=AIRBYTE_KEY_MAX_LENGTH)
AIRBYTE_AB_ID = Field(name=AIRBYTE_ID_NAME, data_type=AIRBYTE_KEY_DATA_TYPE)
AIRBYTE_EMITTED_AT = Field(name=AIRBYTE_EMITTED_AT_NAME, data_type=DataType(name=TIMESTAMP_WITHOUT_TIME_ZONE))


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
        return list(filter(None, [*self._fields, AIRBYTE_AB_ID, AIRBYTE_EMITTED_AT, self.reference_key]))

    def add_field(self, field: Field):
        self._fields.append(field)

    @property
    def field_names(self) -> List[str]:
        return list(map(lambda field: field.name, self.fields))

    def create_statement(self, staging: bool = False) -> str:
        primary_keys = f", PRIMARY KEY({', '.join(self.primary_keys)})"

        foreign_key = ""
        if self.references:
            reference_key = self.reference_key
            foreign_key = f", FOREIGN KEY({reference_key.name}) REFERENCES {self.references.full_name}({AIRBYTE_ID_NAME})"

        fields = ", ".join(map(lambda field: str(field), self.fields))

        return f"""
            CREATE TABLE IF NOT EXISTS {self.schema}.{self.name} (
                {fields}{primary_keys}{foreign_key},
                UNIQUE({AIRBYTE_AB_ID.name})
            )
            BACKUP {'NO' if staging else 'YES'}
            DISTKEY({AIRBYTE_ID_NAME})
            SORTKEY ({AIRBYTE_EMITTED_AT_NAME});
        """

    def truncate_statement(self) -> str:
        return f"TRUNCATE TABLE {self.full_name}"

    def copy_csv_gzip_statement(self, iam_role_arn: str, s3_full_path: str) -> str:
        return f"""
            COPY {self.schema}.{self.name}
            FROM '{s3_full_path}'
            iam_role '{iam_role_arn}'
            FORMAT CSV
            TIMEFORMAT 'auto'
            ACCEPTANYDATE
            TRUNCATECOLUMNS
            IGNOREHEADER 1
            GZIP
        """

    def upsert_statements(self, staging_table: "Table") -> str:
        delete_condition = " AND ".join(
            [f"staging.{column} = {self.name}.{column}" for column in self.primary_keys]
        )

        delete_from_final_table = f"""
            DELETE FROM {self.full_name}
            USING {staging_table.full_name} AS staging WHERE {delete_condition}
        """

        insert_into_final_table = f"""
            INSERT INTO {self.full_name}
            SELECT * FROM {staging_table.full_name}
        """

        truncate_staging_table = f"TRUNCATE TABLE {staging_table.full_name}"

        return f"{delete_from_final_table};{insert_into_final_table};{truncate_staging_table};"

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
