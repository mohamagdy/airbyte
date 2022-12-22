#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase
from unittest.mock import MagicMock

from destination_singlestore.field import Field, DataType

from destination_singlestore.table import Table, AIRBYTE_KEY_DATA_TYPE, AIRBYTE_AB_ID, AIRBYTE_EMITTED_AT


class TestTable(TestCase):
    def setUp(self) -> None:
        self.reference_table_name = "reference"
        self.schema_name = "schema"
        self.table_name = "table"

        field_1_name = "id"
        integer_data_type = DataType(name="INTEGER")
        self.field_1 = Field(name=field_1_name, data_type=integer_data_type)

        field_2_name = "name"
        varchar_data_type = DataType(name="VARCHAR", length="255")
        self.field_2 = Field(name=field_2_name, data_type=varchar_data_type)

        self.fields = [self.field_1, self.field_2]

        primary_keys_mock = MagicMock()
        references_mock = MagicMock()
        references_mock.name = self.reference_table_name
        references_mock.full_name = f"{self.schema_name}.{self.reference_table_name}"

        self.table = Table(
            schema=self.schema_name,
            name=self.table_name,
            fields=self.fields,
            primary_keys=primary_keys_mock,
            references=references_mock
        )

    def test_full_name(self):
        assert self.table.full_name == f"{self.schema_name}.{self.table_name}"

    def test_reference_key(self):
        expected_reference_key = Field(name=f"_airbyte_{self.reference_table_name}_id", data_type=AIRBYTE_KEY_DATA_TYPE)
        actual_reference_key = self.table.reference_key

        assert expected_reference_key.name == actual_reference_key.name

    def test_fields(self):
        fields = self.table.fields
        reference_key = self.table.reference_key

        assert len(fields) == 5
        assert AIRBYTE_AB_ID in fields
        assert AIRBYTE_EMITTED_AT in fields
        assert self.field_1 in fields
        assert self.field_2 in fields
        assert reference_key in fields

    def test_fields_without_reference_key(self):
        self.table.references = None
        fields = self.table.fields

        assert len(fields) == 4
        assert AIRBYTE_AB_ID in fields
        assert AIRBYTE_EMITTED_AT in fields
        assert self.field_1 in fields
        assert self.field_2 in fields

    def test_field_names(self):
        field_names = self.table.field_names

        assert self.field_1.name in field_names
        assert self.field_2.name in field_names

    def test_field_names_contain_airbyte_columns(self):
        field_names = self.table.field_names

        assert AIRBYTE_AB_ID.name in field_names
        assert AIRBYTE_EMITTED_AT.name in field_names

    def test_create_final_table_statement(self):
        actual_create_statement = " ".join(self.table.create_statement().split())

        columns = f"{str(AIRBYTE_AB_ID)}, {str(AIRBYTE_EMITTED_AT)}, {str(self.field_1)}, {str(self.field_2)}, {str(self.table.reference_key)}"
        primary_key = f"PRIMARY KEY({AIRBYTE_AB_ID.name})"
        sort_key = f"SORT KEY({AIRBYTE_EMITTED_AT.name})"
        extras = f"AUTOSTATS_ENABLED = TRUE"

        expected_create_statement = f"CREATE TABLE IF NOT EXISTS {self.table.full_name} ( {columns}, {primary_key}, {sort_key} ) {extras};"

        assert actual_create_statement == expected_create_statement

    def test_create_staging_table_statement(self):
        actual_create_statement = " ".join(self.table.create_statement(staging=True).split())

        columns = f"{str(AIRBYTE_AB_ID)}, {str(AIRBYTE_EMITTED_AT)}, {str(self.field_1)}, {str(self.field_2)}, {str(self.table.reference_key)}"
        primary_key = f"PRIMARY KEY({AIRBYTE_AB_ID.name})"
        sort_key = f"SORT KEY({AIRBYTE_EMITTED_AT.name})"
        extras = f"AUTOSTATS_ENABLED = FALSE"

        expected_create_statement = f"CREATE TABLE IF NOT EXISTS {self.table.full_name} ( {columns}, {primary_key}, {sort_key} ) {extras};"

        assert actual_create_statement == expected_create_statement

    def test_truncate_statement(self):
        assert self.table.truncate_statement() == f"TRUNCATE TABLE {self.table.full_name}"

    def test_load_csv_gzip_statement(self):
        path_mock = "path/mock"
        actual_load_statement = " ".join(self.table.load_csv_gzip_statement(path=path_mock).split())

        extras = "FIELDS TERMINATED BY ',' IGNORE 1 LINES"
        expected_load_statement = f"LOAD DATA LOCAL INFILE '{path_mock}' COMPRESSION GZIP INTO TABLE {self.table.full_name} {extras}"

        assert actual_load_statement == expected_load_statement

    def test_upsert_statement(self):
        staging_table = Table(
            schema=f"{self.schema_name}_staging",
            name=self.table_name,
            fields=self.fields,
            primary_keys=self.table.primary_keys,
            references=self.table.references
        )

        actual_upsert_statement = self.table.upsert_statement(staging_table=staging_table)

        fields = ", ".join(
            [f"{column} = VALUES({column})" for column in self.table.field_names]
        )

        expected_upsert_statement = f"INSERT INTO {self.table.full_name} SELECT * FROM {staging_table.full_name} " \
                                    f"ON DUPLICATE KEY UPDATE {fields};"

        assert actual_upsert_statement == expected_upsert_statement
