#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase
from unittest.mock import MagicMock

from destination_redshift_no_dbt.field import Field, DataType

from destination_redshift_no_dbt.table import Table, AIRBYTE_KEY_DATA_TYPE, AIRBYTE_AB_ID, AIRBYTE_EMITTED_AT


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

        columns = f"{str(self.field_1)}, {str(self.field_2)}, {str(AIRBYTE_AB_ID)}, {str(AIRBYTE_EMITTED_AT)}, {str(self.table.reference_key)}"
        primary_key = f"PRIMARY KEY({AIRBYTE_AB_ID.name})"
        foreign_key = f"FOREIGN KEY({self.table.reference_key.name}) REFERENCES {self.table.references.full_name}({AIRBYTE_AB_ID.name})"
        unique_key = f"UNIQUE({AIRBYTE_AB_ID.name})"
        extras = f"BACKUP YES DISTKEY({AIRBYTE_AB_ID.name}) SORTKEY ({AIRBYTE_EMITTED_AT.name})"

        expected_create_statement = f"CREATE TABLE IF NOT EXISTS {self.table.full_name} ( {columns}, {primary_key}, {foreign_key}, {unique_key} ) {extras};"

        assert actual_create_statement == expected_create_statement

    def test_create_staging_table_statement(self):
        actual_create_statement = " ".join(self.table.create_statement(staging=True).split())

        columns = f"{str(self.field_1)}, {str(self.field_2)}, {str(AIRBYTE_AB_ID)}, {str(AIRBYTE_EMITTED_AT)}, {str(self.table.reference_key)}"
        primary_key = f"PRIMARY KEY({AIRBYTE_AB_ID.name})"
        foreign_key = f"FOREIGN KEY({self.table.reference_key.name}) REFERENCES {self.table.references.full_name}({AIRBYTE_AB_ID.name})"
        unique_key = f"UNIQUE({AIRBYTE_AB_ID.name})"
        extras = f"BACKUP NO DISTKEY({AIRBYTE_AB_ID.name}) SORTKEY ({AIRBYTE_EMITTED_AT.name})"

        expected_create_statement = f"CREATE TABLE IF NOT EXISTS {self.table.full_name} ( {columns}, {primary_key}, {foreign_key}, {unique_key} ) {extras};"

        assert actual_create_statement == expected_create_statement

    def test_truncate_statement(self):
        assert self.table.truncate_statement() == f"TRUNCATE TABLE {self.table.full_name}"

    def test_copy_csv_gzip_statement(self):
        iam_role_arn = "AWS::ARN"
        s3_full_path = "s3://random-bucket/"

        actual_copy_statement = " ".join(self.table.copy_csv_gzip_statement(iam_role_arn=iam_role_arn, s3_full_path=s3_full_path).split())

        extras = "FORMAT CSV TIMEFORMAT 'auto' ACCEPTANYDATE TRUNCATECOLUMNS IGNOREHEADER 1 GZIP"
        expected_copy_statement = f"COPY {self.table.full_name} FROM '{s3_full_path}' iam_role '{iam_role_arn}' {extras}"

        assert actual_copy_statement == expected_copy_statement

    def test_upsert_statements(self):
        staging_table = Table(
            schema=f"{self.schema_name}_staging",
            name=self.table_name,
            fields=self.fields,
            primary_keys=self.table.primary_keys,
            references=self.table.references
        )

        actual_upsert_statement = self.table.upsert_statements(staging_table=staging_table).split(";")

        delete_condition = " AND ".join(
            [f"staging.{column} = {self.table.name}.{column}" for column in self.table.primary_keys]
        )

        expected_delete_statement = f"DELETE FROM {self.table.full_name} USING {staging_table.full_name} AS staging WHERE {delete_condition}"
        expected_insert_statement = f"INSERT INTO {self.table.full_name} SELECT * FROM {staging_table.full_name}"
        expected_truncate_statement = f"TRUNCATE TABLE {staging_table.full_name}"

        assert " ".join(actual_upsert_statement[0].split()) == expected_delete_statement
        assert " ".join(actual_upsert_statement[1].split()) == expected_insert_statement
        assert " ".join(actual_upsert_statement[2].split()) == expected_truncate_statement
