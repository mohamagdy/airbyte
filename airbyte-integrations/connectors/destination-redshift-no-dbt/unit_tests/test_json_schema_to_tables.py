from unittest import TestCase

from destination_redshift_no_dbt.data_type_converter import VARCHAR, TIMESTAMP_WITHOUT_TIME_ZONE, MAX_LENGTH
from destination_redshift_no_dbt.json_schema_to_tables import JsonSchemaToTables
from destination_redshift_no_dbt.table import AIRBYTE_ID_NAME, AIRBYTE_EMITTED_AT_NAME, AIRBYTE_KEY_MAX_LENGTH


class TestJsonSchemaToTables(TestCase):
    def setUp(self) -> None:
        self.id_field_mock = "id"
        self.name_field_mock = "name"

        self.id_field_length_mock = 13
        self.name_field_length_mock = 13

        self.schema_mock = "schema_mock"
        self.root_table_mock = "users"

        self.json_schema_to_tables = JsonSchemaToTables(
            schema=self.schema_mock,
            root_table=self.root_table_mock,
            primary_keys=[["users", self.id_field_mock]]
        )

    def test_convert_simple_json_schema(self):
        json_schema_mock = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                self.id_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.id_field_length_mock
                },
                self.name_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.name_field_length_mock
                }
            }
        }

        self.json_schema_to_tables.convert(json_schema_mock)

        tables = self.json_schema_to_tables.tables

        assert len(tables) == 1

        assert list(tables.keys()) == [self.root_table_mock]

        users_table = list(tables.values())[0]

        assert users_table.name == self.root_table_mock
        assert users_table.schema == self.schema_mock
        assert len(users_table.fields) == 4

        assert users_table.primary_keys == [AIRBYTE_ID_NAME, self.id_field_mock]
        assert users_table.field_names == [self.id_field_mock, self.name_field_mock, AIRBYTE_ID_NAME, AIRBYTE_EMITTED_AT_NAME]

        id_field = list(filter(lambda field: field.name == self.id_field_mock, users_table.fields))[0]

        assert id_field.data_type.name == VARCHAR
        assert id_field.data_type.length == self.id_field_length_mock

        name_field = list(filter(lambda field: field.name == self.name_field_mock, users_table.fields))[0]

        assert name_field.data_type.name == VARCHAR
        assert name_field.data_type.length == self.name_field_length_mock

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

    def test_convert_simple_json_schema_without_primary_keys(self):
        json_schema_to_tables = JsonSchemaToTables(schema=self.schema_mock, root_table=self.root_table_mock, primary_keys=None)

        json_schema_mock = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                self.id_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.id_field_length_mock
                },
                self.name_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.name_field_length_mock
                }
            }
        }

        json_schema_to_tables.convert(json_schema_mock)

        tables = json_schema_to_tables.tables

        assert len(tables) == 1

        assert list(tables.keys()) == [self.root_table_mock]

        users_table = list(tables.values())[0]

        assert users_table.name == self.root_table_mock
        assert users_table.schema == self.schema_mock
        assert len(users_table.fields) == 4

        assert users_table.primary_keys == [AIRBYTE_ID_NAME]
        assert users_table.field_names == [self.id_field_mock, self.name_field_mock, AIRBYTE_ID_NAME, AIRBYTE_EMITTED_AT_NAME]

        id_field = list(filter(lambda field: field.name == self.id_field_mock, users_table.fields))[0]

        assert id_field.data_type.name == VARCHAR
        assert id_field.data_type.length == self.id_field_length_mock

        name_field = list(filter(lambda field: field.name == self.name_field_mock, users_table.fields))[0]

        assert name_field.data_type.name == VARCHAR
        assert name_field.data_type.length == self.name_field_length_mock

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

    def test_convert_nested_objects_in_json_schema(self):
        address_field_mock = "address"
        street_field_mock = "street"

        json_schema_mock = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                self.id_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.id_field_length_mock
                },
                self.name_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.name_field_length_mock
                },
                address_field_mock: {
                    "type": "object",
                    "properties": {
                        "street": {
                            "type": "string"
                        }
                    }
                }
            }
        }

        self.json_schema_to_tables.convert(json_schema_mock)

        tables = self.json_schema_to_tables.tables

        assert len(tables) == 2

        users_table = list(tables.values())[0]

        assert users_table.name == self.root_table_mock
        assert users_table.schema == self.schema_mock
        assert len(users_table.fields) == 4

        assert users_table.primary_keys == [AIRBYTE_ID_NAME, self.id_field_mock]
        assert users_table.field_names == [self.id_field_mock, self.name_field_mock, AIRBYTE_ID_NAME, AIRBYTE_EMITTED_AT_NAME]

        id_field = list(filter(lambda field: field.name == self.id_field_mock, users_table.fields))[0]

        assert id_field.data_type.name == VARCHAR
        assert id_field.data_type.length == self.id_field_length_mock

        name_field = list(filter(lambda field: field.name == self.name_field_mock, users_table.fields))[0]

        assert name_field.data_type.name == VARCHAR
        assert name_field.data_type.length == self.name_field_length_mock

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

        users_address_table = list(tables.values())[1]

        assert users_address_table.name == f"{self.root_table_mock}_{address_field_mock}"
        assert users_address_table.schema == self.schema_mock
        assert len(users_address_table.fields) == 4

        assert users_address_table.primary_keys == [AIRBYTE_ID_NAME, users_address_table.reference_key.name]
        assert users_address_table.field_names == [
            street_field_mock,
            AIRBYTE_ID_NAME,
            AIRBYTE_EMITTED_AT_NAME,
            users_address_table.reference_key.name
        ]

        street_field = list(filter(lambda field: field.name == street_field_mock, users_address_table.fields))[0]
        assert street_field.data_type.name == VARCHAR
        assert street_field.data_type.length == MAX_LENGTH

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_address_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_address_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

        reference_key_field = list(filter(lambda f: f.name == users_address_table.reference_key.name, users_address_table.fields))[0]

        assert reference_key_field.data_type.name == VARCHAR
        assert reference_key_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

    def test_convert_arrays_with_items_without_properties_in_json_schema(self):
        tags_field_mock = "tags"

        json_schema_mock = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                self.id_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.id_field_length_mock
                },
                self.name_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.name_field_length_mock
                },
                tags_field_mock: {
                    "type": "array",
                    "items": {
                        "type": ["null", "string"]
                    }
                }
            }
        }

        self.json_schema_to_tables.convert(json_schema_mock)

        tables = self.json_schema_to_tables.tables

        assert len(tables) == 1

        users_table = list(tables.values())[0]

        assert users_table.name == self.root_table_mock
        assert users_table.schema == self.schema_mock
        assert len(users_table.fields) == 5

        assert users_table.primary_keys == [AIRBYTE_ID_NAME, self.id_field_mock]
        assert users_table.field_names == [
            self.id_field_mock,
            self.name_field_mock,
            tags_field_mock,
            AIRBYTE_ID_NAME,
            AIRBYTE_EMITTED_AT_NAME
        ]

        id_field = list(filter(lambda field: field.name == self.id_field_mock, users_table.fields))[0]

        assert id_field.data_type.name == VARCHAR
        assert id_field.data_type.length == self.id_field_length_mock

        name_field = list(filter(lambda field: field.name == self.name_field_mock, users_table.fields))[0]

        assert name_field.data_type.name == VARCHAR
        assert name_field.data_type.length == self.name_field_length_mock

        flags_field = list(filter(lambda field: field.name == tags_field_mock, users_table.fields))[0]

        assert flags_field.data_type.name == VARCHAR
        assert flags_field.data_type.length == MAX_LENGTH

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

    def test_convert_arrays_without_items_without_properties_in_json_schema(self):
        tags_field_mock = "tags"

        json_schema_mock = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                self.id_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.id_field_length_mock
                },
                self.name_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.name_field_length_mock
                },
                tags_field_mock: {
                    "type": "array"
                }
            }
        }

        self.json_schema_to_tables.convert(json_schema_mock)

        tables = self.json_schema_to_tables.tables

        assert len(tables) == 1

        users_table = list(tables.values())[0]

        assert users_table.name == self.root_table_mock
        assert users_table.schema == self.schema_mock
        assert len(users_table.fields) == 5

        assert users_table.primary_keys == [AIRBYTE_ID_NAME, self.id_field_mock]
        assert users_table.field_names == [
            self.id_field_mock,
            self.name_field_mock,
            tags_field_mock,
            AIRBYTE_ID_NAME,
            AIRBYTE_EMITTED_AT_NAME
        ]

        id_field = list(filter(lambda field: field.name == self.id_field_mock, users_table.fields))[0]

        assert id_field.data_type.name == VARCHAR
        assert id_field.data_type.length == self.id_field_length_mock

        name_field = list(filter(lambda field: field.name == self.name_field_mock, users_table.fields))[0]

        assert name_field.data_type.name == VARCHAR
        assert name_field.data_type.length == self.name_field_length_mock

        flags_field = list(filter(lambda field: field.name == tags_field_mock, users_table.fields))[0]

        assert flags_field.data_type.name == VARCHAR
        assert flags_field.data_type.length == MAX_LENGTH

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

    def test_convert_arrays_with_properties_in_json_schema(self):
        addresses_field_mock = "addresses"
        street_field_mock = "street"

        json_schema_mock = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                self.id_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.id_field_length_mock
                },
                self.name_field_mock: {
                    "type": ["null", "string"],
                    "maxLength": self.name_field_length_mock
                },
                addresses_field_mock: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            street_field_mock: {
                                "type": ["null", "string"]
                            }
                        }
                    }
                }
            }
        }

        self.json_schema_to_tables.convert(json_schema_mock)

        tables = self.json_schema_to_tables.tables

        assert len(tables) == 2

        users_table = list(tables.values())[0]

        assert users_table.name == self.root_table_mock
        assert users_table.schema == self.schema_mock
        assert len(users_table.fields) == 4

        assert users_table.primary_keys == [AIRBYTE_ID_NAME, self.id_field_mock]
        assert users_table.field_names == [
            self.id_field_mock,
            self.name_field_mock,
            AIRBYTE_ID_NAME,
            AIRBYTE_EMITTED_AT_NAME
        ]

        id_field = list(filter(lambda field: field.name == self.id_field_mock, users_table.fields))[0]

        assert id_field.data_type.name == VARCHAR
        assert id_field.data_type.length == self.id_field_length_mock

        name_field = list(filter(lambda field: field.name == self.name_field_mock, users_table.fields))[0]

        assert name_field.data_type.name == VARCHAR
        assert name_field.data_type.length == self.name_field_length_mock

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

        user_addresses_table = list(tables.values())[1]

        assert user_addresses_table.name == f"{self.root_table_mock}_{addresses_field_mock}"
        assert user_addresses_table.schema == self.schema_mock
        assert len(user_addresses_table.fields) == 4

        assert user_addresses_table.primary_keys == [AIRBYTE_ID_NAME]
        assert user_addresses_table.field_names == [
            street_field_mock,
            AIRBYTE_ID_NAME,
            AIRBYTE_EMITTED_AT_NAME,
            user_addresses_table.reference_key.name
        ]

        street_field = list(filter(lambda field: field.name == street_field_mock, user_addresses_table.fields))[0]

        assert street_field.data_type.name == VARCHAR
        assert street_field.data_type.length == MAX_LENGTH

        airbyte_id_field = list(filter(lambda field: field.name == AIRBYTE_ID_NAME, users_table.fields))[0]

        assert airbyte_id_field.data_type.name == VARCHAR
        assert airbyte_id_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH

        airbyte_emitted_at_field = list(filter(lambda field: field.name == AIRBYTE_EMITTED_AT_NAME, users_table.fields))[0]

        assert airbyte_emitted_at_field.data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE

        reference_key_field = list(filter(lambda f: f.name == user_addresses_table.reference_key.name, user_addresses_table.fields))[0]

        assert reference_key_field.data_type.name == VARCHAR
        assert reference_key_field.data_type.length == AIRBYTE_KEY_MAX_LENGTH
