#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase

from destination_redshift_no_dbt.field import DataType, Field


class TestField(TestCase):
    def test_data_type_without_length(self):
        data_type_name = "INTEGER"
        data_type = DataType(name=data_type_name)

        assert str(data_type) == data_type_name

    def test_data_type_with_length(self):
        data_type_name = "VARCHAR"
        data_type_length = "255"
        data_type = DataType(name=data_type_name, length=data_type_length)

        assert str(data_type) == f"{data_type_name}({data_type_length})"

    def test_field(self):
        data_type = DataType(name="INTEGER")

        field_name = "id"
        field = Field(name=field_name, data_type=data_type)

        assert str(field) == f"\"{field_name}\" {str(data_type)}"
