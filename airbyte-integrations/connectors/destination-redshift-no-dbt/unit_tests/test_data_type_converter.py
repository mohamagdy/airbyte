#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from unittest import TestCase

from destination_redshift_no_dbt.data_type_converter import DataTypeConverter, VARCHAR, MAX_LENGTH, DOUBLE, BIGINT, BOOLEAN, \
    TIMESTAMP_WITHOUT_TIME_ZONE, TIME, DATE


class TestDataTypeConverter(TestCase):
    def test_convert_string(self):
        actual_data_type = DataTypeConverter.convert("string")
    
        assert actual_data_type.name == VARCHAR
        assert actual_data_type.length == MAX_LENGTH
    
    def test_convert_string_with_length(self):
        length = "255"
        actual_data_type = DataTypeConverter.convert(json_type="string", json_schema_max_length=length)
    
        assert actual_data_type.name == VARCHAR
        assert actual_data_type.length == length
    
    def test_convert_string_date_time(self):
        actual_data_type = DataTypeConverter.convert(json_type="string", json_schema_format="date-time")
    
        assert actual_data_type.name == TIMESTAMP_WITHOUT_TIME_ZONE
        assert actual_data_type.length is None
    
    def test_convert_string_time(self):
        actual_data_type = DataTypeConverter.convert(json_type="string", json_schema_format="time")
    
        assert actual_data_type.name == TIME
        assert actual_data_type.length is None
    
    def test_convert_string_time(self):
        actual_data_type = DataTypeConverter.convert(json_type="string", json_schema_format="date")
    
        assert actual_data_type.name == DATE
        assert actual_data_type.length is None
    
    def test_convert_number(self):
        actual_data_type = DataTypeConverter.convert(json_type="number")
    
        assert actual_data_type.name == DOUBLE
        assert actual_data_type.length is None
    
    def test_convert_integer(self):
        actual_data_type = DataTypeConverter.convert(json_type="integer")
    
        assert actual_data_type.name == BIGINT
        assert actual_data_type.length is None
    
    def test_convert_boolean(self):
        actual_data_type = DataTypeConverter.convert(json_type="boolean")
    
        assert actual_data_type.name == BOOLEAN
        assert actual_data_type.length is None
