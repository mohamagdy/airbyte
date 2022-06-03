#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import List, Union

from destination_redshift_no_dbt.field import DataType

VARCHAR = "VARCHAR"
DOUBLE = "DOUBLE PRECISION"
BIGINT = "BIGINT"
BOOLEAN = "BOOLEAN"
TIME = "TIME"
DATE = "DATE"
TIMESTAMP_WITHOUT_TIME_ZONE = "TIMESTAMP WITHOUT TIME ZONE"

MAX_LENGTH = "MAX"
FALLBACK_DATATYPE = DataType(name=VARCHAR, length=MAX_LENGTH)


class DataTypeConverter:
    @staticmethod
    def convert(json_type: Union[str, List[str]], json_schema_format: str = None, json_schema_max_length: str = None) -> DataType:
        # If the field accepts more than one type (for example ["null", "string"]) then fallback to string.
        json_type = [json_type] if not isinstance(json_type, list) else json_type
        json_type = next(iter(set(json_type) - {"null"}), "string")

        return {
            "string": DataTypeConverter._convert_string(json_schema_format, json_schema_max_length),
            "number": DataType(name=DOUBLE),
            "integer": DataType(name=BIGINT),
            "boolean": DataType(name=BOOLEAN)
        }.get(json_type, FALLBACK_DATATYPE)

    @staticmethod
    def _convert_string(json_schema_format: str = None, json_schema_max_length: str = None) -> DataType:
        data_type = {
            "date-time": TIMESTAMP_WITHOUT_TIME_ZONE,
            "time": TIME,
            "date": DATE
        }.get(json_schema_format, VARCHAR)

        if data_type == VARCHAR:
            return DataType(name=VARCHAR, length=json_schema_max_length or MAX_LENGTH)
        else:
            return DataType(name=data_type)



