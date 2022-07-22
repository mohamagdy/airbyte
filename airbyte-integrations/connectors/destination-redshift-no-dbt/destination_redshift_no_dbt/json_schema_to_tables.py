#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from collections import OrderedDict
from typing import Optional, List, Dict, Union

from destination_redshift_no_dbt.data_type_converter import DataTypeConverter, FALLBACK_DATATYPE
from destination_redshift_no_dbt.field import Field
from destination_redshift_no_dbt.table import Table

PARENT_CHILD_SPLITTER = "."


class JsonSchemaToTables:
    def __init__(self, schema: str, root_table: str, primary_keys: Optional[List[List[str]]]):
        self.schema = schema
        self.root = root_table

        self.tables = OrderedDict()
        self.primary_keys = primary_keys

    def convert(self, json_schema: Dict[str, Union[str, dict]]):
        for item_key, item_value in json_schema.items():
            if item_key == "properties":
                self._extract_tables(item_value, name=self.root, primary_keys=self.primary_keys)

    def _extract_tables(self, properties: Dict[str, Union[str, dict]], name: str, primary_keys: List[List[str]] = None,
                        references: Table = None, reference_key_as_primary_key: bool = False):
        table_name = name.replace(PARENT_CHILD_SPLITTER, "_")
        table_primary_keys = list(map(lambda pk: pk[-1], filter(lambda pks: pks[0:-1] == name.split("."), primary_keys or [[]])))
        table = Table(schema=self.schema, name=table_name, primary_keys=table_primary_keys, references=references)

        if references and reference_key_as_primary_key:
            table.primary_keys += [table.reference_key.name]

        self.tables[name] = table

        for property_key, property_value in properties.items():
            item_type = property_value.get("type")
            item_type = [item_type] if not isinstance(item_type, list) else item_type
            if not set(item_type).intersection({"object", "array"}):
                data_type = DataTypeConverter.convert(property_value["type"], property_value.get("format"), property_value.get("maxLength"))
                table.add_field(field=Field(name=property_key, data_type=data_type))
            else:
                if set(item_type).intersection({"object"}):
                    self._convert_object_to_table(
                        name=name,
                        property_key=property_key,
                        property_value=property_value,
                        references=table,
                        reference_key_as_primary_key=True
                    )
                else:  # array
                    if "items" in property_value:
                        property_value = property_value.get("items")
                        self._convert_object_to_table(
                            name=name,
                            property_key=property_key,
                            property_value=property_value,
                            references=table,
                            reference_key_as_primary_key=False  # Since it is an array, multiple rows can have the same reference key
                        )
                    else:
                        table.add_field(Field(name=property_key, data_type=FALLBACK_DATATYPE))

    def _convert_object_to_table(self, name: str, property_key: str, property_value: Dict[str, Union[str, dict]], references: Table,
                                 reference_key_as_primary_key: bool):
        if "properties" in property_value:
            properties = property_value.get("properties")
            self._extract_tables(
                properties=properties,
                name=f"{name}.{property_key}",
                primary_keys=self.primary_keys,
                references=references,
                reference_key_as_primary_key=reference_key_as_primary_key
            )
        else:  # If no `properties`, treat the field as a string
            references.add_field(Field(name=property_key, data_type=FALLBACK_DATATYPE))


