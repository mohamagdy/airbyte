#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Dict

from airbyte_cdk.models import DestinationSyncMode
from destination_redshift_no_dbt.table import Table
from pydantic.main import BaseModel


class Stream(BaseModel):
    name: str
    namespace: str
    destination_sync_mode: DestinationSyncMode
    final_tables: Dict[str, Table]
    staging_tables: Dict[str, Table] = dict()

    class Config:
        arbitrary_types_allowed = True
