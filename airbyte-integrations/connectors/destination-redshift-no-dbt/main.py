#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from destination_redshift_no_dbt import DestinationRedshiftNoDbt

if __name__ == "__main__":
    DestinationRedshiftNoDbt().run(sys.argv[1:])
