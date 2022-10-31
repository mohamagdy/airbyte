#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from destination_singlestore import DestinationSinglestore

if __name__ == "__main__":
    DestinationSinglestore().run(sys.argv[1:])
