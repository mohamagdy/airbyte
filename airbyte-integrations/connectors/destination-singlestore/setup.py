#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "airbyte-cdk",
    "singlestoredb==0.4.0",
    "dotmap==1.3.30"
]

TEST_REQUIREMENTS = ["pytest~=6.1"]

setup(
    name="destination_singlestore",
    description="Destination implementation for Singlestore.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
