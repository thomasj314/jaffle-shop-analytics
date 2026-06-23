from setuptools import find_packages, setup

setup(
    name="jaffle_shop_dagster",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "dagster-dbt",
        "databricks-sql-connector",
        "boto3",
    ],
)
