import logging
import os
import sys

import pandas as pd  # type: ignore
import polars as pl
import sqlalchemy.exc


def check_if_valid_data(data: pl.DataFrame) -> None:
    logging.debug("Checking if data is valid")
    logging.debug(data)
    # checking if data is empty
    if data.is_empty():
        logging.error("Data is empty")
        raise ValueError("Data is empty")

    # checking if data has null values
    null_value_count = data.null_count().pipe(sum).item() # type: ignore
    if null_value_count != 0:
        logging.error(f"Data has {null_value_count} null values.\n{data.null_count()}")
        raise ValueError(f"Data has {null_value_count} null values")

    # checking if data has duplicate values in primary keys column
    check_if_pk_unique = data.select(pl.col("id")).is_duplicated()
    if check_if_pk_unique.sum() != 0:
        logging.error("Data has duplicate primary keys")
        raise ValueError("Data has duplicate primary keys")

    logging.info("Data is valid")


def get_data_from_csv(data_path: str, data_schema: dict) -> pl.DataFrame:
    logging.debug(f"Getting data from {data_path}")
    data: pl.DataFrame = pl.read_csv(
        data_path,
        has_header=True,
        dtypes=data_schema,
        try_parse_dates=True,
    )

    check_if_valid_data(data=data)

    logging.info("Data successfully read from csv")
    return data


def write_to_table(df: pl.DataFrame, table_name: str) -> None:
    logging.debug(f"Writing data to {table_name}")
    try:
        df.write_database(
            table_name=table_name,
            connection=os.environ["CONNECTION_STRING"],
            if_table_exists="append",
        )
        logging.info(f"Data successfully written to {table_name}")
    except sqlalchemy.exc.IntegrityError as e:
        logging.exception(e)
        sys.exit(
            "IntegrityError: Data's primary keys already exists in the database. Exiting the script..."
        )


def read_from_table(table_name: str) -> pl.DataFrame:
    logging.debug(f"Reading data from {table_name}")

    try:
        pd_data_from_db = pd.read_sql(
            sql=f"SELECT * FROM {table_name}", con=os.environ.get("CONNECTION_STRING")
        )
    except Exception as e:
        logging.exception(e)
        raise

    data = pl.from_pandas(pd_data_from_db)
    check_if_valid_data(data=data)

    logging.info(f"Data successfully read from {table_name}")
    return data
