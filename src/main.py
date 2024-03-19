import logging

import dotenv
import polars as pl

import datastructs
import helper


def init() -> None:
    dotenv.load_dotenv(override=True)
    logging.basicConfig(
        filename="src/logs/app.log",
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    pl.Config.set_ascii_tables(True)
    logging.info("----------------------------------")
    logging.info("Starting Master Table Pipeline...")
    logging.info("----------------------------------")


def handle_flagship_products() -> None:
    table_name: str = "flagship_product_mt"
    data_path: str = f"src/tests/{table_name}.csv"
    schema: dict = datastructs.product_version.__annotations__

    data: pl.DataFrame = helper.get_data_from_csv(
        data_path=data_path,
        data_schema=schema,
    )

    helper.write_to_table(df=data, table_name=table_name)


def handle_reason_code() -> None:
    table_name: str = "reason_code_mt"
    data_path: str = f"src/tests/{table_name}.csv"
    schema: dict = datastructs.reason_code_mt.__annotations__

    data: pl.DataFrame = helper.get_data_from_csv(
        data_path=data_path,
        data_schema=schema,
    )

    data.drop_in_place("id")

    helper.write_to_table(df=data, table_name=table_name)


def handle_user_input() -> tuple[bool, bool]:
    flagship_module = input("Would you like to handle flagship products? (Y/n) ") == "Y"
    reason_code_module = input("Would you like to handle reason codes? (Y/n) ") == "Y"

    return flagship_module, reason_code_module


def main() -> None:
    init()

    flagship_module, reason_code_module = handle_user_input()

    if flagship_module:
        logging.debug("Starting handle_flagship_products...")
        handle_flagship_products()

    if reason_code_module:
        logging.debug("Starting handle_reason_code...")
        handle_reason_code()

    logging.info("Complete!")


if __name__ == "__main__":
    main()
