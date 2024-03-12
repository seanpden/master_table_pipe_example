import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import logging
import unittest

import dotenv
import polars as pl

import datastructs
import helper

TABLE_NAME = "flagship_product_mt"
DATA_PATH = f"src/tests/{TABLE_NAME}.csv"


class TestDataStructs(unittest.TestCase):
    def test_model_test(self):
        model = datastructs.model_test(
            id="test",
            flagship_product="LME",
            ver="1.0.1",
            freeze_date="2022-01-01",
            sunset_date="2022-01-02",
            release_date="2022-01-03",
            is_active=True,
        )
        self.assertEqual(model.id, "test")
        self.assertEqual(model.flagship_product, "LME")
        self.assertEqual(model.ver, "1.0.1")
        self.assertEqual(model.freeze_date, "2022-01-01")
        self.assertEqual(model.sunset_date, "2022-01-02")
        self.assertEqual(model.release_date, "2022-01-03")
        self.assertTrue(model.is_active, True)

    def test_annotations(self):
        schema = datastructs.model_test.__annotations__
        self.assertEqual(schema["id"], pl.String)
        self.assertEqual(schema["flagship_product"], pl.String)
        self.assertEqual(schema["ver"], pl.String)
        self.assertEqual(schema["freeze_date"], pl.Date)
        self.assertEqual(schema["sunset_date"], pl.Date)
        self.assertEqual(schema["release_date"], pl.Date)
        self.assertEqual(schema["is_active"], pl.Boolean)


class TestHelper(unittest.TestCase):
    def test_get_data_from_csv(self):
        data_path: str = DATA_PATH
        schema: dict = datastructs.model_test.__annotations__

        data: pl.DataFrame = helper.get_data_from_csv(
            data_path=data_path,
            data_schema=schema,
        )

        self.assertEqual(data.shape, (3, 7))

    def test_write_to_table(self):
        data_path: str = DATA_PATH
        schema: dict = datastructs.model_test.__annotations__
        table_name: str = "model_test"

        data: pl.DataFrame = helper.get_data_from_csv(
            data_path=data_path,
            data_schema=schema,
        )

        err = helper.write_to_table(df=data, table_name=table_name)
        self.assertIsNone(err)

    def test_read_from_table(self):
        table_name: str = "model_test"
        data_from_db = helper.read_from_table(table_name=table_name)
        self.assertEqual(data_from_db.shape, (3, 7))

    def test_local_and_db_equal(self):
        table_name: str = "flagship_product_mt"
        pl.Config.set_ascii_tables(True)
        data_from_local = helper.get_data_from_csv(
            data_path=f"src/tests/{table_name}.csv",
            data_schema=datastructs.product_version.__annotations__,
        )
        data_from_db = helper.read_from_table(table_name=table_name)
        self.assertTrue(data_from_db.equals(data_from_local))


if __name__ == "__main__":
    logging.basicConfig(
        filename="src/logs/app.log",
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logging.info("----------------------------------")
    logging.info("Starting Master Table Pipeline Tests...")
    logging.info("----------------------------------")

    dotenv.load_dotenv(override=True)
    unittest.main()
