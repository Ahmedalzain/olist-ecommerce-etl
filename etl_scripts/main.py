"""
main.py — Orchestrates the Olist ETL pipeline: Extract -> Transform -> Load.
--------------------------------------------------------
Run it as: python main.py
"""

import os
import logging

from pyspark.sql import SparkSession

import extract
import transform
import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("olist_etl")


def main():
    spark = SparkSession.builder.appName("Olist_ETL").getOrCreate()

    dfs = extract.extract_all(spark)
    result = transform.transform_all(dfs)

    run_db_load = os.environ.get("RUN_DB_LOAD", "false").lower() == "true"
    if run_db_load:
        load.load_all(result)
    else:
        logger.info("RUN_DB_LOAD is not set to 'true' — skipping PostgreSQL write step.")


if __name__ == "__main__":
    main()
