"""
extract.py — Extract stage of the Olist ETL pipeline.
--------------------------------------------------------
Responsible ONLY for reading the raw CSV files into Spark DataFrames.
No cleaning, no joins, no business logic — just I/O.
"""

import os
import logging

logger = logging.getLogger("olist_etl")

TABLES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}



def get_base_path():
    """
    Resolves the folder containing the raw CSVs.

    Priority:
      1. OLIST_DATA_PATH environment variable, if set.
      2. <project_root>/source_data/ — computed relative to this file's
         location. Since extract.py now lives in etl_scripts/ (one level
         below the project root), this walks up two levels:
         etl_scripts/extract.py -> etl_scripts/ -> project_root/ -> source_data/
    """
    base_path = os.environ.get(
        "OLIST_DATA_PATH",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "source_data",
        ),
    )
    return base_path if base_path.endswith(os.sep) else base_path + os.sep


def extract_all(spark, base_path=None):
    """
    Reads all 9 raw Olist CSVs into a dict of Spark DataFrames.

    Args:
        spark: an active SparkSession.
        base_path: optional override for the folder containing the CSVs.
                    Defaults to get_base_path().

    Returns:
        dict[str, DataFrame] keyed by the short names in TABLES.

    Raises:
        Exception: re-raised after logging, if any file fails to load —
                   fail fast rather than silently continuing with a
                   half-loaded pipeline.
    """
    base_path = base_path or get_base_path()
    dfs = {}

    for name, file in TABLES.items():
        path = base_path + file
        try:
            dfs[name] = spark.read.csv(path, header=True, inferSchema=True)
            logger.info(f"Loaded: {name} ({dfs[name].count()} rows)")
        except Exception as e:
            logger.error(f"Failed to load {name} from {path}: {e}")
            raise

    return dfs