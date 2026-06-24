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
# NOTE: payments, reviews, geolocation, and category_translation are part of
# the original Olist dataset but are NOT currently used anywhere in
# transform.py's Star Schema build (dim_customers, dim_products, dim_sellers,
# dim_date, fact_order_items). They're intentionally left out here to avoid
# loading files that aren't needed. If you later extend transform.py to use
# any of them (e.g. adding review scores to the fact table), add the
# corresponding entry back to this dict:
#   "payments": "olist_order_payments_dataset.csv",
#   "reviews": "olist_order_reviews_dataset.csv",
#   "geolocation": "olist_geolocation_dataset.csv",
#   "category_translation": "product_category_name_translation.csv",


def get_base_path():
    """
    Resolves the folder containing the raw CSVs.

    Priority:
      1. OLIST_DATA_PATH environment variable, if set.
      2. <project_root>/source_data/ — computed relative to this file's
         location. Since extract.py lives directly in the project root
         (alongside transform.py, load.py, main.py), this is simply
         "./source_data/".
    """
    base_path = os.environ.get(
        "OLIST_DATA_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "source_data"),
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