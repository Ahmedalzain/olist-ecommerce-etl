"""
transform.py — Transform stage of the Olist ETL pipeline.
--------------------------------------------------------
Responsible for all cleaning, surrogate-key generation, and the
Star Schema build (dim_customers, dim_products, dim_sellers, dim_date,
fact_order_items). Takes raw DataFrames in, returns clean DataFrames out.
"""

import logging

from pyspark.sql.functions import (
    col, expr, monotonically_increasing_id, upper, trim,
    to_date, year, month, dayofmonth, dayofweek, quarter
)

logger = logging.getLogger("olist_etl")


def clean_order_items(items_df):
    """Casts price/freight_value to double."""
    return items_df \
        .withColumn("price", col("price").cast("double")) \
        .withColumn("freight_value", col("freight_value").cast("double"))


def parse_purchase_timestamps(orders_df):
    """
    Parses order_purchase_timestamp ('M/d/yyyy H:mm') into a real timestamp
    column (order_purchase_ts), and logs/warns if any rows fail to parse.
    """
    orders_df = orders_df.withColumn(
        "order_purchase_ts",
        expr("try_to_timestamp(order_purchase_timestamp, 'M/d/yyyy H:mm')")
    )

    total_orders = orders_df.count()
    null_dates = orders_df.filter(col("order_purchase_ts").isNull()).count()
    logger.info(f"order_purchase_ts parse check: {null_dates}/{total_orders} rows became NULL")
    if null_dates > 0:
        logger.warning(
            f"{null_dates} rows failed timestamp parsing — these orders will be dropped "
            f"from dim_date joins. Inspect original 'order_purchase_timestamp' format."
        )
    return orders_df


def build_fact_base(items_df, orders_df):
    """Joins order_items with orders to get the base fact table columns."""
    return items_df.join(orders_df, "order_id").select(
        "order_id",
        "order_item_id",
        "product_id",
        "customer_id",
        "price",
        "seller_id",
        "order_status",
        "freight_value",
        "order_purchase_ts",
    )


def build_dim_customers(customers_df):
    """
    Builds dim_customers with a cached surrogate key (CustomerKey).

    NOTE on caching: Spark is lazy — without .cache() + .count() here,
    monotonically_increasing_id() would be RE-EVALUATED (and could
    produce DIFFERENT values) every time this DataFrame is read again
    later (validation checks, the join into fact_order_items, the final
    write to Postgres). That mismatch is what causes silent FK violations
    when writing to a database with foreign keys. Caching freezes the
    keys the moment they're generated.
    """
    dim_customers = customers_df.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ).dropDuplicates(["customer_id"])

    dim_customers = dim_customers \
        .withColumn("customer_city", trim(upper(col("customer_city")))) \
        .withColumn("customer_state", trim(upper(col("customer_state"))))

    dim_customers = dim_customers.withColumn("CustomerKey", monotonically_increasing_id() + 1)
    dim_customers = dim_customers.cache()
    dim_customers.count()  # materialize now, before any joins use it

    return dim_customers.select(
        "CustomerKey",
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    )


def build_dim_products(products_df):
    """Builds dim_products with a cached surrogate key (ProductKey)."""
    dim_products = products_df.select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ).dropDuplicates(["product_id"])

    dim_products = dim_products.withColumn(
        "product_category_name", trim(upper(col("product_category_name")))
    )
    dim_products = dim_products.withColumn("ProductKey", monotonically_increasing_id() + 1)
    dim_products = dim_products.cache()
    dim_products.count()

    return dim_products


def build_dim_sellers(sellers_df):
    """Builds dim_sellers with a cached surrogate key (SellerKey)."""
    dim_sellers = sellers_df.select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ).dropDuplicates(["seller_id"])

    dim_sellers = dim_sellers \
        .withColumn("seller_city", upper(trim(col("seller_city")))) \
        .withColumn("seller_state", upper(trim(col("seller_state"))))

    dim_sellers = dim_sellers.withColumn("SellerKey", monotonically_increasing_id() + 1)
    dim_sellers = dim_sellers.cache()
    dim_sellers.count()

    return dim_sellers


def build_dim_date(fact_order_items):
    """Builds dim_date from the distinct purchase dates in the fact table."""
    dates_df = fact_order_items.select("order_purchase_ts").distinct()
    dates_df = dates_df.withColumn("Date", to_date(col("order_purchase_ts")))

    dim_date = dates_df.select(
        "Date",
        year("Date").alias("Year"),
        month("Date").alias("Month"),
        dayofmonth("Date").alias("Day"),
        dayofweek("Date").alias("Weekday"),
        quarter("Date").alias("Quarter"),
    ).dropDuplicates(["Date"])  # explicit subset, not full-row dedup

    dim_date = dim_date.withColumn("DateKey", monotonically_increasing_id() + 1)
    dim_date = dim_date.cache()
    dim_date.count()

    return dim_date


def run_quality_checks(fact_order_items):
    """
    Checks for NULL foreign keys and duplicate (order_id, order_item_id)
    rows in the fact table, logging an explicit WARNING for each problem
    found (not just a silent print).

    Returns a dict of the counts, in case the caller wants to act on them
    (e.g. abort the load if something is non-zero).
    """
    null_customers = fact_order_items.filter(col("CustomerKey").isNull()).count()
    null_products = fact_order_items.filter(col("ProductKey").isNull()).count()
    null_sellers = fact_order_items.filter(col("SellerKey").isNull()).count()
    null_datekey = fact_order_items.filter(col("DateKey").isNull()).count()
    dup_count = (
        fact_order_items.groupBy("order_id", "order_item_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    logger.info(f"NULL CustomerKey = {null_customers}")
    logger.info(f"NULL ProductKey  = {null_products}")
    logger.info(f"NULL SellerKey   = {null_sellers}")
    logger.info(f"NULL DateKey     = {null_datekey}")
    logger.info(f"Duplicate rows in Fact (by order_id, order_item_id) = {dup_count}")

    checks = {
        "CustomerKey": null_customers,
        "ProductKey": null_products,
        "SellerKey": null_sellers,
        "DateKey": null_datekey,
        "duplicates": dup_count,
    }

    for label, n in checks.items():
        if n > 0:
            if label == "duplicates":
                logger.warning(f"{n} duplicate (order_id, order_item_id) combinations found in fact table.")
            else:
                logger.warning(f"{n} fact rows have a NULL {label} — investigate join mismatch before loading to DWH.")

    return checks


def transform_all(dfs):
    """
    Runs the full transform stage: cleaning, dimension building, and the
    fact table join chain.

    Args:
        dfs: dict of raw DataFrames as returned by extract.extract_all().

    Returns:
        dict with keys: dim_customers, dim_products, dim_sellers, dim_date,
        fact_order_items, quality_checks.
    """
    orders_df = dfs["orders"]
    customers_df = dfs["customers"]
    items_df = dfs["order_items"]
    products_df = dfs["products"]
    sellers_df = dfs["sellers"]

    items_df = clean_order_items(items_df)
    orders_df = parse_purchase_timestamps(orders_df)

    fact_order_items = build_fact_base(items_df, orders_df)

    dim_customers = build_dim_customers(customers_df)
    fact_order_items = fact_order_items.join(
        dim_customers.select("customer_id", "CustomerKey"), on="customer_id", how="left"
    )

    dim_products = build_dim_products(products_df)
    fact_order_items = fact_order_items.join(
        dim_products.select("product_id", "ProductKey"), on="product_id", how="left"
    )

    dim_sellers = build_dim_sellers(sellers_df)
    fact_order_items = fact_order_items.join(
        dim_sellers.select("seller_id", "SellerKey"), on="seller_id", how="left"
    )

    dim_date = build_dim_date(fact_order_items)
    fact_order_items = fact_order_items.withColumn("Date", to_date(col("order_purchase_ts")))
    fact_order_items = fact_order_items.join(
        dim_date.select("Date", "DateKey"), on="Date", how="left"
    )

    fact_order_items = fact_order_items.drop("Date").drop("order_purchase_ts")
    fact_order_items = fact_order_items.drop("customer_id").drop("product_id").drop("seller_id")

    fact_order_items = fact_order_items.cache()
    fact_order_items.count()

    quality_checks = run_quality_checks(fact_order_items)

    return {
        "dim_customers": dim_customers,
        "dim_products": dim_products,
        "dim_sellers": dim_sellers,
        "dim_date": dim_date,
        "fact_order_items": fact_order_items,
        "quality_checks": quality_checks,
    }
