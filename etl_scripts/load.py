"""
load.py — Load stage of the Olist ETL pipeline.
--------------------------------------------------------
Responsible ONLY for writing the transformed DataFrames into PostgreSQL.
"""

import logging

logger = logging.getLogger("olist_etl")


def get_db_config():
    """Reads DB connection settings from environment variables."""
    import os

    return {
        "user": os.environ.get("DB_USER", "postgres"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": os.environ.get("DB_PORT", "5432"),
        "dbname": os.environ.get("DB_NAME", "apoo"),
    }


def get_jdbc_url(db_config):
    return f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['dbname']}"


def truncate_tables_in_order(db_config, table_names):
    """
    Truncates all given tables in a single statement, RESTART IDENTITY CASCADE.
    Requires: pip install psycopg2-binary
    """
    import psycopg2

    if not db_config["password"]:
        raise RuntimeError(
            "DB_PASSWORD environment variable is not set. "
            "Set it before running, e.g.: export DB_PASSWORD='your_password'"
        )

    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
    )
    try:
        with conn.cursor() as cur:
            table_list = ", ".join(table_names)
            cur.execute(f"TRUNCATE TABLE {table_list} RESTART IDENTITY CASCADE;")
        conn.commit()
        logger.info(f"Truncated tables: {table_list}")
    finally:
        conn.close()


def write_to_postgres(df, table_name, db_config):
    """
    Appends a DataFrame's rows into an existing PostgreSQL table.
    Assumes the target table has already been truncated.
    """
    if not db_config["password"]:
        raise RuntimeError(
            "DB_PASSWORD environment variable is not set. "
            "Set it before running, e.g.: export DB_PASSWORD='your_password'"
        )

    jdbc_url = get_jdbc_url(db_config)

    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", db_config["user"]) \
        .option("password", db_config["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    logger.info(f"Loaded table: {table_name}")


def load_all(transformed, db_config=None):
    """
    Truncates and reloads the full Star Schema into PostgreSQL.
    """
    db_config = db_config or get_db_config()

    if not db_config["password"]:
        raise RuntimeError(
            "DB_PASSWORD environment variable is not set. "
            "Set it before running, e.g.: export DB_PASSWORD='your_password'"
        )

    truncate_tables_in_order(db_config, [
        "dwh.fact_order_items",
        "dwh.dim_customers",
        "dwh.dim_products",
        "dwh.dim_sellers",
        "dwh.dim_date",
    ])

    write_to_postgres(transformed["dim_customers"], "dwh.dim_customers", db_config)
    write_to_postgres(transformed["dim_products"], "dwh.dim_products", db_config)
    write_to_postgres(transformed["dim_sellers"], "dwh.dim_sellers", db_config)
    write_to_postgres(transformed["dim_date"], "dwh.dim_date", db_config)
    write_to_postgres(transformed["fact_order_items"], "dwh.fact_order_items", db_config)
