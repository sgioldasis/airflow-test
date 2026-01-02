"""
Airflow 3 DAG that runs a DLT pipeline and publishes one asset event
for every table currently in the target schema.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import duckdb
import dlt
from airflow.decorators import dag, task
from airflow.sdk import Asset   # Airflow 3 official import

from include.dlt_pipeline.load_data import local_csvs

# --------------------------------------------------------------------------- #
# CONFIG
# --------------------------------------------------------------------------- #
DLT_PIPELINE_NAME = "load_data_pipeline"
DLT_DATASET_NAME  = "main"
DUCKDB_DATABASE_PATH = os.environ.get(
    "DUCKDB_DATABASE_PATH",
    "/usr/local/airflow/include/jaffle_shop.duckdb",
)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

# --------------------------------------------------------------------------- #
# BUILD OUTLETS LIST (at DAG-parse time)
# --------------------------------------------------------------------------- #
def discover_tables() -> list[Asset]:
    """Return a list of Asset objects for the 3 raw tables that DLT loads."""
    # Always return the 3 specific raw tables that DLT creates
    # These are the exact assets that dbt_dag is looking for
    return [
        Asset("duckdb://main.raw_customers"),
        Asset("duckdb://main.raw_payments"),
        Asset("duckdb://main.raw_orders")
    ]

TABLES = discover_tables()

# --------------------------------------------------------------------------- #
# DAG
# --------------------------------------------------------------------------- #
@dag(
    dag_id="dlt_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dlt", "duckdb", "asset-events"],
    max_active_tasks=1,
)
def dlt_pipeline():
    @task(
        outlets=TABLES,  # <-- These 3 specific assets will get events on success
    )
    def extract_load_dlt():
        pipeline = dlt.pipeline(
            pipeline_name=DLT_PIPELINE_NAME,
            dataset_name=DLT_DATASET_NAME,
            destination=dlt.destinations.duckdb(
                credentials=DUCKDB_DATABASE_PATH,
                config={"default_schema_name": "main"},
            ),
        )
        load_info = pipeline.run(local_csvs())
        print("DLT load_info:", load_info)
        print("Producing assets:", [str(asset) for asset in TABLES])
        
        return f"Loaded data for {len(TABLES)} raw tables"

    extract_load_dlt()

# --------------------------------------------------------------------------- #
# INSTANTIATE
# --------------------------------------------------------------------------- #
dlt_pipeline()