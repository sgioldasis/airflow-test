from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtDag, RenderConfig
from cosmos.profiles import DuckDBUserPasswordProfileMapping
import os
from datetime import datetime

DUCKDB_CONN_ID = os.getenv("DUCKDB_CONN_ID", "duckdb_default")
SCHEMA_NAME = os.getenv("DUCKDB_SCHEMA", "main")

DBT_PROJECT_PATH = "/usr/local/airflow/dbt/jaffle_shop_duckdb"

DBT_EXECUTABLE_PATH = f"{os.getenv('AIRFLOW_HOME')}/dbt_venv/bin/dbt"

# Use the same DuckDB database file path from environment variable
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "/usr/local/airflow/include/jaffle_shop.duckdb")

_project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH
)

_profile_config = ProfileConfig(
    profile_name="airflow_duckdb",
    target_name="dev",
    profile_mapping=DuckDBUserPasswordProfileMapping(
        conn_id=DUCKDB_CONN_ID,
        profile_args={
            "path": DUCKDB_DATABASE_PATH,  # Use the same database file everywhere
            "schema": SCHEMA_NAME,
            "read_write": True
        }
    )
)

_execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH
)

my_dag = DbtDag(
    dag_id="my_dag",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    schedule="@daily",
    start_date=datetime(2025,1,1),
    max_active_runs=1,
    # Add concurrency control to prevent multiple tasks from running simultaneously
    max_active_tasks=1,
    # Run seeds first, then models, then tests
    render_config=RenderConfig(
        select=["*"],  # Select all dbt nodes
        test_behavior="after_all"  # Run tests after all models complete
    )
)