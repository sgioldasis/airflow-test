# DuckDB Lock Conflict Resolution

## Problem Description

The original Airflow DAG was experiencing DuckDB lock conflicts when running dbt operations. The error message indicated:

```
IO Error: Could not set lock on file "/usr/local/airflow/include/jaffle_shop.duckdb": Conflicting lock is held in /usr/local/bin/python3.12 (PID 53)
```

This occurred because multiple dbt processes were trying to access the same DuckDB database file simultaneously.

## Root Cause

1. **Shared Database File**: The dbt project was configured to use a persistent DuckDB file (`/usr/local/airflow/include/jaffle_shop.duckdb`)
2. **Multiple Concurrent Access**: Airflow was trying to run multiple dbt operations that needed to access the same database file
3. **DuckDB Locking**: DuckDB uses file-level locking which prevents concurrent access to the same database file

## Solution

### Environment Variable Configuration (`.env`)

Added environment variable for consistent DuckDB database path usage:

```bash
# DuckDB Database Path for consistent usage across all components
DUCKDB_DATABASE_PATH="/usr/local/airflow/include/jaffle_shop.duckdb"
```

### Updated DAG Configuration (`dags/my_dag.py`)

Updated the DAG configuration to use the environment variable for consistent database path:

```python
# Use the same DuckDB database file path from environment variable
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "/usr/local/airflow/include/jaffle_shop.duckdb")

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
```

```python
my_dag = DbtDag(
    dag_id="my_dag",
    project_config=_project_config,
    profile_config=_profile_config,
    execution_config=_execution_config,
    schedule="@daily",
    start_date=datetime(2025,1,1),
    max_active_runs=1,
    max_active_tasks=1,  # Prevent multiple tasks from running simultaneously
    render_config=RenderConfig(
        select=["*"],  # Select all dbt nodes (seeds, models, tests)
        test_behavior="after_all"  # Run tests after all models complete
    )
)
```

### Updated DBT Profile (`dbt/jaffle_shop_duckdb/profiles.yml`)

Set up the profile to use the environment variable for consistent database path:

```yaml
jaffle_shop:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: '{{ env_var("DUCKDB_DATABASE_PATH", "/usr/local/airflow/include/jaffle_shop.duckdb") }}'  # Use environment variable
      schema: main
      threads: 1  # Single thread to prevent conflicts
```

## Key Changes Made

1. **Environment Variable**: Added `DUCKDB_DATABASE_PATH` in `.env` for centralized configuration
2. **Consistent Database Path**: All components now use the same DuckDB database file path via environment variable
3. **Thread Count**: Reduced from 24 to 1 to prevent internal conflicts
4. **Concurrency Control**: Added `max_active_tasks=1` to prevent multiple tasks running simultaneously
5. **Task Visibility**: Changed `select=["tag:seed"]` to `select=["*"]` to show all dbt tasks
6. **Test Behavior**: Changed `test_behavior="none"` to `test_behavior="after_all"` to run tests

## Files Modified

### Modified Files
- `.env` - Added `DUCKDB_DATABASE_PATH` environment variable
- `dags/my_dag.py` - Updated to use environment variable for database path, all tasks visible
- `dbt/jaffle_shop_duckdb/profiles.yml` - Use environment variable for database path

### Created Files
- `test_duckdb_connection.py` - Test script to verify the fix
- `DUCKDB_LOCK_FIX.md` - This documentation

## Testing

Run the test script to verify the fix:

```bash
python test_duckdb_connection.py
```

This script tests:
- Single connection functionality
- Multiple concurrent connections
- Database file consistency

## Usage

Use the updated `dags/my_dag.py` which now:
- Uses the same DuckDB database file consistently across all components via environment variable
- Shows all dbt tasks (seeds, models, tests)
- Prevents import errors in uv environments
- Maintains all existing dbt functionality

## Benefits

1. **Centralized Configuration**: Database path defined once in `.env` and used everywhere
2. **Consistent Database**: Same DuckDB database file used everywhere via environment variable
3. **All dbt Tasks Visible**: Seeds, models, and tests are now all included
4. **File-based Persistence**: Database file persists and is shared consistently
5. **Cleaner Operations**: Consistent database path across all configurations
6. **Concurrency Control**: Prevents multiple tasks from running simultaneously
7. **Development Friendly**: Avoids import errors in uv environments

## Notes

- Database path defined in `.env` as `DUCKDB_DATABASE_PATH="/usr/local/airflow/include/jaffle_shop.duckdb"`
- All components use this environment variable for consistent database path
- The solution maintains all existing dbt functionality while resolving the lock conflicts
- All dbt tasks (seeds, models, tests) are now visible in the DAG
- Fixed import error by using only cosmos imports
- Concurrency control prevents multiple tasks from running simultaneously