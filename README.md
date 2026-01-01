# Airflow + dbt + DuckDB Demo with Asset-Aware Scheduling

## Overview

This project demonstrates a complete data pipeline using:
- **Apache Airflow** for orchestration
- **dbt (data build tool)** for data transformation
- **DuckDB** as the database
- **Asset-aware scheduling** for automatic DAG triggering

The demo shows how to use Cosmos (Astronomer's dbt integration) with Airflow's modern dataset system to create automatic dependencies between dbt models and downstream processing.

## Project Contents

### DAGs

- **`dbt_dag.py`**: Manual-trigger DAG that runs dbt models to create staging tables
- **`query_dag.py`**: Auto-triggered DAG that queries the staging tables when they're ready
- **`astronauts_dag.py`**: Example DAG showing astronaut data (unchanged from original)

### Data Pipeline

1. **dbt_dag** → Creates `stg_payments` and `stg_orders` tables via dbt
2. **Asset Events** → Emitted when each table is ready
3. **query_dag** → Triggered automatically when both datasets are available
4. **Logging** → Query DAG logs row counts and sample data

## Prerequisites

### Required Tools

- **Docker**: For running Airflow locally
- **Astro CLI**: For managing the Airflow environment
- **Python 3.8+**: For local development (optional)

### Installation

```bash
# Install Astro CLI
curl -sSL https://install.astronomer.io | sudo bash

# Install Docker (if not already installed)
# Follow instructions at: https://docs.docker.com/get-docker/

# Install uv (Python package manager - optional for local testing)
pip install uv
```

## Running the Demo

### 1. Start Airflow

```bash
# Navigate to project directory
cd /path/to/airflow-test

# Start Airflow using Astro CLI
astro dev start
```

This will:
- Build Docker containers
- Start Airflow services (Postgres, Scheduler, Webserver, etc.)
- Open the Airflow UI at http://localhost:8080/

### 2. Wait for Services to Initialize

Wait until you see all services are healthy:
```
✓ Postgres is healthy
✓ Scheduler is healthy  
✓ DAG Processor is healthy
✓ API Server is healthy
✓ Triggerer is healthy
```

### 3. Manually Trigger the dbt_dag

1. **Log in** to Airflow UI at http://localhost:8080/
2. **Find** `dbt_dag` in the DAGs list
3. **Toggle** the DAG to "On" (if not already active)
4. **Click** "Trigger DAG" button

### 4. Watch the Asset-Aware Scheduling in Action

1. **dbt_dag runs** and processes dbt models:
   - Creates `stg_payments` table → emits dataset event
   - Creates `stg_orders` table → emits dataset event

2. **query_dag triggers automatically** when both datasets are ready:
   - No manual intervention needed
   - Runs immediately after both tables are created

3. **Check logs** to see the results:
   - Go to `query_dag` → Graph View
   - Click on `count_stg_tables_rows` task
   - View logs to see row counts from both tables

### 5. Verify the Results

**Expected Output in query_dag logs:**
```
✓ Successfully connected to database at /usr/local/airflow/include/jaffle_shop.duckdb
✓ Found [X] rows in stg_payments table
✓ Found [Y] rows in stg_orders table
✓ Sample data from stg_payments:
  (sample_row_1)
  (sample_row_2)
✓ Sample data from stg_orders:
  (sample_row_1)
  (sample_row_2)
```

## Understanding the Asset Flow

### Dataset URIs

- **stg_payments**: `duckdb:///usr/local/airflow/include/jaffle_shop.duckdb/jaffle_shop/main/stg_payments`
- **stg_orders**: `duckdb:///usr/local/airflow/include/jaffle_shop.duckdb/jaffle_shop/main/stg_orders`

### How It Works

1. **dbt_dag** uses Cosmos with `emit_datasets=True`
2. When each dbt model completes, Cosmos emits a dataset event
3. **query_dag** depends on both datasets via `schedule=[dataset1, dataset2]`
4. Airflow triggers query_dag only when BOTH datasets are updated
5. This creates a dynamic, event-driven pipeline

## Key Features Demonstrated

✅ **Asset-aware scheduling**: Automatic DAG triggering based on dataset events
✅ **Multi-dataset dependencies**: Waiting for multiple tables to be ready
✅ **dbt + Airflow integration**: Using Cosmos for seamless dbt orchestration
✅ **DuckDB integration**: Lightweight database for local development
✅ **Manual + Automatic workflows**: Mix of manual and auto-triggered DAGs

## Troubleshooting

### Common Issues

**Issue: DAGs not appearing in UI**
- Wait 1-2 minutes for DAG parsing
- Check `astro dev logs` for errors
- Ensure all containers are healthy

**Issue: query_dag not triggering**
- Verify dbt_dag completed successfully
- Check dataset URIs match exactly
- Look for dataset events in Airflow UI

**Issue: Database connection errors**
- Ensure DuckDB file exists at correct path
- Check file permissions
- Verify database paths match between DAGs

### Useful Commands

```bash
# View logs
astro dev logs

# Stop Airflow
astro dev stop

# Restart Airflow
astro dev restart

# Run specific DAG
astro dev run dbt_dag
```

## Clean Up

```bash
# Stop Airflow services
astro dev stop

# Remove containers (if needed)
astro dev kill

# For complete cleanup including volumes and database metadata
docker system prune -f --volumes
```

**Note**: The `docker system prune -f --volumes` command will completely remove all Docker resources including the Airflow database volume, which will clear all DAG runs and metadata. This is the most thorough cleanup method.

## Next Steps

- Explore the dbt models in `dbt/jaffle_shop_duckdb/models/`
- Modify the query function to add more analysis
- Add additional datasets to the dependency chain
- Experiment with different dbt configurations
