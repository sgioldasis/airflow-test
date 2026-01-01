from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.datasets import Dataset
import duckdb
import os

def count_stg_payments_rows():
    """Function to count rows in stg_payments table and log the result"""
    # Connect to the DuckDB database - using the exact path from Cosmos
    db_path = "/usr/local/airflow/include/jaffle_shop.duckdb"
    
    try:
        conn = duckdb.connect(db_path)
        
        # Count rows in stg_payments table
        result = conn.execute("SELECT COUNT(*) as row_count FROM stg_payments").fetchone()
        row_count = result[0] if result else 0
        
        print(f"✓ Successfully connected to database at {db_path}")
        print(f"✓ Found {row_count} rows in stg_payments table")
        
        # Also show some sample data
        sample_data = conn.execute("SELECT * FROM stg_payments LIMIT 5").fetchall()
        print(f"✓ Sample data from stg_payments:")
        for row in sample_data:
            print(f"  {row}")
        
        conn.close()
        
        return row_count
        
    except Exception as e:
        print(f"✗ Error accessing database: {e}")
        raise

# Define the dataset for stg_payments table - using the exact URI format from Cosmos
stg_payments_dataset = Dataset("duckdb:///usr/local/airflow/include/jaffle_shop.duckdb/jaffle_shop/main/stg_payments")

# Define the DAG with asset-aware scheduling
query_dag = DAG(
    dag_id="query_dag",
    schedule=[stg_payments_dataset],  # Run when stg_payments dataset is updated
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="DAG to query and log row count from stg_payments table, triggered by asset event"
)

# Define the task
count_rows_task = PythonOperator(
    task_id="count_stg_payments_rows",
    python_callable=count_stg_payments_rows,
    dag=query_dag,
)

# Set task dependencies (just one task in this simple DAG)
count_rows_task