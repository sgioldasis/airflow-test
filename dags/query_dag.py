from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.datasets import Dataset
import duckdb
import os

def count_stg_tables_rows():
    """Function to count rows in stg_payments and stg_orders tables and log the results"""
    # Connect to the DuckDB database - using the exact path from Cosmos
    db_path = "/usr/local/airflow/include/jaffle_shop.duckdb"
    
    try:
        conn = duckdb.connect(db_path)
        
        # Count rows in stg_payments table
        payments_result = conn.execute("SELECT COUNT(*) as row_count FROM stg_payments").fetchone()
        payments_count = payments_result[0] if payments_result else 0
        
        # Count rows in stg_orders table
        orders_result = conn.execute("SELECT COUNT(*) as row_count FROM stg_orders").fetchone()
        orders_count = orders_result[0] if orders_result else 0
        
        print(f"✓ Successfully connected to database at {db_path}")
        print(f"✓ Found {payments_count} rows in stg_payments table")
        print(f"✓ Found {orders_count} rows in stg_orders table")
        
        # Show sample data from both tables
        print(f"✓ Sample data from stg_payments:")
        payments_sample = conn.execute("SELECT * FROM stg_payments LIMIT 3").fetchall()
        for row in payments_sample:
            print(f"  {row}")
        
        print(f"✓ Sample data from stg_orders:")
        orders_sample = conn.execute("SELECT * FROM stg_orders LIMIT 3").fetchall()
        for row in orders_sample:
            print(f"  {row}")
        
        conn.close()
        
        return {"payments_count": payments_count, "orders_count": orders_count}
        
    except Exception as e:
        print(f"✗ Error accessing database: {e}")
        raise

# Define the datasets for both tables - using the exact URI format from Cosmos
stg_payments_dataset = Dataset("duckdb:///usr/local/airflow/include/jaffle_shop.duckdb/jaffle_shop/main/stg_payments")
stg_orders_dataset = Dataset("duckdb:///usr/local/airflow/include/jaffle_shop.duckdb/jaffle_shop/main/stg_orders")

# Define the DAG with asset-aware scheduling - depends on both datasets
query_dag = DAG(
    dag_id="query_dag",
    schedule=[stg_payments_dataset, stg_orders_dataset],  # Run when BOTH datasets are updated
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="DAG to query and log row counts from stg_payments and stg_orders tables, triggered by asset events"
)

# Define the task
count_rows_task = PythonOperator(
    task_id="count_stg_tables_rows",
    python_callable=count_stg_tables_rows,
    dag=query_dag,
)

# Set task dependencies (just one task in this simple DAG)
count_rows_task