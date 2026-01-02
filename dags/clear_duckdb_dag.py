from datetime import datetime
from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
import duckdb
import os

# Get the DuckDB database path from environment variable, with fallback
DUCKDB_DATABASE_PATH = os.getenv("DUCKDB_DATABASE_PATH", "/usr/local/airflow/include/jaffle_shop.duckdb")

# Get the schema name from environment variable, with fallback
SCHEMA_NAME = os.getenv("DUCKDB_SCHEMA", "main")

def drop_all_tables():
    """Function to drop all tables and views in the DuckDB database"""
    # Connect to the DuckDB database using the environment variable
    db_path = DUCKDB_DATABASE_PATH
    
    print(f"🗃️  Using DuckDB database at: {db_path}")
    print(f"📋 Using schema: {SCHEMA_NAME}")
    
    try:
        conn = duckdb.connect(db_path)
        
        # Get all tables and views in the database from all schemas
        objects = conn.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_type IN ('BASE TABLE', 'VIEW')
        """).fetchall()
        
        if not objects:
            print("✓ No objects found to drop")
            return []
          
        dropped_objects = []
          
        # Drop each object (table or view) with full schema qualification
        for obj in objects:
            obj_schema = obj[0]
            obj_name = obj[1]
            obj_type = obj[2]
            full_obj_name = f"{obj_schema}.{obj_name}"
            try:
                if obj_type == 'BASE TABLE':
                    conn.execute(f"DROP TABLE IF EXISTS {full_obj_name}")
                    print(f"✓ Dropped table: {full_obj_name}")
                elif obj_type == 'VIEW':
                    conn.execute(f"DROP VIEW IF EXISTS {full_obj_name}")
                    print(f"✓ Dropped view: {full_obj_name}")
                dropped_objects.append(f"{obj_type}: {full_obj_name}")
            except Exception as e:
                print(f"✗ Error dropping {obj_type.lower()} {full_obj_name}: {e}")
        
        conn.close()
        
        print(f"✓ Successfully dropped {len(dropped_objects)} objects")
        return {"dropped_objects": dropped_objects, "count": len(dropped_objects)}
        
    except Exception as e:
        print(f"✗ Error accessing database: {e}")
        raise

# Define the DAG
@dag(
    dag_id="clear_duckdb_dag",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    description="DAG to drop all tables and views in DuckDB database",
    tags=["utility", "cleanup"]
)
def clear_duckdb_dag():
    # Define the task
    drop_tables_task = PythonOperator(
        task_id="drop_all_tables",
        python_callable=drop_all_tables,
    )

# Instantiate the DAG
clear_duckdb_dag()