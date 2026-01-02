import os
import dlt
import pandas as pd

# Define seed path with environment override
DATA_PATH = os.getenv("DATA_PATH", "/usr/local/airflow/include/dlt_pipeline/data/")

@dlt.source
def local_csvs():
    @dlt.resource(name="raw_customers")
    def customers():
        yield pd.read_csv(os.path.join(DATA_PATH, "raw_customers.csv"))

    @dlt.resource(name="raw_orders")
    def orders():
        yield pd.read_csv(os.path.join(DATA_PATH, "raw_orders.csv"))

    @dlt.resource(name="raw_payments")
    def payments():
        yield pd.read_csv(os.path.join(DATA_PATH, "raw_payments.csv"))

    return customers, orders, payments


if __name__ == "__main__":
    # Load data directly into dev.duckdb
    import duckdb
    import os
    
    # Get DuckDB file path from environment variable
    duckdb_filepath = os.getenv("DUCKDB_FILEPATH", "/usr/local/airflow/include/dev.duckdb")
    
    conn = duckdb.connect(duckdb_filepath)
    
    # Load each CSV file into its own table
    conn.execute("CREATE TABLE IF NOT EXISTS raw_customers AS SELECT * FROM read_csv_auto('/usr/local/airflow/include/dlt_pipeline/data/raw_customers.csv')")
    conn.execute("CREATE TABLE IF NOT EXISTS raw_orders AS SELECT * FROM read_csv_auto('/usr/local/airflow/include/dlt_pipeline/data/raw_orders.csv')")
    conn.execute("CREATE TABLE IF NOT EXISTS raw_payments AS SELECT * FROM read_csv_auto('/usr/local/airflow/include/dlt_pipeline/data/raw_payments.csv')")
    
    conn.close()
    print(f"Data loaded directly into {duckdb_filepath}")