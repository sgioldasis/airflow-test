FROM astrocrpublic.azurecr.io/runtime:3.1-9

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && deactivate

# Note: DuckDB CLI installation is not possible due to permission issues in this environment
# Use the Python method instead:
# python3 -c "import duckdb; conn = duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb'); print([t[0] for t in conn.execute('SHOW TABLES').fetchall()])"