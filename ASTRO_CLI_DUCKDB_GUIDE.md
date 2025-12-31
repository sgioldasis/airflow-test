# Using Astro CLI to Check DuckDB Database

This guide shows you how to use the Astro CLI to inspect and interact with your DuckDB database.

## Prerequisites

1. **Astro CLI installed**: Make sure you have the Astro CLI installed
2. **DuckDB CLI installed**: Install duckdb CLI for database inspection
3. **Running Airflow environment**: Your Airflow environment should be running

## Installation

### Install DuckDB CLI
```bash
# Using pip (recommended)
pip install duckdb

# Using conda
conda install -c conda-forge duckdb

# Using homebrew (macOS)
brew install duckdb
```

### Alternative: Use Python to Access DuckDB
If you can't install the CLI, you can use Python:
```bash
# Install duckdb Python package
pip install duckdb
```

## Astro CLI Commands for DuckDB

### 1. Check if Airflow is Running
```bash
astro dev start
```

### 2. Access the Airflow Environment
```bash
# Open a shell in the Airflow webserver container
astro dev bash

# Or access the scheduler container
astro dev bash --scheduler
```

### 3. Locate the DuckDB Database File
```bash
# Check if the database file exists
ls -la /usr/local/airflow/include/jaffle_shop.duckdb

# Check file size and permissions
stat /usr/local/airflow/include/jaffle_shop.duckdb
```

### 4. Connect to DuckDB Database

#### Method 1: Using DuckDB CLI (if installed and in PATH)
```bash
# Connect to the DuckDB database
duckdb /usr/local/airflow/include/jaffle_shop.duckdb
```

#### Method 2: Using Python (recommended)
```bash
# Start Python in the container
python3

# Then in Python:
import duckdb
conn = duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb')
```

#### Method 3: Using DuckDB CLI with full path
```bash
# Find the duckdb executable
find /usr/local -name "duckdb" 2>/dev/null

# Or use Python to run duckdb
python3 -c "import duckdb; duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb').execute('SHOW TABLES').fetchall()"
```

#### Method 4: Using dbt to inspect (if dbt is available)
```bash
# From the dbt project directory
cd /usr/local/airflow/dbt/jaffle_shop_duckdb
dbt run-operation list_tables
```

## Common DuckDB Queries

### 1. List All Tables
```sql
SHOW TABLES;
```

### 2. Check Database Schema
```sql
DESCRIBE raw_customers;
DESCRIBE raw_orders;
DESCRIBE raw_payments;
DESCRIBE stg_customers;
DESCRIBE stg_orders;
DESCRIBE stg_payments;
DESCRIBE customers;
DESCRIBE orders;
```

### 3. View Sample Data
```sql
-- Check raw data
SELECT * FROM raw_customers LIMIT 5;
SELECT * FROM raw_orders LIMIT 5;
SELECT * FROM raw_payments LIMIT 5;

-- Check staging tables
SELECT * FROM stg_customers LIMIT 5;
SELECT * FROM stg_orders LIMIT 5;
SELECT * FROM stg_payments LIMIT 5;

-- Check final models
SELECT * FROM customers LIMIT 5;
SELECT * FROM orders LIMIT 5;
```

### 4. Check Data Counts
```sql
-- Count records in each table
SELECT 'raw_customers' as table_name, COUNT(*) as count FROM raw_customers
UNION ALL
SELECT 'raw_orders' as table_name, COUNT(*) as count FROM raw_orders
UNION ALL
SELECT 'raw_payments' as table_name, COUNT(*) as count FROM raw_payments
UNION ALL
SELECT 'stg_customers' as table_name, COUNT(*) as count FROM stg_customers
UNION ALL
SELECT 'stg_orders' as table_name, COUNT(*) as count FROM stg_orders
UNION ALL
SELECT 'stg_payments' as table_name, COUNT(*) as count FROM stg_payments
UNION ALL
SELECT 'customers' as table_name, COUNT(*) as count FROM customers
UNION ALL
SELECT 'orders' as table_name, COUNT(*) as count FROM orders;
```

### 5. Check Database Information
```sql
-- Show database metadata
PRAGMA database_size;

-- Show current schema
SHOW SCHEMAS;

-- Show current database path
PRAGMA database_path;
```

## Python Alternative for Database Inspection

If you can't install the DuckDB CLI, use this Python script:

```python
import duckdb

# Connect to the database
conn = duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb')

# List all tables
tables = conn.execute("SHOW TABLES").fetchall()
print("Tables:", [t[0] for t in tables])

# Check data counts
for table in ['raw_customers', 'raw_orders', 'raw_payments', 'stg_customers', 'stg_orders', 'stg_payments', 'customers', 'orders']:
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count} records")

# View sample data
sample = conn.execute("SELECT * FROM customers LIMIT 5").fetchall()
print("Sample customers:", sample)

# Close connection
conn.close()
```

## Quick One-Liner Commands

### List Tables
```bash
python3 -c "import duckdb; print([t[0] for t in duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb').execute('SHOW TABLES').fetchall()])"
```

### Count Records in All Tables
```bash
python3 -c "
import duckdb
conn = duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb')
tables = ['raw_customers', 'raw_orders', 'raw_payments', 'stg_customers', 'stg_orders', 'stg_payments', 'customers', 'orders']
for table in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    print(f'{table}: {count} records')
conn.close()
"
```

### View Sample Data
```bash
python3 -c "
import duckdb
conn = duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb')
sample = conn.execute('SELECT * FROM customers LIMIT 5').fetchall()
for row in sample:
    print(row)
conn.close()
"
```

## Astro CLI Specific Commands

### 1. View Logs to Check Database Operations
```bash
# View webserver logs
astro dev logs webserver

# View scheduler logs
astro dev logs scheduler

# View specific task logs
astro dev logs --task <task_id> --dag <dag_id>
```

### 2. Trigger DAG Run and Monitor
```bash
# Trigger the DAG
astro dev run --dag my_dag

# Check DAG status
astro dev status
```

### 3. Access Database from Different Containers
```bash
# From webserver container
astro dev bash
# Then use Python or check file existence

# From scheduler container
astro dev bash --scheduler
# Then use Python or check file existence

# Check file from host (if mounted)
ls -la /usr/local/airflow/include/jaffle_shop.duckdb
```

## Troubleshooting

### 1. Database File Not Found
```bash
# Check if file exists
ls -la /usr/local/airflow/include/

# Check if directory exists
ls -la /usr/local/airflow/

# Create directory if needed (from container)
mkdir -p /usr/local/airflow/include
```

### 2. Permission Issues
```bash
# Check file permissions
ls -la /usr/local/airflow/include/jaffle_shop.duckdb

# Fix permissions (from container)
chmod 644 /usr/local/airflow/include/jaffle_shop.duckdb
```

### 3. Database Locked
```bash
# Check if database is locked
lsof /usr/local/airflow/include/jaffle_shop.duckdb

# Kill processes holding the lock (if safe)
fuser -k /usr/local/airflow/include/jaffle_shop.duckdb
```

### 4. DuckDB Command Not Found
If `duckdb` command is not available:

#### Option 1: Use Python (recommended)
```bash
# In the container
python3
# Then use the Python code above
```

#### Option 2: Find the duckdb executable
```bash
# Find the duckdb executable
find /usr/local -name "duckdb" 2>/dev/null

# Use the full path
/usr/local/bin/duckdb /usr/local/airflow/include/jaffle_shop.duckdb
```

#### Option 3: Use dbt commands
```bash
# From the dbt project directory
cd /usr/local/airflow/dbt/jaffle_shop_duckdb
dbt run-operation list_tables
```

## Useful Astro CLI Commands

### 1. Environment Management
```bash
# Start environment
astro dev start

# Stop environment
astro dev stop

# Restart environment
astro dev restart

# Clean environment
astro dev kill
```

### 2. Database Operations
```bash
# Reset database (WARNING: This will delete all data)
astro dev db reset

# Backup database
cp /usr/local/airflow/include/jaffle_shop.duckdb /backup/path/
```

### 3. Monitoring
```bash
# View all logs
astro dev logs

# View specific container logs
astro dev logs webserver
astro dev logs scheduler
astro dev logs postgres

# Follow logs in real-time
astro dev logs --follow
```

## Example Session

Here's a complete example session using Python:

```bash
# 1. Start the environment
astro dev start

# 2. Open shell in webserver container
astro dev bash

# 3. Use Python to connect to DuckDB
python3

# 4. In Python, run:
import duckdb
conn = duckdb.connect('/usr/local/airflow/include/jaffle_shop.duckdb')
tables = conn.execute("SHOW TABLES").fetchall()
print("Tables:", [t[0] for t in tables])

# 5. Exit Python
exit()

# 6. Exit container
exit

# 7. Stop environment
astro dev stop
```

## Notes

- The database file is located at `/usr/local/airflow/include/jaffle_shop.duckdb`
- Use the Astro CLI to manage your Airflow environment
- Python is the most reliable way to access DuckDB if CLI is not available
- Use `astro dev logs` to troubleshoot issues
- Always backup important data before making changes