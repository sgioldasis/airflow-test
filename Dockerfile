FROM astrocrpublic.azurecr.io/runtime:3.1-9

# Switch to root to install system packages
USER root

# Install DuckDB CLI binary (architecture-aware)
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget unzip && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
        DUCKDB_ARCH="amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
        DUCKDB_ARCH="aarch64"; \
    else \
        echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    echo "Downloading DuckDB for architecture: $DUCKDB_ARCH" && \
    wget -q "https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-${DUCKDB_ARCH}.zip" -O /tmp/duckdb_cli.zip && \
    unzip -q /tmp/duckdb_cli.zip -d /tmp && \
    mv /tmp/duckdb /usr/local/bin/duckdb && \
    chmod +x /usr/local/bin/duckdb && \
    rm -rf /tmp/duckdb_cli.zip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Switch back to astro user
USER astro

# Install dbt into a virtual environment
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb && deactivate