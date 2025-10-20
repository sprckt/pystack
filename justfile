# Justfile template for Python project

# Activate the virtual environment
venv:
    source .venv/bin/activate

# Run Dagster app
orchestrate: venv
    #!/bin/sh
    echo "Running Dagster app..."
    dagster dev -f src/orchestration/definitions.py  

# Run the Streamlit app
bi: venv
    #!/bin/sh
    echo "Running Streamlit app..."
    cd src/ && uv run streamlit run visualisation/app.py

# Serve dbt documentation
dbt-docs: venv
    #!/bin/sh
    echo "Serving dbt documentation..."
    cd src/transformation
    dbt docs generate
    dbt docs serve

# Show us the data!
duck: venv
    #!/bin/sh
    echo "Running DuckDB..."
    duckdb src/pystack.duckdb

# Show help
help:
    @just --list