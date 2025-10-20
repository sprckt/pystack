# PyDataStack

A modern, open-source data platform built entirely with Python tools, demonstrating a complete end-to-end data pipeline for Star Wars film data.

## Technologies

- **Data Ingestion**: [dlt](https://dlthub.com/) for extracting data from the Star Wars API
- **Data Warehouse**: [DuckDB](https://duckdb.org/) for fast, embedded analytics
- **Data Transformation**: [dbt](https://www.getdbt.com/) for SQL-based data modelling
- **Data Orchestration**: [Dagster](https://dagster.io/) for pipeline management
- **Data Visualization**: [Streamlit](https://streamlit.io/) for interactive dashboards

## Prerequisites

- Python 3.8+
- [just](https://github.com/casey/just) command runner (optional)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/pystack.git
cd pystack
```

2. Create and activate a virtual environment:
```bash
uv sync
```

## Quick Start

### Run the Streamlit Dashboard
```bash
just bi
# Or manually:
cd src && streamlit run visualisation/app.py
```

### Run Dagster Pipeline
```bash
just orchestrate
# Or manually:
dagster dev -f src/orchestration/definitions.py
```

### Query DuckDB
```bash
just duck
# Or manually:
duckdb src/pystack.duckdb
```

### View dbt Documentation
```bash
just dbt-docs
```

## Project Structure

```
pystack/
├── src/
│   ├── orchestration/      # Dagster pipeline definitions
│   ├── transformation/     # dbt models and configurations
│   └── visualisation/      # Streamlit dashboard
├── justfile                # Command shortcuts
└── README.md
```

## Dashboard Features

- **Financials**: View film budgets, box office revenue, and ROI
- **Attributes**: Analyze species, characters, planets, and starships per film

## License

This project is open source and available under the [MIT License](LICENSE).

## Acknowledgments

- Star Wars API (SWAPI) for providing the data
- PyConDE 2025 for inspiration

---

Built using Python for PyCon DE and PyData 2025