import duckdb
import pandas as pd
import streamlit as st
from duckdb import DuckDBPyConnection
from pandas import DataFrame


@st.cache_data
def get_star_wars_data(table_name: str) -> pd.DataFrame:
    
    # Connect to the DuckDB database
    db_path = "pystack.duckdb"  # Adjust the path if necessary
    conn = duckdb.connect(database=db_path, read_only=True)
    
    # Fetch data
    query = f"select * from dev.{table_name}"
    df = conn.execute(query).fetchdf()
    conn.close()
    return df
