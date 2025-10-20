import dagster as dg
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_duckdb import DuckDBResource

from src.orchestration import assets

duckdb_fpath = "/Users/eric.thanenthiran/sandbox/pystack/src/pystack.duckdb"

dlt_resource = DagsterDltResource()

star_wars_assets = dg.load_assets_from_modules([assets])
stack_job = dg.define_asset_job(name="data_stack_run", selection=star_wars_assets)

defs = dg.Definitions(
    assets=star_wars_assets,
    resources={
        "duckdb": DuckDBResource(database=duckdb_fpath),
        "dlt": dlt_resource,
        "dbt": DbtCliResource(project_dir=assets.dbt_project),
    },
    jobs=[stack_job],
)
