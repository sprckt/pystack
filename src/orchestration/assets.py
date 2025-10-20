from pathlib import Path
from typing import Any, Generator, Iterator, Mapping, Optional

import dagster as dg
import dlt
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt.extract.source import DltSource
from dlt.sources.filesystem import filesystem, read_csv
from dlt.sources.helpers.rest_client.paginators import (
    JSONLinkPaginator,
    SinglePagePaginator,
)
from dlt.sources.rest_api import check_connection, rest_api_source

RELATIVE_PATH_TO_DBT = "./transformation"
dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent.joinpath(RELATIVE_PATH_TO_DBT).resolve()
)
dbt_project.prepare_if_dev()


def star_wars_api_source() -> DltSource:
    return rest_api_source(
        {
            "client": {
                "base_url": "https://www.swapi.tech/api/",
            },
            "resource_defaults": {
                "endpoint": {},
            },
            "resources": [
                {
                    "name": "films",
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": "films",
                        "paginator": SinglePagePaginator(),
                    },
                },
                {
                    "name": "starships",
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": "starships",
                        "paginator": JSONLinkPaginator(next_url_path="next"),
                    },
                },
                {
                    "name": "planets",
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": "planets",
                        "paginator": JSONLinkPaginator(next_url_path="next"),
                    },
                },
                {
                    "name": "species",
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": "species",
                        "paginator": JSONLinkPaginator(next_url_path="next"),
                    },
                },
                {
                    "name": "people",
                    "write_disposition": "replace",
                    "endpoint": {
                        "path": "people",
                        "paginator": JSONLinkPaginator(next_url_path="next"),
                    },
                },
            ],
        }
    )


@dlt.source
def star_wars_file_source() -> Iterator:

    filesystem_resource = (
        filesystem(
            bucket_url="file://Users/eric.thanenthiran/sandbox/pystack/src/source_data/",
            file_glob="*.csv",
        )
        | read_csv()
    )
    return filesystem_resource.with_name("star_wars_film_metadata").apply_hints(
        write_disposition="replace"
    )


@dlt_assets(
    dlt_source=star_wars_api_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="star_wars_api",
        dataset_name="sources",
        destination=dlt.destinations.duckdb(
            "/Users/eric.thanenthiran/sandbox/pystack/src/pystack.duckdb"
        ),
    ),
    name="star_wars",
    group_name="api_ingestion",
)
def get_star_wars_api_data(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context)


@dlt_assets(
    dlt_source=star_wars_file_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="star_wars_file",
        dataset_name="sources",
        destination=dlt.destinations.duckdb(
            "/Users/eric.thanenthiran/sandbox/pystack/src/pystack.duckdb"
        ),
    ),
    name="star_wars_film_metadata",
    group_name="file_ingestion",
)
def get_star_wars_file_data(
    context: dg.AssetExecutionContext,
    dlt: DagsterDltResource,
) -> Iterator[str]:
    yield from dlt.run(
        context=context,
    )


@dbt_assets(
    manifest=dbt_project.manifest_path,
)
def transformation_assets(
    context: dg.AssetExecutionContext, dbt: DbtCliResource
) -> Iterator[str]:
    yield from dbt.cli(["build"], context=context).stream()
