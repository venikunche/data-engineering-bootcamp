"""dlt pipeline to ingest NYC taxi data from a paginated REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def taxi_pipeline_rest_api_source():
    """Define dlt resources for the NYC taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            # Base URL for the NYC taxi REST API
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "nyc_taxi_data",
                "endpoint": {
                    # Root path of the Cloud Function; all pagination is via query parameters
                    "path": "",
                    # The API returns 1,000 records per page and stops on an empty page.
                    # We model this with page-number pagination starting from page 1.
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                    },
                    # Adjust this selector if the API wraps data differently
                    "data_selector": "$",
                },
            },
        ],
    }

    yield from rest_api_resources(config)


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dev_mode=True,
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()` during development; remove once stable.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
