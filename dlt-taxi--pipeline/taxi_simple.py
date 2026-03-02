import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

@dlt.resource(name="rides")
def taxi_rides():
    page = 1
    while True:
        response = requests.get(BASE_URL, params={"page": page})
        data = response.json()
        if not data:  # empty page means we're done
            break
        yield data
        page += 1

pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
)

if __name__ == "__main__":
    load_info = pipeline.run(taxi_rides())
    print(load_info)
