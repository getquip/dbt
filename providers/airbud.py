import requests
import json
from google.cloud import storage

# Constants
API_KEY = "sk_2x2_2f3f44a1ac83b493d4fc8d4a6e1a3f140f23b8037b188e574c7ef2f3a74d86d9"
HEADERS = {"X-Recharge-Access-Token": API_KEY}
URL = "https://api.rechargeapps.com/events"
GCS_BUCKET_NAME = "airflow"
BUCKET_PATH = "/get/recharge"
BQ_DATASET_NAME = "recharge"

jsonl_path = 'events'
destination_blob_name = {
    "dag_run_date" : '2024-05-07T02:12:19'
    , "date_range" : "created_at"
}
# Function to get data from an API endpoint
def get_data(URL, HEADERS, **kwargs):
    # Evaluate the request type and set the corresponding value
    params = kwargs.get("params", None)
    data = kwargs.get("data", None)
    json_data = kwargs.get("json", None)
    
    # Make the GET request with appropriate parameters
    response = requests.get(
        URL,
        headers=HEADERS,
        # Only pass if set, will be None if not
        params=params,  
        data=data,
        json=json_data
    )

    # Raise an error if the response status code is not 2xx
    response.raise_for_status()
    return response.json()



# Function to get secrets (could be extended to pull from a secret manager)
def get_secrets():
    secrets = {}
    return secrets

# Function to upload JSON data to Google Cloud Storage (GCS)
def upload_json_to_gcs(json_data, , GCS_BUCKET_NAME, BUCKET_PATH, destination_blob_name):
    # Initialize the GCS client
    storage_client = storage.Client()
    # Get the GCS bucket object
    bucket = storage_client.get_bucket(GCS_BUCKET_NAME)


    # Create a blob (file object) within the bucket


import pandas as pd
df = pd.DataFrame(json_data)
start_date = df[destination_blob_name['date_range']].min()
end_date = df[destination_blob_name['date_range']].max()
    
    file = f"{BUCKET_PATH}/DAG_RUN:{destination_blob_name['dag_run_date']}_{destination_blob_name['date_range']}:{start_date}_to_{end_date}.json"
    blob = bucket.blob(destination_blob_name)

    # Convert each object to a JSON string
    def yield_jsonl():
        for line in json_data:
            yield json.dumps(line)
    
    # Upload the JSON data as a string to GCS
    blob.upload_from_string(yield_jsonl(), content_type='application/json')

# Main function to ingest data (for use in an Airflow DAG)
def ingest_data(
    BASE_URL,
    HEADERS,
    GCS_BUCKET_NAME,
    BUCKET_PATH,
    endpoint,
    destination_blob_name,
    jsonl_path,        # Parse response for jsonl
    paginate=False,    # Initialize pagination flag
    **kwargs
):
    # Authenticate
    auth = get_secrets()

    # Set up variables for specific endpoint
    URL = f"{BASE_URL}{endpoint}"
    BUCKET_PATH = f"{BUCKET_PATH}/{endpoint}"

    # Get data
    data = []
    if paginate:
        # Implement pagination logic here, if required
        response = pass  # paginate_data()
    else:
        # Fetch data using get_data function
        response = get_data(URL, HEADERS)

    # Parse data (select specific path if jsonl_path is provided)
    data = response if jsonl_path is None else response[jsonl_path]

    # Upload raw data to GCS
    upload_json_to_gcs(data, GCS_BUCKET_NAME, BUCKET_PATH, destination_blob_name)

    # Land data in BigQuery

    return data

