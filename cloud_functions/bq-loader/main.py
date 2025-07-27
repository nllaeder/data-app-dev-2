import base64
import json
import os
import functions_framework
from google.cloud import bigquery

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "data-app-dev-2")
BQ_DATASET_ID = os.getenv("BQ_DATASET", "insightiq_data")

# --- Client ---
bq_client = bigquery.Client()

# ===================================================================
#           2. MAIN CLOUD FUNCTION (PUBSUB TRIGGER)
# ===================================================================

@functions_framework.cloud_event
def bq_loader(cloud_event):
    """
    Receives a data payload from a Pub/Sub topic and loads it into BigQuery.
    1. Triggered by a message on the 'bq-loader-topic'.
    2. Decodes the message to get the data and target table name.
    3. Streams the data into the specified BigQuery table.
    """
    # 1. Decode the incoming message
    try:
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode('utf-8')
        payload = json.loads(message_data_decoded)

        table_name = payload.get("table_name")
        data_row = payload.get("data") # This should be a single JSON object/dict

        if not table_name or not data_row:
            print(f"!!! Error: Missing 'table_name' or 'data' in payload: {payload}")
            return
    except Exception as e:
        print(f"!!! Error decoding Pub/Sub message: {e}")
        return

    # 2. Prepare the data for BigQuery
    # The insert_rows_json method expects a list of dictionaries.
    rows_to_insert = [data_row]
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

    print(f"--- Attempting to insert 1 row into table: {table_id} ---")

    # 3. Stream the data into BigQuery
    try:
        errors = bq_client.insert_rows_json(table_id, rows_to_insert)
        if not errors:
            print("Successfully inserted row into BigQuery.")
        else:
            print(f"!!! BigQuery insertion errors: {errors}")

    except Exception as e:
        print(f"!!! An unexpected error occurred loading data to BigQuery: {e}")