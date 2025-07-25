import base64
import json
import os

import functions_framework
from google.cloud import bigquery

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

# --- Environment Variables ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mis581-capstone-data")
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID", "raw_data")

# --- BigQuery Client ---
bq_client = bigquery.Client(project=GCP_PROJECT_ID)


# ===================================================================
#           2. MAIN CLOUD FUNCTION (PUBSUB TRIGGER)
# ===================================================================

@functions_framework.cloud_event
def bq_loader(cloud_event):
    """
    Triggered by a message on 'load-to-bigquery'. Loads a single record
    into the appropriate BigQuery table.
    """
    try:
        # 1. Decode the incoming message
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode("utf-8")
        data_payload = json.loads(message_data_decoded)

        # 2. Extract metadata for routing
        tenant_id = data_payload.get("tenant_id")
        data_type = data_payload.get("data_type")
        record = data_payload.get("data")

        if not all([tenant_id, data_type, record]):
            print(f"ERROR: Missing one or more required fields: tenant_id, data_type, data.")
            return

        print(f"Processing record for tenant '{tenant_id}', type '{data_type}'.")

        # 3. Determine the target BigQuery table and schema
        table_name = f"{tenant_id}_{data_type}_raw"
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_name}"

        schema = [
            bigquery.SchemaField("raw_data", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("tenant_id", "STRING", mode="REQUIRED"),
        ]

        # 4. Prepare the row to insert
        # The incoming 'record' is the raw data itself
        row_to_insert = {
            "raw_data": json.dumps(record),
            "tenant_id": tenant_id
        }

        # 5. Load the data into BigQuery
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        if not errors:
            print(f"Successfully inserted 1 record into {table_id}.")
        else:
            print(f"!!! ERROR inserting record into {table_id}: {errors}")

    except Exception as e:
        print(f"!!! An unexpected error occurred in the BQ Loader: {e}")
