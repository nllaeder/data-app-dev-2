import os
import json
import base64
import functions_framework
from google.cloud import bigquery

# Initialize the BigQuery client.
bq_client = bigquery.Client()

@functions_framework.cloud_event
def process_data_sync(cloud_event):
    """
    Pub/Sub-triggered Cloud Function to process a data synchronization job.
    1. Receives a message from a Pub/Sub topic.
    2. Decodes the message data.
    3. TODO: Process the data and load it into BigQuery.
    """
    # The actual data is in the 'message' key, base64-encoded.
    message_data_encoded = cloud_event.data["message"]["data"]
    message_data_decoded = base64.b64decode(message_data_encoded).decode('utf-8')
    data_payload = json.loads(message_data_decoded)
    
    print(f"Received message payload: {data_payload}")

    # --- TODO: Add your data processing logic here ---
    # This is where you would take the apiKey from the data_payload,
    # connect to the Mailchimp API, fetch the data, clean it,
    # and load it into the correct BigQuery table.
    
    # For now, we will just print the data.
    api_key = data_payload.get("apiKey")
    user_id = data_payload.get("user")
    
    print(f"Processing job for user '{user_id}' with API key '{api_key}'...")
    
    # Example: Loading data to BigQuery (currently commented out)
    # table_id = "your-project.your_dataset.your_table"
    # rows_to_insert = [
    #     {"col1": "value1", "col2": "value2"},
    # ]
    # errors = bq_client.insert_rows_json(table_id, rows_to_insert)
    # if not errors:
    #     print("New rows have been added.")
    # else:
    #     print(f"Encountered errors while inserting rows: {errors}")
    
    print("Function execution complete.")