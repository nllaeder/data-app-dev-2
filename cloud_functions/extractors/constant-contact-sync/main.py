import base64
import json
import os

import functions_framework
import requests
from google.cloud import pubsub_v1, secretmanager

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

# --- Environment Variables ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mis581-capstone-data")
LOAD_TOPIC_NAME = os.getenv("LOAD_TOPIC_NAME", "load-to-bigquery")

# --- Clients ---
publisher = pubsub_v1.PublisherClient()
load_topic_path = publisher.topic_path(GCP_PROJECT_ID, LOAD_TOPIC_NAME)


# ===================================================================
#           2. UTILITY FUNCTIONS
# ===================================================================

def get_constant_contact_credentials(project_id, secret_name):
    """Retrieves Constant Contact API credentials from Google Cloud Secret Manager."""
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_path})
        return json.loads(response.payload.data.decode("UTF-8"))

    except Exception as e:
        print(f"!!! ERROR fetching Constant Contact credentials: {e}")
        raise

def publish_to_load_topic(data_list, data_type, tenant_id):
    """Publishes each item in a list to the central loading topic."""
    if not data_list:
        print(f"No {data_type} to publish.")
        return

    print(f"Publishing {len(data_list)} {data_type} records to {LOAD_TOPIC_NAME}...")
    for item in data_list:
        message_payload = {
            "tenant_id": tenant_id,
            "data_type": data_type,
            "data": item,
        }
        message_data = json.dumps(message_payload).encode("utf-8")
        future = publisher.publish(load_topic_path, message_data)
        future.result()
    print(f"Successfully published {len(data_list)} {data_type} records.")


# ===================================================================
#           3. CONSTANT CONTACT API EXTRACTION FUNCTIONS
# ===================================================================

# TODO: Implement the functions to fetch data from the Constant Contact API.
# You will need to consult the Constant Contact API documentation for details
# on the available endpoints and data formats.

def fetch_contacts(access_token):
    """Placeholder function to fetch contacts."""
    print("Fetching contacts from Constant Contact...")
    # api_url = "https://api.cc.email/v3/contacts"
    # headers = {"Authorization": f"Bearer {access_token}"}
    # ... (add pagination logic)
    return []

def fetch_campaigns(access_token):
    """Placeholder function to fetch campaigns."""
    print("Fetching campaigns from Constant Contact...")
    return []


# ===================================================================
#           4. MAIN CLOUD FUNCTION (PUBSUB TRIGGER)
# ===================================================================

@functions_framework.cloud_event
def constant_contact_sync(cloud_event):
    """
    Triggered by a message on 'trigger-constant-contact-sync'.
    """
    try:
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode("utf-8")
        data_payload = json.loads(message_data_decoded)
        tenant_id = data_payload.get("user")

        if not tenant_id:
            print("!!! ERROR: 'user' (tenant_id) not found in message payload.")
            return

        print(f"--- Starting Constant Contact extraction for tenant: {tenant_id} ---")

    except Exception as e:
        print(f"!!! ERROR decoding Pub/Sub message: {e}")
        return

    try:
        secret_name = f"constant-contact-token-{tenant_id}"
        credentials = get_constant_contact_credentials(GCP_PROJECT_ID, secret_name)
        access_token = credentials.get("access_token")

        if not access_token:
            raise ValueError("Access token not found in credentials.")

    except Exception as e:
        print(f"Error getting credentials: {e}")
        return

    # --- Extraction ---
    all_contacts = fetch_contacts(access_token)
    all_campaigns = fetch_campaigns(access_token)

    # --- Publishing ---
    print("\n--- Publishing extracted data to loader topic ---")
    publish_to_load_topic(all_contacts, "contacts", tenant_id)
    publish_to_load_topic(all_campaigns, "campaigns", tenant_id)

    print(f"\n--- Constant Contact extraction for tenant '{tenant_id}' complete. ---")
