import base64
import json
import os
import functions_framework
import requests
from google.cloud import firestore
from google.cloud import pubsub_v1

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "data-app-dev-2")
LOADER_TOPIC_NAME = os.getenv("LOADER_TOPIC", "bq-loader-topic")

# --- Clients ---
db = firestore.Client()
publisher = pubsub_v1.PublisherClient()
loader_topic_path = publisher.topic_path(GCP_PROJECT_ID, LOADER_TOPIC_NAME)


# ===================================================================
#           2. MAIN CLOUD FUNCTION (PUBSUB TRIGGER)
# ===================================================================

@functions_framework.cloud_event
def mailchimp_sync(cloud_event):
    """
    Extracts data from Mailchimp for a given user.
    1. Triggered by a message on the 'initiate-data-sync' topic.
    2. Fetches the user's access token from Firestore.
    3. Calls the Mailchimp API to get campaign data.
    4. Publishes the extracted data to the 'bq-loader-topic'.
    """
    # 1. Decode the incoming message to get the user_id
    try:
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode('utf-8')
        data_payload = json.loads(message_data_decoded)
        user_id = data_payload.get("user")

        if not user_id:
            print("!!! Error: user_id not found in message payload.")
            return
    except Exception as e:
        print(f"!!! Error decoding Pub/Sub message: {e}")
        return

    print(f"--- Starting Mailchimp sync for user: {user_id} ---")

    # 2. Fetch the user's credentials from Firestore
    try:
        doc_ref = db.collection('user_credentials').document(user_id)
        doc = doc_ref.get()
        if not doc.exists:
            print(f"!!! Error: Could not find credentials for user {user_id} in Firestore.")
            return

        credentials = doc.to_dict()
        access_token = credentials.get('mailchimp_access_token')
        server_prefix = credentials.get('mailchimp_server_prefix')

        if not access_token or not server_prefix:
            print(f"!!! Error: Missing Mailchimp credentials for user {user_id}.")
            return
    except Exception as e:
        print(f"!!! Error fetching credentials from Firestore: {e}")
        return

    # 3. Call the Mailchimp API to get campaign data
    try:
        api_url = f"https://{server_prefix}.api.mailchimp.com/3.0/campaigns"
        headers = {"Authorization": f"Bearer {access_token}"}
        
        print(f"Fetching campaigns from: {api_url}")
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        campaigns_data = response.json()
        
        # We only need the 'campaigns' list from the response
        campaigns = campaigns_data.get("campaigns", [])
        print(f"Successfully fetched {len(campaigns)} campaigns.")

    except requests.exceptions.RequestException as e:
        print(f"!!! Error fetching data from Mailchimp API: {e}")
        return
        
    if not campaigns:
        print("No campaigns found to load. Sync complete.")
        return

    # 4. Publish each campaign to the bq-loader-topic for processing
    try:
        for campaign in campaigns:
            # Add metadata for the loader to know where to save the data
            message_payload = {
                "source": "mailchimp",
                "user_id": user_id,
                "table_name": "mailchimp_campaigns",
                "data": campaign
            }
            message_data = json.dumps(message_payload).encode("utf-8")
            future = publisher.publish(loader_topic_path, message_data)
            future.result() # Wait for the message to be published

        print(f"Successfully published {len(campaigns)} campaign messages to '{LOADER_TOPIC_NAME}'.")
    except Exception as e:
        print(f"!!! Error publishing messages to Pub/Sub: {e}")

    print(f"--- Mailchimp sync for user: {user_id} complete. ---")