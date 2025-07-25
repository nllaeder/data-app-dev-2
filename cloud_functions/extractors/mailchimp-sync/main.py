import base64
import json
import os
import time

import functions_framework
import requests
from google.cloud import pubsub_v1, secretmanager

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

# --- Environment Variables ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mis581-capstone-data")
LOAD_TOPIC_NAME = os.getenv("LOAD_TOPIC_NAME", "load-to-bigquery")
MC_TOKEN_SECRET_NAME = "peer2_mailchimp_token"

# --- Clients ---
publisher = pubsub_v1.PublisherClient()
load_topic_path = publisher.topic_path(GCP_PROJECT_ID, LOAD_TOPIC_NAME)


# ===================================================================
#           2. UTILITY FUNCTIONS
# ===================================================================

def get_mailchimp_credentials(project_id, secret_name):
    """Retrieves Mailchimp API credentials from Google Cloud Secret Manager."""
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = secret_client.access_secret_version(request={"name": secret_path})
        payload = response.payload.data.decode("UTF-8")
        credentials = json.loads(payload)

        access_token = credentials.get("access_token")
        server_prefix = credentials.get("dc")

        if not access_token or not server_prefix:
            raise ValueError("'access_token' or 'dc' not found in secret payload.")

        print(f"Successfully retrieved Mailchimp credentials for server prefix: {server_prefix}")
        return access_token, server_prefix

    except Exception as e:
        print(f"!!! ERROR fetching Mailchimp credentials: {e}")
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
        future.result() # Wait for publish to complete
    print(f"Successfully published {len(data_list)} {data_type} records.")


# ===================================================================
#           3. MAILCHIMP API EXTRACTION FUNCTIONS
# ===================================================================

def fetch_paginated_data(api_url, headers, data_key):
    """Fetches all pages of data from a paginated Mailchimp API endpoint."""
    all_items = []
    page_size = 200
    offset = 0

    while True:
        params = {"count": page_size, "offset": offset}
        try:
            response = requests.get(api_url, headers=headers, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            items = data.get(data_key, [])
            if not items:
                break

            all_items.extend(items)
            print(f"  Fetched {len(items)} {data_key}, total so far: {len(all_items)}")

            offset += page_size
            time.sleep(0.5)

        except requests.exceptions.HTTPError as e:
            print(f"  !!! HTTP Error fetching {data_key}: {e}")
            break
        except Exception as e:
            print(f"  !!! An unexpected error occurred while fetching {data_key}: {e}")
            break

    return all_items

def fetch_all_reports(headers, api_base_url, campaigns):
    """Fetches a performance report for each campaign."""
    all_reports = []
    total_campaigns = len(campaigns)
    print(f"Found {total_campaigns} campaigns. Now fetching a report for each.")

    for i, campaign in enumerate(campaigns):
        campaign_id = campaign.get("id")
        if not campaign_id:
            continue

        print(f"  Fetching report {i + 1} of {total_campaigns} for campaign ID: {campaign_id}")
        report_url = f"{api_base_url}/reports/{campaign_id}"

        try:
            response = requests.get(report_url, headers=headers, timeout=15)
            response.raise_for_status()
            report_data = response.json()
            all_reports.append(report_data)
            time.sleep(0.5)

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"  -> No report found for campaign {campaign_id}.")
            else:
                print(f"  !!! HTTP Error fetching report for campaign {campaign_id}: {e}")
        except Exception as e:
            print(f"  !!! An unexpected error occurred fetching report for {campaign_id}: {e}")

    return all_reports


# ===================================================================
#           4. MAIN CLOUD FUNCTION (PUBSUB TRIGGER)
# ===================================================================

@functions_framework.cloud_event
def mailchimp_sync(cloud_event):
    """
    Triggered by a message on 'trigger-mailchimp-sync'. Extracts all data from Mailchimp
    and publishes it to the 'load-to-bigquery' topic.
    """
    try:
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode("utf-8")
        data_payload = json.loads(message_data_decoded)
        tenant_id = data_payload.get("user")

        if not tenant_id:
            print("!!! ERROR: 'user' (tenant_id) not found in message payload.")
            return

        print(f"--- Starting Mailchimp extraction for tenant: {tenant_id} ---")

    except Exception as e:
        print(f"!!! ERROR decoding Pub/Sub message: {e}")
        return

    try:
        secret_name = f"mailchimp-token-{tenant_id}"
        mc_access_token, mc_server_prefix = get_mailchimp_credentials(
            GCP_PROJECT_ID, secret_name
        )
        api_base_url = f"https://{mc_server_prefix}.api.mailchimp.com/3.0"
        headers = {"Authorization": f"Bearer {mc_access_token}"}
    except Exception:
        return

    # --- Extraction ---
    print("\n--- Step 1: Fetching Members ---")
    lists_url = f"{api_base_url}/lists"
    all_lists = fetch_paginated_data(lists_url, headers, "lists")
    all_members = []
    if all_lists:
        for lst in all_lists:
            list_id = lst.get("id")
            list_name = lst.get("name")
            print(f"Fetching members from list: '{list_name}' (ID: {list_id})")
            members_url = f"{api_base_url}/lists/{list_id}/members"
            members_of_list = fetch_paginated_data(members_url, headers, "members")
            all_members.extend(members_of_list)
    print(f"--- Finished: Found {len(all_members)} members. ---")

    print("\n--- Step 2: Fetching Campaigns ---")
    campaigns_url = f"{api_base_url}/campaigns"
    all_campaigns = fetch_paginated_data(campaigns_url, headers, "campaigns")
    print(f"--- Finished: Found {len(all_campaigns)} campaigns. ---")

    print("\n--- Step 3: Fetching Campaign Reports ---")
    all_reports = []
    if all_campaigns:
        all_reports = fetch_all_reports(headers, api_base_url, all_campaigns)
    print(f"--- Finished: Found {len(all_reports)} reports. ---")

    # --- Publishing ---
    print("\n--- Step 4: Publishing extracted data to loader topic ---")
    publish_to_load_topic(all_members, "members", tenant_id)
    publish_to_load_topic(all_campaigns, "campaigns", tenant_id)
    publish_to_load_topic(all_reports, "reports", tenant_id)

    print(f"\n--- Mailchimp extraction for tenant '{tenant_id}' complete. ---")
