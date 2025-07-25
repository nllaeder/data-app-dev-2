import json
import os
import requests
from google.cloud import secretmanager, pubsub_v1
import functions_framework

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

# --- Environment Variables ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "mis581-capstone-data")
OAUTH_CREDENTIALS_SECRET_NAME = os.getenv("OAUTH_SECRET", "constant_contact_oauth_credentials")
SYNC_TOPIC_NAME = os.getenv("SYNC_TOPIC", "initiate-data-sync")

# --- Clients ---
secret_client = secretmanager.SecretManagerServiceClient()
publisher = pubsub_v1.PublisherClient()
sync_topic_path = publisher.topic_path(GCP_PROJECT_ID, SYNC_TOPIC_NAME)


# ===================================================================
#           2. UTILITY FUNCTIONS
# ===================================================================

def get_oauth_credentials():
    """Retrieves Constant Contact OAuth client_id and client_secret."""
    secret_path = f"projects/{GCP_PROJECT_ID}/secrets/{OAUTH_CREDENTIALS_SECRET_NAME}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_path})
    return json.loads(response.payload.data.decode("UTF-8"))

def create_user_secret(user_id, token_data):
    """Creates a new secret in Secret Manager for a specific user."""
    secret_id = f"constant-contact-token-{user_id}"
    parent = f"projects/{GCP_PROJECT_ID}"

    try:
        secret_client.create_secret(
            request={
                "parent": parent,
                "secret_id": secret_id,
                "secret": {"replication": {"automatic": {}}},
            }
        )
        print(f"Created new secret: {secret_id}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Secret {secret_id} already exists. Adding new version.")
        else:
            raise

    secret_path = f"{parent}/secrets/{secret_id}"
    payload = json.dumps(token_data).encode("UTF-8")
    secret_client.add_secret_version(request={"parent": secret_path, "payload": {"data": payload}})
    print(f"Successfully stored token in secret: {secret_id}")


# ===================================================================
#           3. MAIN CLOUD FUNCTION (HTTP TRIGGER)
# ===================================================================

@functions_framework.http
def constant_contact_oauth_callback(request):
    """
    Handles the OAuth2 redirect from Constant Contact.
    """
    auth_code = request.args.get("code")
    user_id = request.args.get("state")

    if not auth_code or not user_id:
        return "Error: Missing authorization code or user state.", 400

    try:
        oauth_creds = get_oauth_credentials()
        token_url = "https://id.constantcontact.com/as/token.oauth2"
        redirect_uri = request.base_url

        token_payload = {
            "grant_type": "authorization_code",
            "client_id": oauth_creds["client_id"],
            "client_secret": oauth_creds["client_secret"],
            "code": auth_code,
            "redirect_uri": redirect_uri,
        }

        response = requests.post(token_url, data=token_payload, timeout=15)
        response.raise_for_status()
        token_data = response.json()

        create_user_secret(user_id, token_data)

        sync_message = {"source": "constant-contact", "user": user_id}
        message_data = json.dumps(sync_message).encode("utf-8")
        publisher.publish(sync_topic_path, message_data).result()
        print(f"Successfully triggered initial sync for user {user_id}")

        dashboard_url = "http://localhost:3000/dashboard?connected=constant-contact"
        return functions_framework.redirect(dashboard_url)

    except Exception as e:
        print(f"!!! An error occurred during the OAuth callback: {e}")
        return "Internal Server Error", 500
