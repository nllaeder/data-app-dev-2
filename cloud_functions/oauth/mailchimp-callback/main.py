import json
import os
import requests
from google.cloud import secretmanager
from google.cloud import firestore
import functions_framework
from flask import redirect

# ===================================================================
#                      1. CONFIGURATION
# ===================================================================

# --- Environment Variables ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "data-app-dev-2") # Updated to your current project
OAUTH_CREDENTIALS_SECRET_NAME = os.getenv("OAUTH_SECRET", "mailchimp-oauth-credentials")
SYNC_TOPIC_NAME = os.getenv("SYNC_TOPIC", "initiate-data-sync")

# --- Clients ---
secret_client = secretmanager.SecretManagerServiceClient()
db = firestore.Client() # Initialize Firestore client


# ===================================================================
#           2. UTILITY FUNCTION
# ===================================================================

def get_oauth_credentials():
    """Retrieves Mailchimp OAuth client_id and client_secret from Secret Manager."""
    secret_path = f"projects/{GCP_PROJECT_ID}/secrets/{OAUTH_CREDENTIALS_SECRET_NAME}/versions/latest"
    try:
        response = secret_client.access_secret_version(request={"name": secret_path})
        return json.loads(response.payload.data.decode("UTF-8"))
    except Exception as e:
        print(f"!!! Error fetching secret '{OAUTH_CREDENTIALS_SECRET_NAME}': {e}")
        raise


# ===================================================================
#           3. MAIN CLOUD FUNCTION (HTTP TRIGGER)
# ===================================================================

@functions_framework.http
def mailchimp_oauth_callback(request):
    """
    Handles the OAuth2 redirect from Mailchimp.
    1. Exchanges the authorization code for an access token.
    2. Stores the token securely in a user-specific Firestore document.
    3. Redirects the user back to the application.
    """
    # 1. Get the authorization code and user_id from the request
    auth_code = request.args.get("code")
    user_id = request.args.get("state") # We pass the user_id in the 'state' parameter

    if not auth_code or not user_id:
        return "Error: Missing authorization code or user state.", 400

    try:
        # 2. Exchange the code for an access token
        oauth_creds = get_oauth_credentials()
        token_url = "https://login.mailchimp.com/oauth2/token"
        redirect_uri = request.base_url # The URL of this function

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
        access_token = token_data.get('access_token')

        if not access_token:
            print(f"!!! Mailchimp response missing access token: {token_data}")
            return 'Error: Access token not found in response from Mailchimp.', 500

        # 3. Store the token in a user-specific Firestore document
        # We use a collection 'user_credentials' and a document named after the user_id.
        doc_ref = db.collection('user_credentials').document(user_id)
        doc_ref.set({
            'mailchimp_access_token': access_token,
            'mailchimp_server_prefix': token_data.get('dc')
        }, merge=True) # merge=True prevents overwriting credentials from other services
        print(f"Successfully stored token in Firestore for user: {user_id}")

        # 4. Redirect user back to the frontend dashboard
        # TODO: Replace with your actual frontend dashboard URL from an environment variable
        dashboard_url = os.getenv('FRONTEND_URL', 'http://localhost:3000/datasources')
        return redirect(f"{dashboard_url}?connected=mailchimp_success")

    except requests.exceptions.RequestException as e:
        print(f"!!! An error occurred during the token exchange with Mailchimp: {e}")
        if e.response:
            print(f"!!! Response from Mailchimp: {e.response.text}")
        return "Error: Could not connect to Mailchimp.", 502
    except Exception as e:
        print(f"!!! An unexpected error occurred during the OAuth callback: {e}")
        return "Internal Server Error", 500