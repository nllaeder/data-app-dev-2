import os
import requests
import datetime
from utils.gcp_utils import get_secret, load_data_to_bigquery

# --- CONFIGURATION ---
GCP_PROJECT_ID = "mis581-capstone-data"
TENANT_ID = "peer1" # For multi-tenancy 

# The secret name you created in Secret Manager that holds the OAuth Access Token
# Make sure this secret exists in your GCP project.
CC_ACCESS_TOKEN_SECRET = "peer1-cc-access-token" 

# BigQuery table IDs
CONTACTS_TABLE_ID = "raw_data.peer1_constant_contact_contacts_raw"
CAMPAIGNS_TABLE_ID = "raw_data.peer1_constant_contact_campaigns_raw"
REPORTS_TABLE_ID = "raw_data.peer1_constant_contact_campaign_reports_raw"

# Constant Contact API base URL
API_BASE_URL = "https://api.cc.email/v3"

# --- MAIN EXECUTION ---
def main():
    """Main function to orchestrate the data extraction and loading."""
    
    print("--- Starting Constant Contact Data Extraction ---")
    
    # 1. Authenticate
    # Fetch the access token securely from Secret Manager 
    try:
        access_token = get_secret(GCP_PROJECT_ID, CC_ACCESS_TOKEN_SECRET)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
    except Exception as e:
        print(f"Could not authenticate. Halting execution. Error: {e}")
        return

    # 2. Extract, Transform (minimally), and Load data
    # We will add the functions to get contacts, campaigns, etc. here in the next steps.
    
    # --- Example placeholder for next steps ---
    # contacts = get_all_contacts(headers)
    # if contacts:
    #   load_data_to_bigquery(GCP_PROJECT_ID, CONTACTS_TABLE_ID, contacts)
    
    print("--- Constant Contact Data Extraction Finished ---")


if __name__ == "__main__":
    main()