from google.cloud import secretmanager
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """
    Retrieves a secret's payload from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    
    print(f"Attempting to retrieve secret: {name}")
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        print("Successfully retrieved secret.")
        return payload
    except NotFound:
        print(f"Error: Secret '{secret_id}' not found.")
        raise
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise


def load_data_to_bigquery(project_id: str, table_id: str, data: list) -> None:
    """
    Loads a list of dictionaries into a specified BigQuery table.
    """
    if not data:
        print("No data provided to load. Skipping BigQuery load.")
        return

    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{table_id}" # e.g., mis581-capstone-data.raw_data.peer1_contacts_raw

    print(f"Attempting to load {len(data)} rows into BigQuery table: {full_table_id}")
    try:
        errors = client.insert_rows_json(full_table_id, data)
        if not errors:
            print("Data loaded successfully to BigQuery.")
        else:
            print("Encountered errors while inserting rows to BigQuery:")
            for error in errors:
                print(error)
    except Exception as e:
        print(f"An unexpected error occurred during BigQuery load: {e}")
        raise