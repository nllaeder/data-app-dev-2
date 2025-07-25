import os
import json
from google.cloud import pubsub_v1
import functions_framework

# Initialize the Pub/Sub publisher client.
publisher = pubsub_v1.PublisherClient()

# Get the Project ID and Topic Name from environment variables.
# We will set these during deployment.
project_id = os.getenv('GCP_PROJECT')
topic_name = 'initiate-data-sync'
topic_path = publisher.topic_path(project_id, topic_name)

@functions_framework.http
def start_data_sync(request):
    """
    HTTP Cloud Function to trigger a data synchronization job.
    1. Receives a request from the frontend.
    2. Publishes a message to a Pub/Sub topic with the request data.
    """
    
    # --- SECURITY (Placeholder) ---
    # In a real application, you would verify the user's identity here.
    # For example, by validating a Firebase Auth ID token from the
    # 'Authorization' header. For now, we will skip this for testing.
    # auth_header = request.headers.get('Authorization')
    # if not auth_header:
    #     return 'Unauthorized', 401
    
    # Get the data from the incoming HTTP request.
    # We expect the frontend to send JSON data, like {'apiKey': '...'}
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return 'Bad Request: No JSON data found.', 400
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        return 'Bad Request: Invalid JSON format.', 400

    print(f"Received request: {request_json}")

    try:
        # Prepare the message to be sent to Pub/Sub.
        # The message data must be a bytestring.
        message_data = json.dumps(request_json).encode('utf-8')

        # Publish the message to the Pub/Sub topic.
        future = publisher.publish(topic_path, message_data)
        
        # future.result() blocks until the message is published.
        message_id = future.result()
        
        print(f"Message {message_id} published to {topic_path}.")
        
        # Return a success response to the frontend.
        return 'Accepted: Data sync job has been queued.', 202

    except Exception as e:
        print(f"Error publishing to Pub/Sub: {e}")
        return 'Internal Server Error', 500