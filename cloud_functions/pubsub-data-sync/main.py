import os
import json
import base64
import functions_framework
from google.cloud import pubsub_v1

# Initialize the Pub/Sub publisher client.
publisher = pubsub_v1.PublisherClient()
project_id = os.getenv('GCP_PROJECT')

@functions_framework.cloud_event
def process_data_sync(cloud_event):
    """
    Pub/Sub-triggered Cloud Function that acts as a router.
    1. Receives a message from the 'initiate-data-sync' topic.
    2. Inspects the 'source' field in the message data.
    3. Forwards the message to a source-specific topic (e.g., 'trigger-mailchimp-sync').
    """
    try:
        # Decode the incoming message
        message_data_encoded = cloud_event.data["message"]["data"]
        message_data_decoded = base64.b64decode(message_data_encoded).decode('utf-8')
        data_payload = json.loads(message_data_decoded)
        source = data_payload.get("source")

        if not source:
            print("ERROR: 'source' not found in message payload.")
            return 'Bad Request: Missing source', 400

        print(f"Routing job for source: '{source}'")

        # Determine the target topic based on the source
        # This makes the router extensible for future sources.
        target_topic_name = f"trigger-{source}-sync"
        topic_path = publisher.topic_path(project_id, target_topic_name)

        # Republish the original message to the target topic
        future = publisher.publish(topic_path, message_data_encoded)
        message_id = future.result()

        print(f"Message {message_id} published to {topic_path} for routing.")
        return 'Success: Job routed.', 200

    except Exception as e:
        print(f"Error routing Pub/Sub message: {e}")
        return 'Internal Server Error', 500