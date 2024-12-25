import base64
import json
import os
from google.cloud import storage
from datetime import datetime

# Environment variables
BUCKET_NAME = "bdaa-staging"

def pubsub_to_gcs(event, context):
    """
    Triggered by a Pub/Sub message.
    Writes a single row of data to a timestamped CSV file in GCS.
    """
    # Initialize Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # Decode the Pub/Sub message
    if 'data' not in event:
        print("No data in Pub/Sub message.")
        return

    message_data = base64.b64decode(event['data']).decode('utf-8')
    print(f"Received message: {message_data}")

    # Parse the message data (assuming JSON format)
    try:
        row = json.loads(message_data)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return

    # Generate a timestamped filename
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    file_name = f"pubsub_{timestamp}.csv"
    blob = bucket.blob(file_name)

    # Prepare content for the new file
    content = ""
    # Add headers if the file is empty
    content += ",".join(row.keys()) + "\n"
    content += ",".join(str(value) for value in row.values()) + "\n"

    # Upload the file to GCS
    blob.upload_from_string(content)

    print(f"Data written to {file_name} in bucket {BUCKET_NAME}.")
    return f"Data written to {file_name}."
