import base64
import json
import os
from google.cloud import storage

# Environment variables
BUCKET_NAME = "bdaa-staging"

def pubsub_to_gcs(event, context):
    """
    Triggered by a Pub/Sub message.
    Writes the message data to a CSV file in GCS.
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

    # Write the data to GCS (appending to a file)
    file_name = "pubsub_output.csv"
    blob = bucket.blob(file_name)

    # Read the current content of the file, if it exists
    content = ""
    if blob.exists():
        content = blob.download_as_text()

    # Append the new row to the file
    new_content = content
    if not content.strip():  # Add headers if the file is empty
        new_content += ",".join(row.keys()) + "\n"
    new_content += ",".join(str(value) for value in row.values()) + "\n"

    # Upload the updated content back to GCS
    blob.upload_from_string(new_content)

    print(f"Data appended to {file_name} in bucket {BUCKET_NAME}.")
    return f"Data written to {file_name}."
