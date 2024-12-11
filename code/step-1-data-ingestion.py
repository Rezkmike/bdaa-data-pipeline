import csv
import json
import os
from google.cloud import pubsub_v1, storage

# Set up environment variables
# PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC')
PUBSUB_TOPIC = "bdaa-raw"

def gcs_event_to_pubsub(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    Publishes rows of the uploaded CSV file to Pub/Sub.
    """
    # Get the bucket and file name from the event
    bucket_name = event['bucket']
    file_name = event['name']

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # Initialize clients
    storage_client = storage.Client()
    publisher = pubsub_v1.PublisherClient()

    # Read the CSV file from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if not blob.exists():
        print(f"Error: File {file_name} not found in bucket {bucket_name}.")
        return

    csv_data = blob.download_as_text()
    reader = csv.DictReader(csv_data.splitlines())

    # Publish each row of the CSV to Pub/Sub
    for row in reader:
        message = json.dumps(row).encode("utf-8")
        publisher.publish(PUBSUB_TOPIC, data=message)

    print(f"Published rows from {file_name} to Pub/Sub topic: {PUBSUB_TOPIC}")
    return f"Processed file: {file_name} and published data to Pub/Sub."
