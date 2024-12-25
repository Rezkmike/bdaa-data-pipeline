import os
import io
import pandas as pd
from google.cloud import pubsub_v1, storage
import json

# PUBSUB_TOPIC = "projects/enduring-badge-443405-s6/topics/bdaa-raw"
PUBSUB_TOPIC = "bdaa-raw"
PROJECT_ID = "enduring-badge-443405-s6"

def publish_to_pubsub(topic_name, data):
    """
    Publishes data to a Pub/Sub topic.

    :param topic_name: Name of the Pub/Sub topic
    :param data: Data to publish (string)
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, topic_name)

    # Convert data to bytes
    data = data.encode("utf-8")
    future = publisher.publish(topic_path, data)
    future.result()  # Ensure the publish was successful

def process_csv_from_gcs(bucket_name, file_name):
    """
    Process the CSV file directly from GCS and select required columns.

    :param bucket_name: GCS bucket name
    :param file_name: Name of the file in GCS
    :return: Processed data as a dictionary
    """
    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download CSV content as a string
    csv_content = blob.download_as_text()

    # Load the CSV content into a DataFrame
    df = pd.read_csv(io.StringIO(csv_content))

    # Select specific columns
    selected_columns = [
            'ObjectId', 
            'Country', 
            'ISO3', 
            'Industry', 
            'Gas_Type',
            'F1970',
            'F1971',
            'F1972',
            'F1973',
            'F1974',
            'F1975',
            'F1976',
            'F1977',
            'F1978',
            'F1979',
            'F1980',
            'F1981',
            'F1982',
            'F1983',
            'F1984',
            'F1985',
            'F1986',
            'F1987',
            'F1988',
            'F1989',
            'F1990',
            'F1991',
            'F1992',
            'F1993',
            'F1994',
            'F1995',
            'F1996',
            'F1997',
            'F1998',
            'F1999',
            'F2000',
            'F2001',
            'F2002',
            'F2003',
            'F2004',
            'F2005',
            'F2006',
            'F2007',
            'F2008',
            'F2009',
            'F2010',
            'F2011',
            'F2012',
            'F2013',
            'F2014',
            'F2015',
            'F2016',
            'F2017',
            'F2018',
            'F2019',
            'F2020',
            'F2021',
            'F2022',
            'F2023',
            'F2024',
            'F2025',
            'F2026',
            'F2027',
            'F2028',
            'F2029',
            'F2030'
        ]  # Replace with your column names
    processed_df = df[selected_columns]

    # Convert the DataFrame to JSON records
    return processed_df.to_dict(orient="records")

def main(event, context):
    """
    Cloud Function entry point triggered by GCS.

    :param event: Event payload
    :param context: Metadata for the event
    """
    bucket_name = event['bucket']
    file_name = event['name']

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # Process the CSV file directly from GCS
    processed_data = process_csv_from_gcs(bucket_name, file_name)

    # Publish each record to Pub/Sub
    # topic_name = PUBSUB_TOPIC
    for record in processed_data:
        publish_to_pubsub(PUBSUB_TOPIC, json.dumps(record))

    print(f"Published {len(processed_data)} records to Pub/Sub topic: {PUBSUB_TOPIC}")