import csv
import os
import io  # Add this import for StringIO
import pandas as pd
from google.cloud import storage, bigquery

# Environment Variables
PROJECT_ID = "enduring-badge-443405-s6"
DATASET_NAME = "bdaa"
TABLE_METADATA = "tbl_metadata"
TABLE_TIMESERIES = "tbl_timeseries"

def preprocess_and_load_to_bq(event, context):
    """
    Cloud Function triggered by a new file upload in GCS.
    Cleans the data, normalizes it, and pushes to BigQuery.
    """
    file_name = event['name']
    bucket_name = event['bucket']

    print(f"Processing file: {file_name} from bucket: {bucket_name}")

    # Initialize clients
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    # Read the file from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_text()

    # Load data into a Pandas DataFrame
    data = pd.read_csv(io.StringIO(file_content))

    # Data Cleaning and Transformation
    print("Starting data preprocessing...")
    # Handle missing values in ISO2
    data['ISO2'] = data['ISO2'].fillna('UNKNOWN')

    # Drop columns with excessive missing values (e.g., future years)
    data.drop(columns=[col for col in data.columns if data[col].isnull().mean() > 0.5], inplace=True)

    # Normalize time-series data (pivot to long format)
    time_series_columns = [col for col in data.columns if col.startswith('F')]
    metadata_columns = [col for col in data.columns if col not in time_series_columns]
    time_series_data = data.melt(id_vars=metadata_columns, value_vars=time_series_columns,
                                 var_name='Year', value_name='Value')
    time_series_data['Year'] = time_series_data['Year'].str[1:].astype(int)

    # Remove rows with null values in key columns
    time_series_data.dropna(subset=['Value'], inplace=True)

    print("Data preprocessing completed.")

    # Push Metadata to BigQuery
    metadata_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_METADATA}"
    metadata_data = data[metadata_columns].drop_duplicates()
    print(f"Uploading metadata to BigQuery table: {metadata_table_id}")
    bigquery_client.load_table_from_dataframe(metadata_data, metadata_table_id).result()

    # Push Time-Series Data to BigQuery
    timeseries_table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_TIMESERIES}"
    print(f"Uploading time-series data to BigQuery table: {timeseries_table_id}")
    bigquery_client.load_table_from_dataframe(time_series_data, timeseries_table_id).result()

    print(f"Data successfully uploaded to BigQuery tables: {metadata_table_id} and {timeseries_table_id}")

    return "Data preprocessing and loading completed."
