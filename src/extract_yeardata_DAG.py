import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator

from datetime import datetime, timedelta
from vnstock import *
import pandas as pd
from google.cloud import storage

# Define the GCS bucket name at the DAG level
bucket_name = "vnstock_year"
current_date = datetime.now()

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}
dag = DAG(
    'vnstock_year_dag',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # Set the schedule to run daily at 10 a.m.
    start_date=datetime(2023, 9, 1),
    catchup=False,
    
)



def extract_year_data():
    # Get the list of company
    company_ticker = listing_companies()
    # Get the list of ticker
    ticker_list = company_ticker['ticker'].to_list()

    # Set date time variable
    current_date = datetime.now()
    start_date = current_date - timedelta(days=365)
    end_date = current_date
    interval = "1D"

    # Initialize an empty list to store data frames
    historical_data_list = []

    # Loop through the ticker_list and fetch historical data for each ticker
    for ticker in ticker_list:
        try:
            # Convert start_date and end_date to strings
            formatted_start_date = start_date.strftime('%Y-%m-%d')
            formatted_end_date = end_date.strftime('%Y-%m-%d')

            # Fetch historical data using formatted dates
            ticker_data = stock_historical_data(ticker, formatted_start_date, formatted_end_date, interval)
            historical_data_list.append(ticker_data)
        except ValueError as ve:
            print(f"ValueError fetching data for {ticker}: {ve}")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            pass  # Skip processing this ticker and continue with the next one

    # Create a new DataFrame by concatenating the historical_data_list vertically
    combined_data = pd.concat(historical_data_list, ignore_index=True)

    # Upload the DataFrame as a CSV file to another GCS bucket
    bucket_name = "vnstock_year"
    # Update the file_name to include the formatted current date
    formatted_current_date = current_date.strftime('%Y%m%d')
    file_name = f"2023_data_{formatted_current_date}.csv"

    # Save the DataFrame as a CSV file
    combined_data.to_csv(file_name, index=False)

    # Upload the CSV file to the destination bucket
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)

    # Print a success message
    print(f"Combined data has been saved as {file_name} in bucket {bucket_name}")

execute_vnstock_year = PythonOperator(
    task_id='execute_vnstock_year',
    python_callable=extract_year_data,
    dag=dag,
)

# Define the formatted_current_date at the DAG level
formatted_current_date = datetime.now().strftime('%Y%m%d')

# Define the file name here
file_name = f"2023_data_{formatted_current_date}.csv"

transfer_to_bigquery = GCSToBigQueryOperator(
    task_id='transfer_to_bigquery',
    bucket=bucket_name,  # Specify the GCS bucket name
    source_objects=[file_name],  # List of files to transfer
    source_format='CSV',
    schema_fields=[
        {'name': 'time', 'type': 'DATE'},
        {'name': 'open', 'type': 'FLOAT'},
        {'name': 'high', 'type': 'FLOAT'},
        {'name': 'low', 'type': 'FLOAT'},
        {'name': 'close', 'type': 'FLOAT'},
        {'name': 'volume', 'type': 'FLOAT'},
        {'name': 'ticker', 'type': 'STRING'},
    ],

    destination_project_dataset_table='pyspark-practice-395516.Stock_storage.Stock_data_all',  # Replace with your BigQuery table info
    write_disposition='WRITE_TRUNCATE',  # Replace with WRITE_TRUNCATE to replace existing data

    skip_leading_rows=1,  # Skip the header row in the CSV
    dag=dag,
)
# Define the date 1 day ago
one_day_ago = current_date - timedelta(days=1)
formatted_one_day_ago = one_day_ago.strftime('%Y%m%d')
delete_file_name = f"2023_data_{formatted_one_day_ago}.csv"

# Define a task to delete the data from 1 day ago (if it exists)
delete_data_one_day_ago = GoogleCloudStorageDeleteOperator(
    task_id='delete_data_one_day_ago',
    bucket_name=bucket_name,  # Specify the GCS bucket name
    objects=[delete_file_name],  # List of objects (files) to delete
    delegate_to=None,  # Optional: Delegate access to another user or service account
    dag=dag,
)
# Set up task dependencies
execute_vnstock_year >> transfer_to_bigquery>> delete_data_one_day_ago 