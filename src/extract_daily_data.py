import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from vnstock import *
import pandas as pd
from google.cloud import storage


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}
dag = DAG(
    'vnstock_day_dag',
    default_args=default_args,
    schedule_interval='0 10 * * *',  # Set the schedule to run daily at 10 a.m.,
    start_date=datetime(2023, 9, 1),
    catchup=False,
    
)


def fetch_and_upload_historical_data():
    # Set the current date for which you want to fetch historical data
    current_date = datetime.now().date()
    formatted_current_date = current_date.strftime('%Y-%m-%d')  # Format the current date as a string

    # Get the list of company tickers
    company_ticker = listing_companies()
    ticker_list = company_ticker['ticker'].to_list()

    # Get the first 100 tickers
    first_100_tickers = ticker_list[:100]

    # Set interval
    interval = "1D"

    # Initialize an empty list to store data frames
    historical_data_list = []

    # Loop through the first 100 tickers and fetch historical data for the current date
    for ticker in first_100_tickers:
        try:
            # Fetch historical data for the formatted current date
            ticker_data = stock_historical_data(ticker, formatted_current_date, formatted_current_date, interval)
            historical_data_list.append(ticker_data)
        except ValueError as ve:
            print(f"ValueError fetching data for {ticker}: {ve}")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            pass

    # Create a new DataFrame by concatenating the historical_data_list vertically
    combined_data = pd.concat(historical_data_list, ignore_index=True)

    # Define the filename
    file_name = f"stock_{formatted_current_date}.csv"

    # Save the DataFrame as a CSV file
    combined_data.to_csv(file_name, index=False)

    # Upload the CSV file to Google Cloud Storage
    storage_client = storage.Client()
    bucket_name = 'vnstock_day'
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)

    # Print a success message
    print(f"Combined historical data for the first 100 tickers on {formatted_current_date} has been uploaded to {bucket_name}/{file_name}")

execute_vnstock_day = PythonOperator(
    task_id='execute_vnstock_day',
    python_callable=fetch_and_upload_historical_data,
    dag=dag,
)

execute_vnstock_day